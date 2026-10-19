# Wercker Architecture & Code Decisions: Technical Interview Guide

This document analyzes the architecture, internal mechanics, and notable engineering decisions in the `wercker` codebase. It is designed to provide inspiration for technical interview questions spanning systems programming, container runtimes, concurrency in Go, workflow engine resilience, security sandboxing, and protocol design.

---

## Table of Contents
1. [System Architecture Context](#1-system-architecture-context)
2. [Deep Dives: Key Code Decisions & Interview Questions](#2-deep-dives-key-code-decisions--interview-questions)
   - [Decision 1: Persistent Shell Sessions & Sentinel-Based Command Protocol](#decision-1-persistent-shell-sessions--sentinel-based-command-protocol)
   - [Decision 2: Container In-Guest Process Management & File Watcher](#decision-2-container-in-guest-process-management--file-watcher)
   - [Decision 3: Resilient Two-Phase Pipelines, Container Restarts & After-Steps](#decision-3-resilient-two-phase-pipelines-container-restarts--after-steps)
   - [Decision 4: Secrets Sandboxing, Passthrough Rules & Log Redaction](#decision-4-secrets-sandboxing-passthrough-rules--log-redaction)
   - [Decision 5: In-Flight Tar Stream Transformation & Safety Guards](#decision-5-in-flight-tar-stream-transformation--safety-guards)
   - [Decision 6: Polymorphic YAML Parsing & Extensible Step Architecture](#decision-6-polymorphic-yaml-parsing--extensible-step-architecture)
   - [Decision 7: Signal Trapping, Cascading LIFO Shutdown & Deadlock Hazards](#decision-7-signal-trapping-cascading-lifo-shutdown--deadlock-hazards)
3. [Interview Question Quick-Reference Matrix](#3-interview-question-quick-reference-matrix)

---

## 1. System Architecture Context

Wercker is a container-native continuous delivery and local development CLI tool (historically codenamed `sentcli`). Rather than running builds directly on the host machine, Wercker spins up isolated Docker containers to execute user-defined steps.

```
                              +-----------------------+
                              | CLI Entry (cmd/main)  |
                              +-----------+-----------+
                                          |
                        +-----------------+-----------------+
                        |                                   |
              +---------v----------+              +---------v----------+
              | Pipeline Config    |              | Docker Runner      |
              | (core/config.go)   |              | (cmd/runner.go)    |
              +---------+----------+              +---------+----------+
                        |                                   |
              +---------v----------+              +---------v----------+
              | Box & Services     |              | Docker Engine      |
              | (docker/box.go,    |              | (docker/docker.go) |
              |  docker/service.go)|              +---------+----------+
              +---------+----------+                        |
                        |                 +-----------------+-----------------+
                        |                 |                                   |
              +---------v-----------------v----+                    +---------v----------+
              | Guest Container Session        |                    | Normalized Emitter |
              | (core/session.go,              |                    | (event/ handlers)  |
              |  docker/session.go)            |                    +--------------------+
              +--------------------------------+
```

### Core Execution Flow
1. **Config & Environment Resolution (`core/config.go`, `util/environment.go`)**: Parses `wercker.yml` into typed models (`Config`), builds ordered environment variable tables, extracts secret passthroughs (`X_` and `XXX_`), and validates box/step requirements.
2. **Environment & Service Orchestration (`cmd/runner.go`, `docker/service.go`)**: Fetches or builds Docker base images, spins up auxiliary service containers (databases, message brokers) using container links, creates host/cache directories, and initiates the guest execution container.
3. **Interactive / Batch Session Attach (`docker/session.go`, `core/session.go`)**: Attaches raw standard I/O streams to a persistent `/bin/sh` or `/bin/bash` shell inside the guest container.
4. **Step Execution Loop (`cmd/runner.go`, `cmd/main.go`)**: Sequentially dispatches steps, injects step parameters as environment variables, emits status updates to listeners, and watches for sentinel tokens indicating command exit codes.
5. **Failure Handling & After-Steps (`cmd/main.go`)**: On failure or completion, captures artifacts, restarts containers if needed, injects build outcome variables (`WERCKER_RESULT`), and executes post-pipeline cleanup or notification steps.
6. **Telemetry & Artifact Persistence (`event/literalloghandler.go`, `docker/pipeline.go`)**: Emits events to console logs, Keen IO telemetry, or Wercker platform APIs, extracting and uploading build cache and step artifacts.

---

## 2. Deep Dives: Key Code Decisions & Interview Questions

---

### Decision 1: Persistent Shell Sessions & Sentinel-Based Command Protocol

- **Relevant Files:**
  - `core/session.go` (`Session`, `SendChecked`, `smartSplitLines`, `checkLine`)
  - `docker/session.go` (`DockerTransport.Attach`)
  - `core/step.go` (`ExternalStep.Execute`)

#### The Code Decision
Instead of invoking `docker exec` separately for every pipeline step, Wercker starts a long-running container with an interactive shell (`/bin/sh`), attaches raw stdin/stdout/stderr streams via the Docker API, and multiplexes all commands through this single session.

Because a bare streaming shell does not expose structured exit codes, Wercker invents an in-band signaling protocol:
1. It generates a random UUID sentinel:
   ```go
   sentinel := randomSentinel() // UUID v4
   ```
2. It sends user commands, followed immediately by a hidden echo command:
   ```go
   s.Send(sessionCtx, true, fmt.Sprintf("echo %s $?", sentinel))
   ```
3. A background reader reads output line-by-line, scanning for the sentinel UUID prefix. When found, it parses the trailing integer `$?` as the exit code:
   ```go
   func checkLine(line, sentinel string) (bool, int) {
       if !strings.HasPrefix(line, sentinel) { return false, -999 }
       var rand string; var exit int
       fmt.Sscanf(line, "%s %d\n", &rand, &exit)
       return true, exit
   }
   ```
4. To guard against scripts that emit output without a trailing newline, `smartSplitLines` in `core/session.go` inspects chunks to split squeezed tokens.
5. In `ExternalStep.Execute` (`core/step.go`), scripts are executed with `< /dev/null` (`source "$WERCKER_STEP_ROOT/run.sh" < /dev/null`) to prevent guest scripts from stalling while waiting for input on stdin.

#### Context & Tradeoffs
- **Advantages:**
  - Preserves environment variables, working directory changes, and shell state across sequential commands without rebuilding container contexts.
  - Avoids the overhead and latency of negotiating separate `docker exec` sessions for every minor setup command.
- **Vulnerabilities & Edge Cases:**
  - **In-Band Signaling Collision:** Any user command that happens to print the sentinel string or echoes the incoming command could trick the parser into prematurely declaring success or failure.
  - **Line Buffering & Terminal Mangling:** Non-newline-terminated stdout causes the sentinel to merge with user output, requiring heuristic text splitting (`smartSplitLines`).
  - **Terminal Stalls:** An interactive guest command reading from stdin without `< /dev/null` would block forever waiting on the transport pipe.

#### Interview Questions & Discussion Angles
1. **Protocol Design & In-Band Signaling:**
   > *"If you are designing a remote execution engine over a plain text stream (like SSH or attached Docker streams) where you cannot access process exit codes out-of-band, what are the failure modes of sentinel tokens, and how would you design a robust framing protocol?"*
2. **Execution Isolation vs. State Accumulation:**
   > *"What are the architectural tradeoffs between running all pipeline steps inside a single persistent shell session versus running each step in an isolated `docker exec` container process?"*
3. **I/O Streaming & Stream Merging:**
   > *"Notice how `docker/session.go` passes stdout and stderr into the same receiver. What complications arise when multiplexing stdout and stderr into a single channel, and how does that impact log parsing and exit code detection?"*

---

### Decision 2: Container In-Guest Process Management & File Watcher

- **Relevant Files:**
  - `docker/watchstep.go` (`killProcesses`, `watch`, `Execute`)
  - `docker/shellstep.go` (`ShellStep.Execute`)

#### The Code Decision
When developers run `wercker dev`, Wercker watches host project files via `fsnotify` and triggers live rebuilds inside the guest container. To restart the guest application server on code changes, Wercker needs to terminate previous application processes without killing the container itself.

In `docker/watchstep.go`, Wercker implements `killProcesses`:
```go
func (s *WatchStep) killProcesses(containerID string, signal string) error {
    client, err := NewDockerClient(s.dockerOptions)
    if err != nil { return err }
    cmd := []string{
        `/bin/sh`, `-c`,
        fmt.Sprintf(`ps | grep -v PID | awk "{if (\$1 != 1) print \$1}" | xargs -n 1 kill -s %s`, signal),
    }
    return client.ExecOne(containerID, cmd, os.Stdout)
}
```
And to exit the watch step cleanly upon Ctrl+C:
```go
stopWatchHandler := &util.SignalHandler{
    ID: "stop-watch",
    F: func() bool {
        s.logger.Println("Keyboard interrupt detected, finishing step")
        finishedStep <- struct{}{}
        return false // signals that we handled it, stop cascading
    },
}
util.GlobalSigint().Add(stopWatchHandler)
```

#### Context & Tradeoffs
- **PID 1 in Containers:** The root process of a container (PID 1) does not receive default signal handlers from the Linux kernel unless explicitly configured. Killing PID 1 terminates the container immediately.
- **The Shell-Pipeline Kill:** Wercker executes a subshell command that parses `ps` output and kills every PID except 1.
- **Tradeoffs:**
  - Fragile across different minimal Linux base images (e.g. Alpine vs Busybox vs Ubuntu, where `ps` flags and output column formats differ).
  - Processes spawned concurrently during the execution of `killProcesses` can escape or suffer PID recycling races.

#### Interview Questions & Discussion Angles
1. **Linux Container Process Lifecycles:**
   > *"Why does PID 1 in a Linux container behave differently regarding signal handling compared to standard processes? What problems arise when an init process fails to reap zombie child processes?"*
2. **Signal Interception & Cascades:**
   > *"In `docker/watchstep.go`, returning `false` from a signal handler prevents subsequent handlers from running. How does this design allow sub-components to intercept SIGINT without terminating the parent process?"*
3. **Alternative Architecture for Container Hot-Reloading:**
   > *"How would you architect containerized hot-reloading today instead of running an `awk | xargs kill` pipeline inside the guest container?"*

---

### Decision 3: Resilient Two-Phase Pipelines, Container Restarts & After-Steps

- **Relevant Files:**
  - `cmd/main.go` (`executePipeline`)
  - `cmd/runner.go` (`StartBuild`, `StartFullPipeline`)
  - `util/util.go` (`Finisher`)

#### The Code Decision
A critical requirement in CI/CD platforms is that failure notification steps (such as posting to Slack or alerting webhooks) must run even when prior pipeline steps fail, but the execution environment may be in an unknown, broken, or exited state.

Wercker resolves this using:
1. **Pessimistic Completion (`Finisher` Pattern):**
   ```go
   buildFinisher := r.StartBuild(options)
   buildFinishedArgs := &core.BuildFinishedArgs{Box: nil, Result: "failed"}
   defer buildFinisher.Finish(buildFinishedArgs)
   ```
   The build result defaults to `"failed"` and is only flipped to `"passed"` if every single step succeeds.
2. **Container Restart on `after-steps`:**
   In `cmd/main.go`:
   ```go
   // The container may have died, either way we'll have a fresh env
   container, err := box.Restart()
   newSessCtx, newSess, err := r.GetSession(cmdCtx, container.ID)
   ```
3. **Post-Mortem Metadata Export:**
   Before running `after-steps`, Wercker explicitly exports failure information into the restarted container's shell environment:
   ```go
   err = pr.ExportEnvironment(newSessCtx, newSess)
   ```
   This injects environment variables like `WERCKER_RESULT`, `WERCKER_FAILED_STEP_DISPLAY_NAME`, and `WERCKER_FAILED_STEP_MESSAGE`.

#### Context & Tradeoffs
- **State Recovery vs. Clean State:** If a build step crashed the container (OOM killer, kernel panic, or broken shell state), re-using the existing session would fail. Calling `box.Restart()` ensures an operational shell while retaining filesystem state modified by previous steps.
- **Tradeoffs:** `box.Restart()` restarts the container from its existing layer state, but background daemons or temporary mounts started by earlier steps might not resume cleanly.

#### Interview Questions & Discussion Angles
1. **Workflow Engine Failure Domains:**
   > *"When designing a CI/CD workflow engine, how do you handle post-failure actions (cleanup, notifications) when the host or guest container itself has crashed? What are the tradeoffs between reusing a crashed container vs spinning up a clean ephemeral container with mounted volumes?"*
2. **Pessimistic vs Optimistic Completion:**
   > *"Why is a deferred pessimistic completion pattern preferred in systems coordinating external distributed state over an optimistic pattern?"*

---

### Decision 4: Secrets Sandboxing, Passthrough Rules & Log Redaction

- **Relevant Files:**
  - `util/environment.go` (`Environment`, `GetPassthru`, `GetHiddenPassthru`, `Interpolate`)
  - `event/literalloghandler.go` (`Logs`, `shouldPrintLog`)

#### The Code Decision
Environment variables in Wercker must handle three conflicting requirements: ordered evaluation for variable interpolation, container guest export, and secrets protection.

In `util/environment.go`:
1. **Ordered Variable Map:**
   ```go
   type Environment struct {
       Hidden *Environment
       Map    map[string]string
       Order  []string
   }
   ```
2. **Passthrough Conventions:**
   - Variables prefixed with `X_` are stripped of the prefix and passed into the guest container as standard variables.
   - Variables prefixed with `XXX_` are stripped and marked as *hidden* passthrough variables.
3. **Log Redaction:**
   In `event/literalloghandler.go`, any log entry marked `Hidden: true` is suppressed from console display unless `--debug` is active:
   ```go
   func (h *LiteralLogHandler) shouldPrintLog(args *core.LogsArgs) bool {
       if args.Hidden { return false }
       if args.Stream == "stdin" && !h.options.Verbose { return false }
       return true
   }
   ```

#### Context & Tradeoffs
- **Prefix Conventions vs Metadata Stores:** Using string prefixes (`X_`, `XXX_`) allowed passing variables seamlessly through standard environment files and CLI flags without altering command schemas.
- **Security Limitations:**
  - If a user command prints the secret to stdout (e.g. `echo $XXX_KEY`), `LiteralLogHandler` only knows whether the *event args* are hidden (such as during initialization injection), not the raw content emitted by user processes.
  - In `Environment`, `GetInclHidden` looks up secrets for interpolation (`os.Expand`), meaning secrets can get baked into plain-text strings.
  - `Environment` lacks mutex protection despite being accessed across runner setup, step loops, and event emission.

#### Interview Questions & Discussion Angles
1. **Secrets Masking in CI Systems:**
   > *"Why is suppress-by-stream (tagging secret injection commands as hidden) insufficient for protecting secrets in CI runners, and how do modern systems (e.g., GitHub Actions secret masking) perform token-level stream scrubbing on stdout/stderr?"*
2. **Data Structure Design for Config & Env:**
   > *"Why did Wercker implement `Environment` with both a map and an ordering slice? What problems happen during environment variable export if variable evaluation order is not preserved?"*

---

### Decision 5: In-Flight Tar Stream Transformation & Safety Guards

- **Relevant Files:**
  - `util/archive.go` (`ArchiveProcessor`, `Archive.Stream`, `ArchiveMaxSize`)
  - `docker/pipeline.go` (`CollectCache`)

#### The Code Decision
Transferring files into and out of Docker containers (code, steps, cache, artifacts) is done via tar archive streams. Rather than writing archives to temporary files on disk and extracting them, Wercker streams them directly through a chained processing pipeline.

In `util/archive.go`:
```go
type ArchiveProcessor interface {
    Process(*tar.Header, io.Reader) (*tar.Header, io.Reader, error)
}

func (a *Archive) Stream(processors ...ArchiveProcessor) error {
    tarball := tar.NewReader(a.stream)
    for {
        hdr, err := tarball.Next()
        // ...
        for _, processor := range processors {
            hdr, tarfile, err = processor.Process(hdr, tarfile)
            if hdr == nil { continue EntryLoop } // dropped / filtered
        }
    }
}
```
Processors include:
- `ArchiveSingle`: filters the stream to match a single file.
- `ArchiveMaxSize`: tracks cumulative read bytes and aborts if the archive exceeds a threshold.
- `ArchiveTarPath`: rewrites directory paths on the fly.

Meanwhile, in `docker/pipeline.go:CollectCache`:
```go
select {
case err = <-errs:
// TODO(termie): I hate this, but docker command either fails right away
//               or we don't care about it, needs to be replaced by some
//               sort of cancellable context
case <-time.After(1 * time.Second):
    err = <-archive.Multi("cache", p.options.CachePath(), 1024*1024*1000)
}
```

#### Context & Tradeoffs
- **Memory & Disk Efficiency:** Streaming archives directly between the Docker engine socket and extractors eliminates double-buffering large project artifacts on disk.
- **Security Defense:** The `ArchiveMaxSize` filter acts as a guard against decompression bombs (zip bombs) exhausting host disk space.
- **The 1-Second Race Hack:** In `CollectCache`, if the Docker client does not error within 1 second, the code assumes the stream started properly and begins consumption—a classic distributed I/O synchronization compromise.

#### Interview Questions & Discussion Angles
1. **Streaming Interceptor Pipeline Design:**
   > *"How does an in-flight filter chain over `io.Reader` compare to intermediate temporary file buffering in terms of backpressure, memory usage, and error handling?"*
2. **Archive Security:**
   > *"What vulnerabilities (e.g. Zip Slip, symlink exploitation, decompression bombs) must an archive extraction pipeline protect against when extracting untrusted files from user-built container images onto the host?"*

---

### Decision 6: Polymorphic YAML Parsing & Extensible Step Architecture

- **Relevant Files:**
  - `core/config.go` (`RawBoxConfig`, `UnmarshalYAML`, `RawStepConfig`)
  - `core/step.go` (`StepDesc`, `Step`, `ExternalStep`)

#### The Code Decision
To create a clean user experience in `wercker.yml`, configuration syntax allows multiple polymorphic representations:
```yaml
# Style A: Simple string
box: golang:1.6

# Style B: Structured map
box:
  id: golang:1.6
  entrypoint: /bin/bash
```
And for steps:
```yaml
# Style 1: Bare name
- script

# Style 2: Key-value short form
- script:
    name: echo hello
    code: echo "hi"

# Style 3: Parameterized map
- npm-install:
    cwd: ./frontend
```
Rather than forcing a rigid schema, Wercker implements custom `UnmarshalYAML` methods in `core/config.go` that recursively test unmarshaling into a scalar string, a map of maps, and a flat map before resolving into normalized `BoxConfig` and `StepConfig` structs.

#### Context & Tradeoffs
- **User Ergonomics vs Type Safety:** In Go's static type system, polymorphic YAML unmarshaling requires fallback decoding via `func(interface{}) error`.
- **Step Extensibility:** Every external step declares its schema in `step.yml`. Wercker automatically normalizes step properties into guest container environment variables prefixed with `WERCKER_<STEP>_<PROPERTY>`.

#### Interview Questions & Discussion Angles
1. **Dynamic Deserialization in Static Languages:**
   > *"How do you design a clean deserializer in Go when an incoming configuration document supports both scalar strings and complex nested objects in the same field?"*
2. **Plugin Architecture & Contract Definition:**
   > *"How does Wercker's convention of turning YAML step properties into prefixed environment variables (`WERCKER_<STEP>_<PROP>`) simplify step plugin development compared to RPC or IPC plugins (e.g., HashiCorp go-plugin)?"*

---

### Decision 7: Signal Trapping, Cascading LIFO Shutdown & Deadlock Hazards

- **Relevant Files:**
  - `util/signal.go` (`SignalMonkey`, `Dispatch`, `Register`)
  - `cmd/runner.go` (`boxCleanupHandler`)

#### The Code Decision
To avoid leaking running containers or temporary directories when a user interrupts the CLI via `SIGINT` (Ctrl+C) or `SIGTERM`, Wercker introduces a centralized signal manager called `SignalMonkey` in `util/signal.go`.

Key architectural properties of `SignalMonkey`:
1. **LIFO Dispatch:** Handlers are executed in reverse order of registration (teardown runs opposite of setup):
   ```go
   func (s *SignalMonkey) Dispatch() {
       s.mutex.Lock()
       defer s.mutex.Unlock()
       for len(s.handlers) > 0 {
           fn, a := s.handlers[len(s.handlers)-1], s.handlers[:len(s.handlers)-1]
           s.handlers = a
           if fn.F() == false { break }
       }
   }
   ```
2. **Force-Quit Detection:** If a second signal arrives before the first clean shutdown completes, Wercker immediately terminates forcefully:
   ```go
   tries := 0
   for _ = range s.notify {
       go func() {
           if tries == 0 && len(s.handlers) > 0 {
               tries++; s.Dispatch(); tries--
           } else {
               RootLogger().Fatal("Exiting forcefully, containers and data may not have been cleaned up")
           }
       }()
   }
   ```

#### Context & Tradeoffs
- **Deadlock Vulnerability:** In `Dispatch()`, `s.mutex.Lock()` is held while invoking `fn.F()`. If `fn.F()` invokes any function that calls `SignalMonkey.Add()` or `SignalMonkey.Remove()`, it will immediately deadlock.
- **Data Race on `tries`:** The `tries` variable is updated across concurrent goroutines spawned inside the `s.notify` range loop without an atomic integer or lock.

#### Interview Questions & Discussion Angles
1. **Teardown Ordering & LIFO Execution:**
   > *"Why is a LIFO (stack) order preferable for resource cleanup compared to FIFO? What risks arise when cleanup handlers have interdependencies?"*
2. **Mutex Re-Entrancy & Deadlocks in Go:**
   > *"Explain why holding a mutex lock while invoking an external user-supplied callback is a known anti-pattern in Go. How would you refactor `SignalMonkey.Dispatch()` to avoid deadlocks?"*
3. **Graceful vs Forceful Termination:**
   > *"How would you design an application shutdown coordinator that allows graceful teardown up to a deadline, but escalates to forceful termination upon repeated user signals without data races?"*

---

## 3. Interview Question Quick-Reference Matrix

Use this matrix to match interview focus areas with specific Wercker components and challenge scenarios:

| Focus Area | Wercker Mechanism | Target Files | Key Challenge / Discussion Prompt |
| :--- | :--- | :--- | :--- |
| **UNIX Systems & PTY** | In-band sentinel exit status detection | `core/session.go` | How to detect process completion and return codes across raw attached streams without a supervisor. |
| **Container Internals** | Container PID 1 process management | `docker/watchstep.go` | Why killing processes in containers requires avoiding PID 1, and handling container signal propagation. |
| **Distributed Resilience** | Two-phase pipelines with container restart | `cmd/main.go` | Designing workflow engines where failure reporting runs in restarted containers with exported post-mortem state. |
| **Security & Secrets** | Passthrough conventions & log redaction | `util/environment.go`, `event/literalloghandler.go` | Stream-level vs token-level secret masking, and preventing credential leakage during interpolation and debugging. |
| **Streaming I/O & Safety** | Tar stream interceptor chain with max size | `util/archive.go`, `docker/pipeline.go` | Streaming archive transformation without disk buffering; defending against zip bombs; handling Docker stream races. |
| **API & Configuration** | Polymorphic YAML deserialization | `core/config.go` | Writing custom deserializers for multi-format schemas (scalars vs structs) in a statically typed language. |
| **Graceful Teardown** | LIFO signal cascading & re-entrant locks | `util/signal.go` | Clean resource teardown order, double-signal force-quits, and avoiding callback deadlocks while holding locks. |
