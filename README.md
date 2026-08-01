# Pulsar

A Swift port of [`tokio::runtime`](https://docs.rs/tokio) (the reactor half):
a readiness-based event loop on top of `epoll`, conforming to Swift
Concurrency's `SerialExecutor` / `TaskExecutor` (SE-0392 / SE-0431) so that
`Task`s and actors can be **pinned to a single OS thread** — the thread-per-core
model used throughout the Starlight workspace.

> **What this is — and isn't.** Pulsar is the **reactor + executor** layer: one
> `PollEventLoop` per thread drives `epoll_wait`, performs the actual `read(2)` /
> `write(2)` when a fd is ready, and runs the Swift `Task`s / actor methods that
> are pinned to it. It is the analogue of `tokio::runtime`'s I/O driver + blocking
> pool, **not** a full runtime with timers/spawn/sync primitives — those live
> higher up. Built on [`mio`](https://github.com/akvilary/mio) (the Swift port of
> `mio`), exactly as Tokio builds on mio.

## Status

Early / experimental. Used by [`starlight`](https://github.com/akvilary/starlight)
(Swift port of axum) as its multi-threaded runtime. The test suite covers the
core contract: async read/write over a socketpair, watch channels, cross-thread
wakeup, `Task(executorPreference:)` pinning, and read/write deadlines.

## Platform

Linux only at the syscall level (`epoll`, `eventfd`, `timerfd`). All sources are
`#if os(Linux)`, so the module compiles on other platforms but exports nothing.

## Installation

```swift
.package(url: "https://github.com/akvilary/pulsar.git", from: "0.1.0")
```

```swift
.target(name: "YourTarget", dependencies: [
    .product(name: "Pulsar", package: "pulsar"),
])
```

`import Pulsar` re-exports the [`mio`](https://github.com/akvilary/mio) primitives
(`Poll`, `Registry`, `Token`, `Interest`, `Ready`, `Events`, `Waker`) transitively.

## Overview

The core type is `PollEventLoop` — a custom `SerialExecutor` that owns one
`epoll` fd and runs a blocking `epoll_wait` loop on its dedicated thread:

```swift
import Pulsar

let loop = try PollEventLoop(eventsCapacity: 4096)

// Pin a Task (and any actor whose unownedExecutor returns this loop) to it:
Task(executorPreference: loop) {
    // …runs on the loop's thread…
}

// Register a watch channel — e.g. a listening socket drained with accept4(2).
// The handler runs on the loop thread whenever the kernel reports readiness.
let listenId = try loop.registerWatch(fd: listenerFd, interest: .readable) { ready in
    guard ready.isReadable else { return }
    // accept4(2) until EAGAIN…
}

try loop.run()        // blocks the calling thread until loop.shutdown()
```

### Async, zero-copy I/O

Connection channels use `EPOLLONESHOT`: an awaited `read`/`awaitWritable` arms the
interest, and when the kernel reports the fd ready the loop performs the syscall
**on the loop thread** and resumes the continuation — mirroring io_uring's
"kernel does the I/O" model without the kernel-side buffer cost.

```swift
// Inside a Task pinned to the loop:
let n = await loop.read(channelId: id, fd: fd)            // bytes; 0=EOF, -1=err, -2=timeout
let view = loop.getReadView(channelId: id, count: n)      // borrowed view, no memcpy
let writable = await loop.awaitWritable(channelId: id, fd: fd)  // → false on write-timeout
// …caller performs the actual write(2)…
```

Per-channel read buffers are pre-allocated and reused across keep-alive requests,
so allocation per request goes to zero after warmup.

### Bounded waits (Slowloris / write-stall defence)

`read` and `awaitWritable` take absolute deadlines. One periodic `timerfd` per
loop sweeps expired deadlines on each tick (default 500 ms) and resumes the
waiter with a sentinel — no per-op timer allocation.

### Cross-thread wakeup

```swift
// From any thread:
loop.wakeup()        // writes the eventfd; the next epoll_wait returns immediately
// The loop thread then runs handleWakeup() (and the configurable onWakeup hook).
```

Cross-thread `Task` enqueue is lock-based (spinlock + `eventfd`); same-thread
enqueue is a plain array append — no synchronisation.

## Concurrency model & `@unchecked Sendable`

`PollEventLoop` is a `final class: @unchecked Sendable`. This is deliberate and
load-bearing, not a shortcut:

- It is a **custom executor**: Swift `actor`s run *on top of it* (their
  `unownedExecutor` returns the loop). An executor cannot itself be an `actor`
  (it would have to execute on itself), and its `run()` is a blocking `epoll_wait`
  loop that is incompatible with the cooperative pool.
- Its mutable state (`channels`, job queues) carries `CheckedContinuation`s and a
  `~Copyable` epoll buffer that are **inherently non-`Sendable`**. They are
  mutated **only on the loop thread**; cross-thread paths use a spinlock
  (`poolJobs`) or atomics (`loopThreadId`, `stopped`). The `@unchecked` annotation
  is the only way to express that invariant in today's type system — the same
  design used by SwiftNIO's `NIOSelector` and Tokio's runtime.
- Correctness is **enforced at runtime**: `checkIsolated()` compares
  `pthread_self()` against the stored `loopThreadId` (captured in `run()`, not
  `init`), so `Actor.assumeIsolated` traps immediately on any isolation
  violation.

## Contents

```
Sources/Pulsar/
├── PollEventLoop.swift   the reactor + SerialExecutor/TaskExecutor
├── PaddedAtomic.swift    128-byte cache-line-padded atomics (false-sharing guard)
└── ReexportMIO.swift     @_exported import MIO
```

## Why?

A pinned, readiness-based loop per core is how Tokio gets linear scaling, and the
only model that composes cleanly with Swift 6's `SerialExecutor`/`TaskExecutor`
for thread-per-core HTTP serving. Keeping the reactor in its own package (rather
than folded into the server) makes it reusable across drivers and independently
testable — mirroring the tokio/mio split.

## License

MIT — see [LICENSE](LICENSE).
