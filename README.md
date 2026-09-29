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

Production-hardened reactor/executor layer. Used by
[`starlight`](https://github.com/akvilary/starlight) (Swift port of axum)
as its multi-threaded runtime. The test suite covers the core contract:
async read/write over a socketpair, watch channels, cross-thread wakeup,
`Task(executorPreference:)` pinning, read/write deadlines (including
`write`'s whole-operation deadline), errno discrimination
(`lastErrno(channelId:)`), spurious-readiness handling, SIGPIPE
immunity on the socket write path (`MSG_NOSIGNAL`), shutdown semantics
(orphan recovery, graceful in-flight unwinding, terminality,
leak-free window closing), pre-run task enqueue (startup lost-wakeup
regression), a concurrency stress test with slot churn and mid-flight
cancellations, and a ThreadSanitizer-clean run enforced in CI.

## Platform

Linux only at the syscall level (`epoll`, `eventfd`, `timerfd`). All sources are
`#if os(Linux)`, so the module compiles on other platforms but exports nothing.

## Installation

```swift
.package(url: "https://github.com/akvilary/pulsar.git", from: "0.4.0")
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
//
// ⚠️ Level-triggered readiness contract (same as mio/tokio): the handler
// MUST drain the source (accept4(2)/read(2) until EAGAIN). An under-drained
// level-triggered fd stays ready forever and the loop busy-spins. This is
// inherent to readiness APIs — neither edge-triggering (missed events) nor
// auto-draining (the loop cannot know the fd's protocol) avoids it.
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

Channels are tracked in a dense generation-guarded slab (the tokio `slab` model):
`registerChannel()` returns an opaque `ChannelId` — `(generation << 32) | slot`
packed into the epoll token — so token → state resolution on the hot path is an
AND, a shift and two compares (no hashing, no exclusivity accessors), and stale
events for cancelled channels are dropped by the generation check. Slots are
reused LIFO under churn; a handle that outlives its `cancelChannel` traps with a
precondition instead of acting on the slot's new occupant.

```swift
// Inside a Task pinned to the loop:
let n = await loop.read(channelId: id)                    // bytes; 0=EOF, -1=err, -2=timeout
let view = loop.getReadView(channelId: id, count: n)      // borrowed view, no memcpy
let writable = await loop.awaitWritable(channelId: id)    // → false on write-timeout
// …caller performs the actual write(2)…

// Whole-write deadline (stalled-peer defence — see below):
let w = await loop.write(channelId: id, from: buf, deadline: .now + .seconds(10))

// Error discrimination — why did that call return -1 / false / -2?
switch loop.lastErrno(channelId: id) {
case 0:            break              // clean completion / EOF
case ETIMEDOUT:    break              // a deadline expired (read returned -2)
case ECANCELED:    break              // wait cancelled / shutdown / teardown
case EPIPE:        break              // peer gone (send reports it, no SIGPIPE)
case let e:        fatalError("io error \(e)")
}
```

Reads are one maximal `read(2)` per readiness event — with a fixed
destination buffer that is provably optimal (a short read on a stream
fd means the kernel had nothing more; a full read filled the buffer),
so the bulk-throughput lever is `readCapacity`, not extra syscalls.
Spurious readiness (epoll reports readable, `read(2)` returns
`EAGAIN`) does **not** fail the wait: the continuation stays armed and
the interest is re-armed — the tokio semantics; a healthy connection
is never torn down over a spurious wakeup.

Kernel-side, each data channel keeps ONE persistent `EPOLLONESHOT`
registration for its whole lifetime: arming an op is a single
`EPOLL_CTL_MOD`, and going idle costs nothing (the delivered oneshot
auto-disables the fd). The one deliberate exception: an event carrying
`EPOLLERR`/`EPOLLHUP`/`EPOLLRDHUP` tears the registration down when the
channel goes idle — those bits bypass oneshot disarming (the kernel
force-reports them), so a kept registration on a dead peer would
busy-loop. Net effect vs the previous design: two fewer `epoll_ctl`
syscalls per I/O cycle (~+37% echo throughput at 64 connections).

**fd ownership:** `registerChannel(fd:)` adopts the fd — the loop dups
it (`F_DUPFD_CLOEXEC`), forces `O_NONBLOCK` (a blocking fd would wedge
the loop thread; note `O_NONBLOCK` is a property of the shared open
file description, so this flips the caller's fd non-blocking too —
deliberately: a reactor requires non-blocking sources), and owns the
duplicate for the channel's lifetime. `cancelChannel` releases it
(deregister + close) and fully tears the connection down. Because the
loop only ever touches ITS OWN descriptor number — which the kernel
cannot recycle until the loop itself closes it — teardown is precise
by construction: there is no cancel-before-close ordering to get wrong.
Callers may close their own fd at any time (recommended right after
registering, to conserve the fd quota): their close is not the last
reference, and the connection stays live until `cancelChannel`. One
socket, one channel: epoll keys registrations by open file
description, so a second channel dup'ed from the same socket fails its
first arm with a clean `-1` instead of hanging.

**SIGPIPE:** socket channels are probed once at registration
(`getsockopt(SO_TYPE)`) and write through `send(2)` with
`MSG_NOSIGNAL` — a peer that closes mid-write yields a clean
`-1` + `lastErrno == EPIPE` instead of a signal that would kill the
process. Non-socket channels (pipes, files) use plain `write(2)`;
embedders driving EPIPE-prone pipe channels must arrange their own
`SIGPIPE` disposition.

**Setup threading contract:** all channel registration/cancellation is
loop-thread state. Before `run()` it is legal from any single thread
(the conventional register-then-run pattern — the thread spawn itself
provides the happens-before into `run()`); concurrent registration
from two threads before `run()` is a contract violation (unsynchronised
table growth). While the loop runs, only the loop thread (i.e. Tasks
pinned to it).

Per-channel read buffers are pre-allocated (sized via
`registerChannel(fd:readCapacity:)`, default 8 KiB) and reused across
keep-alive requests, so allocation per request goes to zero after warmup.

### Task cancellation (opt-in)

A Task cancelled **before** calling `read`/`awaitWritable`/`write` fails
fast (`-1` / `false`) on every path. Cancelling a Task **while it is
suspended** requires `cancellable: true`:

```swift
// Inside a handler Task that a timeout layer may cancel:
let n = await loop.read(channelId: id, cancellable: true)   // → -1 on cancel
```

The wait fails promptly with `-1` / `false` (best-effort, like tokio: a
readiness racing the cancellation may still deliver data), the channel
stays usable, and a cancel can only ever claim **its own** wait —
cancels are matched by per-call identity, so concurrent calls on other
channels (or later calls on the same one) are untouched.

The opt-in flag exists because Swift's cancellation handler runs its
operation on the global executor, not the caller's: cancellable waits
must round-trip through the loop's ordered request queue (one extra
eventfd wake per call) — a cost the default hot path does not pay.
Concurrent reads (or writes) on a single channel remain a contract
violation regardless of the flag.

### Bounded waits (Slowloris / write-stall defence)

`read`, `awaitWritable` **and `write`** take absolute deadlines — for
`write` the deadline bounds the WHOLE operation (checked at every
writability wait; the optimistic write attempts never block), so a
stalled peer can no longer hang an unbounded write: it returns the
partial byte count and records `lastErrno == ETIMEDOUT`. One periodic
`timerfd` per loop sweeps expired deadlines on each tick (default
500 ms) and resumes the waiter with a sentinel — no per-op timer
allocation. The interval (`timeoutSweepInterval`) is runtime-tunable:
setting it while the loop runs re-arms the timer on the next wakeup,
so live traffic can be retuned without a restart.

The sweep survives task storms: the job drain runs on a budget (1024 jobs)
and performs a non-blocking `epoll_wait` pass between batches, so a task
that synchronously resumes other tasks in a tight loop cannot starve
readiness dispatch or deadline enforcement.

### Shutdown semantics

`shutdown()` is **terminal** (tokio/NIO model): a subsequent `run()`
returns immediately. The loop's exit tail

- resumes every pending read/write with `-1` / `false`,
- deregisters all channel fds, frees buffers, closes the timer,
- drains the jobs those resurrections enqueue, so in-flight Tasks run
  their cleanup to completion.

While shutting down (and after), any further `read`/`awaitWritable` on the
loop fails immediately with `-1` / `false` instead of trapping on the
reset channel table — mid-flight Tasks unwind gracefully.

> **Note:** tasks *spawned onto* the loop after its windows have closed
> are dropped and counted (`droppedJobs` gauge; a debug assertion fires
> at the enqueue site). This is inherent — `SerialExecutor.enqueue` is
> synchronous with no failure channel, and an `UnownedJob` cannot be
> faulted, only run; "rescuing" the job by running it on the enqueuer's
> thread would execute unbounded user code on a caller expecting a
> cheap enqueue. The window closing itself is race-free: the terminal
> flag is stored UNDER the queue locks and both producers check it
> INSIDE the same locks, so a straggler is either drained by the tail
> (before the close) or failed/dropped inline (after it) — nothing can
> be parked in a box with no drainer left. Orchestrated shutdown
> (cancel watches → let in-flight Tasks fail out via `-1`/`false` →
> `shutdown()` → join the loop thread) never drops anything.

`run()` also drains jobs enqueued *before* it started, guards against
concurrent double-`run()` (precondition), and retries transient
`epoll_wait` errors with a backoff sleep before giving up after 32.

### Cross-thread wakeup

```swift
// From any thread:
loop.wakeup()        // writes the eventfd; the next epoll_wait returns immediately
// The loop thread then runs handleWakeup() (and the configurable onWakeup hook).
```

Cross-thread `Task` enqueue is lock-based (a futex-backed `Mutex` — the
sync `SerialExecutor.enqueue` contract makes a lock irreducible here; an
actor would require an async hop); same-thread enqueue is a plain array
append — no synchronisation. Gauges: `overflowEvents` counts full epoll
batches (sizing hint for `eventsCapacity`), `droppedJobs` counts jobs
that arrived after `run()` returned (must stay zero).

## Concurrency model & `@unchecked Sendable`

`PollEventLoop` is a `final class: @unchecked Sendable`. This is deliberate and
load-bearing, not a shortcut:

- It is a **custom executor**: Swift `actor`s run *on top of it* (their
  `unownedExecutor` returns the loop). An executor cannot itself be an `actor`
  (it would have to execute on itself), and its `run()` is a blocking `epoll_wait`
  loop that is incompatible with the cooperative pool.
- Its mutable state (`channels`, job queues) carries `CheckedContinuation`s and a
  `~Copyable` epoll buffer that are **inherently non-`Sendable`**. They are
  mutated **only on the loop thread**; cross-thread paths go through exactly
  two synchronized mechanisms: `LockedBox` — a pthread-mutex-guarded value
  (the `NIOLockedValueBox` pattern: state is unreachable without `withLock`,
  so discipline is type-enforced; pthread rather than
  `Synchronization.Mutex` because the latter's Linux futex protocol is
  invisible to ThreadSanitizer, which would make `swift test
  --sanitize=thread` permanently noisy — the suite is kept TSan-clean) —
  and atomics (`loopThreadId`, `stopped`, `runExited`, sweep-reschedule
  flag, gauge mirrors). The `@unchecked` annotation is the only way to
  express that invariant in today's type system — the same design used by
  SwiftNIO's `NIOSelector` and Tokio's runtime.
- Correctness is **enforced at runtime**: `checkIsolated()` compares
  `pthread_self()` against the stored `loopThreadId` (captured in `run()`, not
  `init`), so `Actor.assumeIsolated` traps immediately on any isolation
  violation.

## Contents

```
Sources/Pulsar/
├── PollEventLoop.swift   the reactor + SerialExecutor/TaskExecutor
├── ChannelSlab.swift     dense generation-guarded channel table (tokio-slab model)
├── RawMutex.swift        pthread mutex + LockedBox (TSan-visible synchronisation)
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
