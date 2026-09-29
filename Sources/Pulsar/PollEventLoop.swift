//===----------------------------------------------------------------------===//
//
//  PollEventLoop.swift
//  Pulsar
//
//  High-level Swift Concurrency event loop built on top of the low-level
//  `Poll` / `Registry` / `Waker` primitives from the `mio` package
//  (https://github.com/akvilary/mio). This is the epoll analogue of
//  `StarlightIORing.IORingEventLoop` — same async `read`/`write`
//  surface, same SerialExecutor semantics, but every operation is
//  driven by readiness notifications on a single epoll fd instead of
//  io_uring submissions.
//
//  The mio primitives are re-exported, so `import Pulsar` is
//  sufficient to reach `Poll`, `Token`, `Interest`, `Ready`, etc.
//
//  Design notes
//  ------------
//  * Each channel uses EPOLLONESHOT. When a Task awaits `read`, the loop
//    arms `[.readable, .oneshot]`; when the kernel reports the fd ready,
//    the loop performs the actual `read(2)` on the loop thread (same
//    thread that will resume the Task) and resumes the continuation with
//    the byte count — mirroring io_uring's "kernel does the read" model
//    without the kernel-side buffer registration cost.
//  * The loop's `run()` blocks on `epoll_wait` and only returns control
//    to `drainJobs()` between waits, so a single thread drives I/O and
//    Task progress. This is the same thread-per-core model used by the
//    io_uring backend.
//  * Cross-thread wakeup is via `Waker` (eventfd, created in init so
//    it exists for the loop's whole lifetime). Cross-thread Task
//    enqueue goes through a futex-mutex-protected pool queue; the
//    same-thread fast path is a plain array append.
//
//===----------------------------------------------------------------------===//

#if os(Linux)

import Foundation
// Direct `import MIO` (not `@_exported`) — the re-export attribute
// conflicts with `~Copyable` type extension visibility in current
// Swift 6.2 toolchains: methods declared on a `~Copyable` struct
// disappear from the re-exporter's view even though they appear in
// the module interface. The re-export is moved to a dedicated file
// `ReexportMIO.swift` that contains nothing else, so the bulk of
// this module sees MIO through the plain `import` below.
import MIO
import Synchronization

#if canImport(Glibc)
import Glibc
#endif

// MARK: - Cancellable-call machinery

/// Per-call state for a cancellable wait: the cancellation flag (set by
/// `onCancel` on the cancelling thread, read by the loop when applying
/// the ordered requests) doubles as the call's IDENTITY — the channel
/// slot stores it next to the continuation so a cancel can only ever
/// claim ITS OWN wait (see `disarmOp`).
fileprivate final class CallState: @unchecked Sendable {
    let cancelled = Atomic<Bool>(false)
}

/// A cross-thread request applied by the loop in strict append order
/// (see `PollEventLoop.loopRequests`). Arms come from the global-pool
/// frames of cancellable `read`/`awaitWritable` calls; cancels come
/// from `onCancel` on the cancelling thread. The single-box lock gives
/// the happens-before edge that makes `CallState.cancelled` visible to
/// any arm request appended after the cancel.
fileprivate enum LoopRequest: Sendable {
    case armRead(
        ChannelId, deadline: ContinuousClock.Instant?,
        CallState, CheckedContinuation<Int, Never>)
    case armWrite(
        ChannelId, deadline: ContinuousClock.Instant?,
        CallState, CheckedContinuation<Bool, Never>)
    case cancel(ChannelId, isRead: Bool, CallState)
}

// MARK: - ChannelState

/// Per-channel pending-op state. Held by the loop, mutated only on the
/// loop thread (with the exception of `cancelChannel`, which is
/// expected to be called from the loop thread as well — it is the
/// connection-loop task that does this).
///
/// A channel is in exactly one of two modes:
///   - **managed**: `watch == nil`. The loop performs the actual
///     `read(2)`/`write(2)` when the fd is ready and resumes the
///     pending continuation. One `EPOLLONESHOT` event per armed op,
///     re-armed by `rearm`.
///   - **watch**: `watch != nil`. The loop does no I/O itself — it calls
///     `watch` with the observed `Ready` and returns. The fd stays armed
///     with the caller-supplied interest (typically level-triggered and
///     persistent, e.g. a listening socket drained with `accept4`).
/// - Note: hot-path layout reality (verified in disassembly): because
/// this struct stores resilient members (`ContinuousClock.Instant`,
/// `Optional<CheckedContinuation>` — both non-frozen stdlib/Foundation
/// types), its size and field offsets are runtime-computed from type
/// metadata even inside this module. The cost is one cached metadata
/// fetch + a handful of same-cache-line offset loads per event
/// (~5-8 ns, pipelined away against the ~1 µs read syscall that
/// follows). `@frozen`/`@usableFromInline`/removing `@inline(__always)`
/// do not change this (tried); the only full elimination is a
/// struct-of-arrays slab with trivially-laid-out fields and raw-word
/// continuation storage — deliberately not taken (fragility outweighs
/// the sub-1% gain). The watch/data dispatch decision itself reads the
/// slab's parallel `watchFlags` byte array and IS fully static.
internal struct PollChannelState {
    /// The fd the loop OWNS — a `dup(2)` of the caller's, made at
    /// registration (see `adoptFd`). Every syscall on the channel
    /// (`read`/`write`/`epoll_ctl`/`close`) uses THIS number, whose
    /// recycling is impossible until the loop itself closes it.
    var fd: CInt
    /// The CALLER's original fd number — kept only so watch channels
    /// can be removed from `watchByFd` (whose public surface,
    /// `cancelWatch(fd:)`, speaks the caller's number). -1 for data
    /// channels.
    var origFd: CInt = -1
    var registered: Bool = false
    var pendingRead: CheckedContinuation<Int, Never>?
    /// Identity of the cancellable call that armed `pendingRead`
    /// (nil for plain arms). Cancels claim their continuation by
    /// identity — see `disarmOp`.
    fileprivate var pendingReadCall: CallState?
    /// Absolute deadline after which a pending read is considered timed
    /// out. Set together with `pendingRead`; cleared together with it.
    /// Enforced by `sweepTimeouts` on each timerfd tick.
    var readDeadline: ContinuousClock.Instant?
    /// Readiness continuation for a pending write-wait (set by
    /// `awaitWritable`). The loop NEVER stores the caller's write
    /// buffer — the caller owns it and performs every `write(2)`
    /// itself, on the loop thread, in synchronous sections between
    /// awaits. This mirrors tokio/mio: the reactor provides only
    /// readiness (`EPOLLOUT`), the I/O type does the syscall.
    var pendingWrite: CheckedContinuation<Bool, Never>?
    /// Identity of the cancellable call that armed `pendingWrite`.
    fileprivate var pendingWriteCall: CallState?
    /// Absolute deadline after which a pending write-wait is considered
    /// timed out. Set together with `pendingWrite`; cleared together.
    var writeDeadline: ContinuousClock.Instant?
    var watch: (@Sendable (Ready) -> Void)?
    /// Per-channel read buffer — pre-allocated, reused across
    /// keep-alive requests. Owned by the eventLoop (NOT by the
    /// decoder). Eliminates @unchecked on H1Conn + ConnState.
    var readBuffer: UnsafeMutablePointer<UInt8>?
    var readCapacity: Int = 8192
    /// True iff the adopted fd is a socket (probed once at
    /// registration via `getsockopt(SO_TYPE)`). Sockets write through
    /// `send(2)` + `MSG_NOSIGNAL`, which turns a peer-close race into
    /// a clean `-1`/`EPIPE` instead of a process-killing `SIGPIPE` —
    /// `write(2)` would deliver the signal, and this library must
    /// never kill the process over a dead peer. Non-socket channels
    /// (pipes, files) keep `write(2)`: `send` requires a socket.
    /// Loop-thread state, like every other field.
    var isSocket: Bool = false
    /// errno of the most recently COMPLETED operation on this channel
    /// (loop thread only — read via `lastErrno(channelId:)`). Written
    /// by exactly the sites that complete a wait or a direct syscall:
    ///   * `0`                  — success (data read, full write,
    ///                            wait satisfied, EOF observed)
    ///   * real errno           — the failing `read(2)`/`send(2)`/
    ///                            `write(2)`/`epoll_ctl` errno
    ///   * `ETIMEDOUT`          — a deadline expired (read: `-2`)
    ///   * `ECANCELED`          — cancelled wait / shutdown failure /
    ///                            teardown recovery
    ///   * `EIO`                — EPOLLERR on a wait (real errno not
    ///                            observable without attempting I/O)
    ///   * `EPIPE`              — write-wait failed via EPOLLHUP
    /// Trivial `CInt` — no ARC traffic, no layout impact on the hot
    /// path.
    var lastErrno: CInt = 0
}

// MARK: - PollEventLoop

/// Async event loop driven by epoll.
///
/// Equivalent to `StarlightIORing.IORingEventLoop` but backed by
/// `Poll`/`Registry`. Conforms to `SerialExecutor` (SE-0392) so that
/// Swift Concurrency Tasks can be pinned to a single loop thread — the
/// thread-per-core model used throughout Starlight.
public final class PollEventLoop: @unchecked Sendable {

    // Epoll primitives.
    public let poll: Poll
    public let registry: Registry
    // `Events` is now a `~Copyable` struct — single-owner, single-thread.
    // Stored as `var` because `Poll.poll` requires `inout` access (it
    // writes the delivered-event count). The compiler now rejects any
    // accidental aliasing or cross-thread sharing that `@unchecked
    // Sendable` on the previous class form silently permitted.
    private var events: Events
    // Created in `init` (NOT `run()`) as an immutable `let`: `wakeup()`
    // may be called from any thread at any moment after init, and a
    // `var ... : Waker?` assigned inside `run()` would be a data race
    // (racy optional load vs. the loop thread's store) plus a lost-
    // wakeup window. Registry calls are thread-safe (kernel-side epoll
    // synchronisation — see mio's Registry docs), so early registration
    // is sound, and the eventfd now exists for the loop's whole
    // lifetime.
    private let waker: Waker

    // Periodic timer (timerfd) that wakes the loop to sweep expired
    // read/write deadlines — the mechanism bounding per-op waits
    // (Slowloris / write-stall defence). Created and registered in
    // run(); one fd per loop, shared by all channels.
    private var timerFd: CInt = -1
    /// Token reserved for the periodic timer. Channel tokens are
    /// `(generation << 32) | slot` with generation ≥ 1 — so `UInt64.max`
    /// (all bits set, needs an impossible 4G-slot table at the highest
    /// generation) can never collide. Handled explicitly in the event
    /// dispatch before `processChannelEvent`, so it never reaches the
    /// channel lookup. Likewise `Token.wakeup == 0` can never collide:
    /// a handed-out generation is never 0.
    private static let timerToken = Token(UInt64.max)
    /// Sweep granularity (default 500 ms). Bounds how late a deadline
    /// can be enforced; cheap because the sweep is O(active channels)
    /// and runs only on each tick, never per request.
    ///
    /// Runtime-tunable: may be set at ANY time, including while the
    /// loop is running — the setter raises a flag and wakes the loop,
    /// which re-arms its timerfd on the next wakeup (applied within at
    /// most one old interval + one wakeup). Backed by a Mutex so a
    /// cross-thread `set` cannot tear against the loop thread's read.
    ///
    /// Must be strictly positive (enforced): `.zero` maps to timerfd
    /// DISARM in mio, which would silently disable every deadline —
    /// the sweep would never run again; values below timer resolution
    /// degrade the loop to a near-busy spin.
    private let sweepBox = LockedBox<Duration>(.milliseconds(500))
    private let sweepReschedule = Atomic<Bool>(false)
    public var timeoutSweepInterval: Duration {
        get { sweepBox.withLock { $0 } }
        set {
            precondition(
                newValue > .zero,
                "timeoutSweepInterval must be strictly positive " +
                "(.zero would disarm the sweep timer and silently " +
                "disable all deadline enforcement)")
            sweepBox.withLock { $0 = newValue }
            if loopThreadId.load(ordering: .acquiring) != 0 {
                sweepReschedule.store(true, ordering: .releasing)
                waker.wake()
            }
        }
    }

    // Per-channel pending-op tracking — loop thread only.
    //
    // Dense slab (see `ChannelSlab`): O(1) token → state resolution on
    // the hot path (one AND, one shift, one generation compare — no
    // hashing, no exclusivity accessors, no copy-out/write-back). The
    // epoll token IS the handle: `(generation << 32) | slot`, so
    // events for a cancelled channel fail the generation check and
    // are dropped, exactly like the dictionary lookup miss this
    // replaces — while slot reuse keeps memory bounded under churn.
    //
    // INVARIANT: state is mutated IN PLACE through the slot pointer.
    // Continuation fields are nilled in the slot BEFORE `resume` (a
    // resumed Task may re-enter `armRead`/`armWritable` for the same
    // slot from `drainJobs`). The slot pointer must not be held
    // across a `watch` handler call — the handler may cancel this
    // very slot (see `processChannelEvent`).
    //
    // Internal (not private) for white-box tests via `@testable`.
    internal let channels = ChannelSlab(initialCapacity: 256)

    // Cross-thread job queue (SerialExecutor surface).
    //
    // Two tiers, mirroring every production reactor (SwiftNIO's
    // EventLoop, tokio's `global_queue`): same-thread enqueues append
    // to `loopJobs` with NO synchronisation (loop thread only);
    // cross-thread enqueues go through `poolQueue`, a `LockedBox` —
    // pthread-backed, so its critical sections are visible to
    // ThreadSanitizer (`Synchronization.Mutex`'s Linux futex protocol
    // is not, which made `--sanitize=thread` permanently noisy — see
    // RawMutex.swift). The state is reachable only via `withLock`:
    // the lock discipline is type-enforced, exactly like
    // `Mutex<State>`. The previous-previous design used a
    // `pthread_spinlock_t`, whose worst case is pathological: if the
    // lock holder is preempted, every enqueuer burns a full scheduler
    // quantum spinning.
    //
    // This queue CANNOT be an actor: `SerialExecutor.enqueue` is a
    // synchronous protocol requirement, and actor isolation would
    // require an async hop. A lock here is the architecturally correct
    // tool, not a compromise.
    private var loopJobs: [UnownedJob] = []
    private let poolQueue = LockedBox<[UnownedJob]>([])
    private let loopThreadId = Atomic<UInt>(0)

    // Loop-thread scratch for `drainJobs`. Swapping `loopJobs` into it
    // (O(1) CoW buffer exchange) avoids the CoW deep-copy that the
    // previous `var jobs = loopJobs; loopJobs.removeAll()` form
    // triggered, and keeps the buffer's capacity recycled across
    // drain cycles. Touched only on the loop thread, like `loopJobs`.
    // `poolDrain` plays the same role for the mutex-protected queue:
    // the O(1) buffer swap happens UNDER the lock (constant lock-hold
    // time), the bulk append happens after it is released.
    private var drainBuffer: [UnownedJob] = []
    private var poolDrain: [UnownedJob] = []

    /// Job budget per `drainJobs()` call. A storm of tasks resuming
    /// each other must not starve the reactor — readiness dispatch AND
    /// the deadline sweep both live behind `epoll_wait`. Every N jobs
    /// the drain performs one NON-blocking I/O service pass (see
    /// `drainJobs`). A blocking wait there would deadlock: same-thread
    /// enqueues carry no eventfd wake.
    private static let maxJobsPerServicePass = 1024

    // Loop state.
    private let stopped = Atomic<Bool>(false)
    /// True once the loop's enqueue/request windows have CLOSED (in
    /// run()'s tail, under the queue locks — see the tail's two-phase
    /// protocol). Distinct from `stopped` (a shutdown REQUEST, during
    /// whose tail window enqueues are still legal and drained): after
    /// `runExited`, cross-thread jobs are dropped+counted and arm
    /// requests are failed inline — nothing can be parked in a box
    /// with no drainer left. See `enqueueJob` / `submitRequest`.
    private let runExited = Atomic<Bool>(false)
    private var consecutiveErrors: Int = 0

    // Stats.
    /// Number of epoll batches that came back COMPLETELY full — i.e.
    /// the kernel ready-list held at least `eventsCapacity` entries
    /// and overflow may have been deferred to the next iteration.
    /// Zero-gauge for sizing `eventsCapacity` in production. Relaxed
    /// atomic, padded against false sharing.
    public let overflowEvents = PaddedAtomicInt64()

    /// Jobs dropped because they were enqueued after `run()` returned
    /// (a terminal loop can never run them). Zero in a correctly
    /// orchestrated shutdown — a rising counter means some code still
    /// spawns Tasks onto this dead loop; a debug assertion fires at
    /// the enqueue site to surface it during development.
    public let droppedJobs = PaddedAtomicInt64()

    /// Number of currently-live channels (relaxed snapshot; safe from
    /// any thread — the underlying stored counters are loop-thread
    /// state).
    public var channelsLiveCount: Int { channels.liveCountApprox }

    /// Number of slots ever touched by the channel table — its
    /// high-water mark under churn (bounded by peak concurrency, not
    /// cumulative registrations). Relaxed snapshot, any thread.
    public var channelsSlotCount: Int { channels.slotCountApprox }

    // Loop-request queue (Task cancellation propagation).
    //
    // `withTaskCancellationHandler`'s operation runs on the GLOBAL
    // concurrent executor (verified empirically on Swift 6.2: the
    // stdlib wrapper does not inherit the caller's executor), so a
    // cancellable wait may NOT arm channel state from its own frame.
    // Instead, both arming and cancellation are expressed as requests
    // appended to ONE ordered box and applied by the loop on ITS
    // thread in strict append order (single LockedBox ⇒ append order
    // == application order, and its lock supplies the happens-before
    // edge that makes the per-call `CallState.cancelled` flag visible
    // to a later arm request — see `handleWakeup`'s drain).
    //
    // This is the canonical tokio `ScheduledIo` shape: per-call
    // registration, ordered application, claim by identity.
    private let loopRequests = LockedBox<[LoopRequest]>([])

    // User hook invoked from the loop thread after the waker fires.
    //
    // Backed by a `LockedBox` so a `set` from any thread cannot race
    // with the loop thread's read in `handleWakeup`. The callback
    // itself is invoked OUTSIDE the lock (see `handleWakeup`) so a
    // callback that re-enters the loop (enqueue, etc.) cannot
    // self-deadlock.
    private let onWakeupBox = LockedBox<(@Sendable () -> Void)?>(nil)
    public var onWakeup: (@Sendable () -> Void)? {
        get { onWakeupBox.withLock { $0 } }
        set { onWakeupBox.withLock { $0 = newValue } }
    }

    // UnownedSerialExecutor / UnownedTaskExecutor handles.
    //
    // These are @frozen structs wrapping a single pointer to `self`.
    // Creating one is a single store instruction (~1ns, stack-allocated,
    // zero heap allocation, no ARC operation). Caching them in a `var`
    // would require synchronization (check-then-set race); the struct
    // is so cheap to create that caching is unnecessary.
    //
    // The Swift runtime identifies executors via
    // isSameExclusiveExecutionContext (which uses `self === other`),
    // NOT via struct identity — so fresh structs wrapping the same
    // PollEventLoop are interchangeable.
    public var cachedExecutor: UnownedSerialExecutor {
        UnownedSerialExecutor(ordinary: self)
    }

    public var cachedTaskExecutor: UnownedTaskExecutor {
        UnownedTaskExecutor(ordinary: self)
    }

    // MARK: Init

    public init(eventsCapacity: Int = 1024) throws {
        self.poll = try Poll()
        self.registry = poll.registry
        self.events = Events(capacity: eventsCapacity)
        self.waker = try Waker(registry: registry, token: .wakeup)
    }

    deinit {
        // Defensive teardown for a loop discarded without a completed
        // run(). Destroying an un-resumed CheckedContinuation traps the
        // Swift runtime ("leaked continuation"), so resume any orphans
        // first, free the raw read buffers, and release every loop-
        // owned dup (deregister + close) so neither fds nor kernel
        // entries leak. (The normal path is run()'s tail, which also
        // drains the resumed jobs — none of that can run here: by
        // definition of deinit, the loop has no thread left. The
        // resumed jobs enqueue into `poolQueue` and are, unavoidably,
        // never run.)
        channels.forEachLive { _, state in
            if state.pointee.registered {
                try? registry.deregister(fd: state.pointee.fd)
            }
            _ = Glibc.close(state.pointee.fd)
            if let cont = state.pointee.pendingRead {
                state.pointee.pendingRead = nil
                state.pointee.lastErrno = ECANCELED
                cont.resume(returning: -1)
            }
            if let cont = state.pointee.pendingWrite {
                state.pointee.pendingWrite = nil
                state.pointee.lastErrno = ECANCELED
                cont.resume(returning: false)
            }
            if let buf = state.pointee.readBuffer {
                state.pointee.readBuffer = nil
                buf.deallocate()
            }
        }
        channels.reset()
        drainPendingLoopRequests()
        // Backstop: run()'s tail normally closes the timer and stores
        // -1; this only fires for future code paths that skip it.
        // Capture the value BEFORE clearing the field.
        if timerFd >= 0 {
            let tfd = timerFd
            timerFd = -1
            _ = Glibc.close(tfd)
        }
    }

    // MARK: Event loop

    /// Drive the loop: block on `epoll_wait`, dispatch readiness, run
    /// queued jobs, repeat until `shutdown()`.
    ///
    /// Lifecycle (single entry, single exit — every path, including
    /// the fatal-error path, runs the same tail):
    ///
    ///   1. Terminal check — `shutdown()` is FINAL (tokio/NIO
    ///      semantics): a `run()` after a completed (or pre-cancelled)
    ///      run returns immediately instead of resurrecting the loop.
    ///   2. Claim the loop-thread identity via CAS — a concurrent
    ///      second `run()` on another thread would data-race the
    ///      `~Copyable Events` buffer and the channel table; trap
    ///      loudly instead of corrupting state.
    ///   3. Arm the periodic timeout-sweep timer.
    ///   4. Initial `drainJobs()` — jobs enqueued BEFORE `run()` (their
    ///      enqueuers saw `loopThreadId == 0` and could not wake the
    ///      loop) would otherwise sit in `poolQueue` while the first
    ///      `epoll_wait` blocks forever.
    ///   5. Loop: block-wait → count overflow → dispatch → drain.
    ///   6. Tail (ALWAYS runs, also on the error break): recover
    ///      orphaned waiters, deregister fds, close the timer, run the
    ///      jobs the recoveries enqueue, and close the enqueue/request
    ///      windows in two flag-under-lock phases (see the tail's
    ///      protocol notes). The previous design `throw`n
    ///      directly from the error path, leaking the timerfd and
    ///      leaving pending continuations un-resumed (a guaranteed
    ///      "leaked continuation" runtime trap at deinit).
    public func run() throws {
        if stopped.load(ordering: .acquiring) { return }
        // Re-open the enqueue window (an error-retry run() accepts
        // jobs from this point on; a fresh loop starts with false).
        runExited.store(false, ordering: .releasing)

        let selfTid = UInt(pthread_self())
        let claimed = loopThreadId.compareExchange(
            expected: 0, desired: selfTid, ordering: .acquiringAndReleasing
        )
        precondition(
            claimed.exchanged,
            "PollEventLoop.run() is already running on thread \(claimed.original)"
        )
        defer { loopThreadId.store(0, ordering: .releasing) }

        var loopError: (any Error)?

        // Arm the periodic timeout-sweep timer. Failure is non-fatal:
        // the loop still serves I/O, just without bounded waits
        // (graceful degradation to pre-timeout behaviour). Re-armed
        // per run() call — the tail closes it before returning, so an
        // error-retry run() gets a fresh timer.
        if timerFd < 0, let tfd = TimerFd.create() {
            timerFd = tfd
            _ = TimerFd.setPeriodic(fd: tfd, interval: timeoutSweepInterval)
            // Level-triggered, persistent (NOT oneshot): the timer stays
            // armed and fires once per interval until drained/closed.
            try? registry.register(
                fd: tfd, token: Self.timerToken, interest: .readable
            )
        }

        // See step 4 above: pick up everything enqueued before run().
        drainJobs()

        while !stopped.load(ordering: .acquiring) {
            // Phase 1: block on epoll_wait until at least one source is
            // ready (or the waker fires, or a signal interrupts).
            do {
                try self.events.wait(on: self.poll, timeout: PollTimeout.blocking)
                consecutiveErrors = 0
            } catch {
                // Recoverable errors: brief sleep, retry. After 32
                // consecutive failures give up — same threshold as the
                // io_uring backend. The sleep matters: without it the
                // retry loop burns CPU in a tight spin for all 32
                // attempts (the sleep was designed but missing before).
                consecutiveErrors += 1
                if consecutiveErrors > 32 {
                    loopError = error
                    break
                }
                Glibc.usleep(1_000)
                continue
            }

            // A full batch means the kernel's ready-list may have had
            // more entries than our buffer — they stay armed and are
            // delivered next iteration (level-triggered sources and
            // undelivered ONESHOTs alike), but count it so operators
            // can size `eventsCapacity` from the gauge.
            if events.count == events.capacity {
                _ = overflowEvents.add(1)
            }

            // Phase 2: dispatch each event. Channel reads/writes run the
            // actual syscall here so the resumed Task sees the result.
            dispatchDeliveredEvents()

            // Phase 3: drain queued jobs (connection Tasks resuming, new
            // Tasks, etc.). Jobs enqueue themselves via `enqueue` which
            // may have been called by Task.runSynchronously in phase 2
            // (a Task awaiting `read` whose body queued another op) or
            // by another thread.
            drainJobs()
        }

        // Shutdown/error tail — always runs. Resume any remaining
        // waiters with errors, then drain the resulting jobs: each
        // resume enqueues a Task continuation. Without this final
        // drain, the Tasks (which hold captures of the loop, connection
        // fds, codecs, etc.) would leak — their cleanup code (which
        // calls closeConnection and returns) never runs.
        //
        // The tail closes the enqueue/request windows in TWO phases,
        // each phase = flag store UNDER the corresponding queue's lock
        // + a drain after it (the check-inside-lock protocol — see
        // `submitRequest` / `enqueueJob`):
        //
        //   1. recover orphans          (resumes → jobs)
        //   2. drainJobs                (recovered Tasks run their
        //                                 cleanup; they may submit arm
        //                                 requests — handled by 4)
        //   3. close REQUEST window     (loopRequests lock + flag)
        //   4. drain pending requests   (straggler arms failed →
        //                                 resumes → more jobs)
        //   5. close JOB window         (poolQueue lock + flag)
        //   6. drainJobs                (runs everything from 1-4 and
        //                                 same-thread spawns; drains
        //                                 until BOTH queues empty)
        //
        // The phase ORDER is load-bearing: the job window must close
        // AFTER the request drain (whose resumes enqueue jobs), and a
        // final drainJobs must follow it — otherwise the request-fail
        // resumes' jobs would land after the last drain, never run,
        // and never be counted. After phase 5, any cross-thread
        // enqueue sees the flag and is dropped+counted; any late
        // submitRequest is failed inline. NOTHING can be parked in a
        // box with no drainer left.
        recoverOrphanedContinuations()
        drainJobs()
        loopRequests.withLock { _ in
            runExited.store(true, ordering: .releasing)
        }
        drainPendingLoopRequests()
        poolQueue.withLock { _ in
            runExited.store(true, ordering: .releasing)
        }
        drainJobs()

        if timerFd >= 0 {
            try? registry.deregister(fd: timerFd)
            let tfd = timerFd
            timerFd = -1
            _ = Glibc.close(tfd)
        }

        if let loopError { throw loopError }
    }

    /// Dispatch whatever `events` currently holds. Shared by the main
    /// loop and `drainJobs`' budgeted I/O service pass.
    @inline(__always)
    private func dispatchDeliveredEvents() {
        events.forEach { event in
            if event.token == .wakeup {
                handleWakeup()
            } else if event.token == Self.timerToken {
                handleTimer()
            } else {
                processChannelEvent(event)
            }
        }
    }

    public func shutdown() {
        stopped.store(true, ordering: .releasing)
        waker.wake()
    }

    /// True after `shutdown()` has been called.
    public var isStopped: Bool {
        stopped.load(ordering: .acquiring)
    }

    // MARK: Wakeup

    @inline(__always)
    private func handleWakeup() {
        _ = waker.reset()
        // Apply a runtime `timeoutSweepInterval` change: re-arm the
        // timerfd with the current value. The flag protocol loses no
        // update — a setter racing this exchange re-raises the flag
        // and wakes again.
        if sweepReschedule.exchange(false, ordering: .acquiringAndReleasing),
           timerFd >= 0 {
            _ = TimerFd.setPeriodic(
                fd: timerFd, interval: timeoutSweepInterval)
        }
        // Apply loop requests (arming + cancellation) in strict append
        // order — internal housekeeping before the user hook.
        var requests: [LoopRequest] = []
        loopRequests.withLock {
            swap(&requests, &$0)
        }
        for request in requests {
            switch request {
            case let .armRead(channelId, deadline, call, cont):
                applyArm(
                    channelId: channelId, deadline: deadline,
                    call: call, cont: cont)
            case let .armWrite(channelId, deadline, call, cont):
                applyArm(
                    channelId: channelId, deadline: deadline,
                    call: call, cont: cont)
            case let .cancel(channelId, isRead, call):
                disarmOp(channelId, isRead: isRead, call: call)
            }
        }
        // Read the callback under the lock, invoke it OUTSIDE so a
        // re-entrant callback (e.g., one that enqueues on the loop)
        // cannot take the same lock recursively.
        let cb = onWakeupBox.withLock { $0 }
        cb?()
    }

    /// Apply a boxed arm request (loop thread). Refuses to arm — and
    /// fails the wait immediately — when the call was cancelled before
    /// the request got here: the single-box lock chain guarantees the
    /// `cancelled` flag is visible whenever the cancel request was
    /// appended BEFORE this arm request. Otherwise this is exactly
    /// `armRead`/`armWritable` (all of their fast-fail paths included).
    private func applyArm(
        channelId: ChannelId,
        deadline: ContinuousClock.Instant?,
        call: CallState,
        cont: CheckedContinuation<Int, Never>
    ) {
        if call.cancelled.load(ordering: .acquiring) {
            setErrno(ECANCELED, on: channelId)
            cont.resume(returning: -1)
            return
        }
        armRead(channelId: channelId, cont: cont, deadline: deadline, call: call)
    }

    private func applyArm(
        channelId: ChannelId,
        deadline: ContinuousClock.Instant?,
        call: CallState,
        cont: CheckedContinuation<Bool, Never>
    ) {
        if call.cancelled.load(ordering: .acquiring) {
            setErrno(ECANCELED, on: channelId)
            cont.resume(returning: false)
            return
        }
        armWritable(channelId: channelId, cont: cont, deadline: deadline, call: call)
    }

    /// Fail one pending op wait as cancelled (`-1` / `false`) WITHOUT
    /// tearing the channel down — cancelling a wait is not cancelling a
    /// channel; the caller may re-arm or `cancelChannel` afterwards.
    ///
    /// Identity-checked: the slot's continuation is resumed only if it
    /// belongs to THIS call (`pendingReadCall === call`). A cancel that
    /// races another call's arm on the same channel — or arrives after
    /// its own call already completed and the slot was re-armed by a
    /// different call — is a harmless no-op. Without this identity
    /// check, a stale or box-phase cancel could kill an unrelated
    /// waiter on the same channel.
    ///
    /// Claim discipline (one of the claim sites, alongside the event,
    /// timeout, cancelChannel, rearm-error and orphan-recovery paths):
    /// whoever nils the slot field first owns the resume. The kernel
    /// interest is recomputed BEFORE resuming — the resumed Task may
    /// synchronously re-enter and re-arm.
    ///
    /// Loop thread only (driven from `handleWakeup`).
    private func disarmOp(_ channelId: ChannelId, isRead: Bool, call: CallState) {
        precondLoopThread("disarmOp")
        guard channels.isValid(slot: channelId.slot, gen: channelId.generation)
        else { return }
        let state = channels.pointer(slot: channelId.slot)
        if isRead {
            guard let cont = state.pointee.pendingRead,
                  state.pointee.pendingReadCall === call
            else { return }
            state.pointee.pendingRead = nil
            state.pointee.pendingReadCall = nil
            state.pointee.readDeadline = nil
            state.pointee.lastErrno = ECANCELED
            rearm(slot: channelId.slot, gen: channelId.generation, state: state)
            cont.resume(returning: -1)
        } else {
            guard let cont = state.pointee.pendingWrite,
                  state.pointee.pendingWriteCall === call
            else { return }
            state.pointee.pendingWrite = nil
            state.pointee.pendingWriteCall = nil
            state.pointee.writeDeadline = nil
            state.pointee.lastErrno = ECANCELED
            rearm(slot: channelId.slot, gen: channelId.generation, state: state)
            cont.resume(returning: false)
        }
    }

    // MARK: Timeout sweep

    /// Drain the periodic timerfd and resume any read/write waits whose
    /// deadline has passed. The timer is level-triggered, so the drain
    /// (an 8-byte `read`) is MANDATORY — without it epoll would report
    /// the timer readable on every subsequent cycle (busy-loop).
    @inline(__always)
    private func handleTimer() {
        var expirations: UInt64 = 0
        // timerFd is non-blocking; read never blocks (returns EAGAIN if
        // the spurious-read race loses, which we ignore).
        _ = withUnsafeMutablePointer(to: &expirations) { ptr in
            Glibc.read(timerFd, ptr, 8)
        }
        sweepTimeouts(now: ContinuousClock.now)
    }

    /// Two-phase sweep. Phase 1 is a read-only scan over the dense
    /// slot array that collects expired, still-pending continuations;
    /// phase 2 (after the scan, so no slot pointer is held while
    /// slots could be vacated) claims each continuation (nils the
    /// slot field), clears its deadline, and resumes it.
    ///
    /// Claiming is the only synchronisation needed vs readiness
    /// (`processChannelEvent`): both run on the loop thread, serialized,
    /// and both null the slot before resuming — so exactly one of them
    /// wins per continuation. `cont.resume()` schedules the Task on the
    /// loop; it does not re-enter this state synchronously.
    private func sweepTimeouts(now: ContinuousClock.Instant) {
        // Phase 1: collect (read-only over the table).
        var readTimedOut: [(slot: Int, cont: CheckedContinuation<Int, Never>)] = []
        var writeTimedOut: [(slot: Int, cont: CheckedContinuation<Bool, Never>)] = []
        channels.forEachLive { slot, state in
            if let d = state.pointee.readDeadline, d <= now,
               let cont = state.pointee.pendingRead {
                readTimedOut.append((slot, cont))
            }
            if let d = state.pointee.writeDeadline, d <= now,
               let cont = state.pointee.pendingWrite {
                writeTimedOut.append((slot, cont))
            }
        }
        // Phase 2: claim + clear + resume (mutating, not iterating).
        // No user code ran between the phases, so the slot cannot have
        // been vacated or reallocated — a plain live check suffices.
        for (slot, cont) in readTimedOut {
            let state = channels.pointer(slot: slot)
            guard state.pointee.pendingRead != nil else { continue }
            state.pointee.pendingRead = nil
            state.pointee.pendingReadCall = nil
            state.pointee.readDeadline = nil
            state.pointee.lastErrno = ETIMEDOUT
            cont.resume(returning: -2)  // read-timeout sentinel
        }
        for (slot, cont) in writeTimedOut {
            let state = channels.pointer(slot: slot)
            guard state.pointee.pendingWrite != nil else { continue }
            state.pointee.pendingWrite = nil
            state.pointee.pendingWriteCall = nil
            state.pointee.writeDeadline = nil
            state.pointee.lastErrno = ETIMEDOUT
            cont.resume(returning: false)  // write-timeout (≡ error → bail)
        }
    }

    /// Wake the loop from any thread. The next `poll()` iteration will
    /// observe the wakeup token and invoke `onWakeup`.
    public func wakeup() {
        _ = waker.wake()
    }

    // MARK: Loop-thread contract enforcement

    /// Enforce the loop-thread contract at every state-mutating entry
    /// point. Two legal regimes:
    ///
    ///   * **Setup** (`loopThreadId == 0`): the loop has not run yet —
    ///     any single thread may set the table up (the conventional
    ///     register-before-`run()` pattern).
    ///   * **Running**: exactly the loop thread may touch the table.
    ///
    /// A violation is a programming error (cross-thread table mutation
    /// races table growth → use-after-free), so it traps loudly —
    /// SwiftNIO-style fail-fast — instead of corrupting state.
    /// `precondition` survives in release builds; the check itself is
    /// one acquire-load + compare, off the per-event path.
    @inline(__always)
    private func precondLoopThread(_ op: StaticString) {
        let tid = loopThreadId.load(ordering: .acquiring)
        if tid != 0 {
            precondition(
                UInt(pthread_self()) == tid,
                "PollEventLoop: \(op) must run on the loop thread while the loop is running (or before run())"
            )
        }
    }

    // MARK: Channel management

    /// Adopt `fd` into the loop's ownership: duplicate it
    /// (`F_DUPFD_CLOEXEC`) and enforce `O_NONBLOCK`.
    ///
    /// The dup is what makes the ownership model sound: the loop's
    /// descriptor number can never be recycled behind its back (the
    /// kernel recycles a number only after the LAST reference closes —
    /// and the loop holds one), so `EPOLL_CTL_DEL` / `close(2)` at
    /// teardown always target precisely this channel's registration.
    /// The old cancel-before-close contract dissolves: callers may
    /// close their own fd at any moment (their close is not the last
    /// reference — the connection stays live until `cancelChannel`).
    ///
    /// `O_NONBLOCK` is enforced because a blocking fd would wedge the
    /// loop thread inside `read(2)`/`write(2)` — the single worst
    /// failure mode this library can have. NOTE: `O_NONBLOCK` is a
    /// property of the open file DESCRIPTION shared by the caller's fd
    /// and this dup — setting it changes the caller's fd flags too.
    /// That is deliberate: a reactor requires non-blocking sources,
    /// and this makes misuse impossible. Typical non-blocking fds
    /// (`accept4(SOCK_NONBLOCK)`) pay one `F_GETFL` only.
    private static func adoptFd(_ fd: CInt) throws -> CInt {
        let owned = Glibc.fcntl(fd, F_DUPFD_CLOEXEC, 0)
        guard owned >= 0 else {
            // Construct the error BEFORE any other syscall: a later
            // call (even a succeeding close) is not guaranteed by
            // POSIX to leave errno untouched.
            throw PollError.fromErrno(function: "fcntl(F_DUPFD_CLOEXEC)")
        }
        let flags = Glibc.fcntl(owned, F_GETFL)
        guard flags >= 0 else {
            let error = PollError.fromErrno(function: "fcntl(F_GETFL)")
            _ = Glibc.close(owned)
            throw error
        }
        if flags & Int32(O_NONBLOCK) == 0 {
            guard Glibc.fcntl(owned, F_SETFL, flags | Int32(O_NONBLOCK)) >= 0 else {
                let error = PollError.fromErrno(function: "fcntl(F_SETFL, O_NONBLOCK)")
                _ = Glibc.close(owned)
                throw error
            }
        }
        return owned
    }

    /// True iff `fd` is a socket — one `getsockopt(SO_TYPE)` probe on
    /// the cold registration path. Drives the `send(MSG_NOSIGNAL)` vs
    /// `write(2)` decision in `write` (see `PollChannelState.isSocket`).
    private static func fdIsSocket(_ fd: CInt) -> Bool {
        var sockType: CInt = 0
        var len = socklen_t(MemoryLayout<CInt>.size)
        return getsockopt(
            fd, SOL_SOCKET, SO_TYPE, &sockType, &len) == 0
    }

    /// Allocate a fresh channel handle bound to `fd`. The loop dups the
    /// fd (see `adoptFd`) and owns the duplicate for the channel's
    /// lifetime — `cancelChannel` releases it (deregister + close). Use
    /// the returned id with `read`/`write`/`awaitWritable`/
    /// `cancelChannel`; the id encodes the slab slot plus a generation
    /// counter, so it can never be confused with a later channel that
    /// reuses the same slot (a stale handle traps with a precondition
    /// instead of acting on the new occupant).
    ///
    /// - Parameter fd: the caller's fd. From this point the caller MAY
    ///   close it at any time (recommended right after this call, to
    ///   conserve the fd quota): the loop works through its own dup,
    ///   and the connection stays live until `cancelChannel`.
    /// - Parameter readCapacity: size of the per-channel read buffer,
    ///   pre-allocated once and reused across keep-alive requests.
    ///   Larger buffers mean fewer wakeups per byte for bulk
    ///   transfers; the default (8 KiB) matches typical HTTP
    ///   request/response sizing.
    ///
    /// - Throws: `PollError` if the fd is invalid or its flags cannot
    ///   be read/set.
    /// - Precondition: called before `run()` or on the loop thread —
    ///   enforced by `precondLoopThread`. Pre-`run()` setup is
    ///   single-threaded by contract: the channel table is plain
    ///   memory with no synchronisation, so two threads registering
    ///   concurrently race table growth (use-after-free). Sequential
    ///   setup from different threads is fine if externally
    ///   synchronised — the canonical register-then-`run()` pattern
    ///   gets that happens-before edge from the thread spawn itself.
    public func registerChannel(
        fd: CInt, readCapacity: Int = 8192
    ) throws -> ChannelId {
        precondLoopThread("registerChannel")
        precondition(readCapacity > 0, "readCapacity must be positive")
        let ownedFd = try Self.adoptFd(fd)
        var state = PollChannelState(fd: ownedFd)
        state.readCapacity = readCapacity
        state.readBuffer = .allocate(capacity: readCapacity)
        state.isSocket = Self.fdIsSocket(ownedFd)
        let (slot, gen) = channels.alloc(state, isWatch: false)
        return packChannelId(slot: slot, gen: gen)
    }

    /// fd → slot for WATCH channels only (listeners — a handful per
    /// loop; data channels never enter it). Loop-thread only, like the
    /// slab. Turns `cancelWatch` from a table scan into an O(1) map
    /// hit on a cold path.
    private var watchByFd: [CInt: Int] = [:]

    /// Register a watch channel — an fd the caller wants to drive
    /// directly via `handler` rather than through the async read/write
    /// API. Returns a fresh `ChannelId` that can later be passed to
    /// `cancelChannel` (or use `cancelWatch(fd:)`).
    ///
    /// The canonical use case is a listening socket: register it with
    /// `.readable` (level-triggered, no `.oneshot`) and drain
    /// `accept4(2)` in `handler` until `EAGAIN`. The loop does no I/O on
    /// a watch channel and does not re-arm it — `handler` is invoked for
    /// every readiness event the kernel reports, matching mio's plain
    /// level-triggered registration.
    ///
    /// `handler` runs on the loop thread. It is stored (escaping) for the
    /// lifetime of the channel; allocate it once at setup, not per event.
    ///
    /// - Precondition: called before `run()` or on the loop thread —
    ///   enforced by `precondLoopThread`.
    public func registerWatch(
        fd: CInt, interest: Interest,
        _ handler: @Sendable @escaping (Ready) -> Void
    ) throws -> ChannelId {
        precondLoopThread("registerWatch")
        // The loop adopts a dup of the listener (see `adoptFd`); the
        // handler keeps using the CALLER's number for accept(2) — both
        // refer to the same open file description. Keep the caller's fd
        // open while the watch is registered.
        let owned = try Self.adoptFd(fd)
        var state = PollChannelState(fd: owned)
        state.origFd = fd
        state.watch = handler
        let (slot, gen) = channels.alloc(state, isWatch: true)
        let handle = packChannelId(slot: slot, gen: gen)
        do {
            try registry.register(fd: owned, token: handle.asToken, interest: interest)
        } catch {
            // Roll the slot back: the caller sees the error and holds
            // no handle, so a live slot (and its stored closure, and
            // the adopted dup) would otherwise leak until shutdown.
            // `remove` also bumps the generation, so the token that
            // may have partially reached the kernel is already stale.
            _ = channels.remove(slot: slot)
            _ = Glibc.close(owned)
            throw error
        }
        channels.pointer(slot: slot).pointee.registered = true
        watchByFd[fd] = slot
        return handle
    }

    /// Cancel any outstanding read/write on `channelId`. Pending
    /// continuations are resumed with `-1` / `false`. Also valid for a
    /// watch channel: its handler closure is released when the entry is
    /// removed. No-op for an already-cancelled id (the generation no
    /// longer matches the slot).
    ///
    /// Releases the loop-owned descriptor (deregister + close): the
    /// connection is fully torn down by this call. Both operations
    /// target the loop's own `dup(2)` — a number that cannot have been
    /// recycled — so teardown is precise by construction, whatever the
    /// caller did with their own fd.
    ///
    /// - Precondition: called before `run()` or on the loop thread —
    ///   enforced by `precondLoopThread`.
    public func cancelChannel(_ channelId: ChannelId) {
        precondLoopThread("cancelChannel")
        guard channels.isValid(slot: channelId.slot, gen: channelId.generation)
        else { return }
        let state = channels.remove(slot: channelId.slot)
        // DEL first: if the caller still holds their own fd, closing
        // our dup would not remove the kernel entry (the file stays
        // open via their reference).
        if state.registered { try? registry.deregister(fd: state.fd) }
        if state.watch != nil { watchByFd.removeValue(forKey: state.origFd) }
        _ = Glibc.close(state.fd)
        // lastErrno is NOT recorded here — the slot is vacated, so the
        // moved-out value is unobservable; `lastErrno(channelId:)`
        // reports ECANCELED for the stale handle by rule (an op that
        // ended via teardown was, by definition, cancelled).
        if let cont = state.pendingRead  { cont.resume(returning: -1) }
        if let cont = state.pendingWrite { cont.resume(returning: false) }
        // Free per-channel read buffer.
        if let buf = state.readBuffer { buf.deallocate() }
    }

    /// Cancel the watch channel registered for `fd` (one fd maps to at
    /// most one channel: epoll registers a fd once). O(1) via the
    /// `watchByFd` side map. Runs `cancelChannel` on the slot, which
    /// deregisters the fd from epoll and releases the stored watch
    /// closure. Intended for the shutdown path (e.g. stopping a
    /// level-triggered listener so it stops firing readability and
    /// busy-looping). No-op if `fd` has no watch channel.
    ///
    /// - Precondition: called before `run()` or on the loop thread —
    ///   enforced by `precondLoopThread`.
    public func cancelWatch(fd: CInt) {
        precondLoopThread("cancelWatch")
        guard let slot = watchByFd[fd] else { return }
        let gen = channels.currentGeneration(slot: slot)
        cancelChannel(packChannelId(slot: slot, gen: gen))
    }

    // MARK: Async read

    /// Await readability on `channelId`, then read into the eventLoop's
    /// internal per-channel buffer. Returns bytes read (0 on EOF, -1 on
    /// error, -2 on timeout). The fd was bound at `registerChannel(fd:)`
    /// — the loop reads through its own dup of it.
    ///
    /// The buffer is owned by the eventLoop — callers access it via
    /// `getReadView(channelId:count:)` after this returns. This
    /// eliminates the need for the caller to own a raw buffer (and
    /// thus the need for @unchecked Sendable on decoder/conn types).
    ///
    /// **Cancellation** (cooperative): a Task cancelled BEFORE the call
    /// fails fast with `-1` on every path. Full cancellation — a Task
    /// cancelled WHILE suspended — requires `cancellable: true`: the
    /// wait is then failed with `-1` promptly (best-effort, like tokio:
    /// a readiness racing the cancellation may still deliver data) and
    /// the channel stays usable. The opt-in exists because Swift's
    /// cancellation handler runs its operation off the caller's
    /// executor, so cancellable waits must round-trip through the
    /// loop's request queue — a cost the hot path does not pay.
    ///
    /// - Parameter deadline: absolute time after which an unanswered
    ///   readiness wait is failed with `-2` (timeout). `nil` disables
    ///   the timeout (compat). Enforced by `sweepTimeouts` on each
    ///   timerfd tick, so granularity ≈ `timeoutSweepInterval`.
    public func read(
        channelId: ChannelId,
        deadline: ContinuousClock.Instant? = nil,
        cancellable: Bool = false
    ) async -> Int {
        // Cheap hygiene on every path: a task already cancelled must
        // not arm a wait at all. The errno record is thread-guarded:
        // a cancellable call's entry may run on the global pool (see
        // setErrnoIfLoopThread), where the table must not be touched.
        if Task.isCancelled {
            setErrnoIfLoopThread(ECANCELED, channelId)
            return -1
        }
        if !cancellable {
            // Hot path: the arm happens synchronously in THIS frame —
            // which runs on the caller's executor (the loop for
            // loop-pinned Tasks) — zero cross-thread machinery.
            return await withCheckedContinuation { cont in
                armRead(channelId: channelId, cont: cont, deadline: deadline)
            }
        }
        // Cancellable path: `withTaskCancellationHandler`'s operation
        // runs on the GLOBAL pool (the stdlib wrapper does not inherit
        // the caller's executor), so channel state may not be touched
        // here — the arm is expressed as a request the loop applies in
        // ordered fashion (see `loopRequests`).
        let call = CallState()
        return await withTaskCancellationHandler {
            if Task.isCancelled { return -1 }
            return await withCheckedContinuation { cont in
                self.submitRequest(
                    .armRead(channelId, deadline: deadline, call, cont))
            }
        } onCancel: {
            call.cancelled.store(true, ordering: .releasing)
            self.submitRequest(.cancel(channelId, isRead: true, call))
        }
    }

    /// Get a view into the per-channel read buffer after `read()`
    /// returns. The pointer is valid until the next `read()` call
    /// on the same channel. Called from the loop thread only —
    /// enforced by `precondLoopThread`.
    public func getReadView(channelId: ChannelId, count: Int) -> UnsafeBufferPointer<UInt8> {
        precondLoopThread("getReadView")
        guard channels.isValid(slot: channelId.slot, gen: channelId.generation),
              let buf = channels.pointer(slot: channelId.slot).pointee.readBuffer
        else {
            return UnsafeBufferPointer(start: nil, count: 0)
        }
        return UnsafeBufferPointer(
            start: buf,
            count: Swift.min(count, channels.pointer(slot: channelId.slot).pointee.readCapacity)
        )
    }

    /// Record `e` as the channel's last outcome. Loop-thread only;
    /// a stale handle is a silent no-op — the slot may since have been
    /// vacated (or reused by a different channel), and writing through
    /// it would corrupt the new occupant. Every completion site routes
    /// through this or sets the field in-place through a validated
    /// slot pointer.
    @inline(__always)
    private func setErrno(_ e: CInt, on channelId: ChannelId) {
        guard channels.isValid(slot: channelId.slot, gen: channelId.generation)
        else { return }
        channels.pointer(slot: channelId.slot).pointee.lastErrno = e
    }

    /// `setErrno`, but only when executing in a regime that owns the
    /// channel table: the loop thread, or the tid == 0 setup/terminal
    /// regime (mirroring `precondLoopThread`). The `read` /
    /// `awaitWritable` ENTRY fast-fails run on the caller's thread —
    /// which for a cancellable call may be the global pool (Swift's
    /// cancellation wrapper hops there), and touching the table from
    /// there would race the loop. A non-cancellable contract violation
    /// still traps at the arm's `precondLoopThread`, untouched.
    @inline(__always)
    private func setErrnoIfLoopThread(_ e: CInt, _ channelId: ChannelId) {
        let tid = loopThreadId.load(ordering: .acquiring)
        if tid == 0 || UInt(pthread_self()) == tid {
            setErrno(e, on: channelId)
        }
    }

    /// errno of the most recently completed operation on the channel —
    /// the discrimination layer over `read`/`write`/`awaitWritable`
    /// return values:
    ///
    ///   * `0` — clean completion (data delivered, full write,
    ///     writability granted, EOF observed).
    ///   * real errno — the failing syscall (`EPIPE`, `EBADF`,
    ///     `ECONNRESET`…), captured at the failure site before
    ///     anything can clobber it.
    ///   * `ETIMEDOUT` — a deadline expired (`read` returned `-2`).
    ///   * `ECANCELED` — the wait was cancelled, or failed because the
    ///     loop is shutting down / the channel was torn down.
    ///   * `EIO` / `EPIPE` — an armed wait failed via `EPOLLERR` /
    ///     `EPOLLHUP` respectively (the kernel reports no errno for
    ///     these; the values are what an attempted syscall would
    ///     return).
    ///
    /// A stale handle also reports `ECANCELED`: an op that ended while
    /// its channel was being torn down was, by definition, cancelled.
    ///
    /// Valid to call immediately after the awaited call returns, from
    /// the same Task (the value is stable until the channel's next
    /// completed operation — sequential ops on one channel are the
    /// supported model). Cancellable calls made from a NON-loop-pinned
    /// task resume on the global pool — such a caller must hop to the
    /// loop (a loop-pinned actor, or re-arming on the loop) before
    /// querying, per the precondition below.
    ///
    /// - Precondition: called on the loop thread — enforced by
    ///   `precondLoopThread`.
    public func lastErrno(channelId: ChannelId) -> CInt {
        precondLoopThread("lastErrno")
        guard channels.isValid(slot: channelId.slot, gen: channelId.generation)
        else { return ECANCELED }
        return channels.pointer(slot: channelId.slot).pointee.lastErrno
    }

    private func armRead(
        channelId: ChannelId,
        cont: CheckedContinuation<Int, Never>,
        deadline: ContinuousClock.Instant?,
        call: CallState? = nil
    ) {
        // Runs in the `read()` continuation body — i.e. on the awaiting
        // Task's thread, which the contract requires to be the loop
        // (or pre-run setup). Enforced here so a mispinned caller fails
        // at the first state mutation, not as a cross-thread race.
        precondLoopThread("read")
        // Shutdown is terminal: fail the wait immediately instead of
        // arming an interest nobody will ever deliver. This is what
        // lets in-flight Tasks unwind cleanly DURING shutdown — after
        // the tail resets the channel table, their handles are stale
        // and the old code trapped on the "stale handle" precondition
        // in exactly this path.
        if stopped.load(ordering: .acquiring) {
            setErrno(ECANCELED, on: channelId)
            cont.resume(returning: -1)
            return
        }
        guard channels.isValid(slot: channelId.slot, gen: channelId.generation) else {
            preconditionFailure(
                "PollEventLoop.armRead: stale channel handle — use after cancelChannel (\(channelId))"
            )
        }
        let state = channels.pointer(slot: channelId.slot)
        // A watch channel's events are dispatched to its handler and
        // never reach the continuation path — arming a read there
        // would hang the awaiting Task forever. Fail fast instead.
        precondition(state.pointee.watch == nil,
            "PollEventLoop: async read is unavailable on watch channels — the handler owns the I/O")
        precondition(state.pointee.pendingRead == nil,
            "PollEventLoop: overlapping read on channelId=\(channelId)")
        state.pointee.pendingRead = cont
        state.pointee.pendingReadCall = call
        state.pointee.readDeadline = deadline
        rearm(slot: channelId.slot, gen: channelId.generation, state: state)
    }

    // MARK: Async write
    //
    // Reactor contract: the loop is a *readiness* reactor for writes.
    // It performs NO `write(2)` itself and stores NO caller buffer.
    // The caller does every `write(2)` in its own synchronous context
    // (on the loop thread); on `EAGAIN` it awaits `awaitWritable`,
    // which arms `EPOLLOUT` oneshot and resumes the caller when the
    // socket has space. This is the tokio/mio model and the symmetric
    // counterpart of the read path — the difference (loop reads,
    // caller writes) follows buffer ownership: the loop owns the read
    // destination buffer, the caller owns the write source buffer.

    /// Optimistic `send(2)`/`write(2)` loop over `buffer`; on `EAGAIN`
    /// arms `EPOLLOUT` and awaits readiness via `awaitWritable`. Returns
    /// total bytes written (0..buffer.count).
    ///
    /// Runs entirely on the caller's executor (the loop thread for
    /// loop-pinned callers). The fast path — socket buffer has room —
    /// never suspends: the optimistic write succeeds and the loop
    /// returns without crossing an await. Only a full socket buffer
    /// triggers `awaitWritable`, which suspends this Task while the
    /// loop serves other connections. The write goes through the
    /// loop-owned dup bound at registration; validity is re-checked
    /// every iteration, so a channel cancelled while this Task was
    /// suspended stops the loop instead of writing through a dead
    /// handle.
    ///
    /// Sockets write via `send(2)` with `MSG_NOSIGNAL`: a peer that
    /// closes mid-write yields a clean `-1`/`EPIPE` (visible through
    /// `lastErrno(channelId:)`) instead of a `SIGPIPE` that would kill
    /// the whole process. Non-socket channels (pipes, files) use plain
    /// `write(2)` — `MSG_NOSIGNAL` requires a socket; embedders
    /// driving pipe channels through EPIPE-prone paths must arrange
    /// their own `SIGPIPE` disposition.
    ///
    /// - Parameter deadline: absolute time bounding the WHOLE write
    ///   (checked at every writability wait; the optimistic write
    ///   attempts themselves never block). `nil` disables the timeout.
    ///   A stalled peer can no longer hang an unbounded write — the
    ///   write-stall counterpart of `read`'s deadline.
    /// - Precondition: the caller MUST own `buffer` for the duration
    ///   of this call (across any internal await). The pointer is
    ///   dereferenced only inside synchronous write attempts.
    /// - Precondition: no other write wait may be in flight on the
    ///   same `channelId`.
    /// - Precondition: called on the loop thread — enforced by
    ///   `precondLoopThread`.
    public func write(
        channelId: ChannelId,
        from buffer: UnsafeRawBufferPointer,
        deadline: ContinuousClock.Instant? = nil,
        cancellable: Bool = false
    ) async -> Int {
        precondLoopThread("write")
        var offset = 0
        // errno outcome for THIS call, applied once at the end (the
        // channel stays untouched while the loop runs):
        //   0    — every byte written (or a zero-length request)
        //   e    — the failing syscall's errno, captured at the site
        //   nil  — the outcome was recorded by a wait-completion site
        //          (awaitWritable's failure path sets lastErrno there);
        //          writing here would overwrite it
        var outcome: CInt? = 0
        while offset < buffer.count {
            guard channels.isValid(slot: channelId.slot, gen: channelId.generation)
            else {
                outcome = nil   // channel gone; slot may be reused — hands off
                break
            }
            let state = channels.pointer(slot: channelId.slot)
            let fd = state.pointee.fd
            let chunk = UnsafeRawBufferPointer(
                rebasing: buffer[offset...])
            let n: Int
            if state.pointee.isSocket {
                n = Glibc.send(fd, chunk.baseAddress!, chunk.count, Int32(MSG_NOSIGNAL))
            } else {
                n = Glibc.write(fd, chunk.baseAddress!, chunk.count)
            }
            if n > 0 { offset += n; continue }
            if n == 0 { break }          // socket: shouldn't happen
            if errno == EINTR { continue }
            if errno == EAGAIN || errno == EWOULDBLOCK {
                if !(await awaitWritable(
                    channelId: channelId, deadline: deadline,
                    cancellable: cancellable)
                ) {
                    outcome = nil        // recorded by the wait's failure site
                    break                // error / hangup / timeout / cancel
                }
                continue
            }
            outcome = errno              // EPIPE / EBADF / ... — captured now
            break
        }
        if let e = outcome { setErrno(e, on: channelId) }
        return offset
    }

    /// Await writability on `channelId`. Arms `EPOLLOUT` (oneshot),
    /// suspends, and resumes with `true` when the socket can accept a
    /// write, or `false` on `EPOLLERR` / `EPOLLHUP` / timeout.
    ///
    /// The caller issues the actual `write(2)` after this returns.
    ///
    /// **Cancellation**: a Task cancelled while suspended (or before
    /// the wait arms) fails fast with `false`; best-effort — a racing
    /// writability may still win. The channel stays usable. `write`
    /// inherits this at its await points.
    ///
    /// - Parameter deadline: absolute time after which an unanswered
    ///   readiness wait is failed with `false`. `nil` disables it.
    public func awaitWritable(
        channelId: ChannelId,
        deadline: ContinuousClock.Instant? = nil,
        cancellable: Bool = false
    ) async -> Bool {
        // See `read`: thread-guarded errno record (a cancellable call
        // may enter on the global pool).
        if Task.isCancelled {
            setErrnoIfLoopThread(ECANCELED, channelId)
            return false
        }
        if !cancellable {
            return await withCheckedContinuation { (cont: CheckedContinuation<Bool, Never>) in
                armWritable(channelId: channelId, cont: cont, deadline: deadline)
            }
        }
        let call = CallState()
        return await withTaskCancellationHandler {
            if Task.isCancelled { return false }
            return await withCheckedContinuation { (cont: CheckedContinuation<Bool, Never>) in
                self.submitRequest(
                    .armWrite(channelId, deadline: deadline, call, cont))
            }
        } onCancel: {
            call.cancelled.store(true, ordering: .releasing)
            self.submitRequest(.cancel(channelId, isRead: false, call))
        }
    }

    /// Append a loop request and wake the loop. Callable from ANY
    /// thread (the cancellable paths use it from the global pool and
    /// from the cancelling thread).
    ///
    /// Terminal-check discipline: the check that `runExited` is false
    /// happens INSIDE the box lock, and run()'s tail flips the flag
    /// UNDER THE SAME LOCK before its final drain. The two locked
    /// sections are therefore strictly ordered: either this append
    /// lands before the close (and the tail's drain fails the request
    /// properly), or this check sees the flag and the request is
    /// failed here. The previous design checked the flag BEFORE taking
    /// the lock, which left an interleaving where an append raced past
    /// the tail's drain AND the flag store — a continuation parked in
    /// a box nobody would ever drain (its Task hangs forever). Same
    /// protocol as `enqueueJob`'s job-window close.
    private func submitRequest(_ request: LoopRequest) {
        var terminal = false
        loopRequests.withLock {
            if runExited.load(ordering: .relaxed) {
                terminal = true
            } else {
                $0.append(request)
            }
        }
        // Resume OUTSIDE the lock: a resume enqueues a job, and even
        // though that takes poolQueue's lock (not this one), keeping
        // critical sections minimal is the discipline that makes the
        // protocol auditable.
        if terminal {
            switch request {
            case let .armRead(_, _, _, cont):
                cont.resume(returning: -1)
            case let .armWrite(_, _, _, cont):
                cont.resume(returning: false)
            case .cancel:
                break
            }
        } else {
            waker.wake()
        }
    }

    private func armWritable(
        channelId: ChannelId,
        cont: CheckedContinuation<Bool, Never>,
        deadline: ContinuousClock.Instant?,
        call: CallState? = nil
    ) {
        // See armRead: the awaiting Task must be loop-pinned, and a
        // terminal shutdown fails the wait gracefully (false) rather
        // than arming an undeliverable interest or trapping on the
        // reset table.
        precondLoopThread("awaitWritable/write")
        if stopped.load(ordering: .acquiring) {
            setErrno(ECANCELED, on: channelId)
            cont.resume(returning: false)
            return
        }
        guard channels.isValid(slot: channelId.slot, gen: channelId.generation) else {
            preconditionFailure(
                "PollEventLoop.armWritable: stale channel handle — use after cancelChannel (\(channelId))"
            )
        }
        let state = channels.pointer(slot: channelId.slot)
        // Symmetric to armRead: a write-wait armed on a watch channel
        // would never be resumed.
        precondition(state.pointee.watch == nil,
            "PollEventLoop: async write is unavailable on watch channels — the handler owns the I/O")
        precondition(state.pointee.pendingWrite == nil,
            "PollEventLoop: overlapping write on channelId=\(channelId) — previous continuation would leak")
        state.pointee.pendingWrite = cont
        state.pointee.pendingWriteCall = call
        state.pointee.writeDeadline = deadline
        rearm(slot: channelId.slot, gen: channelId.generation, state: state)
    }

    // MARK: Re-arm logic

    /// Recompute the interest mask for the channel based on currently
    /// pending ops, then MOD or ADD the fd. Called after each op is
    /// armed and after each event is processed.
    ///
    /// Registration model (nginx-style persistent registration):
    /// once a data channel's fd is first ADDed, the registration STAYS
    /// in the kernel for the channel's whole lifetime. Arming an op is
    /// a single `EPOLL_CTL_MOD`; going idle does NOTHING — the
    /// delivered `EPOLLONESHOT` event auto-disabled the fd, so a spent
    /// registration reports no further readiness, and the next arm
    /// re-enables it with one MOD. This removes the ADD+DEL pair the
    /// previous design paid on every I/O cycle (2 `epoll_ctl` syscalls
    /// per request — MOD alone is also the cheapest mutation: no
    /// rbtree insert/erase, just an in-place mask write).
    ///
    /// The ONE exception is `deregisterIfIdle`: `EPOLLERR`/`EPOLLHUP`
    /// (and defensively `EPOLLRDHUP`) BYPASS ONESHOT disarming — the
    /// kernel force-includes them in every readiness poll of a linked
    /// item — so a kept registration on a dead peer would re-deliver
    /// the level-triggered HUP on every `epoll_wait`, a 100% CPU
    /// busy-loop. When an error/hangup event is being processed and
    /// the channel is going idle, the entry MUST be removed.
    ///
    /// Mutates the state IN PLACE through the slot pointer — no local
    /// copy, no write-back. The registration token carries the handle
    /// `(slot, gen)` verbatim so delivered events resolve back to this
    /// exact allocation.
    ///
    /// - Parameter deregisterIfIdle: tear the registration down if no
    ///   interest remains (set when the event being processed carried
    ///   ERR/HUP/RDHUP — see above).
    private func rearm(
        slot: Int, gen: UInt32,
        state: UnsafeMutablePointer<PollChannelState>,
        deregisterIfIdle: Bool = false
    ) {
        var interest: Interest = []
        if state.pointee.pendingRead != nil  { interest.insert(.readable) }
        if state.pointee.pendingWrite != nil { interest.insert(.writable) }

        // Nothing pending. The spent ONESHOT registration stays (it is
        // auto-disabled — silent until the next MOD), UNLESS the peer
        // is dying: see the HUP busy-loop note in the doc comment.
        guard !interest.isEmpty else {
            if deregisterIfIdle, state.pointee.registered {
                try? registry.deregister(fd: state.pointee.fd)
                state.pointee.registered = false
            }
            return
        }

        // Always one-shot: we want exactly one event per armed op, then
        // the loop decides what to do next.
        interest.insert(.oneshot)

        let token = packChannelId(slot: slot, gen: gen).asToken
        do {
            if state.pointee.registered {
                do {
                    try registry.reregister(
                        fd: state.pointee.fd, token: token, interest: interest
                    )
                } catch {
                    // The `registered` flag has exactly one way to go
                    // stale: the caller closed the fd behind the loop's
                    // back (against the documented cancel-before-close
                    // contract) — `close(2)` auto-removes the kernel
                    // entry, so MOD fails with ENOENT. Heal with a
                    // fresh ADD instead of failing both waiters.
                    try registry.register(
                        fd: state.pointee.fd, token: token, interest: interest
                    )
                }
            } else {
                try registry.register(
                    fd: state.pointee.fd, token: token, interest: interest
                )
            }
            state.pointee.registered = true
        } catch {
            // EBADF / ENOMEM: surface as immediate error to the
            // caller(s) by resuming with -1. The table stays
            // consistent — continuation fields are nilled in the slot
            // BEFORE resume (a resumed Task re-enters via drainJobs).
            // The failing epoll_ctl's errno rides on PollError.code
            // (captured race-free at mio's C layer); anything else is
            // recorded as EIO.
            let err = (error as? PollError)?.code ?? EIO
            if let cont = state.pointee.pendingRead {
                state.pointee.pendingRead = nil
                state.pointee.pendingReadCall = nil
                state.pointee.readDeadline = nil
                state.pointee.lastErrno = err
                cont.resume(returning: -1)
            }
            if let cont = state.pointee.pendingWrite {
                state.pointee.pendingWrite = nil
                state.pointee.pendingWriteCall = nil
                state.pointee.writeDeadline = nil
                state.pointee.lastErrno = err
                cont.resume(returning: false)
            }
        }
    }

    // MARK: Channel-event processing

    /// The hot path: one call per delivered epoll event. Unpacks
    /// `(slot, gen)` from the token, validates the generation (events
    /// for cancelled channels — including events later in the SAME
    /// batch than a `cancelChannel` issued from a watch handler —
    /// fail here and are dropped), then mutates the state in place.
    ///
    /// The watch/data decision reads the parallel `watchFlags` byte
    /// first: one static-offset load, no state-field access on the
    /// data path (state field offsets are runtime-computed for the
    /// non-frozen struct — the flag array avoids paying that per
    /// event).
    ///
    /// Discipline: continuation fields are nilled in the slot before
    /// `resume`; the slot pointer is not held across the `watch`
    /// handler call (the handler may cancel this slot).
    internal func processChannelEvent(_ event: Event) {
        let raw = event.token.raw
        let slot = Int(truncatingIfNeeded: raw & 0xFFFF_FFFF)
        let gen = UInt32(truncatingIfNeeded: raw >> 32)
        guard channels.isValid(slot: slot, gen: gen) else { return }

        // Watch channels: the caller owns I/O. Copy the closure to a
        // local (the handler may cancel this slot, releasing the
        // stored reference while we are inside the call), invoke it,
        // and return without touching the slot again — no re-arming,
        // the fd stays armed with its caller-supplied interest
        // (typically level-triggered + persistent).
        if channels.isWatch(slot: slot),
           let watch = channels.pointer(slot: slot).pointee.watch {
            watch(event.ready)
            return
        }

        let state = channels.pointer(slot: slot)

        let fd = state.pointee.fd

        // Read readiness: issue read(2) into the internal buffer, resume
        // the waiter. EINTR is retried — a signal landing on the syscall
        // must not surface as a spurious -1 ("error") that tears the
        // connection down (the caller-visible result of this branch IS
        // the read's return value).
        //
        // One maximal read per readiness — deliberately. With a fixed
        // destination buffer a drain-until-EAGAIN loop is provably a
        // no-op: a single read(2) of `readCapacity` bytes fills the
        // buffer maximally, and a short read on a stream fd means the
        // kernel had nothing more (read never withholds available
        // data), so looping would only burn a probing syscall. The
        // bulk-throughput lever is `readCapacity`, not extra reads —
        // tokio's poll_read makes the same single-read choice.
        if event.isReadable, let cont = state.pointee.pendingRead {
            enum ReadOutcome {
                case data(Int)
                case eof
                case failed(CInt)
                case spurious
            }
            let capacity = state.pointee.readCapacity
            var total = 0
            var outcome: ReadOutcome = .spurious
            while true {
                let want = capacity - total
                let n = Glibc.read(
                    fd, state.pointee.readBuffer! + total, want)
                if n > 0 {
                    total += Int(n)
                    if total == capacity || Int(n) < want {
                        outcome = .data(total)
                        break
                    }
                    continue
                }
                if n == 0 {
                    outcome = total > 0 ? .data(total) : .eof
                    break
                }
                if errno == EINTR { continue }
                if errno == EAGAIN || errno == EWOULDBLOCK {
                    // Spurious readiness: the kernel reported the fd
                    // readable, but the read found nothing (checksum-
                    // failed data dropped, or a racing consumer — not
                    // possible in this loop's single-reader model, but
                    // the kernel may still do it). Failing the wait
                    // here (the old behaviour: resume -1) tore healthy
                    // connections down. Instead: with data already
                    // gathered, deliver it; with none, keep the wait
                    // armed — the trailing `rearm` re-enables the
                    // interest and readiness is re-evaluated. Tokio
                    // semantics; no busy-loop risk (a MOD-armed
                    // ONESHOT only fires on real readiness).
                    outcome = total > 0 ? .data(total) : .spurious
                    break
                }
                // Real error: capture errno BEFORE anything else on
                // this thread can clobber it.
                outcome = .failed(errno)
                break
            }
            switch outcome {
            case let .data(count):
                state.pointee.pendingRead = nil
                state.pointee.pendingReadCall = nil
                state.pointee.readDeadline = nil
                state.pointee.lastErrno = 0
                cont.resume(returning: count)
            case .eof:
                state.pointee.pendingRead = nil
                state.pointee.pendingReadCall = nil
                state.pointee.readDeadline = nil
                state.pointee.lastErrno = 0
                cont.resume(returning: 0)
            case let .failed(err):
                state.pointee.pendingRead = nil
                state.pointee.pendingReadCall = nil
                state.pointee.readDeadline = nil
                state.pointee.lastErrno = err
                cont.resume(returning: -1)
            case .spurious:
                break  // wait stays armed; see comment above
            }
        }

        // Write readiness: the caller owns the buffer and performs the
        // `write(2)` itself after resuming. Here we only signal
        // writability (`true`). No buffer is dereferenced on the loop
        // side — the write-side symmetric counterpart of the read path,
        // which differs only because the loop owns the read destination.
        if event.isWritable, let cont = state.pointee.pendingWrite {
            state.pointee.pendingWrite = nil
            state.pointee.pendingWriteCall = nil
            state.pointee.writeDeadline = nil
            state.pointee.lastErrno = 0
            cont.resume(returning: true)
        }

        // Error / EOF handling. EPOLLERR surfaces as failure to any
        // remaining waiter (read: -1, write: false). EPOLLHUP (full
        // hangup) without EPOLLERR delivers read-side EOF (0) and
        // write-side failure (false) — note this also catches the
        // EPOLLHUP-without-IN case that `isReadClosed` would otherwise
        // claim. EPOLLRDHUP (peer half-close) alone does NOT fail a
        // pending write: the local side may still flush.
        //
        // lastErrno mapping: ERR waits record EIO (the real errno is
        // not observable without attempting the I/O); HUP read-side is
        // a clean EOF (0), HUP write-side records EPIPE (that is what
        // the next write(2) would return).
        if event.ready.isError {
            if let cont = state.pointee.pendingRead {
                state.pointee.pendingRead = nil
                state.pointee.pendingReadCall = nil
                state.pointee.readDeadline = nil
                state.pointee.lastErrno = EIO
                cont.resume(returning: -1)
            }
            if let cont = state.pointee.pendingWrite {
                state.pointee.pendingWrite = nil
                state.pointee.pendingWriteCall = nil
                state.pointee.writeDeadline = nil
                state.pointee.lastErrno = EIO
                cont.resume(returning: false)
            }
        } else if event.ready.isHangup {
            if let cont = state.pointee.pendingRead {
                state.pointee.pendingRead = nil
                state.pointee.pendingReadCall = nil
                state.pointee.readDeadline = nil
                state.pointee.lastErrno = 0
                cont.resume(returning: 0)
            }
            if let cont = state.pointee.pendingWrite {
                state.pointee.pendingWrite = nil
                state.pointee.pendingWriteCall = nil
                state.pointee.writeDeadline = nil
                state.pointee.lastErrno = EPIPE
                cont.resume(returning: false)
            }
        } else if event.ready.isReadClosed,
                  let cont = state.pointee.pendingRead {
            // EPOLLRDHUP: peer closed write side — deliver read EOF.
            state.pointee.pendingRead = nil
            state.pointee.pendingReadCall = nil
            state.pointee.readDeadline = nil
            state.pointee.lastErrno = 0
            cont.resume(returning: 0)
        }

        // Re-arm with whatever is still pending. If the event carried
        // ERR/HUP/RDHUP, pass `deregisterIfIdle` so a channel going
        // idle on a dead peer drops its kernel registration — those
        // bits bypass ONESHOT disarming and would otherwise re-deliver
        // forever (see `rearm`).
        rearm(
            slot: slot, gen: gen, state: state,
            deregisterIfIdle: event.ready.isError || event.ready.isHangup
                || event.ready.contains(.readHangup)
        )
    }

    /// Fail every still-boxed ARM request (`-1` / `false`). Cancels are
    /// plain no-ops (nothing left to disarm). Runs on the loop thread
    /// from run()'s tail and from deinit — a continuation left in the
    /// box past teardown would strand its Task and, on object death,
    /// trap the runtime as a leaked continuation.
    private func drainPendingLoopRequests() {
        var requests: [LoopRequest] = []
        loopRequests.withLock {
            swap(&requests, &$0)
        }
        for request in requests {
            switch request {
            case let .armRead(channelId, _, _, cont):
                setErrno(ECANCELED, on: channelId)
                cont.resume(returning: -1)
            case let .armWrite(channelId, _, _, cont):
                setErrno(ECANCELED, on: channelId)
                cont.resume(returning: false)
            case .cancel:
                break
            }
        }
    }

    // MARK: Orphan recovery

    private func recoverOrphanedContinuations() {
        channels.forEachLive { _, state in
            // Mirror the kernel state and release the loop-owned dup:
            // the table is about to be wiped, so no later cancelChannel
            // can do this. DEL first (the caller may still hold their
            // own fd — closing our dup alone would not remove the
            // entry), then close.
            if state.pointee.registered {
                try? registry.deregister(fd: state.pointee.fd)
            }
            _ = Glibc.close(state.pointee.fd)
            if let cont = state.pointee.pendingRead {
                state.pointee.pendingRead = nil
                state.pointee.pendingReadCall = nil
                state.pointee.lastErrno = ECANCELED
                cont.resume(returning: -1)
            }
            if let cont = state.pointee.pendingWrite {
                state.pointee.pendingWrite = nil
                state.pointee.pendingWriteCall = nil
                state.pointee.lastErrno = ECANCELED
                cont.resume(returning: false)
            }
            // Free per-channel read buffers here as well: the raw
            // allocations are NOT released by state deinitialization.
            if let buf = state.pointee.readBuffer {
                state.pointee.readBuffer = nil
                buf.deallocate()
            }
        }
        // Releases any held watch closures (e.g. the listener's accept
        // handler) as well as the channel states.
        channels.reset()
        watchByFd.removeAll()
    }

    /// White-box probe for tests (`@testable`): whether a read
    /// continuation is currently armed on the channel. Not part of
    /// the public contract.
    internal func _readPending(_ channelId: ChannelId) -> Bool {
        guard channels.isValid(slot: channelId.slot, gen: channelId.generation)
        else { return false }
        return channels.pointer(slot: channelId.slot).pointee.pendingRead != nil
    }

    // MARK: Job queue (SerialExecutor)

    /// Drain all queued jobs until both queues are empty.
    ///
    /// MUST be a `while` loop (not a single-pass snapshot) because
    /// running a job can enqueue more jobs — most notably the body
    /// of a freshly-spawned `Task { ... }` is enqueued as a separate
    /// job after the Task's setup job runs. A single-pass drain would
    /// leave the body job in `loopJobs` until the next drainJobs()
    /// call (after the next poll() round-trip), which with blocking
    /// epoll_wait means "forever".
    ///
    /// Job budget: every `maxJobsPerServicePass` jobs, one
    /// NON-blocking epoll pass services pending I/O (including the
    /// timerfd — so deadline enforcement survives task storms). A
    /// task that synchronously resumes other tasks in a tight loop
    /// can no longer starve the reactor indefinitely. Blocking here
    /// instead would deadlock: same-thread enqueues carry no wake, so
    /// nothing would ever fire the epoll_wait.
    private func drainJobs() {
        var served = 0
        while true {
            // Move the loop-local queue into the scratch buffer via an
            // O(1) CoW buffer exchange — no element copy, and the
            // buffer's capacity is recycled across drain cycles.
            // loopJobs is loop-thread-only, so no lock.
            swap(&drainBuffer, &loopJobs)

            // Move the cross-thread queue across with an O(1) buffer
            // swap UNDER the lock (constant lock-hold time — the bulk
            // append happens after release). All `poolQueue` access
            // is under its LockedBox lock (TSan-verifiable); the
            // previous design additionally read the pool queue's
            // `isEmpty` WITHOUT any lock — a genuine data race.
            poolQueue.withLock {
                swap(&poolDrain, &$0)
            }
            if !poolDrain.isEmpty {
                drainBuffer.append(contentsOf: poolDrain)
                poolDrain.removeAll(keepingCapacity: true)
            }
            if drainBuffer.isEmpty { return }

            for job in drainBuffer {
                job.runSynchronously(on: cachedExecutor)
                served &+= 1
                if served == Self.maxJobsPerServicePass {
                    served = 0
                    // mio's Events.wait retries EINTR internally and
                    // zeroes the delivered count on any error it does
                    // surface (verified against its source) — so a
                    // failed wait leaves `events` empty. The do/catch
                    // keeps that an explicit local fact rather than a
                    // cross-package assumption: dispatching only on
                    // success stays correct even if mio ever changes
                    // its error-path behaviour. Errors are not
                    // escalated here: the main loop's next blocking
                    // wait hits the same condition and owns the
                    // retry/backoff/give-up policy.
                    do {
                        try events.wait(on: poll, timeout: PollTimeout.immediate)
                        dispatchDeliveredEvents()
                    } catch {
                        // skip this service pass
                    }
                }
            }
            drainBuffer.removeAll(keepingCapacity: true)
        }
    }

    private func enqueueJob(_ job: UnownedJob) {
        // Terminal: the loop thread is gone; this job can never run.
        // `SerialExecutor.enqueue` is synchronous with no failure
        // channel, so the options are drop, trap, or "rescue". The
        // rescue is rejected on architecture grounds: hijacking the
        // enqueuer's thread runs unbounded user code on a caller that
        // expects a cheap enqueue (deadlocks if it holds a lock the
        // job needs), and a dedicated drainer thread has no sound exit
        // condition and races a restart. Drop + count + debug-assert
        // instead: loud in development, observable in production
        // metrics, never crashes a shutting-down server over a
        // straggler. (An UnownedJob cannot be faulted or cancelled —
        // running it is its only fate, and there is no thread left.)
        //
        // The terminal check lives INSIDE poolQueue's lock, and the
        // tail closes the job window by storing the flag under the
        // SAME lock — so a cross-thread enqueue is strictly ordered
        // against the close: either its append precedes the store
        // (the tail's final drainJobs runs it), or its check sees the
        // flag (dropped, counted). No silent stuck-in-box state.
        let tid = loopThreadId.load(ordering: .acquiring)
        if UInt(pthread_self()) == tid {
            // Same-thread fast path: no synchronisation. The loop
            // thread cannot race the tail's close — the tail IS the
            // loop thread, and drainJobs runs until both queues are
            // empty before returning.
            loopJobs.append(job)
            return
        }
        var dropped = false
        let needWake = poolQueue.withLock { (queue: inout [UnownedJob]) -> Bool in
            if runExited.load(ordering: .relaxed) {
                dropped = true
                return false
            }
            queue.append(job)
            return tid != 0
        }
        if dropped {
            _ = droppedJobs.add(1)
            assertionFailure(
                "PollEventLoop: job enqueued after run() returned — the loop " +
                "is terminal; this job will never run (see droppedJobs)"
            )
            return
        }
        // The waker exists from init onward, so this can never be a
        // lost wakeup. (Jobs enqueued pre-run see tid == 0, skip the
        // wake, and are picked up by run()'s initial drainJobs.)
        if needWake { waker.wake() }
    }
}

// MARK: - SerialExecutor conformance

extension PollEventLoop: SerialExecutor {
    public func enqueue(_ job: consuming ExecutorJob) {
        let unowned = UnownedJob(job)
        enqueueJob(unowned)
    }

    public func asUnownedSerialExecutor() -> UnownedSerialExecutor {
        cachedExecutor
    }

    /// Verify we're on the loop thread. Used by `Actor.assumeIsolated`
    /// to check isolation when an actor's `unownedExecutor` returns
    /// this loop.
    ///
    /// The default `SerialExecutor` implementation checks the current
    /// Task's executor — which fails when the call site is a sync
    /// context (e.g., inside `run()`'s watch callback). For our
    /// thread-per-core model, "executing on this loop" means
    /// "executing on the loop's OS thread" — verifiable via
    /// `pthread_self()` against the stored `loopThreadId`.
    ///
    /// `loopThreadId == 0` — the loop is not running — is LEGAL,
    /// mirroring `precondLoopThread`'s two blessed regimes: pre-`run()`
    /// setup (the register-before-run pattern, single-threaded by
    /// contract) and the post-`run()` terminal tail (an error-retry
    /// `run()` re-registers channels through it). While the loop RUNS,
    /// isolation is strictly "the loop's thread". (The same accepted
    /// race window as `precondLoopThread`: a `run()` starting
    /// concurrently may claim the thread between our load and the
    /// decision.)
    public func checkIsolated() {
        let expected = loopThreadId.load(ordering: .acquiring)
        if expected != 0 {
            let current = UInt(pthread_self())
            if current != expected {
                fatalError(
                    "PollEventLoop isolation violation: current thread \(current) is not the loop thread \(expected)"
                )
            }
        }
    }

    public func isSameExclusiveExecutionContext(other: PollEventLoop) -> Bool {
        other === self
    }
}

// MARK: - TaskExecutor conformance
//
// `TaskExecutor` (SE-0431, macOS 15+/iOS 18+) lets us spawn a Task
// pinned to this executor directly via:
//
//     Task(executorPreference: loop.eventLoop) {
//         // runs on the loop's thread
//     }
//
// Without this, the only way to pin a Task to a custom executor was
// to route it through an actor with a `nonisolated unownedExecutor`
// property — that's why EpollConnectionActor existed as an empty
// singleton. With TaskExecutor, the actor wrapper is no longer
// necessary; Tasks can be spawned directly against the loop.
//
// The implementation is trivial because `enqueue` semantics are
// identical to SerialExecutor — the difference is only the API
// surface (Task(executorPreference:) vs. await on actor method).
extension PollEventLoop: TaskExecutor {
    public func asUnownedTaskExecutor() -> UnownedTaskExecutor {
        return cachedTaskExecutor
    }
}

#endif // os(Linux)
