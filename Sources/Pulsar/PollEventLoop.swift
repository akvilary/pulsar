//===----------------------------------------------------------------------===//
//
//  PollEventLoop.swift
//  StarlightPoll
//
//  High-level Swift Concurrency event loop built on top of the low-level
//  `Poll` / `Registry` / `Waker` primitives from the `mio` package
//  (https://github.com/akvilary/mio). This is the epoll analogue of
//  `StarlightIORing.IORingEventLoop` — same async `read`/`write`
//  surface, same SerialExecutor semantics, but every operation is
//  driven by readiness notifications on a single epoll fd instead of
//  io_uring submissions.
//
//  The mio primitives are re-exported, so `import StarlightPoll` is
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
//  * Cross-thread wakeup is via `Waker` (eventfd). Cross-thread Task
//    enqueue uses the same spinlock-protected pool as IORingEventLoop.
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
@usableFromInline
internal struct PollChannelState {
    var fd: CInt
    var registered: Bool = false
    var pendingRead: CheckedContinuation<Int, Never>?
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
    /// Absolute deadline after which a pending write-wait is considered
    /// timed out. Set together with `pendingWrite`; cleared together.
    var writeDeadline: ContinuousClock.Instant?
    var watch: (@Sendable (Ready) -> Void)?
    /// Per-channel read buffer — pre-allocated, reused across
    /// keep-alive requests. Owned by the eventLoop (NOT by the
    /// decoder). Eliminates @unchecked on H1Conn + ConnState.
    var readBuffer: UnsafeMutablePointer<UInt8>?
    var readCapacity: Int = 8192
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
    private var waker: Waker?

    // Periodic timer (timerfd) that wakes the loop to sweep expired
    // read/write deadlines — the mechanism bounding per-op waits
    // (Slowloris / write-stall defence). Created and registered in
    // run(); one fd per loop, shared by all channels.
    private var timerFd: CInt = -1
    /// Token reserved for the periodic timer. Channel tokens are
    /// `Token(channelId)` with channelId ∈ UInt32, so `UInt64.max` can
    /// never collide. Handled explicitly in the event dispatch before
    /// `processChannelEvent`, so it never reaches the channelId lookup.
    private static let timerToken = Token(UInt64.max)
    /// Sweep granularity (default 500 ms). Bounds how late a deadline
    /// can be enforced; cheap because the sweep is O(active channels)
    /// and runs only on each tick, never per request.
    public var timeoutSweepInterval: Duration = .milliseconds(500)

    // Per-channel pending-op tracking — loop thread only.
    //
    // INVARIANT: `PollChannelState` is a value type — any code path that
    // reads a state from this dict, mutates the local copy, and resumes
    // continuations MUST persist the mutated copy back via
    // `channels[channelId] = state` before returning. `rearm(state:)`
    // does NOT read from this dict and does NOT persist its own
    // mutations; the caller owns the single write-back after rearm.
    private var nextChannelId: UInt32 = 1
    private var channels: [UInt32: PollChannelState] = [:]

    // Cross-thread job queue (SerialExecutor surface).
    private var loopJobs: [UnownedJob] = []
    private var poolJobs: [UnownedJob] = []
    private var jobLock = pthread_spinlock_t()
    private let loopThreadId = Atomic<UInt>(0)

    // Loop state.
    private let stopped = Atomic<Bool>(false)
    private var consecutiveErrors: Int = 0

    // Stats.
    public let overflowEvents = PaddedAtomicInt64()

    // User hook invoked from the loop thread after the waker fires.
    public var onWakeup: (@Sendable () -> Void)?

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
        pthread_spin_init(&jobLock, 0)
    }

    deinit {
        if let w = waker { _ = Glibc.close(w.fd) }
        pthread_spin_destroy(&jobLock)
    }

    // MARK: Event loop

    public func run() throws {
        loopThreadId.store(UInt(pthread_self()), ordering: .releasing)
        defer { loopThreadId.store(0, ordering: .releasing) }

        // Register the cross-thread waker on the loop thread.
        self.waker = try Waker(registry: registry, token: .wakeup)

        // Register the periodic timeout-sweep timer. Failure is
        // non-fatal: the loop still serves I/O, just without bounded
        // waits (graceful degradation to pre-timeout behaviour).
        if let tfd = TimerFd.create() {
            self.timerFd = tfd
            _ = TimerFd.setPeriodic(fd: tfd, interval: timeoutSweepInterval)
            // Level-triggered, persistent (NOT oneshot): the timer stays
            // armed and fires once per interval until drained/closed.
            try? registry.register(
                fd: tfd, token: Self.timerToken, interest: .readable
            )
        }

        while !stopped.load(ordering: .acquiring) {
            // Phase 1: block on epoll_wait until at least one source is
            // ready (or the waker fires, or a signal interrupts).
            do {
                try self.events.wait(on: self.poll, timeout: PollTimeout.blocking)
                consecutiveErrors = 0
            } catch {
                // Recoverable errors: log, sleep briefly, retry. After 32
                // consecutive failures give up — same threshold as the
                // io_uring backend.
                consecutiveErrors += 1
                if consecutiveErrors > 32 { throw error }
                continue
            }

            // Phase 2: dispatch each event. Channel reads/writes run the
            // actual syscall here so the resumed Task sees the result.
            events.forEach { event in
                if event.token == .wakeup {
                    handleWakeup()
                } else if event.token == Self.timerToken {
                    handleTimer()
                } else {
                    processChannelEvent(event)
                }
            }

            // Phase 3: drain queued jobs (connection Tasks resuming, new
            // Tasks, etc.). Jobs enqueue themselves via `enqueue` which
            // may have been called by Task.runSynchronously in phase 2
            // (a Task awaiting `read` whose body queued another op) or
            // by another thread.
            drainJobs()
        }

        // Resume any remaining waiters with errors on shutdown.
        // Then drain the resulting jobs: each resume enqueues a Task
        // continuation into loopJobs/poolJobs. Without this final
        // drain, the Tasks (which hold captures of the loop, connection
        // fds, codecs, etc.) would leak — their cleanup code (which
        // calls closeConnection and returns) never runs.
        recoverOrphanedContinuations()
        drainJobs()

        // Tear down the timeout-sweep timer (loop-thread cleanup).
        if timerFd >= 0 {
            try? registry.deregister(fd: timerFd)
            _ = Glibc.close(timerFd)
            timerFd = -1
        }
    }

    public func shutdown() {
        stopped.store(true, ordering: .releasing)
        wakeup()
    }

    /// True after `shutdown()` has been called.
    public var isStopped: Bool {
        stopped.load(ordering: .acquiring)
    }

    // MARK: Wakeup

    @inline(__always)
    private func handleWakeup() {
        _ = waker?.reset()
        onWakeup?()
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

    /// Two-phase sweep. Phase 1 is a read-only scan that collects
    /// expired, still-pending continuations; phase 2 (after the scan,
    /// so the dictionary is not mutated during iteration) claims each
    /// continuation (sets the slot to `nil`), clears its deadline, and
    /// resumes it.
    ///
    /// Claiming is the only synchronisation needed vs readiness
    /// (`processChannelEvent`): both run on the loop thread, serialized,
    /// and both null the slot before resuming — so exactly one of them
    /// wins per continuation. `cont.resume()` schedules the Task on the
    /// loop; it does not re-enter this state synchronously.
    private func sweepTimeouts(now: ContinuousClock.Instant) {
        // Phase 1: collect (read-only over `channels`).
        var readTimedOut: [(UInt32, CheckedContinuation<Int, Never>)] = []
        var writeTimedOut: [(UInt32, CheckedContinuation<Bool, Never>)] = []
        for (id, state) in channels {
            if let d = state.readDeadline, d <= now, state.pendingRead != nil,
               let cont = state.pendingRead {
                readTimedOut.append((id, cont))
            }
            if let d = state.writeDeadline, d <= now, state.pendingWrite != nil,
               let cont = state.pendingWrite {
                writeTimedOut.append((id, cont))
            }
        }
        // Phase 2: claim + clear + resume (mutating, not iterating).
        for (id, cont) in readTimedOut {
            guard var state = channels[id], state.pendingRead != nil else { continue }
            state.pendingRead = nil
            state.readDeadline = nil
            channels[id] = state
            cont.resume(returning: -2)  // read-timeout sentinel
        }
        for (id, cont) in writeTimedOut {
            guard var state = channels[id], state.pendingWrite != nil else { continue }
            state.pendingWrite = nil
            state.writeDeadline = nil
            channels[id] = state
            cont.resume(returning: false)  // write-timeout (≡ error → bail)
        }
    }

    /// Wake the loop from any thread. The next `poll()` iteration will
    /// observe the wakeup token and invoke `onWakeup`.
    public func wakeup() {
        _ = waker?.wake()
    }

    // MARK: Channel management

    /// Allocate a fresh, unique channelId. Use the returned id with
    /// `read`/`write`/`cancelChannel`. The id is never reused, which
    /// prevents fd-recycling misattribution. Allocates a per-channel
    /// read buffer (8KB, reused across keep-alive requests).
    public func registerChannel() -> UInt32 {
        let id = nextChannelId
        nextChannelId &+= 1
        var state = PollChannelState(fd: -1)
        state.readBuffer = .allocate(capacity: state.readCapacity)
        channels[id] = state
        return id
    }

    /// Register a watch channel — an fd the caller wants to drive
    /// directly via `handler` rather than through the async read/write
    /// API. Returns a fresh channelId that can later be passed to
    /// `cancelChannel`.
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
    public func registerWatch(
        fd: CInt, interest: Interest,
        _ handler: @Sendable @escaping (Ready) -> Void
    ) throws -> UInt32 {
        let id = nextChannelId
        nextChannelId &+= 1
        var state = PollChannelState(fd: fd)
        state.watch = handler
        try registry.register(fd: fd, token: Token(id), interest: interest)
        state.registered = true
        channels[id] = state
        return id
    }

    /// Cancel any outstanding read/write on `channelId`. Pending
    /// continuations are resumed with `-1`. Should be called on the
    /// loop thread (typically from the connection-loop Task body).
    /// Also valid for a watch channel: its handler closure is released
    /// when the entry is removed.
    public func cancelChannel(_ channelId: UInt32) {
        guard let state = channels.removeValue(forKey: channelId) else { return }
        if let cont = state.pendingRead  { cont.resume(returning: -1) }
        if let cont = state.pendingWrite { cont.resume(returning: false) }
        if state.registered { try? registry.deregister(fd: state.fd) }
        // Free per-channel read buffer.
        if let buf = state.readBuffer { buf.deallocate() }
    }

    // MARK: Async read

    /// Await readability on `(channelId, fd)`, then read into the
    /// eventLoop's internal per-channel buffer. Returns bytes read
    /// (0 on EOF, -1 on error, -2 on timeout).
    ///
    /// The buffer is owned by the eventLoop — callers access it via
    /// `getReadView(channelId:count:)` after this returns. This
    /// eliminates the need for the caller to own a raw buffer (and
    /// thus the need for @unchecked Sendable on decoder/conn types).
    ///
    /// - Parameter deadline: absolute time after which an unanswered
    ///   readiness wait is failed with `-2` (timeout). `nil` disables
    ///   the timeout (compat). Enforced by `sweepTimeouts` on each
    ///   timerfd tick, so granularity ≈ `timeoutSweepInterval`.
    public func read(
        channelId: UInt32, fd: CInt,
        deadline: ContinuousClock.Instant? = nil
    ) async -> Int {
        return await withCheckedContinuation { cont in
            armRead(channelId: channelId, fd: fd, cont: cont, deadline: deadline)
        }
    }

    /// Get a view into the per-channel read buffer after `read()`
    /// returns. The pointer is valid until the next `read()` call
    /// on the same channel. Called from the loop thread only.
    public func getReadView(channelId: UInt32, count: Int) -> UnsafeBufferPointer<UInt8> {
        guard let state = channels[channelId], let buf = state.readBuffer else {
            return UnsafeBufferPointer(start: nil, count: 0)
        }
        return UnsafeBufferPointer(start: buf, count: Swift.min(count, state.readCapacity))
    }

    @inline(__always)
    private func armRead(
        channelId: UInt32, fd: CInt,
        cont: CheckedContinuation<Int, Never>,
        deadline: ContinuousClock.Instant?
    ) {
        var state = channels[channelId] ?? PollChannelState(fd: fd)
        state.fd = fd
        precondition(state.pendingRead == nil,
            "PollEventLoop: overlapping read on channelId=\(channelId)")
        state.pendingRead = cont
        state.readDeadline = deadline
        rearm(channelId: channelId, state: &state)
        channels[channelId] = state
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

    /// Optimistic `write(2)` loop over `buffer`; on `EAGAIN` arms
    /// `EPOLLOUT` and awaits readiness via `awaitWritable`. Returns
    /// total bytes written (0..buffer.count).
    ///
    /// Runs entirely on the caller's executor (the loop thread for
    /// loop-pinned callers). The fast path — socket buffer has room —
    /// never suspends: the optimistic `write(2)` succeeds and the loop
    /// returns without crossing an await. Only a full socket buffer
    /// triggers `awaitWritable`, which suspends this Task while the
    /// loop serves other connections.
    ///
    /// - Precondition: the caller MUST own `buffer` for the duration
    ///   of this call (across any internal await). The pointer is
    ///   dereferenced only inside synchronous `write(2)` attempts.
    /// - Precondition: no other write wait may be in flight on the
    ///   same `channelId`.
    public func write(
        channelId: UInt32, fd: CInt,
        from buffer: UnsafeRawBufferPointer
    ) async -> Int {
        var offset = 0
        while offset < buffer.count {
            let n = Glibc.write(
                fd, buffer.baseAddress!.advanced(by: offset),
                buffer.count - offset
            )
            if n > 0 { offset += Int(n); continue }
            if n == 0 { break }          // socket: shouldn't happen
            if errno == EINTR { continue }
            if errno == EAGAIN || errno == EWOULDBLOCK {
                if !(await awaitWritable(channelId: channelId, fd: fd)) {
                    break                 // error / hangup
                }
                continue
            }
            break                         // EPIPE / EBADF / ...
        }
        return offset
    }

    /// Await writability on `(channelId, fd)`. Arms `EPOLLOUT` (oneshot),
    /// suspends, and resumes with `true` when the socket can accept a
    /// write, or `false` on `EPOLLERR` / `EPOLLHUP` / timeout.
    ///
    /// The caller issues the actual `write(2)` after this returns.
    ///
    /// - Parameter deadline: absolute time after which an unanswered
    ///   readiness wait is failed with `false`. `nil` disables it.
    public func awaitWritable(
        channelId: UInt32, fd: CInt,
        deadline: ContinuousClock.Instant? = nil
    ) async -> Bool {
        await withCheckedContinuation { (cont: CheckedContinuation<Bool, Never>) in
            armWritable(channelId: channelId, fd: fd, cont: cont, deadline: deadline)
        }
    }

    @inline(__always)
    private func armWritable(
        channelId: UInt32, fd: CInt,
        cont: CheckedContinuation<Bool, Never>,
        deadline: ContinuousClock.Instant?
    ) {
        var state = channels[channelId] ?? PollChannelState(fd: fd)
        state.fd = fd
        precondition(state.pendingWrite == nil,
            "PollEventLoop: overlapping write on channelId=\(channelId) — previous continuation would leak")
        state.pendingWrite = cont
        state.writeDeadline = deadline
        rearm(channelId: channelId, state: &state)
        // Single write-back after rearm (see armRead for rationale).
        channels[channelId] = state
    }

    // MARK: Re-arm logic

    /// Recompute the interest mask for the channel based on currently
    /// pending ops, then ADD or MOD the fd. Called after each op is
    /// armed and after each event is processed.
    @inline(__always)
    private func rearm(channelId: UInt32, state: inout PollChannelState) {
        var interest: Interest = []
        if state.pendingRead != nil  { interest.insert(.readable) }
        if state.pendingWrite != nil { interest.insert(.writable) }

        // If nothing is pending, deregister to free the epoll slot —
        // otherwise the kernel keeps a dangling interest entry.
        guard !interest.isEmpty else {
            if state.registered {
                try? registry.deregister(fd: state.fd)
                state.registered = false
            }
            return
        }

        // Always one-shot: we want exactly one event per armed op, then
        // the loop decides what to do next.
        interest.insert(.oneshot)

        do {
            if state.registered {
                try registry.reregister(
                    fd: state.fd, token: Token(channelId), interest: interest
                )
            } else {
                try registry.register(
                    fd: state.fd, token: Token(channelId), interest: interest
                )
                state.registered = true
            }
        } catch {
            // EBADF / ENOMEM / ENOMEM: surface as immediate error to the
            // caller(s) by resuming with -1. The channels dict stays
            // consistent.
            if let cont = state.pendingRead {
                state.pendingRead = nil
                cont.resume(returning: -1)
            }
            if let cont = state.pendingWrite {
                state.pendingWrite = nil
                cont.resume(returning: false)
            }
        }
    }

    // MARK: Channel-event processing

    @inline(__always)
    private func processChannelEvent(_ event: Event) {
        let channelId = UInt32(truncatingIfNeeded: event.token.raw)
        guard var state = channels[channelId] else { return }

        // Watch channels: the caller owns I/O. Invoke the handler and
        // return without touching the read/write continuation path or
        // re-arming — the fd stays armed with its caller-supplied
        // interest (typically level-triggered + persistent). No write-
        // back needed: `state` is unmodified here.
        if let watch = state.watch {
            watch(event.ready)
            return
        }

        let fd = state.fd

        // Read readiness: issue read(2) into internal buffer, resume waiter.
        if event.isReadable, let cont = state.pendingRead {
            state.pendingRead = nil
            state.readDeadline = nil
            let n = Glibc.read(fd, state.readBuffer!, state.readCapacity)
            cont.resume(returning: Int(n))
        }

        // Write readiness: the caller owns the buffer and performs the
        // `write(2)` itself after resuming. Here we only signal
        // writability (`true`). No buffer is dereferenced on the loop
        // side — the write-side symmetric counterpart of the read path,
        // which differs only because the loop owns the read destination.
        if event.isWritable, let cont = state.pendingWrite {
            state.pendingWrite = nil
            state.writeDeadline = nil
            cont.resume(returning: true)
        }

        // Error / EOF handling. EPOLLERR surfaces as failure to any
        // remaining waiter (read: -1, write: false). EPOLLHUP (full
        // hangup) without EPOLLERR delivers read-side EOF (0) and
        // write-side failure (false) — note this also catches the
        // EPOLLHUP-without-IN case that `isReadClosed` would otherwise
        // claim. EPOLLRDHUP (peer half-close) alone does NOT fail a
        // pending write: the local side may still flush.
        if event.ready.isError {
            if let cont = state.pendingRead {
                state.pendingRead = nil
                state.readDeadline = nil
                cont.resume(returning: -1)
            }
            if let cont = state.pendingWrite {
                state.pendingWrite = nil
                state.writeDeadline = nil
                cont.resume(returning: false)
            }
        } else if event.ready.isHangup {
            if let cont = state.pendingRead {
                state.pendingRead = nil
                state.readDeadline = nil
                cont.resume(returning: 0)
            }
            if let cont = state.pendingWrite {
                state.pendingWrite = nil
                state.writeDeadline = nil
                cont.resume(returning: false)
            }
        } else if event.ready.isReadClosed,
                  let cont = state.pendingRead {
            // EPOLLRDHUP: peer closed write side — deliver read EOF.
            state.pendingRead = nil
            state.readDeadline = nil
            cont.resume(returning: 0)
        }

        // Re-arm with whatever is still pending. If both directions
        // were satisfied, this deregisters. Single write-back after
        // rearm (see armRead for rationale).
        rearm(channelId: channelId, state: &state)
        channels[channelId] = state
    }

    // MARK: Orphan recovery

    private func recoverOrphanedContinuations() {
        for (_, var state) in channels {
            if let cont = state.pendingRead {
                state.pendingRead = nil
                cont.resume(returning: -1)
            }
            if let cont = state.pendingWrite {
                state.pendingWrite = nil
                cont.resume(returning: false)
            }
        }
        // Releases any held watch closures (e.g. the listener's accept
        // handler) as well as the channel states.
        channels.removeAll()
        _ = overflowEvents.increment()
    }

    // MARK: Job queue (SerialExecutor)

    /// Drain all queued jobs until both queues are empty.
    ///
    /// MUST be a `while` loop (not a single-pass snapshot) because
    /// running a job can enqueue more jobs — most notably the body
    /// of a freshly-spawned `Task { ... }` is enqueued as a separate
    /// job after the Task's setup job runs. A single-pass drain would
    /// leave the body job in `loopJobs` until the next drainJobs()
    /// call (after the next poll.poll() round-trip), which with
    /// blocking epoll_wait means "forever".
    private func drainJobs() {
        while true {
            var jobs = loopJobs
            loopJobs.removeAll(keepingCapacity: true)

            if !poolJobs.isEmpty {
                pthread_spin_lock(&jobLock)
                jobs.append(contentsOf: poolJobs)
                poolJobs.removeAll(keepingCapacity: true)
                pthread_spin_unlock(&jobLock)
            }
            if jobs.isEmpty { return }
            if jobs.count > 1 {
            }

            for job in jobs {
                job.runSynchronously(on: cachedExecutor)
            }
        }
    }

    private func enqueueJob(_ job: UnownedJob) {
        let tid = loopThreadId.load(ordering: .acquiring)
        let cur = UInt(pthread_self())
        if cur == tid {
            loopJobs.append(job)
        } else {
            pthread_spin_lock(&jobLock)
            poolJobs.append(job)
            let needWake = tid != 0
            pthread_spin_unlock(&jobLock)
            if needWake { wakeup() }
        }
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
    public func checkIsolated() {
        let expected = loopThreadId.load(ordering: .acquiring)
        let current = UInt(pthread_self())
        if current != expected {
            fatalError(
                "PollEventLoop isolation violation: current thread \(current) is not the loop thread \(expected)"
            )
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
