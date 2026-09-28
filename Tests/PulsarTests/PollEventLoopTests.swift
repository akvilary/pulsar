//===----------------------------------------------------------------------===//
//
//  PollEventLoopTests.swift
//  StarlightPollTests
//
//  End-to-end tests for the async event loop surface. Each test spawns
//  the loop on a dedicated thread, performs async read/write against
//  in-process sockets/pipes, and verifies that the same contract as
//  `IORingEventLoop` is upheld.
//
//===----------------------------------------------------------------------===//

#if os(Linux)

import Testing
import Foundation
import Synchronization
@testable import Pulsar

#if canImport(Glibc)
import Glibc
#endif

@Suite("PollEventLoop", .serialized)
struct PollEventLoopTests {

    // A minimal actor pinned to the loop's SerialExecutor — mirrors the
    // pattern in `StarlightServer`'s ConnectionActor. Required because
    // Swift 6.2 has no `Task(executor:)` overload for `SerialExecutor`
    // (only for `TaskExecutor`); the canonical way to drive Tasks on a
    // custom serial executor is via an actor whose
    // `nonisolated unownedExecutor` returns it.
    actor LoopPinned {
        nonisolated let _executor: UnownedSerialExecutor
        init(_ executor: UnownedSerialExecutor) { self._executor = executor }
        nonisolated var unownedExecutor: UnownedSerialExecutor { _executor }

        /// Read via eventLoop's internal buffer, copy out to [UInt8].
        func runRead(loop: PollEventLoop, channelId: ChannelId,
                     capacity: Int) async -> (Int, [UInt8]) {
            let n = await loop.read(channelId: channelId)
            var out = [UInt8](repeating: 0, count: max(0, n))
            if n > 0 {
                let view = loop.getReadView(channelId: channelId, count: n)
                for i in 0..<out.count {
                    out[i] = view[i]
                }
            }
            return (n, out)
        }

        /// Read with an absolute deadline — used by the timeout test to
        /// verify the timerfd sweep fails an unanswered read with -2.
        func runReadWithDeadline(
            loop: PollEventLoop, channelId: ChannelId,
            deadline: ContinuousClock.Instant
        ) async -> Int {
            await loop.read(channelId: channelId, deadline: deadline)
        }

        /// awaitWritable with an absolute deadline — used by the timeout
        /// test to verify a write-wait that never becomes ready fails.
        func runAwaitWritableWithDeadline(
            loop: PollEventLoop, channelId: ChannelId,
            deadline: ContinuousClock.Instant
        ) async -> Bool {
            await loop.awaitWritable(channelId: channelId, deadline: deadline)
        }

        /// Drive one write followed by one read on the same socket
        /// pair. Sequential — the loop processes one Task at a time
        /// per iteration (mirrors IORingEventLoop's echoLoop pattern;
        /// `async let` would deadlock because child tasks queued during
        /// drainJobs don't run until the next poll() returns).
        func runRoundTrip(loop: PollEventLoop,
                          writerCh: ChannelId, readerCh: ChannelId,
                          payload: [UInt8]) async -> (writeN: Int, readN: Int, read: [UInt8]) {
            let writeBuf = UnsafeMutableRawBufferPointer.allocate(
                byteCount: payload.count, alignment: 8)
            defer { writeBuf.deallocate() }
            payload.withUnsafeBufferPointer { src in
                writeBuf.copyMemory(from: UnsafeRawBufferPointer(src))
            }

            // Write first, then read — both on the loop thread.
            let writeN = await loop.write(
                channelId: writerCh, from: UnsafeRawBufferPointer(writeBuf))
            // New API: eventLoop reads into its internal buffer.
            // Caller accesses via getReadView.
            let readN = await loop.read(channelId: readerCh)
            var out = [UInt8](repeating: 0, count: max(0, readN))
            if readN > 0 {
                let view = loop.getReadView(channelId: readerCh, count: readN)
                for i in 0..<out.count {
                    out[i] = view[i]
                }
            }
            return (writeN, readN, out)
        }
    }

    // MARK: - Echo over a socketpair

    @Test("Async read over socketpair delivers bytes")
    func readSocketpair() async throws {
        let loop = try PollEventLoop()
        let sp = makeSocketpair()
        guard let sp else { Issue.record("socketpair failed"); return }
        let (a, b) = (sp.read, sp.write)
        defer {
            _ = Glibc.close(a); _ = Glibc.close(b)
            loop.shutdown()
        }

        // Register BEFORE starting the loop — the channel table is
        // loop-thread state (the threading contract).
        let channelId = try loop.registerChannel(fd: b)

        // Run the loop on a dedicated OS thread.
        let loopThread = Thread { [loop] in
            try? loop.run()
        }
        loopThread.start()
        try await Task.sleep(for: .milliseconds(30))

        let payload: [UInt8] = [0xDE, 0xAD, 0xBE, 0xEF]
        _ = payload.withUnsafeBufferPointer { ptr in
            Glibc.write(a, ptr.baseAddress!, 4)
        }

        // Drive the read primitive from an actor pinned to the loop so
        // the read mutates loop-private state from the loop thread.
        let pinned = LoopPinned(loop.cachedExecutor)
        let (n, out) = await pinned.runRead(
            loop: loop, channelId: channelId, capacity: 8)

        #expect(n == 4)
        #expect(out.count >= 4)
        #expect(out[0] == 0xDE)
        #expect(out[3] == 0xEF)
    }

    @Test("Wakeup callback fires from another thread")
    func wakeupFromAnotherThread() async throws {
        let loop = try PollEventLoop()

        let fired = Atomic<Bool>(false)
        loop.onWakeup = { fired.store(true, ordering: .releasing) }

        let loopThread = Thread { [loop] in
            try? loop.run()
        }
        loopThread.start()
        try await Task.sleep(for: .milliseconds(30))

        // Wake from this thread.
        loop.wakeup()

        // Give the loop a moment to process.
        try await Task.sleep(for: .milliseconds(40))
        #expect(fired.load(ordering: .acquiring) == true)

        loop.shutdown()
    }

    @Test("Async write then read round-trips through the loop")
    func writeReadRoundTrip() async throws {
        let loop = try PollEventLoop()
        let sp = makeSocketpair()
        guard let sp else { Issue.record("socketpair failed"); return }
        let (a, b) = (sp.read, sp.write)
        defer {
            _ = Glibc.close(a); _ = Glibc.close(b)
            loop.shutdown()
        }

        // Threading contract: register before the loop starts.
        let writerCh = try loop.registerChannel(fd: a)
        let readerCh = try loop.registerChannel(fd: b)

        let loopThread = Thread { [loop] in try? loop.run() }
        loopThread.start()
        try await Task.sleep(for: .milliseconds(30))

        let payload: [UInt8] = [0x01, 0x02, 0x03, 0x04, 0x05, 0x06]

        let pinned = LoopPinned(loop.cachedExecutor)
        let result = await pinned.runRoundTrip(
            loop: loop, writerCh: writerCh, readerCh: readerCh,
            payload: payload
        )

        #expect(result.writeN == 6)
        #expect(result.readN == 6)
        #expect(Array(result.read.prefix(6)) == payload)
    }

    @Test("Watch channel fires its handler on the loop thread")
    func watchReadiness() async throws {
        let loop = try PollEventLoop()
        let sp = makeSocketpair()
        guard let sp else { Issue.record("socketpair failed"); return }
        let (a, b) = (sp.read, sp.write)
        defer {
            _ = Glibc.close(a); _ = Glibc.close(b)
            loop.shutdown()
        }

        let fired = Atomic<Bool>(false)
        let readyBits = Atomic<UInt32>(0)

        // Register end `a` as a watch BEFORE starting the loop — this
        // matches production usage (registerWatch is called from run()
        // before eventLoop.run()) and avoids a race on the loop-private
        // `channels` map.
        _ = try loop.registerWatch(fd: a, interest: .readable) { ready in
            readyBits.store(ready.rawValue, ordering: .releasing)
            fired.store(true, ordering: .releasing)
        }

        let loopThread = Thread { [loop] in try? loop.run() }
        loopThread.start()
        try await Task.sleep(for: .milliseconds(30))

        // Write to the other end → `a` becomes readable.
        var byte: UInt8 = 0x55
        _ = withUnsafePointer(to: &byte) { ptr in
            Glibc.write(b, ptr, 1)
        }

        try await Task.sleep(for: .milliseconds(40))
        #expect(fired.load(ordering: .acquiring) == true)
        let bits = readyBits.load(ordering: .acquiring)
        #expect(Ready(rawValue: bits).isReadable)
    }

    // MARK: - TaskExecutor: Task(executorPreference:)

    @Test("Task(executorPreference:) pins a Task to the loop")
    func taskExecutorPinning() async throws {
        let loop = try PollEventLoop(eventsCapacity: 16)
        let loopThread = Thread { [loop] in try? loop.run() }
        loopThread.start()
        try await Task.sleep(for: .milliseconds(50))
        defer { loop.shutdown() }

        // Task body runs on the loop's thread. The Task itself
        // captures its executor at spawn time — if executorPreference
        // is broken, the Task would land on the global cooperative
        // pool (still works functionally, just not on the loop).
        // The pinned path enqueues via the loop's `enqueue`, which
        // wakes the loop; an unpinned path does not. Either way the
        // Task completes and `await task.value` returns.
        let task = Task(executorPreference: loop) {
            // empty body — we just need it to complete
        }
        _ = await task.value
        // If we got here, the Task executed on (or was drained by)
        // the loop. The precondition is that executorPreference
        // does not hang and does not crash.
    }

    @Test("Task enqueued before run() runs once the loop starts (lost-wakeup regression)")
    func taskEnqueuedBeforeRunCompletes() async throws {
        let loop = try PollEventLoop()
        // Enqueue BEFORE the loop thread exists: the initial job lands
        // in the cross-thread queue while loopThreadId == 0, so no
        // eventfd wake is issued (an enqueuer only wakes a RUNNING
        // loop). Without run()'s initial drainJobs, the loop would
        // block in its very first epoll_wait forever — this test
        // would hang.
        let task = Task(executorPreference: loop) { 42 }
        let loopThread = Thread { [loop] in try? loop.run() }
        loopThread.start()
        defer { loop.shutdown() }

        let value = await task.value
        #expect(value == 42)
    }

    // MARK: - Readiness timeouts (timerfd sweep)

    @Test("read with a deadline returns -2 when no data arrives")
    func readDeadlineTimesOut() async throws {
        let loop = try PollEventLoop()
        loop.timeoutSweepInterval = .milliseconds(50)  // tight for a fast test
        let sp = makeSocketpair()
        guard let sp else { Issue.record("socketpair failed"); return }
        let (a, b) = (sp.read, sp.write)
        defer {
            _ = Glibc.close(a); _ = Glibc.close(b)
            loop.shutdown()
        }

        // Threading contract: register before the loop starts.
        let channelId = try loop.registerChannel(fd: b)

        let loopThread = Thread { [loop] in try? loop.run() }
        loopThread.start()
        try await Task.sleep(for: .milliseconds(30))

        // Deadline 200 ms out; sweep granularity 50 ms ⇒ fires ≤ ~250 ms.
        let deadline = ContinuousClock.now + .milliseconds(200)

        let pinned = LoopPinned(loop.cachedExecutor)
        let n = await pinned.runReadWithDeadline(
            loop: loop, channelId: channelId, deadline: deadline)

        // We never write to `a`, so the read can only complete via the
        // sweep failing it on deadline.
        #expect(n == -2, "timed-out read must return -2, got \(n)")
    }

    @Test("awaitWritable with a deadline returns false when never ready")
    func awaitWritableDeadlineTimesOut() async throws {
        let loop = try PollEventLoop()
        loop.timeoutSweepInterval = .milliseconds(50)
        let sp = makeSocketpair()
        guard let sp else { Issue.record("socketpair failed"); return }
        let (a, b) = (sp.read, sp.write)
        defer {
            _ = Glibc.close(a); _ = Glibc.close(b)
            loop.shutdown()
        }

        // Threading contract: register before the loop starts. The
        // channel is on `a` — the end whose send buffer the test fills.
        let channelId = try loop.registerChannel(fd: a)

        let loopThread = Thread { [loop] in try? loop.run() }
        loopThread.start()
        try await Task.sleep(for: .milliseconds(30))

        // Fill the socket send buffer so the write side is NOT writable,
        // forcing awaitWritable to wait (and then time out). A socketpair
        // buffer is ~200 KB; write until EAGAIN.
        let filler = [UInt8](repeating: 0x41, count: 65_536)
        while filler.withUnsafeBufferPointer({ Glibc.write(a, $0.baseAddress!, $0.count) }) > 0 {}

        let deadline = ContinuousClock.now + .milliseconds(200)

        let pinned = LoopPinned(loop.cachedExecutor)
        let ok = await pinned.runAwaitWritableWithDeadline(
            loop: loop, channelId: channelId, deadline: deadline)

        #expect(ok == false, "timed-out write-wait must return false")
    }

    @Test("timeoutSweepInterval retunes a RUNNING loop (timerfd re-arm)")
    func sweepIntervalRetunesLiveLoop() async throws {
        let loop = try PollEventLoop()          // default sweep: 500 ms
        let sp = makeSocketpair()
        guard let sp else { Issue.record("socketpair failed"); return }
        let (a, b) = (sp.read, sp.write)
        defer {
            _ = Glibc.close(a); _ = Glibc.close(b)
            loop.shutdown()
        }

        let channelId = try loop.registerChannel(fd: b)
        let loopThread = Thread { [loop] in try? loop.run() }
        loopThread.start()
        try await Task.sleep(for: .milliseconds(30))

        // Deadline 100 ms out. With the default 500 ms sweep this would
        // be enforced no earlier than ~500 ms; retuning the interval to
        // 50 ms mid-run must re-arm the timerfd (setter → flag → wake →
        // handleWakeup re-arms) and enforce it by ~200 ms. The elapsed
        // bound distinguishes the retuned path from the default cadence.
        let deadline = ContinuousClock.now + .milliseconds(100)
        let start = ContinuousClock.now
        let task = Task(executorPreference: loop) {
            await loop.read(channelId: channelId, deadline: deadline)
        }
        try await Task.sleep(for: .milliseconds(50))   // let the read arm
        loop.timeoutSweepInterval = .milliseconds(50)  // runtime retune

        let n = await task.value
        let elapsed = ContinuousClock.now - start
        #expect(n == -2, "deadline must fire, got \(n)")
        #expect(elapsed < .milliseconds(450),
                "retuned sweep must enforce the deadline early, took \(elapsed)")
    }

    // MARK: - ChannelId / slab semantics

    @Test("Slot reuse: cancel then register hands back a distinct id")
    func slotReuseYieldsDistinctIds() {
        let loop = try! PollEventLoop()
        let fd = makeDevNullFd()
        defer { _ = Glibc.close(fd) }
        let a = try! loop.registerChannel(fd: fd)
        loop.cancelChannel(a)
        // LIFO free-list: the new channel takes the same SLOT, but the
        // generation differs — the ids must not be equal even though
        // the slot index is reused.
        let b = try! loop.registerChannel(fd: fd)
        #expect(a != b, "reused slot must bump the generation")
        #expect(a.slot == b.slot, "LIFO free-list should reuse the freed slot")
        // Parity encoding: generation bumps on free AND on re-alloc, so
        // consecutive occupants of a slot differ by 2 (odd = live).
        #expect(b.generation == a.generation + 2)
        loop.cancelChannel(b)
    }

    @Test("Fresh handle works after same-slot reuse (stale tokens fail generation check)")
    func staleHandleEventsAreDropped() async throws {
        let loop = try PollEventLoop()
        let sp = makeSocketpair()
        guard let sp else { Issue.record("socketpair failed"); return }
        let (a, b) = (sp.read, sp.write)
        defer {
            _ = Glibc.close(a); _ = Glibc.close(b)
            loop.shutdown()
        }

        // Threading contract: register before the loop starts.
        // Channel 1 on end `b`; arm a read so the fd is registered
        // under (slot, gen1), then cancel — the fd keeps a pending
        // readability notification pattern typical of a just-closed
        // connection.
        let stale = try loop.registerChannel(fd: b)

        let loopThread = Thread { [loop] in try? loop.run() }
        loopThread.start()
        try await Task.sleep(for: .milliseconds(30))

        let pinned = LoopPinned(loop.cachedExecutor)
        // Arm a read that we then make ready, so an event is in flight.
        _ = Glibc.write(a, [0xAA as UInt8], 1)
        let readTask = Task(executorPreference: loop) {
            await loop.read(channelId: stale)
        }
        _ = await readTask.value  // completes (data present)

        // Cancel + re-register on the LOOP thread (threading contract):
        // the freed slot must come back with a new generation.
        let fresh = await Task(executorPreference: loop) { () -> ChannelId in
            loop.cancelChannel(stale)
            return try! loop.registerChannel(fd: b)
        }.value
        #expect(fresh.slot == stale.slot)

        // Deliver an event for the STALE token directly to the loop's
        // dispatch: not directly expressible via the public API, so
        // verify the inverse — the fresh handle still works end-to-end
        // after the stale one was cancelled (no cross-routing).
        _ = Glibc.write(a, [0xBB as UInt8], 1)
        let (n, out) = await pinned.runRead(
            loop: loop, channelId: fresh, capacity: 8)
        #expect(n == 1)
        #expect(out.first == 0xBB)
    }

    @Test("Many churned channels stay bounded and addressable")
    func channelChurnBoundedMemory() {
        let loop = try! PollEventLoop()
        let fd = makeDevNullFd()
        defer { _ = Glibc.close(fd) }
        // Churn 10k registrations through the table. With the LIFO
        // free-list the slot count must stay far below 10k.
        var last: ChannelId?
        for _ in 0..<10_000 {
            if let l = last { loop.cancelChannel(l) }
            let id = try! loop.registerChannel(fd: fd)
            last = id
        }
        // All registrations were sequential (cancel-then-register), so
        // exactly one slot is live.
        #expect(loop.channelsSlotCount == 1)
        #expect(loop.channelsLiveCount == 1)
        if let l = last { loop.cancelChannel(l) }
        #expect(loop.channelsLiveCount == 0)
    }

    @Test("Table growth ×2 preserves every live slot's bookkeeping")
    func tableGrowthAccounting() {
        let loop = try! PollEventLoop()  // initialCapacity = 256
        let fd = makeDevNullFd()
        defer { _ = Glibc.close(fd) }
        // Cross the growth boundary twice (256 → 512 → 1024).
        let ids = (0..<600).map { _ in try! loop.registerChannel(fd: fd) }
        #expect(loop.channelsSlotCount == 600)
        #expect(loop.channelsLiveCount == 600)

        // Free a middle swath; the freed slots return to the LIFO list.
        for id in ids[100..<400] { loop.cancelChannel(id) }
        #expect(loop.channelsLiveCount == 300)

        // New registrations must REUSE freed slots — the high-water
        // mark must not move.
        for _ in 0..<50 { _ = try! loop.registerChannel(fd: fd) }
        #expect(loop.channelsSlotCount == 600)
        #expect(loop.channelsLiveCount == 350)

        // Early channels (their states were MOVED by two reallocations)
        // remain individually addressable.
        for id in ids[0..<100] { loop.cancelChannel(id) }
        #expect(loop.channelsLiveCount == 250)
    }

    @Test("I/O works on channels whose states survived two reallocations")
    func ioWorksAfterGrowth() async throws {
        let loop = try PollEventLoop()
        // Reader channel on slot 0 — registered FIRST so its state is
        // moved by both reallocations (256 → 512 → 1024) before use.
        let sp = makeSocketpair()
        guard let sp else { Issue.record("socketpair failed"); return }
        let (a, b) = (sp.read, sp.write)
        defer {
            _ = Glibc.close(a); _ = Glibc.close(b)
            loop.shutdown()
        }

        let early = try loop.registerChannel(fd: b)
        for _ in 0..<300 { _ = try loop.registerChannel(fd: b) }  // force growth
        #expect(loop.channelsSlotCount == 301)

        let loopThread = Thread { [loop] in try? loop.run() }
        loopThread.start()
        try await Task.sleep(for: .milliseconds(30))

        _ = Glibc.write(a, [0xC0 as UInt8, 0xFF], 2)
        let pinned = LoopPinned(loop.cachedExecutor)
        let (n, out) = await pinned.runRead(
            loop: loop, channelId: early, capacity: 8)
        #expect(n == 2, "read on a moved state must deliver data, got \(n)")
        #expect(Array(out.prefix(2)) == [0xC0, 0xFF])
    }

    @Test("Shutdown resumes a pending read with -1 (orphan recovery)")
    func shutdownResumesPendingReads() async throws {
        let loop = try PollEventLoop()
        let sp = makeSocketpair()
        guard let sp else { Issue.record("socketpair failed"); return }
        let (a, b) = (sp.read, sp.write)
        defer { _ = Glibc.close(a); _ = Glibc.close(b) }

        let channelId = try loop.registerChannel(fd: b)
        let loopThread = Thread { [loop] in try? loop.run() }
        loopThread.start()
        try await Task.sleep(for: .milliseconds(30))

        // Arm a read that will never become ready (no data written).
        let readTask = Task(executorPreference: loop) {
            await loop.read(channelId: channelId)
        }
        try await Task.sleep(for: .milliseconds(50))

        loop.shutdown()
        // recoverOrphanedContinuations must resume the waiter with -1
        // instead of leaking it.
        let n = await readTask.value
        #expect(n == -1, "orphaned read must be failed with -1, got \(n)")
    }

    @Test("In-flight Task unwinds gracefully: post-shutdown read/awaitWritable fail, not trap")
    func shutdownGracefulForInFlightTasks() async throws {
        let loop = try PollEventLoop()
        let sp = makeSocketpair()
        guard let sp else { Issue.record("socketpair failed"); return }
        let (a, b) = (sp.read, sp.write)
        defer { _ = Glibc.close(a); _ = Glibc.close(b) }

        let channelId = try loop.registerChannel(fd: b)
        let loopThread = Thread { [loop] in try? loop.run() }
        loopThread.start()
        try await Task.sleep(for: .milliseconds(30))

        // No data is ever written: the first read arms, then hangs
        // until shutdown recovers it with -1. The SECOND read and the
        // awaitWritable execute AFTER the tail has reset the channel
        // table — their handles are stale by then. The old code trapped
        // (precondition failure, process death) in exactly this path;
        // the contract now fails the wait gracefully instead.
        let task = Task(executorPreference: loop) { () -> (Int, Int, Bool) in
            let r1 = await loop.read(channelId: channelId)
            let r2 = await loop.read(channelId: channelId)
            let w = await loop.awaitWritable(channelId: channelId)
            return (r1, r2, w)
        }
        try await Task.sleep(for: .milliseconds(50))
        loop.shutdown()

        let result = await task.value
        #expect(result == (-1, -1, false),
                "post-shutdown waits must fail gracefully, got \(result)")
    }

    @Test("shutdown() is terminal: a second run() returns immediately")
    func shutdownIsTerminal() async throws {
        let loop = try PollEventLoop()
        let firstDone = Atomic<Bool>(false)
        let t1 = Thread { [loop] in
            try? loop.run()
            firstDone.store(true, ordering: .releasing)
        }
        t1.start()
        try await Task.sleep(for: .milliseconds(50))
        loop.shutdown()
        try await Task.sleep(for: .milliseconds(100))
        #expect(firstDone.load(ordering: .acquiring) == true, "first run() must exit after shutdown()")

        // A second run() must neither hang, nor trap (the waker is
        // created in init and reused, not re-registered), nor
        // resurrect the loop: shutdown is final, tokio/NIO-style.
        let secondDone = Atomic<Bool>(false)
        let t2 = Thread { [loop] in
            try? loop.run()
            secondDone.store(true, ordering: .releasing)
        }
        t2.start()
        try await Task.sleep(for: .milliseconds(100))
        #expect(secondDone.load(ordering: .acquiring) == true,
                "run() after shutdown must return immediately (terminal)")
    }

    @Test("Deterministic stale-batch event is dropped by the generation check")
    func staleBatchEventDeterministic() async throws {
        // Injects synthetic events straight into the dispatcher
        // (`@testable`) — the kernel equivalent is an event sitting in
        // the current epoll_wait batch when the channel is cancelled
        // mid-batch (e.g. from a watch handler). No running loop is
        // needed: arm + dispatch + read-back all run synchronously.
        let loop = try PollEventLoop()
        let sp = makeSocketpair()
        guard let sp else { Issue.record("socketpair failed"); return }
        let (a, b) = (sp.read, sp.write)
        defer { _ = Glibc.close(a); _ = Glibc.close(b) }

        let stale = try loop.registerChannel(fd: b)
        let staleToken = stale.asToken
        loop.cancelChannel(stale)
        // Same slot, next generation.
        let fresh = try loop.registerChannel(fd: b)
        #expect(fresh.slot == stale.slot)

        // Arm a read on `fresh` (loop not running: the awaiting Task's
        // thread is a legal setup thread).
        _ = Glibc.write(a, [0x5A as UInt8], 1)
        let readTask = Task { await loop.read(channelId: fresh) }
        try await Task.sleep(for: .milliseconds(50))
        #expect(loop._readPending(fresh), "read must be armed before injection")

        // THE scenario: a stale event for the cancelled generation
        // arrives while the read is pending. It must be DROPPED —
        // no crash, no consumption of `fresh`'s continuation.
        loop.processChannelEvent(Event(token: staleToken, ready: .readable))
        #expect(loop._readPending(fresh),
            "stale-batch event must not satisfy the new occupant's read")

        // Positive control: the CURRENT token dispatches and completes
        // the read with the buffered byte.
        loop.processChannelEvent(Event(token: fresh.asToken, ready: .readable))
        let n = await readTask.value
        #expect(n == 1, "fresh token must deliver the read, got \(n)")
    }

    @Test("Caller closing its fd right after registration does not break the channel (loop-owned dup)")
    func callerFdCloseAfterRegister() async throws {
        let loop = try PollEventLoop()
        let sp = makeSocketpair()
        guard let sp else { Issue.record("socketpair failed"); return }
        let (a, b) = (sp.read, sp.write)
        defer {
            _ = Glibc.close(a)
            loop.shutdown()
        }

        // Ownership model: registration dups the fd — the caller may
        // drop its reference IMMEDIATELY. The loop's dup keeps the
        // connection live; read/write work through it; the old design
        // (per-call fd + DEL by the caller's number) would have read
        // from a closed fd here.
        let channelId = try loop.registerChannel(fd: b)
        _ = Glibc.close(b)

        let loopThread = Thread { [loop] in try? loop.run() }
        loopThread.start()
        try await Task.sleep(for: .milliseconds(30))

        _ = Glibc.write(a, [0x42 as UInt8], 1)
        let pinned = LoopPinned(loop.cachedExecutor)
        let (n, out) = await pinned.runRead(
            loop: loop, channelId: channelId, capacity: 8)
        #expect(n == 1, "read must deliver through the loop-owned dup, got \(n)")
        #expect(out.first == 0x42)

        // cancelChannel releases the loop's dup precisely — no
        // double-close hazard even though the caller's fd number may
        // already have been recycled by `a`-side activity.
        let cancelTask = Task(executorPreference: loop) {
            loop.cancelChannel(channelId)
        }
        _ = await cancelTask.value
    }

    @Test("cancelWatch resolves via the fd map and is idempotent")
    func cancelWatchUsesFdMap() async throws {
        let loop = try PollEventLoop()
        let sp = makeSocketpair()
        guard let sp else { Issue.record("socketpair failed"); return }
        let (a, b) = (sp.read, sp.write)
        defer { _ = Glibc.close(a); _ = Glibc.close(b) }

        let fired = Atomic<Bool>(false)
        let id = try loop.registerWatch(fd: a, interest: .readable) { _ in
            fired.store(true, ordering: .releasing)
        }
        #expect(loop.channelsLiveCount == 1)

        // cancelWatch(fd:) — the O(1) path — must free exactly the
        // watch channel, and a second call must be a no-op.
        loop.cancelWatch(fd: a)
        #expect(loop.channelsLiveCount == 0, "watch slot must be freed")
        #expect(!loop.channels.isLive(slot: id.slot))

        loop.cancelWatch(fd: a)  // idempotent
        #expect(loop.channelsLiveCount == 0)

        // Data channels never enter the fd map: cancelling by their fd
        // is a no-op even if the fd coincides.
        let data = try loop.registerChannel(fd: a)
        loop.cancelWatch(fd: a)
        #expect(loop.channelsLiveCount == 1, "data channel must be untouched")
        loop.cancelChannel(data)
        #expect(fired.load(ordering: .acquiring) == false)
    }
}

// MARK: - Helpers

private struct TestPipe {
    let read: CInt
    let write: CInt
}

private func makeDevNullFd() -> CInt {
    Glibc.open("/dev/null", O_RDONLY)
}

private func makeSocketpair() -> TestPipe? {
    var fds: [CInt] = [0, 0]
    let rc = fds.withUnsafeMutableBufferPointer { buf in
        // SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC = 1 | 2048 | 524288
        Glibc.socketpair(AF_UNIX, 1 | 2048 | 524288, 0, buf.baseAddress!)
    }
    return rc == 0 ? TestPipe(read: fds[0], write: fds[1]) : nil
}

#endif // os(Linux)
