//===----------------------------------------------------------------------===//
//
//  PollEventLoopTests.swift
//  PulsarTests
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

        /// Read with an absolute deadline and report the recorded
        /// errno alongside — validates the lastErrno contract.
        func runReadWithDeadline(
            loop: PollEventLoop, channelId: ChannelId,
            deadline: ContinuousClock.Instant
        ) async -> (Int, CInt) {
            let n = await loop.read(channelId: channelId, deadline: deadline)
            return (n, loop.lastErrno(channelId: channelId))
        }

        /// Cancellable read — errno read back from the SAME task right
        /// after the cancelled wait returns.
        func runCancellableReadErrno(
            loop: PollEventLoop, channelId: ChannelId
        ) async -> (Int, CInt) {
            let n = await loop.read(channelId: channelId, cancellable: true)
            return (n, loop.lastErrno(channelId: channelId))
        }

        /// Write `data` with an absolute deadline; returns bytes
        /// written and the recorded errno.
        func runWriteWithDeadline(
            loop: PollEventLoop, channelId: ChannelId, data: [UInt8],
            deadline: ContinuousClock.Instant
        ) async -> (Int, CInt) {
            let buf = UnsafeMutableRawBufferPointer.allocate(
                byteCount: data.count, alignment: 8)
            defer { buf.deallocate() }
            data.withUnsafeBufferPointer { src in
                buf.copyMemory(from: UnsafeRawBufferPointer(src))
            }
            let n = await loop.write(
                channelId: channelId, from: UnsafeRawBufferPointer(buf),
                deadline: deadline)
            return (n, loop.lastErrno(channelId: channelId))
        }

        /// Plain write (no deadline) — for the EPIPE path.
        func runWrite(
            loop: PollEventLoop, channelId: ChannelId, data: [UInt8]
        ) async -> (Int, CInt) {
            let buf = UnsafeMutableRawBufferPointer.allocate(
                byteCount: data.count, alignment: 8)
            defer { buf.deallocate() }
            data.withUnsafeBufferPointer { src in
                buf.copyMemory(from: UnsafeRawBufferPointer(src))
            }
            let n = await loop.write(
                channelId: channelId, from: UnsafeRawBufferPointer(buf))
            return (n, loop.lastErrno(channelId: channelId))
        }

        /// Loop-read until `total` bytes accumulate (or EOF/error) —
        /// exercises multi-read reassembly across readCapacity
        /// boundaries. Returns the payload and the final errno.
        func runBulkRead(
            loop: PollEventLoop, channelId: ChannelId, total: Int
        ) async -> ([UInt8], CInt) {
            var out = [UInt8]()
            out.reserveCapacity(total)
            while out.count < total {
                let n = await loop.read(channelId: channelId)
                if n <= 0 { break }
                let view = loop.getReadView(channelId: channelId, count: n)
                out.append(contentsOf: view[0..<n])
            }
            return (out, loop.lastErrno(channelId: channelId))
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
        let (n, err) = await pinned.runReadWithDeadline(
            loop: loop, channelId: channelId, deadline: deadline)

        // We never write to `a`, so the read can only complete via the
        // sweep failing it on deadline.
        #expect(n == -2, "timed-out read must return -2, got \(n)")
        #expect(err == ETIMEDOUT,
                "timed-out read must record ETIMEDOUT, got \(err)")
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
        // All-on-the-loop-thread discipline (same as the
        // spurious-readiness test): the arm, the injected STALE event
        // and the positive control run as jobs on the loop's serial
        // executor, and the probe handshakes via Task.sleep until the
        // read is armed. The previous version armed via an unpinned
        // Task and injected from the test thread — a theoretical
        // cross-thread table race (TSan-clean by luck, not by
        // construction).
        let loop = try PollEventLoop()
        let sp = makeSocketpair()
        guard let sp else { Issue.record("socketpair failed"); return }
        let (a, b) = (sp.read, sp.write)
        defer {
            _ = Glibc.close(a); _ = Glibc.close(b)
            loop.shutdown()
        }

        // Setup regime (pre-run): the stale channel is cancelled and
        // the freed slot immediately reused — same slot, next
        // generation. (ChannelId is a plain value; reading `.slot`
        // needs no table access.)
        let stale = try loop.registerChannel(fd: b)
        let staleToken = stale.asToken
        loop.cancelChannel(stale)
        let fresh = try loop.registerChannel(fd: b)
        #expect(fresh.slot == stale.slot)

        let loopThread = Thread { [loop] in try? loop.run() }
        loopThread.start()
        try await Task.sleep(for: .milliseconds(30))

        // Task 1: arms a read on `fresh`; parks until the probe's
        // positive-control dispatch delivers the data byte.
        let readTask = Task(executorPreference: loop) { () -> Int in
            await loop.read(channelId: fresh)
        }
        // Task 2: handshake until armed, then THE scenario — an event
        // for the STALE token (the kernel analogue: an entry sitting
        // in the current epoll_wait batch when the channel is
        // cancelled mid-batch). The data byte is written BEFORE the
        // stale injection on purpose: if the generation check were
        // ever broken and the stale event satisfied `fresh`'s
        // continuation, the byte is right there to consume — making
        // the corruption observable as a pendingRead that went false
        // (and a read completed by the WRONG dispatch).
        let probeTask = Task(executorPreference: loop) { () -> [Bool]? in
            var armed = false
            for _ in 0..<200 {  // bounded: ~1 s ceiling, no hang on regression
                if loop._readPending(fresh) { armed = true; break }
                try? await Task.sleep(for: .milliseconds(5))
            }
            guard armed else { return nil }
            var trace: [Bool] = [true]
            _ = Glibc.write(a, [0x5A as UInt8], 1)
            loop.processChannelEvent(
                Event(token: staleToken, ready: .readable))
            trace.append(loop._readPending(fresh))
            // Positive control: the CURRENT token dispatches and
            // completes the read with the buffered byte.
            loop.processChannelEvent(
                Event(token: fresh.asToken, ready: .readable))
            trace.append(loop._readPending(fresh))
            return trace
        }

        let trace = await probeTask.value
        // Fail fast on a broken arm (the read task is parked without a
        // deadline; the deferred shutdown recovers it with -1).
        guard let trace else {
            Issue.record("read never armed within 1 s — ordering broken")
            return
        }
        let n = await readTask.value

        #expect(trace[1] == true,
                "stale-batch event must not satisfy the new occupant's read")
        #expect(trace[2] == false,
                "current-token dispatch must consume the read")
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

    // MARK: - Task cancellation propagation (opt-in `cancellable: true`)

    @Test("Cancelled cancellable read fails promptly instead of hanging")
    func cancelledCancellableReadFailsPromptly() async throws {
        let loop = try PollEventLoop()
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

        // No data, NO deadline: without cancellation support this read
        // would hang forever. Cancelled mid-wait it must return -1.
        let task = Task(executorPreference: loop) { () -> Int in
            await loop.read(channelId: channelId, cancellable: true)
        }
        try await Task.sleep(for: .milliseconds(50))   // let it arm
        task.cancel()

        let n = await task.value
        #expect(n == -1, "cancelled read must fail with -1, got \(n)")

        // Cancelling a wait must NOT tear the channel: a fresh read
        // delivers subsequently-arriving data.
        _ = Glibc.write(a, [0x7F as UInt8], 1)
        let pinned = LoopPinned(loop.cachedExecutor)
        let (n2, out) = await pinned.runRead(
            loop: loop, channelId: channelId, capacity: 8)
        #expect(n2 == 1, "channel must remain usable after wait cancellation")
        #expect(out.first == 0x7F)
    }

    @Test("Task cancelled before its read runs fails fast (pre-arm)")
    func cancelledBeforeArmFailsFast() async throws {
        let loop = try PollEventLoop()
        let sp = makeSocketpair()
        guard let sp else { Issue.record("socketpair failed"); return }
        let (a, b) = (sp.read, sp.write)
        defer {
            _ = Glibc.close(a); _ = Glibc.close(b)
            loop.shutdown()
        }

        let channelId = try loop.registerChannel(fd: b)
        // Spawn BEFORE the loop runs: the job sits in the pool queue,
        // then cancel it, then start the loop — the read() body first
        // observes Task.isCancelled and never arms.
        let task = Task(executorPreference: loop) {
            await loop.read(channelId: channelId, deadline: nil,
                            cancellable: true)
        }
        task.cancel()
        let loopThread = Thread { [loop] in try? loop.run() }
        loopThread.start()

        let n = await task.value
        #expect(n == -1, "pre-armed cancel must fast-fail with -1, got \(n)")
    }

    @Test("Cancel landing while the arm request is still in the queue (box order)")
    func cancelBeatsBoxedArmRequest() async throws {
        let loop = try PollEventLoop()
        let sp = makeSocketpair()
        guard let sp else { Issue.record("socketpair failed"); return }
        let (a, b) = (sp.read, sp.write)
        defer {
            _ = Glibc.close(a); _ = Glibc.close(b)
            loop.shutdown()
        }

        let channelId = try loop.registerChannel(fd: b)
        // A NON-loop-pinned task (pre-run setup regime): its cancellable
        // read runs on the global pool and parks the ARM REQUEST in the
        // loop's queue while the loop is not yet running — the exact
        // interleaving where a naive implementation would arm first and
        // hang. Cancel lands after the arm request; the single-box
        // happens-before chain must make the refusal see the flag.
        let task = Task {
            await loop.read(channelId: channelId, cancellable: true)
        }
        try await Task.sleep(for: .milliseconds(50))   // arm request parked
        task.cancel()
        try await Task.sleep(for: .milliseconds(30))   // cancel request parked

        let loopThread = Thread { [loop] in try? loop.run() }
        loopThread.start()

        let n = await task.value
        #expect(n == -1, "boxed arm must be refused after cancel, got \(n)")
    }

    @Test("Cancelled cancellable awaitWritable fails promptly")
    func cancelledCancellableAwaitWritableFails() async throws {
        let loop = try PollEventLoop()
        let sp = makeSocketpair()
        guard let sp else { Issue.record("socketpair failed"); return }
        let (a, b) = (sp.read, sp.write)
        defer {
            _ = Glibc.close(a); _ = Glibc.close(b)
            loop.shutdown()
        }

        // Fill a's send buffer so awaitWritable genuinely waits.
        let channelId = try loop.registerChannel(fd: a)
        let loopThread = Thread { [loop] in try? loop.run() }
        loopThread.start()
        try await Task.sleep(for: .milliseconds(30))
        let filler = [UInt8](repeating: 0x41, count: 65_536)
        while filler.withUnsafeBufferPointer({ Glibc.write(a, $0.baseAddress!, $0.count) }) > 0 {}

        let task = Task(executorPreference: loop) { () -> Bool in
            await loop.awaitWritable(channelId: channelId, cancellable: true)
        }
        try await Task.sleep(for: .milliseconds(50))
        task.cancel()

        let ok = await task.value
        #expect(ok == false, "cancelled write-wait must fail with false")
    }

    @Test("Non-cancellable read on a cancelled task also fails fast")
    func nonCancellableReadFastFailsWhenCancelled() async throws {
        let loop = try PollEventLoop()
        let sp = makeSocketpair()
        guard let sp else { Issue.record("socketpair failed"); return }
        let (a, b) = (sp.read, sp.write)
        defer {
            _ = Glibc.close(a); _ = Glibc.close(b)
            loop.shutdown()
        }

        let channelId = try loop.registerChannel(fd: b)
        let task = Task(executorPreference: loop) {
            await loop.read(channelId: channelId)
        }
        task.cancel()
        let loopThread = Thread { [loop] in try? loop.run() }
        loopThread.start()

        let n = await task.value
        #expect(n == -1, "cancelled task's plain read must fast-fail, got \(n)")
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

    // MARK: - write deadline / errno discrimination / SIGPIPE

    @Test("write with a deadline bails out of a stalled peer (write-stall defence)")
    func writeDeadlineTimesOut() async throws {
        let loop = try PollEventLoop()
        loop.timeoutSweepInterval = .milliseconds(50)
        let sp = makeSocketpair()
        guard let sp else { Issue.record("socketpair failed"); return }
        let (a, b) = (sp.read, sp.write)
        defer {
            _ = Glibc.close(a); _ = Glibc.close(b)
            loop.shutdown()
        }

        // Channel on `a`; fill a's send buffer so writes stall.
        let channelId = try loop.registerChannel(fd: a)
        let loopThread = Thread { [loop] in try? loop.run() }
        loopThread.start()
        try await Task.sleep(for: .milliseconds(30))
        let filler = [UInt8](repeating: 0x41, count: 65_536)
        while filler.withUnsafeBufferPointer({ Glibc.write(a, $0.baseAddress!, $0.count) }) > 0 {}

        let payload = [UInt8](repeating: 0x42, count: 1 << 20)  // 1 MiB
        let deadline = ContinuousClock.now + .milliseconds(200)
        let start = ContinuousClock.now

        let pinned = LoopPinned(loop.cachedExecutor)
        let (n, err) = await pinned.runWriteWithDeadline(
            loop: loop, channelId: channelId, data: payload,
            deadline: deadline)

        #expect(n < payload.count,
                "a stalled peer cannot accept the full payload (wrote \(n))")
        #expect(err == ETIMEDOUT,
                "stalled write must record ETIMEDOUT, got \(err)")
        let elapsed = ContinuousClock.now - start
        #expect(elapsed >= .milliseconds(150),
                "deadline must fire around 200ms, took \(elapsed)")
    }

    @Test("write to a closed peer yields EPIPE, not SIGPIPE (MSG_NOSIGNAL)")
    func writeToClosedPeerYieldsEPIPE() async throws {
        let loop = try PollEventLoop()
        let sp = makeSocketpair()
        guard let sp else { Issue.record("socketpair failed"); return }
        let (a, b) = (sp.read, sp.write)
        defer {
            _ = Glibc.close(a)
            loop.shutdown()
        }

        // Channel on `a`; the peer (`b`) is closed AFTER registration.
        // The loop's write goes through send(MSG_NOSIGNAL): EPIPE comes
        // back as -1 instead of killing the process with SIGPIPE. If
        // this regression breaks, the test process dies before any
        // expectation runs — loud enough.
        let channelId = try loop.registerChannel(fd: a)
        let loopThread = Thread { [loop] in try? loop.run() }
        loopThread.start()
        try await Task.sleep(for: .milliseconds(30))
        _ = Glibc.close(b)

        let pinned = LoopPinned(loop.cachedExecutor)
        let (n, err) = await pinned.runWrite(
            loop: loop, channelId: channelId,
            data: [UInt8](repeating: 0x43, count: 1024))

        #expect(n == 0, "nothing can be written to a dead peer, got \(n)")
        #expect(err == EPIPE, "dead peer must record EPIPE, got \(err)")
    }

    @Test("Bulk payload reassembles across readCapacity boundaries")
    func bulkPayloadReassembles() async throws {
        let loop = try PollEventLoop()
        let sp = makeSocketpair()
        guard let sp else { Issue.record("socketpair failed"); return }
        let (a, b) = (sp.read, sp.write)
        defer {
            _ = Glibc.close(a); _ = Glibc.close(b)
            loop.shutdown()
        }

        let channelId = try loop.registerChannel(fd: b)  // 8 KiB capacity
        // 40 KB — five reads at the default capacity, with a short
        // final read. The socket buffer (~200 KB) holds it all.
        let payload = (0..<40_000).map { UInt8(truncatingIfNeeded: $0 &* 31 &+ 7) }
        _ = payload.withUnsafeBufferPointer { ptr in
            Glibc.write(a, ptr.baseAddress!, payload.count)
        }

        let loopThread = Thread { [loop] in try? loop.run() }
        loopThread.start()
        try await Task.sleep(for: .milliseconds(30))

        let pinned = LoopPinned(loop.cachedExecutor)
        let (out, err) = await pinned.runBulkRead(
            loop: loop, channelId: channelId, total: payload.count)

        #expect(out.count == payload.count,
                "must reassemble \(payload.count) bytes, got \(out.count)")
        #expect(out == payload, "payload must round-trip byte-exact")
        #expect(err == 0, "successful reads record errno 0, got \(err)")
    }

    @Test("Cancelled cancellable read records ECANCELED")
    func cancelledReadRecordsECANCELED() async throws {
        let loop = try PollEventLoop()
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

        // No data, no deadline: the read parks until cancelled.
        let pinned = LoopPinned(loop.cachedExecutor)
        let task = Task(executorPreference: loop) { () -> (Int, CInt) in
            await pinned.runCancellableReadErrno(
                loop: loop, channelId: channelId)
        }
        try await Task.sleep(for: .milliseconds(50))
        task.cancel()

        let (n, err) = await task.value
        #expect(n == -1)
        #expect(err == ECANCELED, "cancelled wait must record ECANCELED, got \(err)")
    }

    // MARK: - Spurious readiness

    @Test("Spurious readiness (EAGAIN) keeps the wait armed instead of failing it")
    func spuriousReadinessKeepsWaitArmed() async throws {
        // Everything runs ON the loop thread: the arm (task 1) and
        // the injected event (task 2) touch the channel table only
        // from the loop's serial executor, so there is no cross-thread
        // table access anywhere. (Injecting from the TEST thread into
        // an unpinned Task's arm — the pattern this test originally
        // used — is a data race that TSan correctly flags.) The probe
        // below does NOT rely on Task spawn order: `Task(executor-
        // Preference:)` start-up hops make initial-job ordering
        // non-deterministic, so the probe HANDSHAKES instead — it
        // sleeps (yielding the executor) until the read is armed.
        let loop = try PollEventLoop()
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

        // Task 1: arms the read, parks (the socket has no data).
        let readTask = Task(executorPreference: loop) { () -> Int in
            await loop.read(channelId: channelId)
        }
        // Task 2: waits (ON the loop thread) until task 1's read is
        // armed — `Task.sleep` suspends the probe, handing the serial
        // executor to task 1 regardless of which initial job was
        // enqueued first; the polling reads of `_readPending` are
        // then same-thread with the arm, race-free by construction.
        // Then: inject the spurious event, assert the wait SURVIVED,
        // deliver real data, assert the wait completed.
        let probeTask = Task(executorPreference: loop) { () -> [Bool]? in
            var armed = false
            for _ in 0..<200 {  // bounded: ~1 s ceiling, no hang on regression
                if loop._readPending(channelId) { armed = true; break }
                try? await Task.sleep(for: .milliseconds(5))
            }
            guard armed else { return nil }
            var trace: [Bool] = [true]
            // The spurious event: readable, but the socket has no
            // data — read(2) returns EAGAIN. The old code resumed the
            // waiter with -1 (connection torn down); the fix keeps
            // the continuation armed.
            loop.processChannelEvent(
                Event(token: channelId.asToken, ready: .readable))
            trace.append(loop._readPending(channelId))
            // Positive control: real data + real dispatch must consume
            // the still-armed wait. (The loop thread is inside this
            // job — epoll cannot interleave; a natural readiness event
            // would run on this same thread, serialized, and find the
            // continuation already claimed.)
            var byte: UInt8 = 0x9B
            _ = withUnsafePointer(to: &byte) { Glibc.write(a, $0, 1) }
            loop.processChannelEvent(
                Event(token: channelId.asToken, ready: .readable))
            trace.append(loop._readPending(channelId))
            return trace
        }

        let trace = await probeTask.value
        // Fail FAST on a broken arm: the read task is parked without a
        // deadline, so awaiting it here would hang the suite forever.
        // (Returning is safe: the deferred loop.shutdown() recovers
        // the parked read with -1.)
        guard let trace else {
            Issue.record("read never armed within 1 s — ordering broken")
            return
        }
        let n = await readTask.value

        #expect(trace[1] == true,
                "spurious readiness must NOT fail or satisfy the wait")
        #expect(trace[2] == false, "real dispatch must consume the armed wait")
        #expect(n == 1, "armed wait must complete on real data, got \(n)")
    }

    // MARK: - Isolation regimes

    @Test("checkIsolated passes in the pre-run setup regime (any thread)")
    func checkIsolatedAllowsSetupRegime() async throws {
        let loop = try PollEventLoop()
        // loopThreadId == 0: pre-run setup — the same regime
        // `precondLoopThread` blesses for registerChannel et al. The
        // old checkIsolated trapped here (regime inconsistency).
        loop.checkIsolated()  // must NOT trap
        loop.shutdown()
    }

    // MARK: - Optimistic read fast path

    @Test("Optimistic read: pre-buffered data answers without touching epoll")
    func optimisticReadSkipsRegistration() async throws {
        let loop = try PollEventLoop()
        let sp = makeSocketpair()
        guard let sp else { Issue.record("socketpair failed"); return }
        let (a, b) = (sp.read, sp.write)
        defer {
            _ = Glibc.close(a); _ = Glibc.close(b)
            loop.shutdown()
        }

        let channelId = try loop.registerChannel(fd: b)
        // Data buffered BEFORE the read: the fast path must answer
        // immediately — and `registered == false` proves NO epoll_ctl
        // was ever issued (the channel's first registration would only
        // happen at the first arm).
        let payload: [UInt8] = [0x11, 0x22, 0x33]
        _ = payload.withUnsafeBufferPointer {
            Glibc.write(a, $0.baseAddress!, payload.count)
        }

        let loopThread = Thread { [loop] in try? loop.run() }
        loopThread.start()
        try await Task.sleep(for: .milliseconds(30))

        let pinned = LoopPinned(loop.cachedExecutor)
        let (n, out) = await pinned.runRead(
            loop: loop, channelId: channelId, capacity: 8)
        #expect(n == 3, "fast path must deliver the buffered bytes, got \(n)")
        #expect(Array(out.prefix(3)) == [0x11, 0x22, 0x33])

        let registered = await Task(executorPreference: loop) { () -> Bool in
            loop.channels.pointer(slot: channelId.slot).pointee.registered
        }.value
        #expect(registered == false,
                "pre-buffered data must not cost an epoll_ctl (registration)")
    }

    @Test("Cooperative budget: fast path yields to the arm path after 64 answers")
    func optimisticReadCooperativeYield() async throws {
        let loop = try PollEventLoop()
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

        // 70 read/write round-trips, data always pre-buffered: reads
        // #1..#64 answer optimistically (no registration); read #65
        // hits the yield threshold → the arm path runs → the fd gets
        // registered for the first time → the immediate event
        // completes the read. `registered` turning true mid-test is
        // the observable proof the cooperative yield ENGAGED; it
        // staying false through read #10 proves the fast path is
        // actually fast (zero epoll_ctl).
        let observed = await Task(executorPreference: loop) { () -> [Bool] in
            var trace: [Bool] = []
            var byte: UInt8 = 0x77
            for i in 0..<70 {
                _ = withUnsafePointer(to: &byte) { Glibc.write(a, $0, 1) }
                let n = await loop.read(channelId: channelId)
                precondition(n == 1, "read \(i) must deliver the byte")
                if i == 9 || i == 69 {
                    trace.append(
                        loop.channels.pointer(slot: channelId.slot)
                            .pointee.registered)
                }
            }
            return trace
        }.value

        #expect(observed[0] == false,
                "the first 64 reads must stay off epoll (fast path pure)")
        #expect(observed[1] == true,
                "read #65 must force an arm-yield (registration created)")
    }

    // MARK: - Stress: concurrent echo with churn and cancellation

    @Test("Stress: concurrent round-trips with slot churn and cancellations")
    func stressEchoWithChurnAndCancellation() async throws {
        let loop = try PollEventLoop()
        let K = 16, J = 25
        // Everything registers BEFORE the loop starts (setup regime —
        // the channel table is loop-thread state once run() begins).
        struct Pair {
            let fdA: CInt, fdB: CInt          // caller fds, kept open
            let writer: ChannelId             // channel on fdA (task writes)
            let reader: ChannelId             // channel on fdB (task reads)
        }
        var pairs: [Pair] = []
        for _ in 0..<K {
            guard let sp = makeSocketpair() else {
                Issue.record("socketpair failed"); return
            }
            pairs.append(Pair(
                fdA: sp.read, fdB: sp.write,
                writer: try loop.registerChannel(fd: sp.read),
                reader: try loop.registerChannel(fd: sp.write)))
        }
        var cancelPairs: [(TestPipe, ChannelId)] = []
        for _ in 0..<4 {
            guard let sp = makeSocketpair() else { break }
            cancelPairs.append((sp, try loop.registerChannel(fd: sp.write)))
        }
        defer {
            for p in pairs {
                _ = Glibc.close(p.fdA); _ = Glibc.close(p.fdB)
            }
            for (sp, _) in cancelPairs {
                _ = Glibc.close(sp.read); _ = Glibc.close(sp.write)
            }
            loop.shutdown()
        }

        let loopThread = Thread { [loop] in try? loop.run() }
        loopThread.start()
        try await Task.sleep(for: .milliseconds(30))

        // Echo tasks: write 256 B through the pair's writer channel
        // (data surfaces at the other end), read it back through the
        // reader channel, verify. Every 5th iteration the reader
        // channel is cancelled and re-registered through the still-
        // open caller fd — driving the LIFO free-list and generation
        // checks under live traffic. (Churn happens between
        // iterations, when no data is in flight.)
        let echoTasks: [Task<Bool, Never>] = pairs.map { pair in
            Task(executorPreference: loop) { () -> Bool in
                var readerCh = pair.reader
                defer {
                    loop.cancelChannel(readerCh)
                    loop.cancelChannel(pair.writer)
                }
                for i in 0..<J {
                    if i > 0 && i % 5 == 0 {
                        loop.cancelChannel(readerCh)
                        guard let fresh = try? loop.registerChannel(fd: pair.fdB)
                        else { return false }
                        readerCh = fresh
                    }
                    let payload = [UInt8](
                        repeating: UInt8(truncatingIfNeeded: i &* 31 &+ 7),
                        count: 256)
                    let buf = UnsafeMutableRawBufferPointer.allocate(
                        byteCount: 256, alignment: 8)
                    defer { buf.deallocate() }
                    payload.withUnsafeBufferPointer { src in
                        buf.copyMemory(from: UnsafeRawBufferPointer(src))
                    }
                    let wn = await loop.write(
                        channelId: pair.writer, from: UnsafeRawBufferPointer(buf))
                    if wn != 256 { return false }
                    var got = 0
                    while got < 256 {
                        let n = await loop.read(channelId: readerCh)
                        if n <= 0 { return false }
                        let view = loop.getReadView(channelId: readerCh, count: n)
                        for j in 0..<n {
                            if view[j] != payload[got + j] { return false }
                        }
                        got += n
                    }
                }
                return true
            }
        }

        // Meanwhile: the four cancellable reads on never-readable
        // channels get cancelled mid-wait — they must fail with
        // -1/ECANCELED without disturbing the echo traffic.
        let cancelTasks: [Task<(Int, CInt), Never>] = cancelPairs.map { pair in
            let ch = pair.1
            return Task(executorPreference: loop) { () -> (Int, CInt) in
                let n = await loop.read(channelId: ch, cancellable: true)
                return (n, loop.lastErrno(channelId: ch))
            }
        }
        try await Task.sleep(for: .milliseconds(50))
        for t in cancelTasks { t.cancel() }

        for (i, t) in echoTasks.enumerated() {
            let ok = await t.value
            #expect(ok, "echo task \(i) must complete all \(J) round-trips")
        }
        for t in cancelTasks {
            let (n, err) = await t.value
            #expect(n == -1, "cancelled stress read must fail with -1, got \(n)")
            #expect(err == ECANCELED, "got \(err)")
        }

        // Echo tasks cancelled BOTH their channels in their defers →
        // only the four cancelled-read channels remain (a cancelled
        // WAIT is not a torn-down CHANNEL — shutdown recovers them).
        #expect(loop.channelsLiveCount == cancelPairs.count,
                "expected \(cancelPairs.count) live channels, got \(loop.channelsLiveCount)")
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
