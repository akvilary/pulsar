//===----------------------------------------------------------------------===//
//
//  PulsarBench — A/B benchmark harness for the channel-table hot path.
//
//  Phases:
//   1. churn      — registerChannel + cancelChannel throughput (table
//                   alloc/lookup/free; the 8 KB read-buffer malloc
//                   dominates but is identical across versions — a
//                   sanity metric).
//   2. echo       — sustained readiness throughput: a writer thread
//                   floods K blocking socketpair ends with 8 KB chunks
//                   for T seconds; K loop-pinned reader Tasks drain the
//                   non-blocking ends via `loop.read`. Measures the
//                   full dispatch path (epoll_wait → token → state →
//                   read syscall → re-arm) end to end.
//
//  The source compiles verbatim against BOTH the dictionary version
//  (UInt32 channel ids) and the slab version (ChannelId): every id is
//  obtained from `registerChannel()` and passed back opaquely, so type
//  inference hides the difference.
//
//===----------------------------------------------------------------------===//

#if os(Linux)

import Foundation
import Pulsar
import Synchronization

#if canImport(Glibc)
import Glibc
#endif

let totalBytes = Atomic<Int64>(0)
let readCalls = Atomic<Int64>(0)

func elapsedSeconds(since start: ContinuousClock.Instant) -> Double {
    let d = ContinuousClock.now - start
    return Double(d.components.seconds) + Double(d.components.attoseconds) / 1e18
}

// MARK: - Phase 1: churn

func phaseChurn() -> Double {
    let loop = try! PollEventLoop()
    let N = 200_000
    let t0 = ContinuousClock.now
    for _ in 0..<N {
        let id = loop.registerChannel()
        loop.cancelChannel(id)
    }
    let dt = elapsedSeconds(since: t0)
    return Double(N) / dt
}

// MARK: - Phase 2: sustained echo

func makePair() -> (blocking: CInt, nonblocking: CInt) {
    var fds: [CInt] = [0, 0]
        let rc = fds.withUnsafeMutableBufferPointer { buf in
        // Both ends blocking + CLOEXEC; b is flipped to non-blocking
        // below via fcntl (socketpair applies one type to both ends).
        Glibc.socketpair(AF_UNIX, Int32(SOCK_STREAM.rawValue) | Int32(524288), 0, buf.baseAddress!)
    }
    precondition(rc == 0, "socketpair failed: errno=\(errno)")
    let fl = Glibc.fcntl(fds[1], F_GETFL, 0)
    _ = Glibc.fcntl(fds[1], F_SETFL, fl | Int32(O_NONBLOCK))
    return (fds[0], fds[1])
}

func phaseEcho(connections: Int, seconds: Double) async {
    let loop = try! PollEventLoop()
    let K = connections
    let pairs: [(CInt, CInt)] = (0..<K).map { _ in makePair() }
    // Id type is inferred: UInt32 (dict) or ChannelId (slab).
    let ids = (0..<K).map { _ in loop.registerChannel() }

    let loopThread = Thread { [loop] in try? loop.run() }
    loopThread.start()
    try? await Task.sleep(for: .milliseconds(50))

    totalBytes.store(0, ordering: .relaxed)
    readCalls.store(0, ordering: .relaxed)
    let stopAt = ContinuousClock.now + .seconds(Int(seconds))

    // Writer: flood the blocking ends round-robin; blocking writes
    // self-regulate against the reader's pace.
    let writer = Thread {
        let chunk = [UInt8](repeating: 0x42, count: 8192)
        var round = 0
        while ContinuousClock.now < stopAt {
            let i = round % K
            _ = chunk.withUnsafeBufferPointer { ptr in
                Glibc.write(pairs[i].0, ptr.baseAddress!, chunk.count)
            }
            round += 1
        }
        // EOF: close writer ends so readers observe hangup and exit.
        for p in pairs { _ = Glibc.close(p.0) }
    }

    let t0 = ContinuousClock.now

    // Readers: one loop-pinned Task per connection.
    var tasks: [Task<Void, Never>] = []
    for i in 0..<K {
        let fd = pairs[i].1
        let id = ids[i]
        tasks.append(Task(executorPreference: loop) { [loop] in
            while true {
                let n = await loop.read(channelId: id, fd: fd)
                _ = readCalls.add(1, ordering: .relaxed)
                if n > 0 {
                    _ = totalBytes.add(Int64(n), ordering: .relaxed)
                } else {
                    break  // EOF / error / hangup → drain finished
                }
            }
        })
    }
    writer.start()

    for t in tasks { _ = await t.value }
    let secs = elapsedSeconds(since: t0)
    let bytes = totalBytes.load(ordering: .relaxed)
    let reads = readCalls.load(ordering: .relaxed)

    loop.shutdown()
    for p in pairs { _ = Glibc.close(p.1) }

    print(String(
        format: "      %8.0f reads/s   %7.1f MB/s   (%.2f s wall, %.1f MB total)",
        Double(reads) / secs, Double(bytes) / secs / 1e6, secs, Double(bytes) / 1e6))
}

// MARK: - main

print("PulsarBench")
print("  phase 1: churn (register+cancel ×200k, pre-run)")
for _ in 0..<3 {
    let ops = phaseChurn()
    print(String(format: "      %10.0f ops/s", ops))
}
print("  phase 2: echo, 64 connections × 8 KB chunks, 5 s")
for _ in 0..<3 {
    await phaseEcho(connections: 64, seconds: 5)
}

#endif // os(Linux)
