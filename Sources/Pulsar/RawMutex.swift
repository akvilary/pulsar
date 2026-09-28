//===----------------------------------------------------------------------===//
//
//  RawMutex.swift
//  Pulsar
//
//  Minimal pthread mutex wrapper.
//
//  WHY not `Synchronization.Mutex` (the obvious choice)?
//  ----------------------------------------------------
//  On Linux, Swift's `Mutex` is implemented on a raw futex protocol that
//  ThreadSanitizer does not recognize as a lock — every cross-thread
//  access inside `withLock` is reported as a "Swift access race" false
//  positive. A permanently-warn TSan run is worse than no TSan run: it
//  trains developers to ignore the tool and buries REAL regressions.
//
//  Verified against the swiftlang/swift `main` branch (Sept 2026, i.e.
//  ahead of any release): stdlib/public/Synchronization/Mutex/
//  LinuxImpl.swift is still the plain-futex, three-state-word protocol
//  with NO `__tsan_*` annotations anywhere in the module. The gap is
//  not fixed upstream — re-evaluate when annotations land.
//
//  glibc's `pthread_mutex_t` is equally futex-backed (same uncontended
//  fast path: one userspace CAS) but its lock/unlock calls are
//  intercepted by TSan, which turns the lock discipline of everything
//  it guards into a machine-checked invariant: `swift test
//  --sanitize=thread` becomes a trustworthy CI gate for this package.
//
//  Used for exactly the irreducible cross-thread state of the loop:
//  the job pool queue, the `onWakeup` callback slot, and the sweep
//  interval — everything else is loop-thread-owned or atomic.
//
//===----------------------------------------------------------------------===//

#if os(Linux)

import Foundation

#if canImport(Glibc)
import Glibc
#endif

/// Pthread mutex with scoped locking. `@unchecked Sendable` is sound:
/// `pthread_mutex_t` is thread-safe by contract; the wrapper adds no
/// state of its own.
internal final class RawMutex: @unchecked Sendable {
    private var mutex = pthread_mutex_t()

    init() {
        pthread_mutex_init(&mutex, nil)
    }

    deinit {
        // Reachable only when no user of the lock remains (class
        // deinit implies zero references → no in-flight withLock).
        pthread_mutex_destroy(&mutex)
    }

    @inline(__always)
    func lock() {
        pthread_mutex_lock(&mutex)
    }

    @inline(__always)
    func unlock() {
        pthread_mutex_unlock(&mutex)
    }

    @inline(__always)
    func withLock<T>(_ body: () throws -> T) rethrows -> T {
        lock()
        defer { unlock() }
        return try body()
    }
}

/// A value plus the lock that guards it, fused into one allocation —
/// the `NIOLockedValueBox` pattern. The state is reachable ONLY
/// through `withLock`, so the lock discipline is enforced by the TYPE
/// SYSTEM exactly as with `Synchronization.Mutex<State>` — while the
/// underlying `pthread_mutex_t` keeps every critical section visible
/// to ThreadSanitizer.
///
/// Swapping the internals to `Mutex<State>` later is a one-line change
/// (identical surface) once the Synchronization module's Linux futex
/// protocol becomes TSan-recognised — see the file header for the
/// upstream verification note.
internal final class LockedBox<State: Sendable>: @unchecked Sendable {
    private let lock = RawMutex()
    private var state: State

    init(_ initial: State) {
        self.state = initial
    }

    @inline(__always)
    func withLock<R>(_ body: (inout State) throws -> R) rethrows -> R {
        lock.lock()
        defer { lock.unlock() }
        return try body(&state)
    }
}

#endif // os(Linux)
