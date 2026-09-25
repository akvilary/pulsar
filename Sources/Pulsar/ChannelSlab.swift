//===----------------------------------------------------------------------===//
//
//  ChannelSlab.swift
//  Pulsar
//
//  Dense, generation-guarded table of `PollChannelState` — the Swift
//  analogue of the `slab` crate tokio/mio use for token → state maps.
//
//  Why not Dictionary (the previous design)
//  ----------------------------------------
//  The hot path (`processChannelEvent`) runs once per delivered epoll
//  event. A `[UInt32: PollChannelState]` lookup costs a hash of the
//  key, a probe, `swift_beginAccess`/`swift_endAccess` exclusivity
//  calls, and a copy-out of the value (with ARC traffic on its
//  closure/continuation fields). This table replaces all of that with:
//
//      slot = token & 0xFFFFFFFF          ; one AND
//      gen  = token >> 32                 ; one shift
//      valid = gen == generations[slot]   ; one load + compare
//      state = &states[slot]              ; one addressed load
//
//  — four instructions to a mutable, in-place pointer. No hashing, no
//  exclusivity instrumentation (raw memory is untracked), no copy-out
//  / write-back round trip.
//
//  Layout & lifetime
//  -----------------
//  Four flat buffers sized to `capacity` (grown ×2 on exhaustion):
//
//    states[slot]       PollChannelState — INITIALIZED iff the slot is
//                       live (see below); mutated in place by the loop.
//    generations[slot]  UInt32 — allocation generation. Bumped on both
//                       alloc and free, so **odd = live, even = vacant**
//                       (the same parity trick the Rust `slab` crate
//                       stores in vacant entries' next-pointers).
//    watchFlags[slot]   Bool — parallel byte marking watch channels.
//                       Exists so the per-event dispatch decision is a
//                       single static-offset byte load, WITHOUT reading
//                       the state itself (state field access goes
//                       through runtime-computed offsets for the
//                       non-frozen struct; the flag array does not).
//    freeStack[0..<freeTop]
//                       LIFO stack of vacant slot indices — the free
//                       list. LIFO (not FIFO) on purpose: the most
//                       recently freed slot is the most cache-warm, and
//                       reuse-first keeps the live set compact under
//                       churn (short-lived connections reuse slots
//                       instead of growing the table).
//
//  Slots `[0, slotCount)` have been touched at least once; slots
//  `[slotCount, capacity)` are untouched. A *vacant* slot inside
//  `[0, slotCount)` holds moved-out (uninitialized) state memory —
//  only `generations`, `watchFlags` and `freeStack` may be read for it.
//
//  Generations & stale-token safety
//  --------------------------------
//  A handle handed out by `alloc` is `(slot, generation)`. The epoll
//  registration token carries both (`(UInt64(gen) << 32) | slot`), so
//  events delivered for a cancelled channel fail the generation check
//  and are dropped — the slab equivalent of the old dictionary lookup
//  miss. This is what makes slot *reuse* safe: without generations, a
//  stale event from the current `epoll_wait` batch could be delivered
//  to the new occupant of the slot.
//
//  Generation wraparound is handled by RETIREMENT, not aliasing: when
//  a free would wrap the generation past `UInt32.max`, the slot is
//  permanently taken out of the free list instead (a ~120 B hole after
//  4 billion reuses of one slot — never observed in practice, and
//  strictly safer than the Rust `slab` crate, which does alias).
//
//  Reserved-token audit (see PollEventLoop):
//    * `Token.wakeup == 0`  — unreachable: a handed-out generation is
//      always odd (≥ 1), so every channel token has raw ≥ 2^32.
//    * `timerToken == UInt64.max` — would need gen == slot ==
//      0xFFFFFFFF simultaneously; slot indices are bounded by
//      `grow()`'s precondition long before 2^32.
//
//  Threading: **loop thread only**, exactly like the `PollChannelState`
//  values it stores (see PollEventLoop's channel-management contract;
//  the loop enforces it at every entry point).
//
//===----------------------------------------------------------------------===//

#if os(Linux)

import MIO

/// Dense table of per-channel states indexed by slot, with a LIFO
/// free-list and generation-guarded handles.
internal final class ChannelSlab {

    private var states: UnsafeMutablePointer<PollChannelState>
    private var generations: UnsafeMutablePointer<UInt32>
    private var watchFlags: UnsafeMutablePointer<Bool>
    private var freeStack: UnsafeMutablePointer<UInt32>
    private var freeTop: Int = 0

    /// Number of slots ever touched (live + vacant), `<= capacity`.
    private(set) var slotCount: Int = 0
    /// Total buffer size. Grows ×2 when a fresh slot is needed and no
    /// vacant one exists.
    private(set) var capacity: Int
    /// Number of currently live (allocated) slots.
    private(set) var liveCount: Int = 0

    /// - Precondition: `initialCapacity > 0`.
    init(initialCapacity: Int = 256) {
        precondition(initialCapacity > 0, "ChannelSlab capacity must be > 0")
        self.capacity = initialCapacity
        self.states = .allocate(capacity: initialCapacity)
        self.generations = .allocate(capacity: initialCapacity)
        self.watchFlags = .allocate(capacity: initialCapacity)
        self.freeStack = .allocate(capacity: initialCapacity)
        generations.initialize(repeating: 0, count: initialCapacity)
        watchFlags.initialize(repeating: false, count: initialCapacity)
    }

    deinit {
        // Defensive: a loop discarded without `run()` completing still
        // owns its live states. `recoverOrphanedContinuations` is the
        // normal cleanup path (it resumes continuations first); this
        // only guarantees the ARC members (watch closures) are
        // released and the buffers returned.
        deinitializeStates()
        states.deallocate()
        generations.deallocate()
        watchFlags.deallocate()
        freeStack.deallocate()
    }

    // MARK: Allocation

    /// Insert `initial` at a fresh slot — reusing the most recently
    /// freed one (LIFO) when available, otherwise touching a new slot
    /// or growing the table. Returns the `(slot, generation)` handle;
    /// the generation is what future `isValid` checks compare against.
    ///
    /// `isWatch` records the channel kind in the parallel flag array
    /// so the event dispatch never has to read the state just to tell
    /// a watch channel from a data channel.
    ///
    /// The returned handle must be packed into the epoll registration
    /// token by the caller (`(gen << 32) | slot`).
    func alloc(
        _ initial: PollChannelState, isWatch: Bool
    ) -> (slot: Int, gen: UInt32) {
        let slot: Int
        if freeTop > 0 {
            freeTop -= 1
            slot = Int(freeStack[freeTop])
        } else if slotCount < capacity {
            slot = slotCount
            slotCount += 1
        } else {
            grow()
            slot = slotCount
            slotCount += 1
        }
        let gen = generations[slot] &+ 1
        // The handed-out generation must ALWAYS be odd (live). Vacant
        // slots hold even values (untouched = 0); the assert catches
        // any future violation of that invariant at the earliest
        // possible point instead of as a mystery trap in `remove`.
        assert(gen & 1 == 1, "ChannelSlab.alloc: generation parity violated")
        generations[slot] = gen
        watchFlags[slot] = isWatch
        (states + slot).initialize(to: initial)
        liveCount += 1
        return (slot, gen)
    }

    /// Vacate `slot`, moving its state out for the caller to consume
    /// (resume continuations, deregister fd, free the read buffer).
    /// The generation is bumped immediately, so any epoll event or
    /// caller handle still referencing this slot fails `isValid` from
    /// this point on — even before the state is fully torn down.
    ///
    /// If the generation would wrap past `UInt32.max`, the slot is
    /// RETIRED instead of recycled: it stays vacant forever (never
    /// pushed to the free list), so no ancient handle can ever alias
    /// a future occupant.
    ///
    /// - Precondition: `slot` is live (the caller validates the handle
    ///   first; a double-free is a logic bug and traps below).
    func remove(slot: Int) -> PollChannelState {
        precondition(slot < slotCount && generations[slot] & 1 == 1,
            "ChannelSlab.remove: slot \(slot) is not live")
        let state = (states + slot).move()
        generations[slot] &+= 1
        if generations[slot] != 0 {
            freeStack[freeTop] = UInt32(slot)
            freeTop += 1
        }
        // else: 0xFFFFFFFF → 0 wrapped — retired, not recycled.
        liveCount -= 1
        return state
    }

    // MARK: Hot-path accessors

    /// Validate a `(slot, gen)` handle. O(1): one bounds compare, one
    /// generation compare. A matching generation implies liveness
    /// (generations are bumped on free), so no separate live bitmap
    /// is needed.
    func isValid(slot: Int, gen: UInt32) -> Bool {
        slot < slotCount && generations[slot] == gen
    }

    /// Direct pointer to a live slot's state, for in-place mutation.
    /// **Unchecked** — the caller must have validated the handle via
    /// `isValid(slot:gen:)` (or hold the slot from `alloc`), and must
    /// not retain the pointer across code that can vacate the slot
    /// (a `watch` handler calling `cancelChannel`, table teardown).
    func pointer(slot: Int) -> UnsafeMutablePointer<PollChannelState> {
        states + slot
    }

    /// Liveness test by slot index (no handle). Used by full-table
    /// scans (`sweepTimeouts`, shutdown recovery) and white-box tests.
    /// Odd generation == live.
    func isLive(slot: Int) -> Bool {
        slot < slotCount && generations[slot] & 1 == 1
    }

    /// Current generation of a slot — used to reconstruct a handle
    /// when only the slot index is known (e.g. `cancelWatch`'s fd map,
    /// which observes slots directly).
    ///
    /// - Precondition: `slot < slotCount` (the caller just observed
    ///   the slot live on this thread).
    func currentGeneration(slot: Int) -> UInt32 {
        generations[slot]
    }

    /// Watch-channel flag for a live slot — the dispatch decision on
    /// the per-event hot path (one static-offset byte load, no state
    /// access). Written at `alloc`; vacant slots' flags are never
    /// consulted.
    ///
    /// - Precondition: the caller validated the handle (live slot).
    func isWatch(slot: Int) -> Bool {
        watchFlags[slot]
    }

    /// Iterate every live slot. The pointer is borrowed for the
    /// callback only — the body must not store it.
    func forEachLive(_ body: (Int, UnsafeMutablePointer<PollChannelState>) -> Void) {
        for slot in 0..<slotCount where generations[slot] & 1 == 1 {
            body(slot, states + slot)
        }
    }

    // MARK: Teardown

    /// Deinitialize every live state. Vacant slots hold moved-out
    /// memory and are skipped. Called on the shutdown path (after the
    /// caller has resumed pending continuations and freed read
    /// buffers) and from `deinit`.
    private func deinitializeStates() {
        for slot in 0..<slotCount where generations[slot] & 1 == 1 {
            (states + slot).deinitialize(count: 1)
            generations[slot] &+= 1  // mark vacant; keeps invariants if reused
        }
        liveCount = 0
    }

    /// Shutdown form of `deinitializeStates`: also empties the free
    /// list and resets `slotCount`, returning the table to a
    /// pristine state (buffers keep their capacity).
    func reset() {
        deinitializeStates()
        freeTop = 0
        slotCount = 0
    }

    // MARK: Growth

    /// Double the buffers. Only live slots are moved: vacant slots
    /// hold uninitialized (moved-out) memory and must not be copied.
    /// `initialize` + `deinitialize` on the old buffer performs a
    /// reference-count-neutral move of the ARC members.
    private func grow() {
        let newCapacity = capacity * 2
        // The handle layout reserves 32 bits for the slot index;
        // crossing 2^32 slots would alias the token space (and the
        // reserved `wakeup`/timer tokens' audit). Unreachable in
        // practice (4G × ~120 B ≈ 0.5 TB of states) — trap rather
        // than silently truncate in `packChannelId`.
        precondition(newCapacity <= 0xFFFF_FFFF,
            "ChannelSlab: slot space exhausted (token layout)")
        let newStates = UnsafeMutablePointer<PollChannelState>.allocate(
            capacity: newCapacity)
        let newGenerations = UnsafeMutablePointer<UInt32>.allocate(
            capacity: newCapacity)
        let newWatchFlags = UnsafeMutablePointer<Bool>.allocate(
            capacity: newCapacity)
        let newFreeStack = UnsafeMutablePointer<UInt32>.allocate(
            capacity: newCapacity)

        for slot in 0..<slotCount where generations[slot] & 1 == 1 {
            (newStates + slot).initialize(to: (states + slot).pointee)
        }
        // Zero the WHOLE generations/watchFlags buffers, then copy the
        // old values over the prefix. The untouched tails are what
        // future fresh-slot allocations read (`generations[slot] &+ 1`)
        // — leaving generations uninitialized once handed out even
        // generations, silently breaking the odd-live/even-vacant
        // parity (found the hard way: an even generation reaches a
        // handle, passes `isValid`, and traps in `remove`'s parity
        // precondition — or worse, doesn't).
        newGenerations.initialize(repeating: 0, count: newCapacity)
        newGenerations.update(from: generations, count: slotCount)
        newWatchFlags.initialize(repeating: false, count: newCapacity)
        newWatchFlags.update(from: watchFlags, count: slotCount)
        // freeStack is write-before-read beyond freeTop (remove writes
        // freeStack[freeTop] before freeTop advances) — no zeroing
        // needed, but copy the live prefix.
        newFreeStack.initialize(from: freeStack, count: freeTop)

        for slot in 0..<slotCount where generations[slot] & 1 == 1 {
            (states + slot).deinitialize(count: 1)
        }
        states.deallocate()
        generations.deallocate()
        watchFlags.deallocate()
        freeStack.deallocate()

        states = newStates
        generations = newGenerations
        watchFlags = newWatchFlags
        freeStack = newFreeStack
        capacity = newCapacity
    }
}

// MARK: - ChannelId

/// Opaque handle to a registered channel — the epoll token layout
/// `(generation << 32) | slotIndex` packed into one `UInt64`.
///
/// Returned by `PollEventLoop.registerChannel()` /
/// `registerWatch(fd:interest:_:)` and passed back into every
/// per-channel API. A handle stays valid until the matching
/// `cancelChannel(_:)`; afterwards the generation no longer matches
/// the slot and any use traps with a "stale channel handle"
/// precondition (fail-fast instead of silently acting on whichever
/// channel now owns the recycled slot).
///
/// `Hashable` so downstream code can key dictionaries by it (e.g.
/// per-worker connection accounting); `Sendable` — it is a trivial
/// value with no lifetime of its own.
@frozen
public struct ChannelId: Sendable, Hashable {
    /// `(generation << 32) | slotIndex`. Never `0` (the waker token)
    /// and never `UInt64.max` (the timer token) — see ChannelSlab.
    public let raw: UInt64

    @inlinable
    public init(raw: UInt64) {
        self.raw = raw
    }
}

@inline(__always)
internal func packChannelId(slot: Int, gen: UInt32) -> ChannelId {
    ChannelId(raw: (UInt64(gen) << 32) | UInt64(truncatingIfNeeded: slot))
}

extension ChannelId {
    /// Slot index (low 32 bits). Non-negative by construction.
    @inline(__always)
    internal var slot: Int {
        Int(truncatingIfNeeded: raw & 0xFFFF_FFFF)
    }

    /// Generation (high 32 bits).
    @inline(__always)
    internal var generation: UInt32 {
        UInt32(truncatingIfNeeded: raw >> 32)
    }

    /// The epoll registration token carrying this handle verbatim.
    /// The kernel echoes it back in delivered events; the loop unpacks
    /// slot + generation from it in `processChannelEvent`.
    @inline(__always)
    internal var asToken: Token {
        Token(raw)
    }
}

#endif // os(Linux)
