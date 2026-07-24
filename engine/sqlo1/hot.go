package sqlo1

import (
	"bytes"
	"hash/maphash"
)

// HotTable is the hot-tier structure set from doc 04 section 3: packed
// headers in a dense slice, key and value bytes in chunked arenas, and a
// hash-to-slot index (the Go 1.24+ builtin map is a Swiss table, which is
// the doc's stated reason the layout is arrays behind a map instead of a
// map of structs). The doc 04 section 4 state machine lives here too:
// writes dirty, drain cools dirty into resident, eviction drops resident
// to cold or ghost, and deletes leave dirty tombstones until drain
// carries them to disk. The eviction and drain policies (who calls
// drained and evict, and when) belong to later slices; the transitions
// themselves are pinned by an executable property test.
//
// Point ops on existing keys, delete-reinsert cycles, and full
// drain-evict-reinsert cycles run without allocating; the alloczero lab
// gates that.
type HotTable struct {
	seed  maphash.Seed
	index map[uint64]uint32
	// dups carries the extra slots when two live keys share a 64-bit
	// hash: the doc expects a handful at most at any scale that fits
	// RAM, so the side table stays tiny and the fast path is one map
	// hit plus one key compare.
	dups      map[uint64][]uint32
	hdrs      []hdr
	freeSlots []uint32
	keys      arena
	vals      arena
	ghosts    ghostRing
	// tick is the coarse stamp clock and nowMs the exact wall clock
	// behind it, both moved by SetNow (SetTick projects nowMs to the
	// tick's end for tick-driven tests); live counts keys visible to
	// reads, so tombstones are out.
	tick  uint32
	nowMs int64
	live  int
	// keyDelta is the hot tier's correction to the store's key count,
	// the doc 11 section 4 DBSIZE feed: +1 for each live key the store
	// is not known to hold yet (fresh writes, vptr 0), -1 for each
	// pending tombstone over a key the store does hold (vptr nonzero).
	// Maintained by keyDeltaOf brackets at every header transition. A
	// blind overwrite of a cold key counts +1 until its drain replaces
	// the cold record, the one sanctioned over-count window.
	keyDelta int64
	// The write-behind queue: slot indices in first-dirtied order, a
	// ring preallocated to capacity. The header's queued flag keeps
	// entries unique, so re-dirtying a dirty key never re-enqueues it
	// (that is the coalescing rule made structural) and the ring only
	// grows if transitions happen outside the drain scheduler (tests).
	// dirtyBytes tracks key plus value bytes across all dirty slots,
	// the doc 04 section 7 drain trigger quantity.
	dirty      []uint32
	dirtyHead  int
	dirtyN     int
	dirtyBytes int
	// pinSlot is the one queue entry drains must skip while a dual
	// command is mid-flight (pinRoot below); one pin at a time is the
	// shard discipline, single owner, one command in flight.
	pinSlot uint32
	pinOK   bool
	// vacates counts chunk-vacate passes, the class-migration deadlock
	// breaker below; the stats surface it so a workload paying this cost
	// is visible.
	vacates int64
	// intShadow holds the parsed int64 behind headers flagged
	// tagIntShadow, keyed by slot: doc 05's integer fast path pays its
	// 8 bytes only on keys that have actually seen INCR-family ops, so
	// the value lives in a side map instead of a header field. The map
	// is allocated on first arm. Entry lifetime follows the flag bit:
	// dropShadow retires both together, and PutGen, Del, and removeSlot
	// are the only places a flagged slot's value or identity can change.
	intShadow map[uint32]int64
}

// maxKlen is the klen field's reach. Longer keys are legal in Redis but
// pathological; they are rejected here and the compat milestone decides
// whether they route around the hot tier or hard-error.
const maxKlen = 1<<16 - 1

// NewHotTable preallocates for capacity resident keys; Put fails once
// the header slice is full until eviction (a later slice) makes room.
func NewHotTable(capacity int) *HotTable {
	return &HotTable{
		seed:      maphash.MakeSeed(),
		index:     make(map[uint64]uint32, capacity),
		dups:      make(map[uint64][]uint32),
		hdrs:      make([]hdr, 0, capacity),
		freeSlots: make([]uint32, 0, capacity),
		ghosts:    newGhostRing(capacity / 16),
		dirty:     make([]uint32, max(capacity, 8)),
	}
}

// enqueueDirty files slot s in the write-behind queue on its transition
// into dirty; a slot already queued stays where it is, which is what
// keeps drain order first-dirtied-first under overwrite churn. Roots
// are the one exception, rule W1 of doc 06 section 5: a re-dirtied
// root marks itself for deferral, and popDirty re-files it at the tail
// instead of handing it out from its old position. A root's image
// summarizes segment records that later commands enqueue behind it, so
// holding its early position could drain the summary in a batch before
// the segments it counts; deferral makes the root land with or after
// every segment it summarizes, and the with-or-after gap is exactly
// the crash window rule W3's replay reconciliation covers.
func (t *HotTable) enqueueDirty(s uint32) {
	hd := &t.hdrs[s]
	if hd.queued&queuedBit != 0 {
		if hd.typeTag&TagRoot != 0 {
			hd.queued |= queuedDefer
		}
		return
	}
	hd.queued = queuedBit
	t.pushDirty(s)
}

// pushDirty appends s to the queue ring, growing it when full.
func (t *HotTable) pushDirty(s uint32) {
	if t.dirtyN == len(t.dirty) {
		grown := make([]uint32, 2*len(t.dirty))
		for i := range t.dirtyN {
			grown[i] = t.dirty[(t.dirtyHead+i)%len(t.dirty)]
		}
		t.dirty, t.dirtyHead = grown, 0
	}
	t.dirty[(t.dirtyHead+t.dirtyN)%len(t.dirty)] = s
	t.dirtyN++
}

// popDirty hands the drain scheduler the oldest queue entry and clears
// its queued flag. An entry marked for deferral re-files at the tail
// with the mark cleared instead of popping, so it hands out after
// everything queued before this moment; the loop terminates because
// each pass either returns or clears one deferral mark, and nothing
// sets marks while the drain scheduler runs (single-owner). The pinned
// slot re-files with its flags intact and never hands out; when it is
// the only entry left the pop reports empty. Termination holds because
// between two pin sightings some other entry pops, and every non-pin
// pop either returns or clears one deferral mark. The caller must
// check the slot is still dirty: an entry goes stale when its slot was
// drained directly or freed and reused, and a stale entry is a skip,
// never an error.
func (t *HotTable) popDirty() (uint32, bool) {
	for t.dirtyN > 0 {
		s := t.dirty[t.dirtyHead]
		t.dirtyHead = (t.dirtyHead + 1) % len(t.dirty)
		t.dirtyN--
		if t.pinOK && s == t.pinSlot {
			only := t.dirtyN == 0
			t.pushDirty(s)
			if only {
				return 0, false
			}
			continue
		}
		hd := &t.hdrs[s]
		if hd.queued&queuedDefer != 0 {
			hd.queued = queuedBit
			t.pushDirty(s)
			continue
		}
		// Only the queue-membership bits clear: the volatile deferral
		// lap count survives the pop, so the drainer can cap laps
		// across cycles. A fresh transition into dirty resets it.
		hd.queued &^= queuedBit | queuedDefer
		return s, true
	}
	return 0, false
}

// SetTick moves the coarse clock the stamps record. Stamps shift last
// into prev only when the tick has moved, so repeated touches within one
// tick cost one compare (the WATT-lite two-stamp rule, hotclock lab).
// The wall clock is projected to the end of the tick, so tick-driven
// tests keep the old "expired at its tick" reading; the runtime calls
// SetNow with real milliseconds for the exact confirm.
func (t *HotTable) SetTick(tick uint32) {
	t.tick = tick
	t.nowMs = int64(tick)<<10 | 1023
}

// SetNow moves both clocks from wall milliseconds: the coarse tick the
// stamps and the wheel use, and the exact time the expiry confirm
// checks. The runtime refreshes it on Tick and the server refreshes it
// per dispatch, so lazy expiry is millisecond-exact at command time.
func (t *HotTable) SetNow(ms int64) {
	t.tick = uint32(uint64(ms) >> 10)
	t.nowMs = ms
}

func (t *HotTable) touchRead(hd *hdr) {
	if hd.lastRead != t.tick {
		hd.prevRead = hd.lastRead
		hd.lastRead = t.tick
	}
}

func (t *HotTable) touchWrite(hd *hdr) {
	if hd.lastWrite != t.tick {
		hd.prevWrite = hd.lastWrite
		hd.lastWrite = t.tick
	}
}

// Len returns the live key count; tombstones do not count.
func (t *HotTable) Len() int {
	return t.live
}

// expired reports whether hd's key is past its expiry: the coarse
// projection triggers, the exact millisecond confirms, per doc 11 (the
// projection stamps floor, so the trigger can fire up to a second
// before the true death and the confirm holds the line). This is the
// hot-tier layer of lazy expiry (E-I1): an expired key is invisible
// from the millisecond it expires regardless of reaper progress.
func (t *HotTable) expired(hd *hdr) bool {
	return hd.expireLo != 0 && hd.expireLo <= t.tick && expMsOf(hd) <= t.nowMs
}

// expMsOf reconstructs a header's exact expire_ms from the projection
// and the remainder; 0 means no expiry.
func expMsOf(hd *hdr) int64 {
	if hd.expireLo == 0 {
		return 0
	}
	return int64(hd.expireLo)<<10 | int64(hd.expireRem)
}

// Get returns the value for key, aliasing the arena until the next write.
// A dirty tombstone is a miss: the key is gone, its header just has not
// drained yet. An expired key is a miss too; the wheel reaps it later.
func (t *HotTable) Get(key []byte) ([]byte, bool) {
	v, hit, _ := t.probeRead(key)
	return v, hit
}

// probeRead is the tiered read path's hot step. A live key answers with
// its value and a read-stamp touch. A tombstone or an expired key answers
// definitively with no value: the key is gone, and whatever the store
// still holds under it is stale or already dead, so the caller must not
// go cold. Only a key the table has never heard of leaves the question
// open (definitive false), and that is the cold-miss door.
func (t *HotTable) probeRead(key []byte) (val []byte, hit, definitive bool) {
	val, _, hit, definitive = t.probeReadTag(key)
	return val, hit, definitive
}

// probeReadTag is probeRead with the header's type tag alongside, the
// hot half of Tiered.Lookup: the root bit in the tag is how the type
// layer recognizes a promoted root payload.
func (t *HotTable) probeReadTag(key []byte) (val []byte, tag uint8, hit, definitive bool) {
	val, tag, _, hit, definitive = t.probeEntry(key)
	return val, tag, hit, definitive
}

// probeEntry is the widest hot probe: value, tag, and the exact
// expire_ms, the hot half of Tiered.LookupEntry. The command layer
// needs the expiry beside the value for TTL answers and KEEPTTL.
func (t *HotTable) probeEntry(key []byte) (val []byte, tag uint8, expMs int64, hit, definitive bool) {
	s, ok := t.lookup(maphash.Bytes(t.seed, key), key)
	if !ok {
		return nil, 0, 0, false, false
	}
	hd := &t.hdrs[s]
	if hd.valRef == 0 || t.expired(hd) {
		return nil, 0, 0, false, true
	}
	t.touchRead(hd)
	return t.vals.data(hd.valRef), hd.typeTag, expMsOf(hd), true, true
}

// liveTag is hd's true type nibble. A promoted root header carries
// TagString regardless of the real type, because maybePromote has
// only the seam's Record to go by and that carries no type tag, so
// any root answers from its payload sub byte instead of trusting the
// nibble. Callers must have checked the header is live (valRef set).
func (t *HotTable) liveTag(hd *hdr) (uint8, error) {
	if hd.typeTag&TagRoot == 0 {
		return hd.typeTag & 0x0F, nil
	}
	tag, _, err := sniffRoot(t.vals.data(hd.valRef))
	return tag, err
}

// has reports raw residency in any state (live, dirty, tombstone,
// expired), without touching read stamps. The reaper uses it to yield
// to the hot copy: whatever the table holds under key is newer than
// the index record a scan found.
func (t *HotTable) has(key []byte) bool {
	_, ok := t.lookup(maphash.Bytes(t.seed, key), key)
	return ok
}

// dirtyKey reports whether key currently sits dirty in the table, so
// the type layer can see when a root still holds a drain-queue
// position from an earlier write.
func (t *HotTable) dirtyKey(key []byte) bool {
	s, ok := t.lookup(maphash.Bytes(t.seed, key), key)
	return ok && t.hdrs[s].state == stateDirty
}

// pinRoot holds key's dirty-queue entry out of drain hands until
// unpinRoot. A dual command's mid-flight flushes (segment and run
// splits) drain the queue while the command's root write is still
// deferred, and the queue may hold the previous command's root image;
// the deferral re-file would land that stale root behind the torn
// command's fresh segment images in the same batch, and a root frame
// is the plane's replay commit point, so it would absolve exactly the
// partial images the rollback discipline exists to drop. Pinned, the
// root stays queued but out of every batch the command emits, and the
// command's closing root write re-files the fresh image for the next
// cycle. Only a dirty slot pins; a clean root has no image to leak.
func (t *HotTable) pinRoot(key []byte) {
	s, ok := t.lookup(maphash.Bytes(t.seed, key), key)
	if ok && t.hdrs[s].state == stateDirty {
		t.pinSlot, t.pinOK = s, true
	}
}

func (t *HotTable) unpinRoot() {
	t.pinOK = false
}

// setExpireMs stamps the exact expiry on key's header, split into the
// coarse projection and the millisecond remainder; atMs 0 is PERSIST.
// It reports the slot for wheel filing, whether the stamp changed (an
// unchanged stamp must not file a duplicate entry), and whether the
// key was there to stamp: tombstones and expired-unreaped keys are
// already gone. A stamp reaches the store only when the record drains,
// so a changed stamp on a resident header re-dirties it and the edit
// rides the next cycle with the record image (doc 11's PEXPIRE WAL
// frame later makes this cheaper than a full rewrite).
func (t *HotTable) setExpireMs(key []byte, atMs int64) (slot uint32, changed, ok bool) {
	s, ok := t.lookup(maphash.Bytes(t.seed, key), key)
	if !ok {
		return 0, false, false
	}
	hd := &t.hdrs[s]
	if hd.valRef == 0 || t.expired(hd) {
		return 0, false, false
	}
	lo, rem := splitExpMs(atMs)
	if hd.expireLo == lo && hd.expireRem == rem {
		return s, false, true
	}
	hd.expireLo, hd.expireRem = lo, rem
	t.touchWrite(hd)
	if hd.state == stateResident {
		hd.state = stateDirty
		t.dirtyBytes += int(hd.klen) + len(t.vals.data(hd.valRef))
		t.enqueueDirty(s)
	}
	return s, true, true
}

// splitExpMs splits an exact expire_ms into the header's projection
// and remainder; 0 stays the no-expiry sentinel.
func splitExpMs(atMs int64) (lo uint32, rem uint16) {
	if atMs <= 0 {
		return 0, 0
	}
	// The header holds 42 bits of wall milliseconds, which reaches May
	// 2109. A later deadline clamps to that horizon instead of wrapping
	// into the past and deleting the key.
	const maxHdrExpMs = int64(1)<<42 - 1
	if atMs > maxHdrExpMs {
		atMs = maxHdrExpMs
	}
	return uint32(uint64(atMs) >> 10), uint16(atMs & 1023)
}

// Put inserts or overwrites key and marks it dirty. It reports false
// when the table is full, the arena budget refuses the bytes, or the key
// is longer than klen reaches; a failed Put changes nothing, not even a
// stamp, so the caller can evict and retry.
func (t *HotTable) Put(key, val []byte, tag uint8) bool {
	return t.PutGen(key, val, tag, 0)
}

// PutGen is Put carrying the record generation for segment records;
// the drain hands it to the store as Record.Gen. User records and
// roots write gen 0 through Put.
func (t *HotTable) PutGen(key, val []byte, tag uint8, gen uint32) bool {
	if len(key) > maxKlen {
		return false
	}
	h := maphash.Bytes(t.seed, key)
	if s, ok := t.lookup(h, key); ok {
		hd := &t.hdrs[s]
		before := keyDeltaOf(hd)
		// Delta describes the whole dirty window, not just this write:
		// the drained frame coalesces every write since the last drain,
		// so the flag survives only onto a window that has been delta
		// writes throughout. Joining a window that holds a structural
		// root write, a non-root value, or a tombstone strips it; a
		// clean slot starts a fresh window and keeps the caller's bit.
		if tag&TagDelta != 0 && hd.state == stateDirty &&
			(hd.valRef == 0 || hd.typeTag&(TagRoot|TagDelta) != TagRoot|TagDelta) {
			tag &^= TagDelta
		}
		if hd.valRef != 0 && t.expired(hd) {
			// The old life is over and this write starts a new one, so
			// the dead expiry must not ride along. A live key's expiry
			// survives writes; command-level rules like SET clearing the
			// TTL belong to the command layer.
			hd.expireLo, hd.expireRem = 0, 0
		}
		t.dropShadow(s)
		switch hd.valRef {
		case 0:
			// Reviving a tombstone: the header never left, so this is
			// an overwrite that happens to bring the key back to life.
			// The tombstone was dirty, so only the value bytes are new.
			ref := t.vals.alloc(val)
			if ref == 0 {
				return false
			}
			hd.valRef = ref
			t.live++
			t.dirtyBytes += len(val)
		default:
			oldLen := len(t.vals.data(hd.valRef))
			if !t.vals.update(hd.valRef, val) {
				// Alloc before release so a budget refusal leaves the
				// old value standing; only a grow lands here, so the old
				// slot's smaller class could not have served val anyway.
				ref := t.vals.alloc(val)
				if ref == 0 {
					return false
				}
				t.vals.release(hd.valRef)
				hd.valRef = ref
			}
			if hd.state == stateDirty {
				t.dirtyBytes += len(val) - oldLen
			} else {
				t.dirtyBytes += int(hd.klen) + len(val)
			}
		}
		hd.state = stateDirty
		hd.typeTag = tag
		hd.gen = gen
		t.keyDelta += keyDeltaOf(hd) - before
		t.touchWrite(hd)
		t.enqueueDirty(s)
		return true
	}

	s, ok := t.claimSlot(key, val, false)
	if !ok {
		return false
	}
	hd := &t.hdrs[s]
	hd.state = stateDirty
	hd.typeTag = tag
	hd.gen = gen
	if g, ok := t.ghosts.take(h); ok {
		// The key was evicted recently enough for its ghost to survive:
		// restore its stamps so the evictor sees its real history, not a
		// newborn (hotclock lab: ghost restore is what makes the stingy
		// promotion coin safe).
		hd.lastRead = g.lastRead
		hd.lastWrite = g.lastWrite
	}
	t.keyDelta += keyDeltaOf(hd)
	t.touchWrite(hd)
	t.live++
	t.dirtyBytes += len(key) + len(val)
	t.enqueueDirty(s)
	t.fileIndex(h, s)
	return true
}

// claimSlot picks a free header slot without taking it, then funds the
// arena bytes; the slot is only claimed once every alloc stands, so
// failure unwinds to nothing. A tomb claim takes no value bytes at all,
// which is distinct from an empty value. The header comes back filled
// with the refs, klen, and a zero state the caller completes; the index
// is not touched.
func (t *HotTable) claimSlot(key, val []byte, tomb bool) (uint32, bool) {
	var s uint32
	fromFree := len(t.freeSlots) > 0
	switch {
	case fromFree:
		s = t.freeSlots[len(t.freeSlots)-1]
	case len(t.hdrs) < cap(t.hdrs):
		s = uint32(len(t.hdrs))
	default:
		return 0, false
	}
	keyRef := t.keys.alloc(key)
	if keyRef == 0 {
		return 0, false
	}
	var valRef uint32
	if !tomb {
		valRef = t.vals.alloc(val)
		if valRef == 0 {
			t.keys.release(keyRef)
			return 0, false
		}
	}
	if fromFree {
		t.freeSlots = t.freeSlots[:len(t.freeSlots)-1]
	} else {
		t.hdrs = t.hdrs[:len(t.hdrs)+1]
	}
	t.hdrs[s] = hdr{
		keyRef: keyRef,
		valRef: valRef,
		klen:   uint16(len(key)),
	}
	return s, true
}

// fileIndex maps h to slot s, spilling to the dup side table when another
// key already holds the primary entry.
func (t *HotTable) fileIndex(h uint64, s uint32) {
	if _, occupied := t.index[h]; occupied {
		t.dups[h] = append(t.dups[h], s)
	} else {
		t.index[h] = s
	}
}

// promote inserts key as resident, doc 04 section 4's cold-to-resident
// transition: the store's copy stays authoritative, the RAM copy is a
// free-to-evict cache, and nothing here dirties, enqueues, or counts
// toward drain. The ghost ring hands back the key's stamps when it
// remembers them, and the promotion itself is a read, so the read stamp
// touches. It reports false when the table or arenas refuse; the caller
// treats that as promotion skipped, never as pressure. A key already
// live in the table (a duplicate cold miss inside one batch) is a
// touch-and-succeed; a tombstone wins over any promotion, because the
// deletion just has not drained yet.
func (t *HotTable) promote(key, val []byte, tag uint8, gen uint32, expMs int64) bool {
	if len(key) > maxKlen {
		return false
	}
	h := maphash.Bytes(t.seed, key)
	if s, ok := t.lookup(h, key); ok {
		hd := &t.hdrs[s]
		if hd.valRef == 0 {
			return false
		}
		t.touchRead(hd)
		return true
	}
	s, ok := t.claimSlot(key, val, false)
	if !ok {
		return false
	}
	hd := &t.hdrs[s]
	// vptr 1 is "the store holds this record": disk positions never
	// cross the seam, and the state machine only needs
	// nonzero-when-clean; cold reads go back through BatchGet.
	hd.vptr = 1
	hd.state = stateResident
	hd.typeTag = tag
	hd.gen = gen
	hd.expireLo, hd.expireRem = splitExpMs(expMs)
	if g, ok := t.ghosts.take(h); ok {
		hd.lastRead = g.lastRead
		hd.lastWrite = g.lastWrite
	}
	t.touchRead(hd)
	t.live++
	t.fileIndex(h, s)
	return true
}

// delCold files a dirty tombstone for a key the table does not hold but
// the store does, so a deletion of a cold key drains exactly like any
// other write. The caller has already established cold existence; a key
// the table does hold is its own Del's business, and delCold refuses it.
// Any surviving ghost is dropped: the deleted life's history must not
// warm a future reinsert. keyClass says the cold record names an
// addressable key (not a plane record): the tombstone then carries
// vptr 1, "the store holds this key", and counts -1 in keyDelta until
// the deletion drains.
func (t *HotTable) delCold(key []byte, keyClass bool) bool {
	if len(key) > maxKlen {
		return false
	}
	h := maphash.Bytes(t.seed, key)
	if _, ok := t.lookup(h, key); ok {
		return false
	}
	s, ok := t.claimSlot(key, nil, true)
	if !ok {
		return false
	}
	hd := &t.hdrs[s]
	hd.state = stateDirty
	if keyClass {
		hd.vptr = 1
	}
	t.keyDelta += keyDeltaOf(hd)
	t.ghosts.take(h)
	t.touchWrite(hd)
	t.dirtyBytes += len(key)
	t.enqueueDirty(s)
	t.fileIndex(h, s)
	return true
}

// keyDeltaOf is hd's contribution to keyDelta. Plane records (segments,
// fences) are not keys; a live header only counts while the store is
// not known to hold the key (vptr 0), and a tombstone only counts,
// negatively, while it still shadows a cold record the store holds.
func keyDeltaOf(hd *hdr) int64 {
	if hd.gen != 0 || hd.typeTag&TagFence != 0 {
		return 0
	}
	switch {
	case hd.valRef != 0 && hd.vptr == 0:
		return 1
	case hd.valRef == 0 && hd.vptr != 0:
		return -1
	}
	return 0
}

// Del writes a dirty tombstone: the key vanishes from reads immediately,
// but the header stays until drain carries the deletion to disk and
// drained retires the slot. It reports whether the key was live.
func (t *HotTable) Del(key []byte) bool {
	s, ok := t.lookup(maphash.Bytes(t.seed, key), key)
	if !ok {
		return false
	}
	hd := &t.hdrs[s]
	if hd.valRef == 0 {
		return false
	}
	before := keyDeltaOf(hd)
	if hd.state == stateDirty {
		t.dirtyBytes -= len(t.vals.data(hd.valRef))
	} else {
		t.dirtyBytes += int(hd.klen)
	}
	t.dropShadow(s)
	t.vals.release(hd.valRef)
	hd.valRef = 0
	hd.expireLo, hd.expireRem = 0, 0
	hd.state = stateDirty
	t.keyDelta += keyDeltaOf(hd) - before
	t.touchWrite(hd)
	t.enqueueDirty(s)
	t.live--
	return true
}

// drained is the drain scheduler's word that slot s is durable in a
// record group: dirty cools to resident with its new disk position, and
// the RAM copy stays so a later eviction is free. A drained tombstone
// has nothing left to say in RAM, so its header and index entry retire.
// It reports false when the slot is not dirty (the drain ran twice or
// raced a state change), and the caller must treat that as a no-op.
func (t *HotTable) drained(s uint32, vptr uint64) bool {
	hd := &t.hdrs[s]
	if hd.state != stateDirty {
		return false
	}
	if hd.valRef == 0 {
		t.dirtyBytes -= int(hd.klen)
		t.removeSlot(maphash.Bytes(t.seed, t.keys.data(hd.keyRef)), s)
		return true
	}
	t.dirtyBytes -= int(hd.klen) + len(t.vals.data(hd.valRef))
	before := keyDeltaOf(hd)
	hd.state = stateResident
	hd.vptr = vptr
	t.keyDelta += keyDeltaOf(hd) - before
	// Delta is a dirty-window property and the drain just consumed the
	// window; a resident header must not carry it into the next one
	// (setExpireMs re-dirties without reassigning the tag, and an
	// expiry edit is exactly what segment replay cannot reconstruct).
	hd.typeTag &^= TagDelta
	return true
}

// evict drops resident slot s per doc 04 section 4: to cold (keep
// nothing) or to ghost (the stamps move to the ghost ring so a returning
// key gets its history back). Dirty is never evictable, it must drain
// first; that invariant is structural here, not a caller courtesy.
func (t *HotTable) evict(s uint32, toGhost bool) bool {
	hd := &t.hdrs[s]
	if hd.state != stateResident {
		return false
	}
	h := maphash.Bytes(t.seed, t.keys.data(hd.keyRef))
	if toGhost {
		t.ghosts.put(h, hd.lastRead, hd.lastWrite)
	}
	t.removeSlot(h, s)
	return true
}

// dropShadow retires slot s's int shadow, flag bit and map entry
// together. Every writer that lands on a flagged slot must pass
// through here (or through PutGen's wholesale typeTag assignment,
// which pairs with the explicit call just before it) so the map never
// outlives the value it mirrors.
func (t *HotTable) dropShadow(s uint32) {
	hd := &t.hdrs[s]
	if hd.typeTag&tagIntShadow != 0 {
		hd.typeTag &^= tagIntShadow
		delete(t.intShadow, s)
	}
}

// armIntShadow caches n as key's parsed integer value. Only the INCR
// family calls it, right after the canonical decimal string landed via
// Set, so a flagged header always mirrors arena bytes that
// strconv.AppendInt produced for n. Arming a key that is not a live
// plain string is refused rather than an error: the shadow is an
// optimization, never state.
func (t *HotTable) armIntShadow(key []byte, n int64) {
	s, ok := t.lookup(maphash.Bytes(t.seed, key), key)
	if !ok {
		return
	}
	hd := &t.hdrs[s]
	if hd.valRef == 0 || t.expired(hd) || hd.typeTag&^tagIntShadow != TagString {
		return
	}
	if t.intShadow == nil {
		t.intShadow = make(map[uint32]int64)
	}
	hd.typeTag |= tagIntShadow
	t.intShadow[s] = n
}

// intShadowOf answers key's cached int64 and exact expire_ms when the
// shadow is armed and the key is still live and unexpired. A miss
// means nothing: the caller falls back to reading and parsing the
// bytes. The expiry rides along because the header is already in
// hand and the caller's restamp rule needs it either way.
func (t *HotTable) intShadowOf(key []byte) (n, expMs int64, ok bool) {
	s, ok := t.lookup(maphash.Bytes(t.seed, key), key)
	if !ok {
		return 0, 0, false
	}
	hd := &t.hdrs[s]
	if hd.typeTag&tagIntShadow == 0 || hd.valRef == 0 || t.expired(hd) {
		return 0, 0, false
	}
	n, ok = t.intShadow[s]
	return n, expMsOf(hd), ok
}

// canPut reports whether PutGen (or delCold when tomb) would find the
// slot and arena bytes it needs right now, mirroring the claim paths
// exactly: an existing slot updates in place when the value fits its
// capacity and reallocates otherwise, a new key needs a header slot,
// key bytes, and value bytes. The vacate loop uses this as its exit
// condition, so it must never say no when the write would succeed.
func (t *HotTable) canPut(key, val []byte, tomb bool) bool {
	if s, ok := t.lookup(maphash.Bytes(t.seed, key), key); ok {
		if tomb {
			return true // delCold refuses a held key; nothing to allocate
		}
		hd := &t.hdrs[s]
		if hd.valRef != 0 && t.vals.slotFootprint(hd.valRef) >= int64(len(val))+arenaAlign {
			return true
		}
		return t.vals.canAlloc(len(val))
	}
	if len(t.freeSlots) == 0 && len(t.hdrs) >= cap(t.hdrs) {
		return false
	}
	if !t.keys.canAlloc(len(key)) {
		return false
	}
	return tomb || t.vals.canAlloc(len(val))
}

// vacateValChunk breaks the arena class-migration deadlock: freelists
// recycle slots within a size class and never return budget, so once
// the budget saturates, the first allocation in a class the workload
// has not touched can starve while other classes sit on plenty of free
// slots (a paged stream root growing across a class boundary is the
// natural trigger). It picks the standard value chunk with the least
// live payload whose live slots are all clean residents, evicts exactly
// those residents, and reclaims the then-empty chunk back to the shared
// budget. Dirty never evicts (R-I3), so a chunk holding any dirty slot
// is skipped; the caller drains first to make chunks eligible. It
// reports whether a chunk was retired.
func (t *HotTable) vacateValChunk() bool {
	n := len(t.vals.chunks)
	if n == 0 {
		return false
	}
	resident := make([]int64, n)
	tainted := make([]bool, n)
	for i := range t.hdrs {
		hd := &t.hdrs[i]
		if hd.keyRef == 0 || hd.valRef == 0 {
			continue
		}
		ci := hd.valRef >> 16
		if hd.state != stateResident {
			tainted[ci] = true
			continue
		}
		resident[ci] += t.vals.slotFootprint(hd.valRef)
	}
	target := -1
	for ci := range t.vals.chunks {
		if t.vals.chunks[ci] == nil || len(t.vals.chunks[ci]) != arenaChunkSize ||
			uint32(ci) == t.vals.cur || tainted[ci] || t.vals.liveFp[ci] == 0 {
			continue
		}
		// Only a chunk whose live footprint is fully explained by clean
		// residents can go empty; chunk 0's reserved pad never matches,
		// which keeps ref 0 dead.
		if resident[ci] != t.vals.liveFp[ci] {
			continue
		}
		if target < 0 || resident[ci] < resident[target] {
			target = ci
		}
	}
	if target < 0 {
		return false
	}
	for i := range t.hdrs {
		hd := &t.hdrs[i]
		if hd.keyRef != 0 && hd.valRef != 0 && hd.state == stateResident &&
			hd.valRef>>16 == uint32(target) {
			t.evict(uint32(i), true)
		}
	}
	t.vacates++
	t.keys.reclaim()
	return t.vals.reclaim()
}

// removeSlot frees a header slot and its arena bytes and repairs the
// index, promoting a dup when the primary occupant goes.
func (t *HotTable) removeSlot(h uint64, s uint32) {
	hd := &t.hdrs[s]
	t.keyDelta -= keyDeltaOf(hd)
	t.dropShadow(s)
	t.keys.release(hd.keyRef)
	if hd.valRef != 0 {
		t.vals.release(hd.valRef)
		t.live--
	}
	*hd = hdr{}
	t.freeSlots = append(t.freeSlots, s)

	if t.index[h] == s {
		if ds := t.dups[h]; len(ds) > 0 {
			t.index[h] = ds[len(ds)-1]
			t.shrinkDups(h, len(ds)-1)
		} else {
			delete(t.index, h)
		}
		return
	}
	ds := t.dups[h]
	for i, d := range ds {
		if d == s {
			ds[i] = ds[len(ds)-1]
			t.shrinkDups(h, len(ds)-1)
			break
		}
	}
}

func (t *HotTable) shrinkDups(h uint64, n int) {
	if n == 0 {
		delete(t.dups, h)
		return
	}
	t.dups[h] = t.dups[h][:n]
}

// lookup resolves h to key's header slot, walking the collision side
// table only when the primary slot holds a different key.
func (t *HotTable) lookup(h uint64, key []byte) (uint32, bool) {
	s, ok := t.index[h]
	if !ok {
		return 0, false
	}
	if t.keyIs(s, key) {
		return s, true
	}
	for _, d := range t.dups[h] {
		if t.keyIs(d, key) {
			return d, true
		}
	}
	return 0, false
}

func (t *HotTable) keyIs(s uint32, key []byte) bool {
	hd := &t.hdrs[s]
	return int(hd.klen) == len(key) && bytes.Equal(t.keys.data(hd.keyRef), key)
}
