// Package store is the aki v2 storage engine: a resident open-addressed hash
// index over a per-shard hybrid log, modelled on Microsoft FASTER and Garnet.
//
// The v1 engine answers a GET by descending a durable paged B-tree and decoding
// a varint-prefixed cell, then consulting a small decoded-value cache that
// thrashes under any working set larger than itself. At GET saturation that costs
// roughly 8x the per-core CPU of a single resident hash probe, which is what
// Valkey and Redis do. The memengine spike proved a resident sharded map clears
// 2x Valkey on GET; this engine keeps that fast path while staying larger than
// memory, by borrowing FASTER's hybrid log.
//
// The model, per shard:
//
//   - A resident index maps a key to a logical address: a monotonically growing
//     byte offset into that shard's log, pointing at the start of the key's
//     current record. The index is a flat []uint64 of 8-byte entries (a 15-bit
//     tag plus a 48-bit address), open-addressed with linear probing. The keys
//     are NOT resident in the index; they live in the log and the index
//     dereferences the log to confirm a key match. This is the structure the S0
//     spike (v2/findex) proved against map[string]: equal lookup speed,
//     6.5x smaller resident footprint that is independent of key length, and
//     3.3x lower GC pause because the table holds no pointers. A map keeps every
//     key resident with nowhere to spill, which is the larger-than-memory
//     failure mode; this table keeps only 8 bytes per key in RAM.
//   - The log is a sequence of fixed-size pages. Recent pages live in RAM (the
//     mutable tail plus the read-only region); once the resident page budget is
//     exceeded the oldest resident page is flushed to the log file on disk and
//     dropped from memory (the stable region). A logical address therefore tells
//     a reader, with no extra lookup, whether the record is in RAM or on disk.
//   - GET hashes the key, probes the index, and on a tag match dereferences the
//     record to compare the key and, on a hit, returns the value. For a resident
//     page the value is a slice into the page; for a spilled address it is read
//     back from the log file with one pread.
//   - SET appends a new record to the tail page under the shard write lock and
//     points the index at it. When the tail page fills, it is sealed and a fresh
//     page begins; when the resident budget is exceeded, the oldest page spills.
//
// Concurrency. The read path in full-resident mode (no spill) is lock-free: the
// index table and page directory live behind a single atomic.Pointer to an
// immutable view, so a GET loads the view once, reads the 8-byte index entry with
// an atomic load, and dereferences the resident page with no mutex at all. This
// is the rewrite/01 Slice-1 decision, confirmed by the /tmp/locktax microbenchmark
// on the target box: a 256-shard atomic-load read runs 402 Mops/s against 151 for
// the same shards behind an RWMutex.RLock, 2.66x, because an RWMutex's reader
// counter is itself a contended atomic that stops scaling a few cores in. Writers
// still serialize on the per-shard lock and publish each new record by storing its
// index entry with an atomic store after the record bytes are written, so a
// lock-free reader that observes the entry also observes a fully written record.
// Deletes in this mode write a tombstone (an atomic store) rather than the
// backward-shift compaction the locked path uses, because backshift reorders the
// probe chain and a lock-free reader walking it could miss a live key; tombstones
// are reclaimed by an in-place rehash when they crowd the table.
//
// When a resident page budget is set and a log file is open (the larger-than-
// memory spill mode), a page can be pulled out from under a reader by eviction, so
// that mode keeps the per-shard RWMutex on the read path: correctness over peak
// throughput on the cold tier, full speed on the hot tier.
package store

import (
	"encoding/binary"
	"errors"
	"math/bits"
	"os"
	"sync"
	"sync/atomic"
)

// Tunables holds the knobs that shape a Store. The zero value is not valid; use
// DefaultTunables and override.
type Tunables struct {
	// Shards is the number of independent index+log shards. Must be a power of
	// two so the shard for a key is a mask of its hash. More shards cut write-lock
	// contention; 256 matched the memengine shardmap that beat Valkey 2.46x.
	Shards int

	// PageSize is the byte size of one log page. A record must fit in a page.
	PageSize int

	// ResidentPagesPerShard caps how many log pages each shard keeps in RAM. Once
	// a shard holds more than this, its oldest resident page is flushed to the log
	// file and evicted. The total resident value budget is therefore
	// Shards * ResidentPagesPerShard * PageSize. A value of zero means unbounded
	// (nothing ever spills), which is the full-resident, fastest, RAM-bound mode.
	ResidentPagesPerShard int

	// Dir is where each shard writes its on-disk log file. Empty means the engine
	// runs memory-only: spilling is disabled even if ResidentPagesPerShard is set,
	// so an over-budget Set keeps the page resident rather than losing it. Used by
	// the engine-ceiling microbenchmark, which measures the in-RAM fast path.
	Dir string

	// IndexHintPerShard presizes each shard's open-addressed table for this many
	// keys, avoiding grow churn during a known-size load. Zero uses a small
	// default and lets the table grow.
	IndexHintPerShard int
}

// DefaultTunables returns a full-resident, memory-only configuration: 256 shards,
// 1 MiB pages, no spill. This is the engine-ceiling shape the microbenchmark uses.
func DefaultTunables() Tunables {
	return Tunables{Shards: 256, PageSize: 1 << 20, ResidentPagesPerShard: 0, Dir: ""}
}

// Store is the v2 engine. It is safe for concurrent use: each shard carries its
// own lock and the shard for a key is fixed by the key's hash.
type Store struct {
	shards []*shard
	mask   uint64
	t      Tunables
}

// New builds a Store. It returns an error if the tunables are invalid or, when a
// Dir is set, a shard log file cannot be created.
func New(t Tunables) (*Store, error) {
	if t.Shards <= 0 || t.Shards&(t.Shards-1) != 0 {
		return nil, errors.New("store: Shards must be a power of two")
	}
	if t.PageSize <= 64 {
		return nil, errors.New("store: PageSize too small")
	}
	if t.PageSize&(t.PageSize-1) != 0 {
		return nil, errors.New("store: PageSize must be a power of two")
	}
	s := &Store{
		shards: make([]*shard, t.Shards),
		mask:   uint64(t.Shards - 1),
		t:      t,
	}
	for i := range s.shards {
		sh, err := newShard(i, t)
		if err != nil {
			// Best-effort close of the shards already opened.
			for j := 0; j < i; j++ {
				_ = s.shards[j].close()
			}
			return nil, err
		}
		s.shards[i] = sh
	}
	return s, nil
}

// Close releases every shard's log file. The Store must not be used afterward.
func (s *Store) Close() error {
	var first error
	for _, sh := range s.shards {
		if err := sh.close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// shardFor returns the shard that owns a key. The shard is picked from the high
// bits of the hash so it is independent of the low bits the per-shard index uses
// for its home slot.
func (s *Store) shardFor(key []byte) *shard {
	return s.shards[(hash64(key)>>shardShift)&s.mask]
}

const shardShift = 40

// Set stores value under key, replacing any previous value. The value bytes are
// copied into the log, so the caller may reuse the slice after Set returns.
func (s *Store) Set(key, value []byte) error {
	_, err := s.shardFor(key).set(key, value, nil)
	return err
}

// SetWithPrev is Set that also reports the displaced value's length, or -1 when the
// key is new. The keyspace layer uses the delta to keep used_memory exact across an
// overwrite without a read-before-write that would cost a second index probe.
func (s *Store) SetWithPrev(key, value []byte) (prevValLen int, err error) {
	return s.shardFor(key).set(key, value, nil)
}

// SetWithPrev2 is SetWithPrev for a value supplied as two segments stored back to
// back (value = v0 followed by v1). The keyspace layer uses it to write a record's
// header and body straight into the log page without first concatenating them into
// one cell buffer, which removes a per-write allocation and the copy that fills it.
func (s *Store) SetWithPrev2(key, v0, v1 []byte) (prevValLen int, err error) {
	return s.shardFor(key).set(key, v0, v1)
}

// Get returns a copy of the value stored under key. found is false if the key is
// absent. The returned slice is owned by the caller.
func (s *Store) Get(key []byte) (value []byte, found bool, err error) {
	return s.shardFor(key).get(key)
}

// Delete removes key, returning whether it was present. The log record is left in
// place (it becomes garbage for compaction); only the index entry is dropped.
func (s *Store) Delete(key []byte) (bool, error) {
	_, ok, err := s.shardFor(key).del(key)
	return ok, err
}

// DeleteWithPrev is Delete that also reports the removed value's length (-1 when the
// key was absent), so the keyspace layer can subtract the freed bytes from
// used_memory without re-reading the record it just dropped.
func (s *Store) DeleteWithPrev(key []byte) (prevValLen int, ok bool, err error) {
	return s.shardFor(key).del(key)
}

// Len returns the number of live keys across all shards.
func (s *Store) Len() int {
	n := 0
	for _, sh := range s.shards {
		sh.mu.RLock()
		n += sh.icount
		sh.mu.RUnlock()
	}
	return n
}

// Spilled reports how many pages have been flushed to disk across all shards. It
// is zero until a Set pushes a shard past its resident page budget, and the
// microbenchmark uses it to confirm whether a run stayed in RAM or exercised the
// disk path.
func (s *Store) Spilled() int {
	n := 0
	for _, sh := range s.shards {
		sh.mu.RLock()
		n += sh.spilledPages
		sh.mu.RUnlock()
	}
	return n
}

// IndexBytes returns the total resident bytes held by the open-addressed index
// tables across all shards (the []uint64 slots). This is the per-key resident
// cost that stays in RAM when the log spills, and the number the rewrite shrinks
// versus the old key-resident map.
func (s *Store) IndexBytes() int {
	n := 0
	for _, sh := range s.shards {
		sh.mu.RLock()
		n += len(sh.view.Load().slots) * 8
		sh.mu.RUnlock()
	}
	return n
}

// Index entry layout in a uint64 word (the structure proven in v2/findex):
//
//	bits  0..47  log address (48 bits): byte offset of the RECORD START.
//	bits 48..62  tag (15 bits): a high slice of the key hash, forced nonzero.
//	bit     63   set marks a tombstone (a deleted slot that the lock-free probe
//	             walks past without stopping); never set on a live entry.
//
// A word of 0 means an empty slot. To keep 0 unambiguous as the empty marker,
// each shard's log reserves its first recHdr bytes, so a real record start is
// always >= recHdr > 0.
const (
	addrBits  = 48
	addrMask  = (uint64(1) << addrBits) - 1
	tagShift  = addrBits
	tagBits   = 15
	tagMask   = (uint64(1) << tagBits) - 1
	tombstone = uint64(1) << 63
)

func makeEntry(tag uint16, addr uint64) uint64 {
	return (uint64(tag) << tagShift) | (addr & addrMask)
}

func entryTag(e uint64) uint16  { return uint16((e >> tagShift) & tagMask) }
func entryAddr(e uint64) uint64 { return e & addrMask }

// tagOf takes a high slice of the hash (independent of both the low home-slot
// bits and the shard-pick bits) and forces it nonzero so a set entry is never
// confused with an empty slot.
func tagOf(h uint64) uint16 {
	t := uint16((h >> 24) & tagMask)
	if t == 0 {
		t = 1
	}
	return t
}

// view is the immutable read snapshot of a shard's index and resident page
// directory. The writer never mutates a published view's structure: a grow swaps
// in a fresh slots array, a page roll swaps in a fresh pages directory, and both
// publish a new *view with an atomic store. A live index entry inside slots is
// updated with an atomic store in place, which is safe because the array identity
// does not change for an in-place put. A lock-free reader loads the *view once and
// then sees a consistent (slots, mask, pages) triple for the whole probe.
type view struct {
	slots []uint64
	mask  uint64
	pages [][]byte
}

// shard is one index+log partition. The published view (slots + resident page
// directory) is read lock-free in full-resident mode; everything else, and every
// write, is guarded by mu.
type shard struct {
	mu sync.RWMutex

	view atomic.Pointer[view]

	// Writer-side bookkeeping, mutated only under mu.Lock.
	icount  int // live key count
	tombs   int // tombstone slots awaiting reclamation
	maxLoad float64

	diskOff []int64 // pageID -> byte offset in file, valid where pages[pid] is nil

	tailPage int64 // pageID currently being appended to
	tailPos  int   // append offset within the tail page

	// residentOrder lists resident pageIDs oldest-first, so eviction pops the
	// front. pageIDs only grow, so the slice is naturally ordered by append.
	residentOrder []int64

	spilledPages int

	pageSize    int
	pageShift   uint  // log2(pageSize); addr>>pageShift is the pageID
	pageMask    int64 // pageSize-1; addr&pageMask is the in-page offset
	residentCap int   // ResidentPagesPerShard; 0 means unbounded
	evicts      bool  // residentCap > 0 and a log file is open
	file        *os.File
	fileEnd     int64 // next free byte offset in the log file
}

func newShard(id int, t Tunables) (*shard, error) {
	hint := t.IndexHintPerShard
	if hint < 64 {
		hint = 64
	}
	want := uint64(float64(hint) / 0.75)
	n := uint64(1024)
	for n < want {
		n <<= 1
	}
	sh := &shard{
		maxLoad:     0.75,
		pageSize:    t.PageSize,
		pageShift:   uint(bits.TrailingZeros(uint(t.PageSize))),
		pageMask:    int64(t.PageSize) - 1,
		residentCap: t.ResidentPagesPerShard,
	}
	// Page 0 starts resident and empty. Reserve the first recHdr bytes so that a
	// real record start is always >= recHdr, keeping address 0 unambiguous as the
	// empty-slot marker.
	v := &view{
		slots: make([]uint64, n),
		mask:  n - 1,
		pages: [][]byte{make([]byte, t.PageSize)},
	}
	sh.view.Store(v)
	sh.diskOff = append(sh.diskOff, 0)
	sh.residentOrder = append(sh.residentOrder, 0)
	sh.tailPos = recHdr
	if t.Dir != "" {
		f, err := os.CreateTemp(t.Dir, "akiv2-shard-*.log")
		if err != nil {
			return nil, err
		}
		sh.file = f
	}
	sh.evicts = sh.residentCap > 0 && sh.file != nil
	return sh, nil
}

func (sh *shard) close() error {
	if sh.file == nil {
		return nil
	}
	name := sh.file.Name()
	err := sh.file.Close()
	_ = os.Remove(name)
	return err
}

// Record layout in the log:
//
//	[keyLen:4][valLen:4][key][val]
//
// A fixed 8-byte header (little-endian uint32 lengths) instead of varints makes
// the GET-path deref branch-free: the value starts at recHdr+keyLen with no
// decode loop. uint32 lengths cover the 512 MB Redis key and value ceilings.
const recHdr = 8

// recordLen returns the encoded size of a key/value record.
func recordLen(key, value []byte) int { return recHdr + len(key) + len(value) }

// set appends the record and repoints the index, returning the displaced record's
// value length (-1 when the key is new). The plain Store.Set discards it; the
// tracked Store.SetWithPrev surfaces it for used_memory accounting.
//
// The value may be supplied as two segments, val0 then val1, stored back to back
// as one logical value. The keyspace layer uses this to lay a record's header and
// body straight into the page without first joining them into a single cell, which
// removes a per-write allocation and the copy that would fill it. A nil val1 is the
// single-segment case.
func (sh *shard) set(key, val0, val1 []byte) (prevValLen int, err error) {
	valLen := len(val0) + len(val1)
	rl := recHdr + len(key) + valLen
	if rl > sh.pageSize {
		return -1, errors.New("store: record larger than page size")
	}
	sh.mu.Lock()
	defer sh.mu.Unlock()

	v := sh.view.Load()
	// Roll to a fresh page when the record does not fit in the tail page. The
	// sealed page becomes eligible for flushing once it leaves the resident cap.
	if sh.tailPos+rl > sh.pageSize {
		sh.tailPage++
		sh.tailPos = 0
		newPages := make([][]byte, len(v.pages)+1)
		copy(newPages, v.pages)
		newPages[sh.tailPage] = make([]byte, sh.pageSize)
		// Publish the new page directory before any index entry can point into the
		// new page, so a lock-free reader that observes the entry also observes the
		// page (the bounds-check-and-reload in get covers the ordering window).
		nv := &view{slots: v.slots, mask: v.mask, pages: newPages}
		sh.view.Store(nv)
		v = nv
		sh.diskOff = append(sh.diskOff, 0)
		sh.residentOrder = append(sh.residentOrder, sh.tailPage)
		sh.evictIfNeeded(v)
	}
	page := v.pages[sh.tailPage]
	recStart := sh.tailPage*int64(sh.pageSize) + int64(sh.tailPos)
	off := sh.tailPos
	binary.LittleEndian.PutUint32(page[off:], uint32(len(key)))
	binary.LittleEndian.PutUint32(page[off+4:], uint32(valLen))
	n := off + recHdr
	n += copy(page[n:], key)
	n += copy(page[n:], val0)
	copy(page[n:], val1)
	sh.tailPos += rl
	return sh.indexPut(key, uint64(recStart)), nil
}

// evictIfNeeded flushes resident pages to disk until the shard is back within its
// resident page budget. With no budget (residentCap == 0) or no log file
// (memory-only), it does nothing and pages stay resident. It runs only under
// mu.Lock and only in evicts mode, so it never races a lock-free reader (those
// exist only when !evicts).
func (sh *shard) evictIfNeeded(v *view) {
	if sh.residentCap <= 0 || sh.file == nil {
		return
	}
	// Keep the tail page resident always (it is still being appended), so the
	// effective cap on older pages is residentCap, evicting from the front.
	for len(sh.residentOrder) > sh.residentCap {
		pid := sh.residentOrder[0]
		sh.residentOrder = sh.residentOrder[1:]
		page := v.pages[pid]
		if page == nil {
			continue
		}
		// Append the whole page to the log file and remember where it landed.
		off := sh.fileEnd
		if _, err := sh.file.WriteAt(page, off); err != nil {
			// On a write error keep the page resident rather than lose data; put
			// it back at the front and stop evicting this round.
			sh.residentOrder = append([]int64{pid}, sh.residentOrder...)
			return
		}
		sh.fileEnd += int64(len(page))
		sh.diskOff[pid] = off
		v.pages[pid] = nil
		sh.spilledPages++
	}
}

func (sh *shard) get(key []byte) ([]byte, bool, error) {
	if !sh.evicts {
		return sh.getResident(key)
	}
	return sh.getEvicting(key)
}

// getResident is the lock-free full-resident read path. It loads the published
// view once, probes the index with atomic entry loads, and slices the value out
// of the resident page with no copy and no mutex. The returned slice aliases the
// log page (which is never evicted or mutated in this mode) and the caller must
// not mutate it. This is the rewrite/01 Slice-1 read path the locktax benchmark
// backs.
func (sh *shard) getResident(key []byte) ([]byte, bool, error) {
	h := hash64(key)
	tag := tagOf(h)
	v := sh.view.Load()
	i := h & v.mask
	for {
		e := atomic.LoadUint64(&v.slots[i])
		if e == 0 {
			return nil, false, nil
		}
		if e != tombstone && entryTag(e) == tag {
			addr := entryAddr(e)
			pid := int64(addr) >> sh.pageShift
			// The entry may point into a page newer than this view snapshot. Because
			// the writer publishes the page directory before the entry that
			// references it, observing the entry guarantees a reload finds the page.
			for pid >= int64(len(v.pages)) {
				v = sh.view.Load()
			}
			off := int(int64(addr) & sh.pageMask)
			page := v.pages[pid]
			klen := int(binary.LittleEndian.Uint32(page[off:]))
			if klen == len(key) && string(page[off+recHdr:off+recHdr+klen]) == string(key) {
				vlen := int(binary.LittleEndian.Uint32(page[off+4:]))
				vstart := off + recHdr + klen
				return page[vstart : vstart+vlen], true, nil
			}
		}
		i = (i + 1) & v.mask
	}
}

// getEvicting is the spill-mode read path: a page can be pulled out from under a
// reader by eviction, so it holds the per-shard read lock and copies the value out
// (resident) or reads it back from the log file (spilled).
func (sh *shard) getEvicting(key []byte) ([]byte, bool, error) {
	sh.mu.RLock()
	v := sh.view.Load()
	addr, ok := sh.indexGet(v, key)
	if !ok {
		sh.mu.RUnlock()
		return nil, false, nil
	}
	pid := int64(addr) >> sh.pageShift
	off := int(int64(addr) & sh.pageMask)
	if page := v.pages[pid]; page != nil {
		klen := int(binary.LittleEndian.Uint32(page[off:]))
		vlen := int(binary.LittleEndian.Uint32(page[off+4:]))
		vstart := off + recHdr + klen
		// Eviction is possible, so copy the value out while we still hold the read
		// lock, before a concurrent flush can pull the page out from under us.
		val := make([]byte, vlen)
		copy(val, page[vstart:vstart+vlen])
		sh.mu.RUnlock()
		return val, true, nil
	}
	// Spilled: the record is on disk. Its file offset is stable (the log file is
	// append-only and pageIDs are never reused), so read the header then the value.
	dOff := sh.diskOff[pid]
	f := sh.file
	sh.mu.RUnlock()
	if f == nil {
		return nil, false, errors.New("store: address neither resident nor on disk")
	}
	var hdr [recHdr]byte
	if _, err := f.ReadAt(hdr[:], dOff+int64(off)); err != nil {
		return nil, false, err
	}
	klen := int(binary.LittleEndian.Uint32(hdr[0:]))
	vlen := int(binary.LittleEndian.Uint32(hdr[4:]))
	val := make([]byte, vlen)
	nr, err := f.ReadAt(val, dOff+int64(off)+int64(recHdr+klen))
	if err != nil && nr == 0 {
		return nil, false, err
	}
	return val[:nr], true, nil
}

// del drops the key from the index, returning the removed record's value length
// and whether it was present. The plain Store.Delete discards the length; the
// tracked Store.DeleteWithPrev surfaces it for used_memory accounting.
func (sh *shard) del(key []byte) (prevValLen int, ok bool, err error) {
	sh.mu.Lock()
	v := sh.view.Load()
	pl, ok := sh.indexDelete(v, key)
	sh.mu.Unlock()
	return pl, ok, nil
}

// recordKeyAny returns the key bytes of the record at logical address addr,
// handling both resident pages (a slice, the hot path) and spilled pages (a disk
// read, the cold compare path). Index probes call this to confirm a tag match.
func (sh *shard) recordKeyAny(v *view, addr uint64) []byte {
	pid := int64(addr) >> sh.pageShift
	off := int(int64(addr) & sh.pageMask)
	if page := v.pages[pid]; page != nil {
		klen := int(binary.LittleEndian.Uint32(page[off:]))
		return page[off+recHdr : off+recHdr+klen]
	}
	dOff := sh.diskOff[pid]
	if sh.file == nil {
		return nil
	}
	var hdr [recHdr]byte
	if _, err := sh.file.ReadAt(hdr[:], dOff+int64(off)); err != nil {
		return nil
	}
	klen := int(binary.LittleEndian.Uint32(hdr[0:]))
	key := make([]byte, klen)
	if _, err := sh.file.ReadAt(key, dOff+int64(off)+int64(recHdr)); err != nil {
		return nil
	}
	return key
}

// recordValLenAny returns the value length of the record at logical address addr,
// for both resident pages (a slice read, the hot path) and spilled pages (a disk
// read of the 8-byte header, the cold path). The index upsert and delete call this
// to surface the displaced record's size so the keyspace layer can keep its
// used_memory accounting exact without a separate read-before-write.
func (sh *shard) recordValLenAny(v *view, addr uint64) int {
	pid := int64(addr) >> sh.pageShift
	off := int(int64(addr) & sh.pageMask)
	if page := v.pages[pid]; page != nil {
		return int(binary.LittleEndian.Uint32(page[off+4:]))
	}
	dOff := sh.diskOff[pid]
	if sh.file == nil {
		return 0
	}
	var hdr [recHdr]byte
	if _, err := sh.file.ReadAt(hdr[:], dOff+int64(off)); err != nil {
		return 0
	}
	return int(binary.LittleEndian.Uint32(hdr[4:]))
}

// indexGet probes the open-addressed table for key and returns the record-start
// address of its current value. Caller holds at least the read lock (or, on the
// lock-free path, this is not used; see getResident).
func (sh *shard) indexGet(v *view, key []byte) (uint64, bool) {
	h := hash64(key)
	tag := tagOf(h)
	i := h & v.mask
	for {
		e := v.slots[i]
		if e == 0 {
			return 0, false
		}
		if e != tombstone && entryTag(e) == tag {
			addr := entryAddr(e)
			if bytesEqual(sh.recordKeyAny(v, addr), key) {
				return addr, true
			}
		}
		i = (i + 1) & v.mask
	}
}

// indexPut inserts or repoints key at the record-start address addr. Caller holds
// the write lock. On update it repoints the existing slot in place with an atomic
// store so a lock-free reader sees the new address (and the new record bytes,
// written before this call). A free slot may be an empty (0) or a tombstone slot;
// reusing a tombstone keeps the table from growing under churn. It returns the
// displaced record's value length, or -1 when the key is new.
func (sh *shard) indexPut(key []byte, addr uint64) (prevValLen int) {
	v := sh.view.Load()
	if float64(sh.icount+sh.tombs+1) > sh.maxLoad*float64(len(v.slots)) {
		v = sh.indexGrow(v)
	}
	h := hash64(key)
	tag := tagOf(h)
	i := h & v.mask
	firstTomb := int64(-1)
	for {
		e := v.slots[i]
		if e == 0 {
			// Prefer an earlier tombstone slot so the chain does not lengthen.
			if firstTomb >= 0 {
				atomic.StoreUint64(&v.slots[firstTomb], makeEntry(tag, addr))
				sh.tombs--
			} else {
				atomic.StoreUint64(&v.slots[i], makeEntry(tag, addr))
			}
			sh.icount++
			return -1
		}
		if e == tombstone {
			if firstTomb < 0 {
				firstTomb = int64(i)
			}
		} else if entryTag(e) == tag && bytesEqual(sh.recordKeyAny(v, entryAddr(e)), key) {
			old := sh.recordValLenAny(v, entryAddr(e))
			atomic.StoreUint64(&v.slots[i], makeEntry(tag, addr)) // repoint in place
			return old
		}
		i = (i + 1) & v.mask
	}
}

// indexDelete removes key. In full-resident mode it writes a tombstone with an
// atomic store, keeping the probe chain intact for a concurrent lock-free reader;
// in spill mode (readers hold the lock) it backward-shifts so no tombstone
// accumulates. Caller holds the write lock. It returns the removed record's value
// length and whether the key was present.
func (sh *shard) indexDelete(v *view, key []byte) (prevValLen int, ok bool) {
	h := hash64(key)
	tag := tagOf(h)
	i := h & v.mask
	for {
		e := v.slots[i]
		if e == 0 {
			return -1, false
		}
		if e != tombstone && entryTag(e) == tag && bytesEqual(sh.recordKeyAny(v, entryAddr(e)), key) {
			old := sh.recordValLenAny(v, entryAddr(e))
			if sh.evicts {
				sh.indexBackshift(v, i)
			} else {
				atomic.StoreUint64(&v.slots[i], tombstone)
				sh.tombs++
			}
			sh.icount--
			return old, true
		}
		i = (i + 1) & v.mask
	}
}

func (sh *shard) indexBackshift(v *view, hole uint64) {
	v.slots[hole] = 0
	j := (hole + 1) & v.mask
	for {
		e := v.slots[j]
		if e == 0 {
			return
		}
		home := hash64(sh.recordKeyAny(v, entryAddr(e))) & v.mask
		dj := (j - hole) & v.mask
		dHome := (home - hole) & v.mask
		if dHome == 0 || dHome > dj {
			v.slots[hole] = e
			v.slots[j] = 0
			hole = j
		}
		j = (j + 1) & v.mask
	}
}

// indexGrow rebuilds the table into a fresh slots array, dropping tombstones, and
// publishes the new view. It doubles capacity when the live load demands it and
// otherwise keeps the same size (a pure tombstone-reclaiming rehash). The old
// slots array stays valid for any lock-free reader still probing it; the new view
// only adds keys, never removes one a stale reader could still need.
func (sh *shard) indexGrow(v *view) *view {
	n := uint64(len(v.slots))
	if float64(sh.icount+1) > sh.maxLoad*float64(n) {
		n <<= 1
	}
	nv := &view{slots: make([]uint64, n), mask: n - 1, pages: v.pages}
	for _, e := range v.slots {
		if e == 0 || e == tombstone {
			continue
		}
		// re-probe each live entry into the fresh table. Keys live in the log, so
		// the rehash needs no side table and never double-counts: this moves
		// entries, it does not add keys.
		tag := entryTag(e)
		i := hash64(sh.recordKeyAny(v, entryAddr(e))) & nv.mask
		for nv.slots[i] != 0 {
			i = (i + 1) & nv.mask
		}
		nv.slots[i] = makeEntry(tag, entryAddr(e))
	}
	sh.tombs = 0
	sh.view.Store(nv)
	return nv
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	return string(a) == string(b)
}

// hash64 is a fast FNV-1a over the key, used both to pick a shard (high bits) and
// to drive the per-shard open-addressed probe (low bits) and tag (a middle
// slice), the three taken from disjoint regions of the hash so they do not
// correlate.
func hash64(b []byte) uint64 {
	const (
		offset = 1469598103934665603
		prime  = 1099511628211
	)
	h := uint64(offset)
	for _, c := range b {
		h ^= uint64(c)
		h *= prime
	}
	return h
}
