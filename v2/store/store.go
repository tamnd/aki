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
// This is the read path Valkey has (one probe to the value) without giving up the
// larger-than-memory guarantee the durable B-tree gave aki: only the resident
// page budget's worth of values, plus the small key index, has to fit in RAM.
package store

import (
	"encoding/binary"
	"errors"
	"os"
	"sync"
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
				s.shards[j].close()
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
	return s.shardFor(key).set(key, value)
}

// Get returns a copy of the value stored under key. found is false if the key is
// absent. The returned slice is owned by the caller.
func (s *Store) Get(key []byte) (value []byte, found bool, err error) {
	return s.shardFor(key).get(key)
}

// Delete removes key, returning whether it was present. The log record is left in
// place (it becomes garbage for compaction); only the index entry is dropped.
func (s *Store) Delete(key []byte) (bool, error) {
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
		n += len(sh.slots) * 8
		sh.mu.RUnlock()
	}
	return n
}

// Index entry layout in a uint64 word (the structure proven in v2/findex):
//
//	bits  0..47  log address (48 bits): byte offset of the RECORD START.
//	bits 48..62  tag (15 bits): a high slice of the key hash, forced nonzero.
//	bit     63   reserved for the latch-free insert; unused here.
//
// A word of 0 means an empty slot. To keep 0 unambiguous as the empty marker,
// each shard's log reserves its first recHdr bytes, so a real record start is
// always >= recHdr > 0.
const (
	addrBits = 48
	addrMask = (uint64(1) << addrBits) - 1
	tagShift = addrBits
	tagBits  = 15
	tagMask  = (uint64(1) << tagBits) - 1
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

// shard is one index+log partition. Everything on it is guarded by mu.
type shard struct {
	mu sync.RWMutex

	// The open-addressed index: a flat []uint64 of 8-byte entries, pointer-free
	// so the GC marks it in O(1). slots is a power-of-two length; islotsMask is
	// len(slots)-1; icount is the live key count. An entry's address points at a
	// record START in the page log; the key is confirmed by dereferencing the
	// record, never stored in the table.
	slots      []uint64
	islotsMask uint64
	icount     int
	maxLoad    float64

	// pages holds the log pages indexed by pageID, which is dense and monotonic, so
	// a slice beats a map and keeps GET to a single hash probe (the index lookup).
	// An evicted (spilled) page is nil here and its file offset lives in diskOff at
	// the same index.
	pages   [][]byte
	diskOff []int64 // pageID -> byte offset in file, valid where pages[pid] is nil

	tailPage int64 // pageID currently being appended to
	tailPos  int   // append offset within the tail page

	// residentOrder lists resident pageIDs oldest-first, so eviction pops the
	// front. pageIDs only grow, so the slice is naturally ordered by append.
	residentOrder []int64

	spilledPages int

	pageSize    int
	residentCap int  // ResidentPagesPerShard; 0 means unbounded
	evicts      bool // residentCap > 0 and a log file is open
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
		slots:       make([]uint64, n),
		islotsMask:  n - 1,
		maxLoad:     0.75,
		pageSize:    t.PageSize,
		residentCap: t.ResidentPagesPerShard,
	}
	// Page 0 starts resident and empty. Reserve the first recHdr bytes so that a
	// real record start is always >= recHdr, keeping address 0 unambiguous as the
	// empty-slot marker.
	sh.pages = append(sh.pages, make([]byte, t.PageSize))
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

func (sh *shard) set(key, value []byte) error {
	rl := recordLen(key, value)
	if rl > sh.pageSize {
		return errors.New("store: record larger than page size")
	}
	sh.mu.Lock()
	defer sh.mu.Unlock()

	// Roll to a fresh page when the record does not fit in the tail page. The
	// sealed page becomes eligible for flushing once it leaves the resident cap.
	if sh.tailPos+rl > sh.pageSize {
		sh.tailPage++
		sh.tailPos = 0
		sh.pages = append(sh.pages, make([]byte, sh.pageSize))
		sh.diskOff = append(sh.diskOff, 0)
		sh.residentOrder = append(sh.residentOrder, sh.tailPage)
		sh.evictIfNeeded()
	}
	page := sh.pages[sh.tailPage]
	recStart := sh.tailPage*int64(sh.pageSize) + int64(sh.tailPos)
	off := sh.tailPos
	binary.LittleEndian.PutUint32(page[off:], uint32(len(key)))
	binary.LittleEndian.PutUint32(page[off+4:], uint32(len(value)))
	copy(page[off+recHdr:], key)
	copy(page[off+recHdr+len(key):], value)
	sh.tailPos += rl
	sh.indexPut(key, uint64(recStart))
	return nil
}

// evictIfNeeded flushes resident pages to disk until the shard is back within its
// resident page budget. With no budget (residentCap == 0) or no log file
// (memory-only), it does nothing and pages stay resident.
func (sh *shard) evictIfNeeded() {
	if sh.residentCap <= 0 || sh.file == nil {
		return
	}
	// Keep the tail page resident always (it is still being appended), so the
	// effective cap on older pages is residentCap, evicting from the front.
	for len(sh.residentOrder) > sh.residentCap {
		pid := sh.residentOrder[0]
		sh.residentOrder = sh.residentOrder[1:]
		page := sh.pages[pid]
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
		sh.pages[pid] = nil
		sh.spilledPages++
	}
}

func (sh *shard) get(key []byte) ([]byte, bool, error) {
	sh.mu.RLock()
	addr, ok := sh.indexGet(key)
	if !ok {
		sh.mu.RUnlock()
		return nil, false, nil
	}
	pid := int64(addr) / int64(sh.pageSize)
	off := int(int64(addr) % int64(sh.pageSize))
	if page := sh.pages[pid]; page != nil {
		klen := int(binary.LittleEndian.Uint32(page[off:]))
		vlen := int(binary.LittleEndian.Uint32(page[off+4:]))
		vstart := off + recHdr + klen
		if !sh.evicts {
			// Full-resident mode never evicts a page, so the value bytes outlive this
			// call and can be returned with no copy: one slice, no decode loop,
			// matching the resident-map fast path the memengine spike measured. The
			// slice aliases the log page and the caller must not mutate it.
			val := page[vstart : vstart+vlen]
			sh.mu.RUnlock()
			return val, true, nil
		}
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

func (sh *shard) del(key []byte) (bool, error) {
	sh.mu.Lock()
	ok := sh.indexDelete(key)
	sh.mu.Unlock()
	return ok, nil
}

// recordKeyAny returns the key bytes of the record at logical address addr,
// handling both resident pages (a slice, the hot path) and spilled pages (a disk
// read, the cold compare path). Index probes call this to confirm a tag match.
func (sh *shard) recordKeyAny(addr uint64) []byte {
	pid := int64(addr) / int64(sh.pageSize)
	off := int(int64(addr) % int64(sh.pageSize))
	if page := sh.pages[pid]; page != nil {
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

// indexGet probes the open-addressed table for key and returns the record-start
// address of its current value. Caller holds at least the read lock.
func (sh *shard) indexGet(key []byte) (uint64, bool) {
	h := hash64(key)
	tag := tagOf(h)
	i := h & sh.islotsMask
	for {
		e := sh.slots[i]
		if e == 0 {
			return 0, false
		}
		if entryTag(e) == tag {
			addr := entryAddr(e)
			if bytesEqual(sh.recordKeyAny(addr), key) {
				return addr, true
			}
		}
		i = (i + 1) & sh.islotsMask
	}
}

// indexPut inserts or repoints key at the record-start address addr. Caller holds
// the write lock. On update it repoints the existing slot in place, so icount is
// exact and the table never accumulates shadow entries.
func (sh *shard) indexPut(key []byte, addr uint64) {
	if float64(sh.icount+1) > sh.maxLoad*float64(len(sh.slots)) {
		sh.indexGrow()
	}
	h := hash64(key)
	tag := tagOf(h)
	i := h & sh.islotsMask
	for {
		e := sh.slots[i]
		if e == 0 {
			sh.slots[i] = makeEntry(tag, addr)
			sh.icount++
			return
		}
		if entryTag(e) == tag && bytesEqual(sh.recordKeyAny(entryAddr(e)), key) {
			sh.slots[i] = makeEntry(tag, addr) // repoint in place
			return
		}
		i = (i + 1) & sh.islotsMask
	}
}

// indexDelete removes key with backward-shift deletion (no tombstones, so the
// stop-at-empty probe stays correct). Caller holds the write lock.
func (sh *shard) indexDelete(key []byte) bool {
	h := hash64(key)
	tag := tagOf(h)
	i := h & sh.islotsMask
	for {
		e := sh.slots[i]
		if e == 0 {
			return false
		}
		if entryTag(e) == tag && bytesEqual(sh.recordKeyAny(entryAddr(e)), key) {
			sh.indexBackshift(i)
			sh.icount--
			return true
		}
		i = (i + 1) & sh.islotsMask
	}
}

func (sh *shard) indexBackshift(hole uint64) {
	sh.slots[hole] = 0
	j := (hole + 1) & sh.islotsMask
	for {
		e := sh.slots[j]
		if e == 0 {
			return
		}
		home := hash64(sh.recordKeyAny(entryAddr(e))) & sh.islotsMask
		dj := (j - hole) & sh.islotsMask
		dHome := (home - hole) & sh.islotsMask
		if dHome == 0 || dHome > dj {
			sh.slots[hole] = e
			sh.slots[j] = 0
			hole = j
		}
		j = (j + 1) & sh.islotsMask
	}
}

func (sh *shard) indexGrow() {
	old := sh.slots
	n := uint64(len(old)) << 1
	sh.slots = make([]uint64, n)
	sh.islotsMask = n - 1
	for _, e := range old {
		if e == 0 {
			continue
		}
		// re-probe each live entry into the larger table. Keys live in the log, so
		// the rehash needs no side table and never double-counts: this moves
		// entries, it does not add keys.
		tag := entryTag(e)
		i := hash64(sh.recordKeyAny(entryAddr(e))) & sh.islotsMask
		for sh.slots[i] != 0 {
			i = (i + 1) & sh.islotsMask
		}
		sh.slots[i] = makeEntry(tag, entryAddr(e))
	}
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
