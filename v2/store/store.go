// Package store is the aki v2 storage engine: a resident hash index over a
// per-shard hybrid log, modelled on Microsoft FASTER and Garnet.
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
//     byte offset into that shard's log. The index is a Go map, so the keys are
//     resident (a known slice-1 limit, see the package doc on residency); the
//     values are not.
//   - The log is a sequence of fixed-size pages. Recent pages live in RAM (the
//     mutable tail plus the read-only region); once the resident page budget is
//     exceeded the oldest resident page is flushed to the log file on disk and
//     dropped from memory (the stable region). A logical address therefore tells
//     a reader, with no extra lookup, whether the record is in RAM or on disk.
//   - GET hashes the key, takes the shard read lock, reads the address, and either
//     copies the value out of the resident page or, for a spilled address, reads
//     it back from the log file with one pread outside the lock.
//   - SET appends a new record to the tail page under the shard write lock and
//     points the index at it. When the tail page fills, it is sealed and a fresh
//     page begins; when the resident budget is exceeded, the oldest page spills.
//
// This is the read path Valkey has (one probe to the value) without giving up the
// larger-than-memory guarantee the durable B-tree gave aki: only the resident
// page budget's worth of values, plus the key index, has to fit in RAM.
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

// shardFor returns the shard that owns a key.
func (s *Store) shardFor(key []byte) *shard {
	return s.shards[hash64(key)&s.mask]
}

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

// Len returns the number of live keys across all shards.
func (s *Store) Len() int {
	n := 0
	for _, sh := range s.shards {
		sh.mu.RLock()
		n += len(sh.index)
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

// valLoc points the index straight at a value in the log: addr is the logical
// address of the value's first byte, vlen its length. Pointing at the value
// instead of the record header lets a resident GET return the value with no
// varint decode.
type valLoc struct {
	addr int64
	vlen uint32
}

// shard is one index+log partition. Everything on it is guarded by mu.
type shard struct {
	mu sync.RWMutex

	// index maps a key to the location of its current value. addr is the logical
	// address of the value bytes themselves (not the record header), so a resident
	// GET slices straight to the value with no record decode; vlen is the value
	// length. A logical address is pageID*pageSize + offsetWithinPage.
	index map[string]valLoc

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

	pageSize     int
	residentCap  int    // ResidentPagesPerShard; 0 means unbounded
	evicts       bool   // residentCap > 0 and a log file is open
	file         *os.File
	fileEnd      int64 // next free byte offset in the log file
	scratch      []byte // reusable record-encode buffer, only touched under mu
}

func newShard(id int, t Tunables) (*shard, error) {
	sh := &shard{
		index:       make(map[string]valLoc),
		pageSize:    t.PageSize,
		residentCap: t.ResidentPagesPerShard,
		scratch:     make([]byte, 0, 256),
	}
	// Page 0 starts resident and empty.
	sh.pages = append(sh.pages, make([]byte, t.PageSize))
	sh.diskOff = append(sh.diskOff, 0)
	sh.residentOrder = append(sh.residentOrder, 0)
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

// recordLen returns the encoded size of a key/value record.
func recordLen(key, value []byte) int {
	return uvarintLen(uint64(len(key))) + len(key) +
		uvarintLen(uint64(len(value))) + len(value)
}

// encodeRecord writes a key/value record into dst (which must be at least
// recordLen long) and returns the number of bytes written.
func encodeRecord(dst, key, value []byte) int {
	n := binary.PutUvarint(dst, uint64(len(key)))
	n += copy(dst[n:], key)
	n += binary.PutUvarint(dst[n:], uint64(len(value)))
	n += copy(dst[n:], value)
	return n
}

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
	n := encodeRecord(page[sh.tailPos:], key, value)
	sh.tailPos += n
	// The value sits after the key-length varint, the key, and the value-length
	// varint. Point the index straight at it so reads skip the record decode.
	valOff := uvarintLen(uint64(len(key))) + len(key) + uvarintLen(uint64(len(value)))
	sh.index[string(key)] = valLoc{addr: recStart + int64(valOff), vlen: uint32(len(value))}
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
	loc, ok := sh.index[string(key)]
	if !ok {
		sh.mu.RUnlock()
		return nil, false, nil
	}
	pid := loc.addr / int64(sh.pageSize)
	off := int(loc.addr % int64(sh.pageSize))
	if page := sh.pages[pid]; page != nil {
		if !sh.evicts {
			// Full-resident mode never evicts a page, so the value bytes outlive this
			// call and can be returned with no copy: the index already points at the
			// value, so this is a single slice with no decode, matching the resident-
			// map fast path the memengine spike measured. The slice aliases the log
			// page and the caller must not mutate it.
			val := page[off : off+int(loc.vlen)]
			sh.mu.RUnlock()
			return val, true, nil
		}
		// Eviction is possible, so copy the value out while we still hold the read
		// lock, before a concurrent flush can pull the page out from under us.
		val := make([]byte, loc.vlen)
		copy(val, page[off:off+int(loc.vlen)])
		sh.mu.RUnlock()
		return val, true, nil
	}
	// Spilled: the page is on disk. Its file offset is stable (the log file is
	// append-only and pageIDs are never reused), so read exactly the value bytes
	// back outside the lock.
	dOff := sh.diskOff[pid]
	f := sh.file
	sh.mu.RUnlock()
	if f == nil {
		return nil, false, errors.New("store: address neither resident nor on disk")
	}
	val := make([]byte, loc.vlen)
	nr, err := f.ReadAt(val, dOff+int64(off))
	if err != nil && nr == 0 {
		return nil, false, err
	}
	return val[:nr], true, nil
}

// uvarintLen returns the number of bytes binary.PutUvarint would write for x.
func uvarintLen(x uint64) int {
	n := 1
	for x >= 0x80 {
		x >>= 7
		n++
	}
	return n
}

// hash64 is a fast FNV-1a over the key, used only to pick a shard. The index map
// does its own hashing for lookups.
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
