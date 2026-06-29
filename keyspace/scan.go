package keyspace

import (
	"bytes"
	"cmp"
	"slices"

	"github.com/tamnd/aki/encoding"
)

// ScanEntry is one live key visited by Keys or Scan, paired with its value type
// so a caller can apply a TYPE filter without a second lookup.
type ScanEntry struct {
	Key  []byte
	Type uint8
}

// Keys returns every live key in the DB with its type, in B-tree order. Expired
// keys are skipped. The order is the composite-key order from value.go, which is
// what KEYS and RANDOMKEY treat as unspecified.
func (db *DB) Keys() ([]ScanEntry, error) {
	var out []ScanEntry
	err := db.forEachLive(func(ck []byte, h ValueHeader) error {
		out = append(out, ScanEntry{Key: copyRaw(ck), Type: h.Type})
		return nil
	})
	return out, err
}

// scanSlotBudget caps how many hash slots one Scan call examines before it
// yields with a resumable cursor. It is the safety valve for a sparsely
// populated slot range: without it a single call over a near-empty keyspace
// would seek every one of the numSlots slots before returning. A call still
// stops the moment it has gathered count keys, so on a populated keyspace it
// almost never reaches this cap, and an empty shard short-circuits on the nil
// tree without a seek, so the cap costs at most one O(log n) seek per empty
// slot it does touch.
const scanSlotBudget = 2048

// Scan returns up to count live keys at or above the cursor, plus the cursor to
// resume from. A cursor of 0 starts a new scan and a returned cursor of 0 means
// the keyspace is exhausted.
//
// The cursor is a hash slot number in [0, numSlots]. Every key's slot is a pure
// function of the key (HashSlot), so a key present for the whole scan lives in
// one fixed slot and is returned in exactly the one call whose window covers
// that slot; a key deleted mid-scan only drops itself, and a key inserted into
// an already-passed slot is skipped, both within the SCAN contract. A call seeks
// straight to its first slot and walks at most scanSlotBudget slots, so its work
// and memory are bounded by the page it returns, not the size of the keyspace:
// no whole-keyspace materialize, no O(n log n) sort, no O(n) memory spike on a
// keyspace larger than RAM.
//
// The hybrid-log engine has no slot-ordered index (its keys are not slot-prefixed
// and Each is unordered), so that path stays on the hash-order materialize in
// hlScan. The hybrid index is resident, so its scan is bounded by RAM by
// construction; the unbounded case this guards is the on-disk B-tree engine.
func (db *DB) Scan(cursor uint64, count int) (uint64, []ScanEntry, error) {
	if count <= 0 {
		count = 10
	}
	if db.newHL != nil {
		return db.hlScan(cursor, count)
	}
	slot := min(cursor, numSlots)
	var out []ScanEntry
	seeks := 0
	for slot < numSlots && len(out) < count && seeks < scanSlotBudget {
		did, err := db.scanSlot(uint16(slot), &out)
		if err != nil {
			return 0, nil, err
		}
		if did {
			// Only a slot whose shard holds a tree costs a seek; an empty shard
			// short-circuits for free, so the budget bounds real work and an empty
			// keyspace sweeps all numSlots slots in one call and finishes at 0.
			seeks++
		}
		slot++
	}
	if slot >= numSlots {
		return 0, out, nil
	}
	return slot, out, nil
}

// scanSlot appends every live key in hash slot s to out, in B-tree order, and
// reports whether it performed a seek (true unless the shard has no tree). The
// slot's keys share an identical 2-byte composite-key prefix (the little-endian
// slot), so they are contiguous in shard s&shardMask: the cursor seeks that
// prefix and walks until the prefix changes. The walk uses the arena cursor and
// copies each retained key, since the arena bytes alias a reused buffer.
func (db *DB) scanSlot(s uint16, out *[]ScanEntry) (bool, error) {
	shard := int(s) & shardMask
	db.shards[shard].mu.RLock()
	defer db.shards[shard].mu.RUnlock()
	t := db.loadShardTree(shard)
	if t == nil {
		return false, nil
	}
	var seek [6]byte
	encoding.PutU16(seek[:2], s) // bytes 2..6 (the length prefix) stay zero
	c := t.Cursor()
	c.UseArena()
	if err := c.Seek(seek[:]); err != nil {
		return true, err
	}
	for c.Valid() {
		k := c.Key()
		if len(k) < 2 || encoding.U16(k) != s {
			break
		}
		h, _, ok := parseHeader(c.Value())
		if ok && !db.expired(h) {
			*out = append(*out, ScanEntry{Key: copyRaw(k), Type: h.Type})
		}
		if err := c.Next(); err != nil {
			return true, err
		}
	}
	return true, nil
}

// hlScan is the hybrid-log engine's Scan: it materializes every live key in hash
// order and returns the count-sized window at or above the cursor. The hybrid
// index is resident and its Each enumeration is unordered with no resumable
// position, so a slot-window cursor is not available on this path; the hash-order
// window keeps the scan stateless and complete the same way the B-tree path's
// slot window does. Memory is bounded by RAM because the index is resident.
//
// The cursor is the FNV-1a hash of a key's composite key truncated to 48 bits. A
// key present for the whole scan has a fixed hash, so it is returned in exactly
// the one call whose hash window covers it, and a key deleted mid-scan only drops
// itself rather than cutting the scan short.
func (db *DB) hlScan(cursor uint64, count int) (uint64, []ScanEntry, error) {
	type cand struct {
		entry ScanEntry
		hash  uint64
		ck    []byte
	}
	var cands []cand
	err := db.forEachLive(func(ck []byte, h ValueHeader) error {
		hv := fnv48(ck)
		if hv < cursor {
			return nil
		}
		cands = append(cands, cand{
			entry: ScanEntry{Key: copyRaw(ck), Type: h.Type},
			hash:  hv,
			ck:    bytes.Clone(ck),
		})
		return nil
	})
	if err != nil {
		return 0, nil, err
	}
	slices.SortFunc(cands, func(a, b cand) int {
		if a.hash != b.hash {
			return cmp.Compare(a.hash, b.hash)
		}
		return bytes.Compare(a.ck, b.ck)
	})

	take := count
	if take < len(cands) {
		// Keep a whole hash group together so the next call's window does not
		// skip keys that share the boundary hash.
		last := cands[take-1].hash
		for take < len(cands) && cands[take].hash == last {
			take++
		}
	}
	if take >= len(cands) {
		take = len(cands)
	}

	out := make([]ScanEntry, take)
	for i := range take {
		out[i] = cands[i].entry
	}
	if take == len(cands) {
		return 0, out, nil
	}
	return cands[take-1].hash + 1, out, nil
}

// forEachLive calls fn for every unexpired key across all shards. Within each
// shard the composite key passed to fn is in B-tree byte order, which is the
// memcmp order of the little-endian slot prefix then the key, so a slot's keys
// are contiguous but the slots themselves are not visited in numeric order. Keys
// route to shards by HashSlot(key)&shardMask, so shard s holds the slots
// congruent to s modulo NumShards, not a contiguous slot range. Callers that
// need numeric slot order (Scan) drive the slots themselves rather than relying
// on this iteration order.
func (db *DB) forEachLive(fn func(ck []byte, h ValueHeader) error) error {
	if db.newHL != nil {
		return db.hlForEachLive(fn)
	}
	for s := range NumShards {
		db.shards[s].mu.RLock()
		t := db.loadShardTree(s)
		if t == nil {
			db.shards[s].mu.RUnlock()
			continue
		}
		c := t.Cursor()
		if err := c.First(); err != nil {
			db.shards[s].mu.RUnlock()
			return err
		}
		for c.Valid() {
			h, _, ok := parseHeader(c.Value())
			if ok && !db.expired(h) {
				if err := fn(c.Key(), h); err != nil {
					db.shards[s].mu.RUnlock()
					return err
				}
			}
			if err := c.Next(); err != nil {
				db.shards[s].mu.RUnlock()
				return err
			}
		}
		db.shards[s].mu.RUnlock()
	}
	return nil
}

// copyRaw extracts the original key from a composite key into a fresh slice the
// caller owns.
func copyRaw(ck []byte) []byte {
	return bytes.Clone(rawKey(ck))
}

// fnv48 is the 48-bit truncation of FNV-1a over a composite key, used as the
// SCAN cursor hint. The mixing is inlined rather than going through
// fnv.New64a(), which heap-allocates a hasher on every call; SCAN hashes every
// live key per call, so the allocation showed up as O(n) garbage per scan.
func fnv48(b []byte) uint64 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	h := uint64(offset64)
	for _, c := range b {
		h ^= uint64(c)
		h *= prime64
	}
	return h & 0xFFFFFFFFFFFF
}
