package keyspace

import (
	"bytes"
	"cmp"
	"hash/fnv"
	"slices"
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

// Scan returns up to count live keys at or above the cursor in hash order, plus
// the cursor to resume from. A cursor of 0 starts a new scan and a returned
// cursor of 0 means the keyspace is exhausted.
//
// The cursor is the FNV-1a hash of a key's composite B-tree key truncated to 48
// bits. Emitting keys in hash order keeps the scan stateless and complete: a key
// present for the whole scan has a fixed hash, so it is returned in exactly the
// one call whose hash window covers it, and a key deleted mid-scan only drops
// itself rather than cutting the scan short. Each call walks the whole tree, so
// this is O(n log n) per call; a later milestone replaces it with an incremental
// B-tree cursor.
func (db *DB) Scan(cursor uint64, count int) (uint64, []ScanEntry, error) {
	if count <= 0 {
		count = 10
	}
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

// forEachLive calls fn for every unexpired key in B-tree order. The composite
// key passed to fn is owned by the cursor and is only valid until fn returns.
func (db *DB) forEachLive(fn func(ck []byte, h ValueHeader) error) error {
	t := db.loadTree()
	if t == nil {
		return nil
	}
	c := t.Cursor()
	if err := c.First(); err != nil {
		return err
	}
	for c.Valid() {
		h, _, ok := parseHeader(c.Value())
		if ok && !db.expired(h) {
			if err := fn(c.Key(), h); err != nil {
				return err
			}
		}
		if err := c.Next(); err != nil {
			return err
		}
	}
	return nil
}

// copyRaw extracts the original key from a composite key into a fresh slice the
// caller owns.
func copyRaw(ck []byte) []byte {
	return bytes.Clone(rawKey(ck))
}

// fnv48 is the 48-bit truncation of FNV-1a over a composite key, used as the
// SCAN cursor hint.
func fnv48(b []byte) uint64 {
	h := fnv.New64a()
	_, _ = h.Write(b)
	return h.Sum64() & 0xFFFFFFFFFFFF
}
