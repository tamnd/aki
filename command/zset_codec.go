package command

import (
	"bytes"
	"cmp"
	"errors"
	"math"
	"slices"
	"strconv"

	"github.com/tamnd/aki/encoding"
	"github.com/tamnd/aki/keyspace"
)

// errCorruptZSet is returned when a stored sorted-set body cannot be decoded,
// which means the value record is damaged.
var errCorruptZSet = errors.New("corrupt zset value")

// Sorted-set OBJECT ENCODING thresholds live in encLimits (enc_limits.go), read
// from zset-max-listpack-entries and zset-max-listpack-value. aki stores its own
// physical form (a length-prefixed pair sequence in score order), so they only
// decide which Redis encoding name the key reports, matching the t_zset.c rule.

// zmember is one (member, score) pair of a sorted set.
type zmember struct {
	member []byte
	score  float64
}

// zsetDecode unpacks a stored sorted-set body into its pairs. The body is a
// uvarint pair count followed by each pair as a uvarint-length member, the
// member bytes, and an 8-byte little-endian score. Pairs are stored in
// (score, member) order, so the decoded slice is already sorted.
func zsetDecode(body []byte) ([]zmember, error) {
	if len(body) == 0 {
		return nil, nil
	}
	n, off, err := encoding.Uvarint(body)
	if err != nil {
		return nil, err
	}
	out := make([]zmember, 0, n)
	for range n {
		l, m, err := encoding.Uvarint(body[off:])
		if err != nil {
			return nil, err
		}
		off += m
		if off+int(l)+8 > len(body) {
			return nil, errCorruptZSet
		}
		member := make([]byte, l)
		copy(member, body[off:off+int(l)])
		off += int(l)
		score := encoding.F64(body[off : off+8])
		off += 8
		out = append(out, zmember{member: member, score: score})
	}
	return out, nil
}

// zsetEncode packs pairs back into the stored body form. Callers keep the slice
// in (score, member) order so the body stays sorted.
func zsetEncode(members []zmember) []byte {
	body := encoding.AppendUvarint(nil, uint64(len(members)))
	for _, zm := range members {
		body = encoding.AppendUvarint(body, uint64(len(zm.member)))
		body = append(body, zm.member...)
		body = encoding.AppendF64(body, zm.score)
	}
	return body
}

// zsetEncoding picks the reported encoding for a sorted set. A sorted set never
// downgrades, so prev pins the floor: once skiplist it stays skiplist.
func zsetEncoding(lim encLimits, members []zmember, prev uint8) uint8 {
	if prev == keyspace.EncSkiplist {
		return keyspace.EncSkiplist
	}
	if int64(len(members)) > lim.zsetEntries {
		return keyspace.EncSkiplist
	}
	for _, zm := range members {
		if int64(len(zm.member)) > lim.zsetValue {
			return keyspace.EncSkiplist
		}
	}
	return keyspace.EncListpack
}

// zcmp orders pair a against b in the (score ascending, member bytewise
// ascending) total order Redis uses, returning -1, 0 or +1.
func zcmp(a, b zmember) int {
	if a.score != b.score {
		return cmp.Compare(a.score, b.score)
	}
	return bytes.Compare(a.member, b.member)
}

// zsetSort orders the pairs by the sorted-set total order. slices.SortFunc avoids
// the reflection-based element swaps that sort.Slice incurs.
func zsetSort(members []zmember) {
	slices.SortFunc(members, zcmp)
}

// zsetFind returns the index of a member, or -1 when it is absent. The lookup is
// a linear scan, which suits the listpack-sized sets this milestone stores.
func zsetFind(members []zmember, member []byte) int {
	for i := range members {
		if bytes.Equal(members[i].member, member) {
			return i
		}
	}
	return -1
}

// getZSet reads the sorted set at key and decodes it. The returned header
// carries the type and encoding so callers can check for WRONGTYPE and keep the
// encoding floor. A missing key returns found false with no error. A large sorted
// set lives in the btree-backed sub-tree form (zset_tree.go); getZSet materializes
// it in (score, member) order so every read caller (ZRANGE, ZRANK, the set algebra,
// ZSCORE, ZPOP, DUMP/RDB, SORT, GEO) works on either form unchanged. The ZADD,
// ZINCRBY and ZREM write commands branch on hdr.IsColl() before getZSet so they
// never rewrite a whole blob for a btree-backed set.
func getZSet(db *keyspace.DB, key []byte) ([]zmember, keyspace.ValueHeader, bool, error) {
	body, hdr, found, err := db.Get(key)
	if err != nil || !found {
		return nil, hdr, found, err
	}
	if hdr.Type != keyspace.TypeZSet {
		return nil, hdr, true, nil
	}
	if hdr.IsColl() {
		members, e := collectZSetMembers(db, key)
		return members, hdr, true, e
	}
	members, err := zsetDecode(body)
	if err != nil {
		return nil, hdr, true, err
	}
	return members, hdr, true, nil
}

// parseScore parses a score string. It accepts the inf and -inf literals that
// Redis allows for scores and rejects NaN, unlike parseFloat which rejects every
// non-finite value. strconv.ParseFloat handles the inf and infinity spellings
// case-insensitively and rejects surrounding whitespace.
func parseScore(b []byte) (float64, bool) {
	if len(b) == 0 {
		return 0, false
	}
	f, err := strconv.ParseFloat(string(b), 64)
	if err != nil || math.IsNaN(f) {
		return 0, false
	}
	return f, true
}

// hotGetZSet tries to decode the sorted set at key from the lock-free hot
// cache. Returns (members, true) on a hit and (nil, false) on a miss.
// A miss includes wrong-type keys so callers can fall back to view().
func hotGetZSet(ctx *Ctx, key []byte) ([]zmember, bool) {
	e := ctx.d.engine
	if e == nil {
		return nil, false
	}
	body, hdr, ok := e.viewHotGet(ctx.Conn.DB(), key)
	if !ok {
		return nil, false
	}
	if hdr.Type != keyspace.TypeZSet {
		return nil, false
	}
	members, err := zsetDecode(body)
	if err != nil {
		return nil, false
	}
	return members, true
}
