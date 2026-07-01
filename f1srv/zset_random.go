package f1srv

import (
	"encoding/binary"
	"math"
	"math/rand/v2"
)

// ZRANDMEMBER is the non-destructive random member read for the sorted set (spec
// 2064/f1_rewrite_ltm/07 section 2, the random view). It samples off the member family, which is in
// member order, with the same order-statistic seek SRANDMEMBER uses: a random index is one O(log n)
// descent into the ordered index, never an O(n) count. WITHSCORES reads each drawn member's score
// straight from its row value, so a scored draw is the sample plus one map read per member, no
// second family walk.
//
// The count sign convention is the Redis compatibility trap it shares with SRANDMEMBER: a positive
// count returns up to that many distinct members (capped at the cardinality, no duplicates), while a
// negative count returns exactly abs(count) members with replacement, so duplicates are possible and
// the result is never capped.

// zsetWalkAllKeys appends every member-family key of a zset, in member order, to dst as arena-stable
// full keys (prefix included, so the caller can both slice the member and read the score row value).
// It is the whole-set walk the large-count sampler falls back to.
func (c *connState) zsetWalkAllKeys(prefix []byte, dst [][]byte) [][]byte {
	var after []byte
	scan := make([][]byte, 0, hashScanBatch)
	for {
		keys, last := c.srv.store.CollScan(prefix, after, hashScanBatch, scan[:0])
		if len(keys) == 0 {
			break
		}
		dst = append(dst, keys...)
		if last == nil {
			break
		}
		after = last
	}
	return dst
}

// zsetSampleDistinctKeys returns count distinct member-family keys of a zset of cardinality card
// (count is assumed already clamped to at most card), as arena-stable full keys. It mirrors the set
// sampler's crossover: below half the cardinality it draws uniform random indices into the ordered
// index and dedups on the index, so each member appears at most once and every draw is a descent; at
// or above half it walks once and partial-shuffles, avoiding the retry storm the dedup path hits as
// count nears card. The caller serializes the zset's writers so card and the index agree.
func (c *connState) zsetSampleDistinctKeys(prefix []byte, card, count int) [][]byte {
	if count >= card {
		return c.zsetWalkAllKeys(prefix, make([][]byte, 0, card))
	}
	if count*2 >= card {
		all := c.zsetWalkAllKeys(prefix, make([][]byte, 0, card))
		for i := 0; i < count; i++ {
			j := i + rand.IntN(len(all)-i)
			all[i], all[j] = all[j], all[i]
		}
		return all[:count]
	}
	seen := make(map[int]struct{}, count)
	out := make([][]byte, 0, count)
	for len(out) < count {
		idx := rand.IntN(card)
		if _, dup := seen[idx]; dup {
			continue
		}
		seen[idx] = struct{}{}
		k, ok := c.srv.store.CollSelectAt(prefix, idx)
		if !ok {
			continue
		}
		out = append(out, k)
	}
	return out
}

// emitRandMember writes a drawn member, and its score after it when withScores is set. k is a full
// member-family key; the member is its tail past the prefix and the score is its row value.
func (c *connState) emitRandMember(k []byte, plen int, withScores bool) {
	c.writeBulk(k[plen:])
	if withScores {
		v, ok := c.srv.store.GetKind(k, c.vbuf[:0], kindZsetMember)
		c.vbuf = v
		if ok {
			c.writeScore(math.Float64frombits(binary.LittleEndian.Uint64(v)))
		} else {
			c.writeScore(0)
		}
	}
}

func (c *connState) cmdZRandMember(argv [][]byte) {
	// ZRANDMEMBER key [count [WITHSCORES]]
	if len(argv) < 2 || len(argv) > 4 {
		c.writeErr("ERR wrong number of arguments for 'zrandmember' command")
		return
	}
	zkey := argv[1]

	if len(argv) == 2 {
		// No-count form: one member as a bulk string, or nil for a missing (or wrong-type) key.
		if c.stringConflict(zkey) {
			c.writeErr(wrongType)
			return
		}
		card := c.zsetCard(zkey)
		if card == 0 {
			c.writeNil()
			return
		}
		prefix := c.zmemberPrefix(zkey)
		k, ok := c.srv.store.CollSelectAt(prefix, rand.IntN(int(card)))
		if !ok {
			c.writeNil()
			return
		}
		c.writeBulk(k[len(prefix):])
		return
	}

	count, err := atoi64(argv[2])
	if err != nil {
		c.writeErr("ERR value is not an integer or out of range")
		return
	}
	withScores := false
	if len(argv) == 4 {
		if !eqFold(argv[3], "WITHSCORES") {
			c.writeErr("ERR syntax error")
			return
		}
		withScores = true
	}

	// The stripe lock keeps the cardinality and the ordered index consistent across a multi-pick
	// sample, the same serialization the zset's writers take.
	mu := &c.srv.incrMu[c.srv.stripe(zkey)]
	mu.Lock()
	if c.stringConflict(zkey) {
		mu.Unlock()
		c.writeErr(wrongType)
		return
	}
	card := int(c.zsetCard(zkey))
	if count == 0 || card == 0 {
		mu.Unlock()
		c.writeArrayHeader(0)
		return
	}
	prefix := c.zmemberPrefix(zkey)
	plen := len(prefix)

	if count < 0 {
		// With replacement: exactly abs(count) members, duplicates allowed.
		n := int(-count)
		hdr := n
		if withScores {
			hdr = n * 2
		}
		c.writeArrayHeader(hdr)
		for i := 0; i < n; i++ {
			k, ok := c.srv.store.CollSelectAt(prefix, rand.IntN(card))
			if !ok {
				c.writeNil()
				if withScores {
					c.writeScore(0)
				}
				continue
			}
			c.emitRandMember(k, plen, withScores)
		}
		mu.Unlock()
		return
	}

	want := int(count)
	if want > card {
		want = card
	}
	keys := c.zsetSampleDistinctKeys(prefix, card, want)
	hdr := len(keys)
	if withScores {
		hdr = len(keys) * 2
	}
	c.writeArrayHeader(hdr)
	for _, k := range keys {
		c.emitRandMember(k, plen, withScores)
	}
	mu.Unlock()
}
