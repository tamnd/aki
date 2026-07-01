package f1srv

import (
	"bytes"
	"encoding/binary"
	"math"
	"sort"
)

// Sorted-set algebra (ZUNION/ZINTER/ZDIFF and ZINTERCARD) combines several sorted sets, or plain
// sets read with an implicit score of 1, into one scored result (spec 2064/f1_rewrite_ltm/07 section
// 2). Unlike the set algebra, which is a pure membership merge, the zset forms aggregate a score per
// member and must return the result in score order, so the working result is materialized the same
// way Redis materializes its temporary dict: a union is the sum of the sources, an intersection is
// bounded by the smallest source, a difference is bounded by the first. The sources themselves are
// read off the element index in bounded batches, never decoded whole, so a source that is larger
// than memory is streamed, not loaded.
//
// WEIGHTS scale each source's scores before aggregation and AGGREGATE picks how scores for a shared
// member combine (SUM, MIN, or MAX); ZDIFF takes neither since its scores come only from the first
// set. Locking mirrors the set algebra: every source's stripe lock is taken in ascending order for
// the span of the read, so the sources cannot change under the aggregation.

const (
	zaggSum = iota
	zaggMin
	zaggMax
)

// zScored is one accumulated result member and its aggregated score, the unit the reply sorts and
// emits.
type zScored struct {
	member []byte
	score  float64
}

// zAlgOpts holds a parsed ZUNION/ZINTER/ZDIFF request: the source keys, one weight per source
// (default 1), the aggregation mode (default SUM), and whether scores are returned.
type zAlgOpts struct {
	keys       [][]byte
	weights    []float64
	aggregate  int
	withScores bool
}

// weighted scales a source score by its weight, mapping a NaN product (inf times zero) to zero the
// way Redis does, so a weighted score never poisons the aggregation.
func weighted(score, w float64) float64 {
	r := score * w
	if math.IsNaN(r) {
		return 0
	}
	return r
}

// zAggregate folds a new weighted score into a member's running score under the chosen mode. SUM
// resets a NaN sum (adding opposite infinities) to zero, matching Redis.
func zAggregate(cur, val float64, mode int) float64 {
	switch mode {
	case zaggMin:
		if val < cur {
			return val
		}
		return cur
	case zaggMax:
		if val > cur {
			return val
		}
		return cur
	default:
		r := cur + val
		if math.IsNaN(r) {
			return 0
		}
		return r
	}
}

// zsetSourceCard returns a source's cardinality, from the zset or the set header.
func (c *connState) zsetSourceCard(key []byte, kind keyKind) uint64 {
	if kind == keyZset {
		return c.zsetCard(key)
	}
	return c.setCard(key)
}

// zsetSourceEach iterates every (member, score) of a source that is a sorted set or a plain set, in
// member-byte order, calling emit for each until emit returns false. A set member carries score 1.
// Members are arena-stable subslices. It scans the element index in bounded batches, so a source is
// streamed, never decoded whole. The prefix and buffers are local, so this can run as the driver of
// an aggregation while point probes into other sources use the shared scratch buffers.
func (c *connState) zsetSourceEach(key []byte, kind keyKind, emit func(member []byte, score float64) bool) {
	var prefix []byte
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], uint64(len(key)))
	prefix = append(prefix, tmp[:n]...)
	prefix = append(prefix, key...)
	if kind == keyZset {
		prefix = append(prefix, zsetMemberTag)
	}
	plen := len(prefix)
	var after []byte
	batch := make([][]byte, 0, hashScanBatch)
	var vbuf []byte
	for {
		keys, last := c.srv.store.CollScan(prefix, after, hashScanBatch, batch[:0])
		batch = keys
		if len(keys) == 0 {
			return
		}
		for _, k := range keys {
			score := 1.0
			if kind == keyZset {
				v, ok := c.srv.store.GetKind(k, vbuf[:0], kindZsetMember)
				vbuf = v
				if ok {
					score = math.Float64frombits(binary.LittleEndian.Uint64(v))
				}
			}
			if !emit(k[plen:], score) {
				return
			}
		}
		if last == nil {
			return
		}
		after = last
	}
}

// zsetSourceScore point-probes one member in a source and returns its score. A zset returns the
// stored score, a set returns 1 when the member is present. ok is false when the member is absent.
// It uses the shared scratch buffers, so the caller must not be mid-iteration over those.
func (c *connState) zsetSourceScore(key []byte, kind keyKind, m []byte) (float64, bool) {
	if kind == keyZset {
		mk := c.zmemberKey(key, m)
		v, ok := c.srv.store.GetKind(mk, c.vbuf[:0], kindZsetMember)
		c.vbuf = v
		if !ok {
			return 0, false
		}
		return math.Float64frombits(binary.LittleEndian.Uint64(v)), true
	}
	if c.srv.store.ExistsKind(c.memberKey(key, m), kindSetMember) {
		return 1, true
	}
	return 0, false
}

// resolveZSources maps each source key to its type, rejecting a string or hash source as WRONGTYPE.
// A missing source resolves to keyMissing, which contributes nothing to a union and empties an
// intersection. It runs under the source locks so the resolved types match what the aggregation
// reads.
func (c *connState) resolveZSources(keys [][]byte) ([]keyKind, bool) {
	kinds := make([]keyKind, len(keys))
	for i, k := range keys {
		t := c.keyTypeOf(k)
		if t == keyString || t == keyHash {
			c.writeErr(wrongType)
			return nil, false
		}
		kinds[i] = t
	}
	return kinds, true
}

// parseZUnionInter parses the shared ZUNION/ZINTER/ZDIFF head and options. allowWA enables WEIGHTS
// and AGGREGATE, which ZDIFF does not accept. It writes the matching Redis error and returns ok
// false on a malformed request.
func (c *connState) parseZUnionInter(argv [][]byte, name string, allowWA bool) (*zAlgOpts, bool) {
	if len(argv) < 3 {
		c.writeErr("ERR wrong number of arguments for '" + name + "' command")
		return nil, false
	}
	numkeys, err := atoi64(argv[1])
	if err != nil {
		c.writeErr("ERR value is not an integer or out of range")
		return nil, false
	}
	if numkeys <= 0 {
		c.writeErr("ERR at least 1 input key is needed for '" + name + "' command")
		return nil, false
	}
	nk := int(numkeys)
	keysEnd := 2 + nk
	if keysEnd > len(argv) {
		c.writeErr("ERR syntax error")
		return nil, false
	}
	opts := &zAlgOpts{
		keys:      argv[2:keysEnd],
		weights:   make([]float64, nk),
		aggregate: zaggSum,
	}
	for i := range opts.weights {
		opts.weights[i] = 1
	}
	i := keysEnd
	for i < len(argv) {
		tok := argv[i]
		switch {
		case allowWA && eqFold(tok, "WEIGHTS"):
			if i+1+nk > len(argv) {
				c.writeErr("ERR syntax error")
				return nil, false
			}
			for j := 0; j < nk; j++ {
				w, err := parseScore(argv[i+1+j])
				if err != nil {
					c.writeErr("ERR weight value is not a float")
					return nil, false
				}
				opts.weights[j] = w
			}
			i += 1 + nk
		case allowWA && eqFold(tok, "AGGREGATE"):
			if i+1 >= len(argv) {
				c.writeErr("ERR syntax error")
				return nil, false
			}
			switch {
			case eqFold(argv[i+1], "SUM"):
				opts.aggregate = zaggSum
			case eqFold(argv[i+1], "MIN"):
				opts.aggregate = zaggMin
			case eqFold(argv[i+1], "MAX"):
				opts.aggregate = zaggMax
			default:
				c.writeErr("ERR syntax error")
				return nil, false
			}
			i += 2
		case eqFold(tok, "WITHSCORES"):
			opts.withScores = true
			i++
		default:
			c.writeErr("ERR syntax error")
			return nil, false
		}
	}
	return opts, true
}

// zunionAccumulate folds every source into one member-to-score map: a member's score is the
// aggregate of its weighted score in each source it appears in. The caller holds the source locks.
func (c *connState) zunionAccumulate(opts *zAlgOpts, kinds []keyKind) map[string]float64 {
	acc := make(map[string]float64)
	for i, key := range opts.keys {
		w := opts.weights[i]
		mode := opts.aggregate
		c.zsetSourceEach(key, kinds[i], func(m []byte, s float64) bool {
			val := weighted(s, w)
			ms := string(m)
			if cur, seen := acc[ms]; seen {
				acc[ms] = zAggregate(cur, val, mode)
			} else {
				acc[ms] = val
			}
			return true
		})
	}
	return acc
}

// zinterAccumulate builds the intersection scored map: it seeds from the smallest source and probes
// the rest, dropping any member a source lacks and aggregating the ones it holds. An empty or
// missing source makes the whole intersection empty. The caller holds the source locks.
func (c *connState) zinterAccumulate(opts *zAlgOpts, kinds []keyKind) map[string]float64 {
	driver := 0
	minCard := ^uint64(0)
	for i, key := range opts.keys {
		card := c.zsetSourceCard(key, kinds[i])
		if card == 0 {
			return map[string]float64{}
		}
		if card < minCard {
			minCard = card
			driver = i
		}
	}
	acc := make(map[string]float64)
	dw := opts.weights[driver]
	c.zsetSourceEach(opts.keys[driver], kinds[driver], func(m []byte, s float64) bool {
		acc[string(m)] = weighted(s, dw)
		return true
	})
	for j, key := range opts.keys {
		if j == driver {
			continue
		}
		w := opts.weights[j]
		for ms := range acc {
			sc, ok := c.zsetSourceScore(key, kinds[j], []byte(ms))
			if !ok {
				delete(acc, ms)
				continue
			}
			acc[ms] = zAggregate(acc[ms], weighted(sc, w), opts.aggregate)
		}
	}
	return acc
}

// sortZScored turns an accumulated map into the sorted result: ascending score, ties broken by
// ascending member bytes, the order Redis returns.
func sortZScored(acc map[string]float64) []zScored {
	res := make([]zScored, 0, len(acc))
	for m, s := range acc {
		res = append(res, zScored{member: []byte(m), score: s})
	}
	sort.Slice(res, func(i, j int) bool {
		if res[i].score != res[j].score {
			return res[i].score < res[j].score
		}
		return bytes.Compare(res[i].member, res[j].member) < 0
	})
	return res
}

// writeZScored frames and emits a sorted result, interleaving scores when withScores is set.
func (c *connState) writeZScored(res []zScored, withScores bool) {
	hdr := len(res)
	if withScores {
		hdr *= 2
	}
	c.writeArrayHeader(hdr)
	for _, e := range res {
		c.writeBulk(e.member)
		if withScores {
			c.writeScore(e.score)
		}
	}
}

// cmdZUnion answers ZUNION: the scored union of every source, in score order.
func (c *connState) cmdZUnion(argv [][]byte) {
	opts, ok := c.parseZUnionInter(argv, "zunion", true)
	if !ok {
		return
	}
	unlock := c.lockStripes(opts.keys)
	kinds, ok := c.resolveZSources(opts.keys)
	if !ok {
		unlock()
		return
	}
	acc := c.zunionAccumulate(opts, kinds)
	unlock()
	c.writeZScored(sortZScored(acc), opts.withScores)
}

// cmdZInter answers ZINTER: the scored intersection of every source, in score order.
func (c *connState) cmdZInter(argv [][]byte) {
	opts, ok := c.parseZUnionInter(argv, "zinter", true)
	if !ok {
		return
	}
	unlock := c.lockStripes(opts.keys)
	kinds, ok := c.resolveZSources(opts.keys)
	if !ok {
		unlock()
		return
	}
	acc := c.zinterAccumulate(opts, kinds)
	unlock()
	c.writeZScored(sortZScored(acc), opts.withScores)
}

// zdiffAccumulate builds the difference scored map: the members of the first source that no other
// source holds, keyed to the first source's score. The caller holds the source locks.
func (c *connState) zdiffAccumulate(keys [][]byte, kinds []keyKind) map[string]float64 {
	acc := make(map[string]float64)
	rest := keys[1:]
	restKinds := kinds[1:]
	c.zsetSourceEach(keys[0], kinds[0], func(m []byte, s float64) bool {
		for j, key := range rest {
			if _, ok := c.zsetSourceScore(key, restKinds[j], m); ok {
				return true
			}
		}
		acc[string(m)] = s
		return true
	})
	return acc
}

// cmdZDiff answers ZDIFF: the members of the first source that no other source holds, with the first
// source's scores, in score order. It takes no WEIGHTS or AGGREGATE.
func (c *connState) cmdZDiff(argv [][]byte) {
	opts, ok := c.parseZUnionInter(argv, "zdiff", false)
	if !ok {
		return
	}
	unlock := c.lockStripes(opts.keys)
	kinds, ok := c.resolveZSources(opts.keys)
	if !ok {
		unlock()
		return
	}
	acc := c.zdiffAccumulate(opts.keys, kinds)
	unlock()
	c.writeZScored(sortZScored(acc), opts.withScores)
}

// cmdZInterCard answers ZINTERCARD numkeys key [key ...] [LIMIT limit]: the size of the
// intersection, stopping at a positive LIMIT so a bounded check on huge sets never counts the whole
// intersection. LIMIT 0 means count them all.
func (c *connState) cmdZInterCard(argv [][]byte) {
	if len(argv) < 3 {
		c.writeErr("ERR wrong number of arguments for 'zintercard' command")
		return
	}
	numkeys, err := atoi64(argv[1])
	if err != nil {
		c.writeErr("ERR value is not an integer or out of range")
		return
	}
	if numkeys <= 0 {
		c.writeErr("ERR numkeys should be greater than 0")
		return
	}
	nk := int(numkeys)
	keysEnd := 2 + nk
	if keysEnd > len(argv) {
		c.writeErr("ERR Number of keys can't be greater than number of args")
		return
	}
	keys := argv[2:keysEnd]
	limit := 0
	rest := argv[keysEnd:]
	if len(rest) > 0 {
		if len(rest) != 2 || !eqFold(rest[0], "LIMIT") {
			c.writeErr("ERR syntax error")
			return
		}
		l, err := atoi64(rest[1])
		if err != nil || l < 0 {
			c.writeErr("ERR LIMIT can't be negative")
			return
		}
		limit = int(l)
	}
	unlock := c.lockStripes(keys)
	kinds, ok := c.resolveZSources(keys)
	if !ok {
		unlock()
		return
	}
	// Drive off the smallest source and probe the rest; an empty source means an empty intersection.
	driver := 0
	minCard := ^uint64(0)
	empty := false
	for i, key := range keys {
		card := c.zsetSourceCard(key, kinds[i])
		if card == 0 {
			empty = true
			break
		}
		if card < minCard {
			minCard = card
			driver = i
		}
	}
	count := 0
	if !empty {
		c.zsetSourceEach(keys[driver], kinds[driver], func(m []byte, _ float64) bool {
			for j, key := range keys {
				if j == driver {
					continue
				}
				if _, ok := c.zsetSourceScore(key, kinds[j], m); !ok {
					return true
				}
			}
			count++
			return !(limit > 0 && count >= limit)
		})
	}
	unlock()
	c.writeInt(int64(count))
}
