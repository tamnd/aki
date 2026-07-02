package f1srv

import (
	"encoding/binary"
	"math"
)

// The STORE forms (ZUNIONSTORE/ZINTERSTORE/ZDIFFSTORE) compute the same scored merge as their read
// cousins and write the result into a destination sorted set as element-per-row rows plus the
// maintained header, returning the stored cardinality (spec 2064/f1_rewrite_ltm/07 section 2).
//
// Two things the reads never had to handle show up here, and both fall out of the read path already
// materializing its result into a member-to-score map keyed by copied member bytes:
//
//   - Aliasing. The destination may also be a source (ZUNIONSTORE dst 2 dst other). The accumulate
//     step copies every member out of the arena into the result map before anything is cleared, so
//     clearing the destination afterward cannot pull the ground out from under the merge. No
//     separate buffered branch is needed the way the set STORE needs one, because the zset merge is
//     inherently materialized: its scores have to be aggregated and sorted before emission.
//   - Destination overwrite. The destination is replaced regardless of its prior type, so a plain
//     string there is dropped, not a WRONGTYPE. The WRONGTYPE check covers the sources only, exactly
//     as the reads do. An empty result deletes the destination (empty zset is no zset), matching
//     Redis.
//
// The destination and every source take their stripe locks for the whole operation through
// lockStripes, so nothing changes under the merge and the destination write is atomic with respect
// to concurrent readers of any key involved.

// zStoreOpts holds a parsed ZUNIONSTORE/ZINTERSTORE/ZDIFFSTORE request: the destination key, the
// source keys, one weight per source (default 1), and the aggregation mode (default SUM). The STORE
// forms take no WITHSCORES, since scores are always stored.
type zStoreOpts struct {
	dest      []byte
	keys      [][]byte
	weights   []float64
	aggregate int
}

// parseZStore parses the shared STORE head and options: destination, numkeys, the key list, then
// WEIGHTS and AGGREGATE when allowWA is set (ZDIFFSTORE takes neither). It writes the matching Redis
// error and returns ok false on a malformed request.
func (c *connState) parseZStore(argv [][]byte, name string, allowWA bool) (*zStoreOpts, bool) {
	if len(argv) < 4 {
		c.writeErr("ERR wrong number of arguments for '" + name + "' command")
		return nil, false
	}
	numkeys, err := atoi64(argv[2])
	if err != nil {
		c.writeErr("ERR value is not an integer or out of range")
		return nil, false
	}
	if numkeys <= 0 {
		c.writeErr("ERR at least 1 input key is needed for '" + name + "' command")
		return nil, false
	}
	nk := int(numkeys)
	keysEnd := 3 + nk
	if keysEnd > len(argv) {
		c.writeErr("ERR syntax error")
		return nil, false
	}
	opts := &zStoreOpts{
		dest:      argv[1],
		keys:      argv[3:keysEnd],
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
		default:
			c.writeErr("ERR syntax error")
			return nil, false
		}
	}
	return opts, true
}

// zsetClear drops every row of a zset, both families and the header, in bounded batches so clearing
// a huge destination never materializes the whole set. It scans the member family from the start
// each round and drops the batch: a delete removes only the ordered-index slot, so the next scan
// from the start returns the next survivors. Each member's score row is dropped alongside it. Any
// string at the key is left alone; the STORE handlers drop that separately with store.Delete.
func (c *connState) zsetClear(zkey []byte) {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], uint64(len(zkey)))
	prefix := make([]byte, 0, n+len(zkey)+1)
	prefix = append(prefix, tmp[:n]...)
	prefix = append(prefix, zkey...)
	prefix = append(prefix, zsetMemberTag)
	plen := len(prefix)
	batch := make([][]byte, 0, hashScanBatch)
	var vbuf []byte
	for {
		keys, _ := c.srv.store.CollScan(prefix, nil, hashScanBatch, batch[:0])
		batch = keys
		if len(keys) == 0 {
			break
		}
		for _, k := range keys {
			score := 0.0
			if v, ok := c.srv.store.GetKind(k, vbuf[:0], kindZsetMember); ok {
				vbuf = v
				score = math.Float64frombits(binary.LittleEndian.Uint64(v))
			}
			c.dropZsetMember(zkey, k[plen:], score)
		}
	}
	c.srv.store.DeleteKind(zkey, kindZsetMeta)
}

// zsetWriteResult writes a sorted result into a fresh destination zset as element-per-row member and
// score rows, folding the encoding tag forward, and returns the stored cardinality. The caller has
// already cleared the destination and holds its stripe lock. The result members are arena-independent
// copies, so writing them is safe even when the destination aliased a source.
func (c *connState) zsetWriteResult(dest []byte, res []zScored) (int, error) {
	var enc = encNone
	count := uint64(0)
	for _, e := range res {
		score := normalizeZero(e.score)
		mk := c.zmemberKey(dest, e.member)
		if err := c.zsetInsertNew(dest, e.member, mk, score); err != nil {
			return int(count), err
		}
		count++
		enc = foldZsetEnc(enc, e.member, count)
	}
	if count > 0 {
		if err := c.zsetPutHeader(dest, count, enc); err != nil {
			return int(count), err
		}
	}
	return int(count), nil
}

// zsetStore is the shared body of the three STORE forms: it locks the destination and every source,
// rejects a source held by a plain string, computes the result with the chosen accumulator, replaces
// the destination with it, and replies with the stored cardinality.
func (c *connState) zsetStore(argv [][]byte, name string, kind int) {
	opts, ok := c.parseZStore(argv, name, kind != zStoreDiff)
	if !ok {
		return
	}
	all := make([][]byte, 0, len(opts.keys)+1)
	all = append(all, opts.dest)
	all = append(all, opts.keys...)
	unlock := c.lockStripes(all)
	// WRONGTYPE covers the sources only: the destination is overwritten whatever it held.
	kinds, ok := c.resolveZSources(opts.keys)
	if !ok {
		unlock()
		return
	}
	var acc map[string]float64
	switch kind {
	case zStoreDiff:
		acc = c.zdiffAccumulate(opts.keys, kinds)
	case zStoreInter:
		acc = c.zinterAccumulate(&zAlgOpts{keys: opts.keys, weights: opts.weights, aggregate: opts.aggregate}, kinds)
	default:
		acc = c.zunionAccumulate(&zAlgOpts{keys: opts.keys, weights: opts.weights, aggregate: opts.aggregate}, kinds)
	}
	res := sortZScored(acc)
	// Replace the destination: drop whatever it held (any type), then write the result.
	c.srv.store.Delete(opts.dest)
	c.zsetClear(opts.dest)
	count, err := c.zsetWriteResult(opts.dest, res)
	if err != nil {
		unlock()
		c.writeErr("ERR " + err.Error())
		return
	}
	unlock()
	c.writeInt(int64(count))
}

// The three merge kinds the STORE body dispatches on.
const (
	zStoreUnion = iota
	zStoreInter
	zStoreDiff
)

// cmdZUnionStore stores the scored union of the sources into the destination.
func (c *connState) cmdZUnionStore(argv [][]byte) {
	c.zsetStore(argv, "zunionstore", zStoreUnion)
}

// cmdZInterStore stores the scored intersection of the sources into the destination.
func (c *connState) cmdZInterStore(argv [][]byte) {
	c.zsetStore(argv, "zinterstore", zStoreInter)
}

// cmdZDiffStore stores the difference of the first source minus the rest into the destination.
func (c *connState) cmdZDiffStore(argv [][]byte) {
	c.zsetStore(argv, "zdiffstore", zStoreDiff)
}
