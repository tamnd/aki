package f1srv

import (
	"encoding/binary"
	"math"
	"strconv"
)

// ZRANGESTORE computes the same window as ZRANGE and writes it into a destination sorted set instead
// of emitting it on the wire (spec 2064/f1_rewrite_ltm/07). The command is
//
//	ZRANGESTORE dst src start stop [BYSCORE | BYLEX] [REV] [LIMIT offset count]
//
// It takes no WITHSCORES: members are always stored with their source scores. The window is gathered
// through the exact rank-boundary and collect machinery the ZRANGE reads use (zIndexWindow for the
// default rank form, zScoreWindow for BYSCORE, zLexWindow for BYLEX), so the store path is bounded to
// the window and never materializes the whole source, the same LTM property the reads have.
//
// The window keys are arena subslices that stay valid across the destination clear (the arena is
// grow-only, and clearing frees only index slots), and each member is copied nowhere before storage,
// so the destination aliasing the source (ZRANGESTORE k k 0 -1) is safe: the window is read off the
// index before the destination is cleared, and the members it points at survive the clear. The
// destination is replaced whatever it held (a plain string is dropped, not a WRONGTYPE); WRONGTYPE
// covers the source only. An empty window deletes the destination and replies with 0.

// cmdZRangeStore parses ZRANGESTORE, gathers the source window, and stores it in the destination.
func (c *connState) cmdZRangeStore(argv [][]byte) {
	if len(argv) < 5 {
		c.writeErr("ERR wrong number of arguments for 'zrangestore' command")
		return
	}
	dst := argv[1]
	src := argv[2]

	rev := false
	byScore := false
	byLex := false
	hasLimit := false
	offset := 0
	count := 0
	for i := 5; i < len(argv); i++ {
		switch {
		case eqFold(argv[i], "REV"):
			rev = true
		case eqFold(argv[i], "BYSCORE"):
			byScore = true
		case eqFold(argv[i], "BYLEX"):
			byLex = true
		case eqFold(argv[i], "LIMIT"):
			if i+2 >= len(argv) {
				c.writeErr("ERR syntax error")
				return
			}
			o, err1 := strconv.Atoi(string(argv[i+1]))
			n, err2 := strconv.Atoi(string(argv[i+2]))
			if err1 != nil || err2 != nil {
				c.writeErr("ERR value is not an integer or out of range")
				return
			}
			hasLimit = true
			offset = o
			count = n
			i += 2
		default:
			c.writeErr("ERR syntax error")
			return
		}
	}
	if byScore && byLex {
		c.writeErr("ERR syntax error, BYSCORE and BYLEX options at the same time are not compatible")
		return
	}
	if hasLimit && !byScore && !byLex {
		c.writeErr("ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX")
		return
	}

	// Lock the destination and source together so the window read and the destination write are
	// atomic with respect to concurrent readers, and aliasing is handled under one lock set.
	unlock := c.lockStripes([][]byte{dst, src})

	var res []zScored
	var errMsg string
	switch {
	case byScore:
		lo, hi := argv[3], argv[4]
		if rev {
			lo, hi = argv[4], argv[3]
		}
		keys, plen, e := c.zScoreWindow(src, lo, hi, rev, hasLimit, offset, count)
		errMsg = e
		if e == "" {
			res = scoreKeysToPairs(keys, plen)
		}
	case byLex:
		lo, hi := argv[3], argv[4]
		if rev {
			lo, hi = argv[4], argv[3]
		}
		keys, plen, e := c.zLexWindow(src, lo, hi, rev, hasLimit, offset, count)
		errMsg = e
		if e == "" {
			res = c.memberKeysToPairs(src, keys, plen)
		}
	default:
		keys, plen, e := c.zIndexWindow(src, argv[3], argv[4], rev)
		errMsg = e
		if e == "" {
			res = scoreKeysToPairs(keys, plen)
		}
	}
	if errMsg != "" {
		unlock()
		c.writeErr(errMsg)
		return
	}

	// Replace the destination: drop whatever it held (any type), then write the gathered window.
	c.srv.store.Delete(dst)
	c.zsetClear(dst)
	n, err := c.zsetWriteResult(dst, res)
	if err != nil {
		unlock()
		c.writeErr("ERR " + err.Error())
		return
	}
	unlock()
	c.writeInt(int64(n))
}

// scoreKeysToPairs turns score-family window keys (used by the rank and BYSCORE forms) into scored
// members without a second index read: the member is the key tail past the prefix and the 8 sortable
// score bytes, and the score decodes straight from those bytes. The returned members are arena
// subslices that stay valid across the destination clear.
func scoreKeysToPairs(keys [][]byte, plen int) []zScored {
	res := make([]zScored, 0, len(keys))
	for _, k := range keys {
		res = append(res, zScored{
			member: k[plen+8:],
			score:  decodeSortableScore(k[plen : plen+8]),
		})
	}
	return res
}

// memberKeysToPairs turns member-family window keys (used by the BYLEX form) into scored members. The
// member is the key tail past the prefix, but the score is not in the key here, so it is read from
// the member row's value (the little-endian score bits). Building the member-row key in kbuf does not
// disturb the arena-backed member subslices the window keys point at.
func (c *connState) memberKeysToPairs(src []byte, keys [][]byte, plen int) []zScored {
	res := make([]zScored, 0, len(keys))
	for _, k := range keys {
		member := k[plen:]
		score := 0.0
		mk := c.zmemberKey(src, member)
		if v, ok := c.srv.store.GetKind(mk, c.vbuf[:0], kindZsetMember); ok {
			c.vbuf = v
			score = math.Float64frombits(binary.LittleEndian.Uint64(v))
		}
		res = append(res, zScored{member: member, score: score})
	}
	return res
}
