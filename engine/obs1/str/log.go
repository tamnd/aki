package str

import (
	"strconv"

	"github.com/tamnd/aki/engine/obs1/shard"
)

// Emission helpers for writes whose effect frame is not the argument value
// (spec 2064/obs1 doc 04 section 2, frames carry post-decision effects):
// the INCR family records the resulting number, APPEND and SETRANGE record
// the whole resulting string, and every TTL-preserving write records the
// absolute deadline the key still rides under. Both helpers gate on cx.Log
// themselves so a volatile runtime never pays the read-back, and both
// return false only after writing the error reply, so a caller acks
// nothing it could not frame.

// logCounter frames an INCR-family result: the value the store now holds
// as canonical integer text with the counter ladder bit.
func logCounter(cx *shard.Ctx, key []byte, n int64, r shard.Reply) bool {
	if cx.Log == nil {
		return true
	}
	var nb [20]byte
	out := strconv.AppendInt(nb[:0], n, 10)
	if err := cx.LogStrSet(key, out, cx.St.ExpireAt(key, cx.NowMs), true); err != nil {
		r.Err(err.Error())
		return false
	}
	return true
}

// logReadBack frames a write whose resulting value lives only in the store
// (APPEND, SETRANGE): the whole string is read back, chunked values
// assembled through the stream the giant band serves reads with, and the
// deadline that rode through is read beside it. The write just landed, so
// an absent key cannot happen here; the guard keeps the helper total.
func logReadBack(cx *shard.Ctx, key []byte, r shard.Reply) bool {
	if cx.Log == nil {
		return true
	}
	v, cs, ok := cx.St.GetStream(key, cx.NowMs, cx.Val)
	cx.Val = v
	if !ok {
		return true
	}
	if cs != nil {
		total := int(cs.Total())
		if cap(cx.Val) < total {
			cx.Val = make([]byte, total)
		}
		buf := cx.Val[:total]
		filled := 0
		for filled < total {
			n, err := cs.Next(buf[filled:])
			if err != nil || n == 0 {
				cs.Release()
				if err != nil {
					r.Err(storeErr(err))
					return false
				}
				break
			}
			filled += n
		}
		cs.Release()
		v = buf[:filled]
		cx.Val = v
	}
	if err := cx.LogStrSet(key, v, cx.St.ExpireAt(key, cx.NowMs), false); err != nil {
		r.Err(err.Error())
		return false
	}
	return true
}
