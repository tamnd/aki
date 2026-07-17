package str

import (
	"strconv"

	"github.com/tamnd/aki/engine/obs1/shard"
)

// Emission helper for writes whose effect frame is not the argument value
// (spec 2064/obs1 doc 04 section 2, frames carry post-decision effects):
// the INCR family records the resulting number as the value the store now
// holds. APPEND and SETRANGE frame through the shared read-back seam,
// shard.Ctx.LogStrReadBack, which the bit and HLL surfaces use too.

// logCounter frames an INCR-family result: the value the store now holds
// as canonical integer text with the counter ladder bit. Gates on cx.Log
// itself so a volatile runtime never pays the formatting, and returns
// false only after writing the error reply, so a caller acks nothing it
// could not frame.
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
