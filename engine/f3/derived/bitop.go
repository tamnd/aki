package derived

// BITOP (spec 2064/f3/15 section 5): AND, OR, XOR over one or more source
// bitmaps, or NOT over exactly one, with the result stored at the destination
// and its length replied. The co-located case (destination and every source on
// one owner) runs the whole streaming algebra on that owner through the store,
// bounded to (sources + 1) chunks. The cross-shard case rides the F17 hop plan
// and lands in a later slice; until then it answers a clear refusal, the same
// shape XREAD-across-shards uses.

import (
	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/engine/f3/store"
	"github.com/tamnd/aki/f3srv/resp"
)

const (
	errBitopNot     = "ERR BITOP NOT must be called with a single source key."
	errBitopTooLong = "ERR string exceeds maximum allowed size (proto-max-bulk-len)"
	errBitopCross   = "ERR BITOP across shards is not supported yet"
)

// BitOp answers BITOP <AND|OR|XOR|NOT> destkey srckey [srckey ...] on the owner
// shared by the destination and every source (the co-located fast path). args
// is the tail after the verb: the operation token, the destination, then the
// sources.
func BitOp(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	op, ok := parseBitOp(args[0])
	if !ok {
		r.Err(errSyntax)
		return
	}
	if op == store.BitNot && len(args) != 3 {
		r.Err(errBitopNot)
		return
	}
	n, err := cx.St.BitOp(op, args[1], args[2:], cx.NowMs)
	if err != nil {
		if err == store.ErrTooBig {
			r.Err(errBitopTooLong)
			return
		}
		r.Err("ERR " + err.Error())
		return
	}
	r.Int(n)
}

// BitOpCross is the cross-shard entry dispatch routes to when the destination
// and sources span shards. The F17 streaming coordinator lands in the next
// slice; for now it refuses cleanly rather than compute a wrong answer.
func BitOpCross(t *shard.Txn, args [][]byte) []byte {
	return resp.AppendError(nil, errBitopCross)
}

// parseBitOp maps the operation token to its store code, case-insensitively.
func parseBitOp(tok []byte) (int, bool) {
	switch upper(tok) {
	case "AND":
		return store.BitAnd, true
	case "OR":
		return store.BitOr, true
	case "XOR":
		return store.BitXor, true
	case "NOT":
		return store.BitNot, true
	}
	return 0, false
}
