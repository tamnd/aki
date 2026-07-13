package derived

// Bitmaps (spec 2064/f3/15 section 2) are a bit-level view over the string
// store: SETBIT and GETBIT run on the same keyspace SET and GET use, with no
// distinct value type, so a value written by SET is readable bit by bit and a
// bitmap is readable whole by GET.

import (
	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/engine/f3/store"
)

// maxBitOffset is the highest legal bit offset: byte index offset>>3 must stay
// under 2^29 (the proto-max-bulk-len value ceiling), so the offset caps at
// 2^32-1, the same wire limit Redis enforces for SETBIT and GETBIT.
const maxBitOffset = (1 << 32) - 1

const (
	errBitOffset = "ERR bit offset is not an integer or out of range"
	errBitValue  = "ERR bit is not an integer or out of range"
)

// SetBit answers SETBIT key offset value: set the addressed bit and reply with
// its previous value. The offset is validated against the 4Gib bit ceiling and
// the value against 0/1 before any write, so a bad argument never grows the
// key.
func SetBit(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	offset, ok := store.ParseInt(args[1])
	if !ok || offset < 0 || offset > maxBitOffset {
		r.Err(errBitOffset)
		return
	}
	bit, ok := store.ParseInt(args[2])
	if !ok || (bit != 0 && bit != 1) {
		r.Err(errBitValue)
		return
	}
	old, err := cx.St.SetBit(args[0], offset, int(bit), cx.NowMs)
	if err != nil {
		r.Err("ERR " + err.Error())
		return
	}
	r.Int(int64(old))
}

// GetBit answers GETBIT key offset: the addressed bit, 0 past the end or on a
// missing key. A read never grows the value, so an offset past the current
// length answers 0 from metadata without touching data, and the offset ceiling
// is the SETBIT ceiling.
func GetBit(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	offset, ok := store.ParseInt(args[1])
	if !ok || offset < 0 || offset > maxBitOffset {
		r.Err(errBitOffset)
		return
	}
	r.Int(int64(cx.St.GetBit(args[0], offset, cx.NowMs)))
}
