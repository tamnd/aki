package str

import (
	"github.com/tamnd/aki/engine/obs1/shard"
	"github.com/tamnd/aki/engine/obs1/store"
)

// SETEX and PSETEX, the classic sugar predating SET's expiry options:
// one unconditional write with a relative deadline. Unlike the EXPIRE
// family a non-positive time is refused, the same rule SET EX applies,
// with the command's own name in the error.

// setexGeneric writes value under a relative deadline resolved in unit.
func setexGeneric(cx *shard.Ctx, args [][]byte, r shard.Reply, name string, unit int) {
	key, val := args[0], args[2]
	n, ok := store.ParseInt(args[1])
	if !ok {
		r.Err("ERR value is not an integer or out of range")
		return
	}
	atMs, ok := deadline(cx.NowMs, unit, n)
	if !ok {
		r.Err("ERR invalid expire time in '" + name + "' command")
		return
	}
	if err := cx.St.SetString(key, val, cx.NowMs, atMs, false); err != nil {
		if cx.ParkFull(err) {
			return
		}
		r.Err(storeErr(err))
		return
	}
	if err := cx.LogStrSet(key, val, atMs, false); err != nil {
		r.Err(err.Error())
		return
	}
	r.Status("OK")
}

// Setex answers SETEX key seconds value.
func Setex(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	setexGeneric(cx, args, r, "setex", unitEXsec)
}

// Psetex answers PSETEX key milliseconds value.
func Psetex(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	setexGeneric(cx, args, r, "psetex", unitPXms)
}
