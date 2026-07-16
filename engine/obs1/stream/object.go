package stream

import (
	"github.com/tamnd/aki/engine/obs1/hash"
	"github.com/tamnd/aki/engine/obs1/shard"
)

// Object answers OBJECT ENCODING key for a stream (spec 2064/f3/14 section 6.8):
// a stream always reports the encoding "stream", whichever band it is in, which
// is what Redis reports and what the differential test checks. A key this package
// does not own falls through to the hash handler, which reports the hash bands and
// then delegates down the chain to list, set, and the string store, so the one
// OBJECT verb answers for every type (stream then hash then list then set then
// string).
func Object(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	if eqFold(args[0], "ENCODING") && len(args) == 2 {
		if _, ok := registry(cx).m[string(args[1])]; ok {
			r.Bulk([]byte("stream"))
			return
		}
	}
	hash.Object(cx, args, r)
}

// eqFold is a case-insensitive ASCII compare of b against the uppercase word s,
// the subcommand token check without allocating a lowercase copy.
func eqFold(b []byte, s string) bool {
	if len(b) != len(s) {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := b[i]
		if c >= 'a' && c <= 'z' {
			c -= 32
		}
		if c != s[i] {
			return false
		}
	}
	return true
}
