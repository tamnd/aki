package stream

import (
	"github.com/tamnd/aki/engine/f3/hash"
	"github.com/tamnd/aki/engine/f3/shard"
)

// Object answers OBJECT ENCODING key for a stream (spec 2064/f3/14 section 6.8):
// a stream always reports the encoding "stream", whichever band it is in, which
// is what Redis reports and what the differential test checks. A key this package
// does not own falls through to the hash handler, which reports the hash bands and
// then delegates down the chain to list, set, zset, and the string store, so the
// one OBJECT verb answers for every type (stream then hash then list then set then
// zset then string).
//
// The stream probe reaches the registry through regs.Load, not registry(), so a
// read-only OBJECT against a non-stream key on a shard that never ran a stream
// command builds no registry and registers no gc maintainer, the same discipline
// Has and the TYPE probe keep: an encoding query must not leave residency state
// or a per-idle-boundary maintainer behind.
func Object(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	if eqFold(args[0], "ENCODING") && len(args) == 2 {
		if v, ok := regs.Load(cx.St); ok {
			if _, exists := v.(*reg).m[string(args[1])]; exists {
				r.Bulk([]byte("stream"))
				return
			}
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
