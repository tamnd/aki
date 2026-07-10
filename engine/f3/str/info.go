package str

import (
	"encoding/binary"

	"github.com/tamnd/aki/engine/f3/shard"
)

// InfoShard answers an INFO sub-command: one shard's counters as the
// fixed-width FanStats blob, 8 bytes little-endian per field in the
// shard.Stat order. The gather side sums the blobs position-wise and renders
// the text reply, so the fields here must be sum-friendly (byte and record
// counts, never ratios). This is the RAM-exceeded evidence surface of doc 09
// section 8: the harness reads resident bytes against the log bytes and the
// band census to prove the working set outgrew RAM and the store kept
// serving.
func InfoShard(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	var b [shard.NumStats * 8]byte
	put := func(i int, v uint64) { binary.LittleEndian.PutUint64(b[i*8:], v) }
	st := cx.St
	put(shard.StatKeys, uint64(st.Len()))
	used, total := st.ArenaBytes()
	put(shard.StatArenaUsed, used)
	put(shard.StatArenaTotal, total)
	lt, ld := st.LogBytes()
	put(shard.StatVlogBytes, lt)
	put(shard.StatVlogDead, ld)
	bs := st.Stats()
	put(shard.StatVlogRuns, bs.LogRuns)
	put(shard.StatBandInt, bs.Int)
	put(shard.StatBandEmbedded, bs.Embedded)
	put(shard.StatBandSeparated, bs.Separated)
	put(shard.StatBandChunked, bs.Chunked)
	r.Raw(b[:])
}
