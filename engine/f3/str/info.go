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
//
// It is the string-store view; the dispatch INFO handler wraps it through
// WriteInfoBlob to fold in the collection keyspaces the keys count would
// otherwise miss.
func InfoShard(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	WriteInfoBlob(cx, 0, r)
}

// WriteInfoBlob builds and writes one shard's FanStats blob. extraKeys is added
// to the string store's own key count so the gathered keys:N line counts every
// keyspace, not just strings; the dispatch INFO handler supplies the collection
// key totals because the type packages cannot be imported here without a cycle.
// The blob stays on the stack: r.Raw copies it into the fan buffer before this
// returns, so nothing escapes.
func WriteInfoBlob(cx *shard.Ctx, extraKeys uint64, r shard.Reply) {
	var b [shard.NumStats * 8]byte
	put := func(i int, v uint64) { binary.LittleEndian.PutUint64(b[i*8:], v) }
	st := cx.St
	m := st.Mem()
	put(shard.StatKeys, m.Keys+extraKeys)
	put(shard.StatArenaUsed, m.ArenaAllocBytes)
	put(shard.StatArenaTotal, m.ArenaTotalBytes)
	put(shard.StatVlogBytes, m.VlogTotalBytes)
	put(shard.StatVlogDead, m.VlogTotalBytes-m.VlogLiveBytes)
	bs := st.Stats()
	put(shard.StatVlogRuns, bs.LogRuns)
	put(shard.StatBandInt, bs.Int)
	put(shard.StatBandEmbedded, bs.Embedded)
	put(shard.StatBandSeparated, bs.Separated)
	put(shard.StatBandChunked, bs.Chunked)
	put(shard.StatUsedMemory, m.UsedMemory())
	put(shard.StatIndexBytes, m.IndexBytes)
	put(shard.StatArenaLive, m.ArenaLiveBytes)
	put(shard.StatChunkedBytes, m.ChunkedBytes)
	rs := st.Resid()
	put(shard.StatVlogReads, rs.LogReads)
	put(shard.StatPromotes, rs.Promotes)
	put(shard.StatDemotes, rs.Demotes)
	put(shard.StatBackpressureWaits, cx.BackpressureWaits())
	put(shard.StatBackpressureStalls, cx.BackpressureStalls())
	r.Raw(b[:])
}
