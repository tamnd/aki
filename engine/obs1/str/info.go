package str

import (
	"encoding/binary"

	"github.com/tamnd/aki/engine/obs1/shard"
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
	m := st.Mem()
	put(shard.StatKeys, m.Keys)
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
	// The obs1 park-reason split (engine/obs1/shard park.go, doc 04 section
	// 6): waits and stalls per reason, summing to the two totals above.
	put(shard.StatParkWaitsResident, cx.ParkWaits(shard.ParkResident))
	put(shard.StatParkWaitsFlushlag, cx.ParkWaits(shard.ParkFlushlag))
	put(shard.StatParkWaitsLease, cx.ParkWaits(shard.ParkLease))
	put(shard.StatParkStallsResident, cx.ParkStalls(shard.ParkResident))
	put(shard.StatParkStallsFlushlag, cx.ParkStalls(shard.ParkFlushlag))
	put(shard.StatParkStallsLease, cx.ParkStalls(shard.ParkLease))
	r.Raw(b[:])
}

// DBSizeShard answers a DBSIZE sub-command: one shard's live key count as a
// FanCount partial; the gather sums the shards into the single integer reply,
// so DBSIZE is O(shards), never a scan. With used_memory this is the
// bytes-per-key denominator the benchmark harness reads.
func DBSizeShard(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	r.FanCount(int64(cx.St.Len()))
}
