package shard

import (
	"strconv"

	"github.com/tamnd/aki/obs1srv/resp"
)

// The stats fan-out schema: every shard answers a FanStats partial of exactly
// these fields, 8 bytes little-endian each, in this order, and the gather side
// sums them position-wise. The blob is the RAM-exceeded evidence surface the
// string LTM scenario reads (doc 09 section 8): resident bytes, value-log
// bytes, and per-band record counts.
const (
	StatKeys = iota
	StatArenaUsed
	StatArenaTotal
	StatVlogBytes
	StatVlogDead
	StatVlogRuns
	StatBandInt
	StatBandEmbedded
	StatBandSeparated
	StatBandChunked
	StatUsedMemory
	StatIndexBytes
	StatArenaLive
	StatChunkedBytes
	StatVlogReads
	StatPromotes
	StatDemotes
	StatBackpressureWaits
	StatBackpressureStalls
	StatParkWaitsResident
	StatParkWaitsFlushlag
	StatParkWaitsLease
	StatParkStallsResident
	StatParkStallsFlushlag
	StatParkStallsLease
	NumStats
)

var statNames = [NumStats]string{
	StatKeys:          "keys",
	StatArenaUsed:     "arena_used_bytes",
	StatArenaTotal:    "arena_total_bytes",
	StatVlogBytes:     "vlog_bytes",
	StatVlogDead:      "vlog_dead_bytes",
	StatVlogRuns:      "vlog_runs",
	StatBandInt:       "band_int",
	StatBandEmbedded:  "band_embedded",
	StatBandSeparated: "band_separated",
	StatBandChunked:   "band_chunked",
	StatUsedMemory:    "used_memory",
	StatIndexBytes:    "index_bytes",
	StatArenaLive:     "arena_live_bytes",
	StatChunkedBytes:  "chunked_bytes",
	StatVlogReads:     "vlog_reads",
	StatPromotes:      "ltm_promotes",
	StatDemotes:       "ltm_demotes",

	StatBackpressureWaits:  "backpressure_waits",
	StatBackpressureStalls: "backpressure_stalls",

	// The doc 04 section 6 park-reason split (park.go): waits and stalls per
	// reason, summing to the two totals above. Only the resident rows can move
	// until the WAL and lease slices raise the other two reasons.
	StatParkWaitsResident:  "backpressure_waits_resident",
	StatParkWaitsFlushlag:  "backpressure_waits_flushlag",
	StatParkWaitsLease:     "backpressure_waits_lease",
	StatParkStallsResident: "backpressure_stalls_resident",
	StatParkStallsFlushlag: "backpressure_stalls_flushlag",
	StatParkStallsLease:    "backpressure_stalls_lease",
}

// appendStat writes one name:value INFO line.
func appendStat(text []byte, name string, v uint64) []byte {
	text = append(text, name...)
	text = append(text, ':')
	text = strconv.AppendUint(text, v, 10)
	return append(text, '\r', '\n')
}

// renderStats formats the summed counters as the INFO bulk reply. The Memory
// section leads with the fields a Redis INFO parser looks for: used_memory is
// the shards' allocator-held bytes (store.MemLedger.UsedMemory: index tables
// plus the arena's touched-segment fill, dead-but-uncompacted bytes included,
// the figure comparable to redis's allocator-held used_memory; no Go runtime
// slack, no value-log bytes, which are disk), so it is an honest account, not
// RSS.
// used_memory_rss is the kernel's resident figure where the platform exposes
// it, and the two are expected to differ. The f3 section keeps the per-band
// census and log accounting the LTM harness reads. The transport's "# Net"
// section, when a driver registered one through SetNetInfo, comes last.
func (r *Runtime) renderStats(dst []byte, stats []uint64) []byte {
	get := func(i int) uint64 {
		if i < len(stats) {
			return stats[i]
		}
		return 0
	}
	var text []byte
	text = append(text, "# Memory\r\n"...)
	text = appendStat(text, "used_memory", get(StatUsedMemory))
	if rss := readRSS(); rss != 0 {
		text = appendStat(text, "used_memory_rss", rss)
	}
	text = append(text, "\r\n# f3\r\n"...)
	for i := 0; i < NumStats && i < len(stats); i++ {
		if i == StatUsedMemory {
			continue
		}
		text = appendStat(text, statNames[i], stats[i])
	}
	if r.walInfo != nil {
		text = r.walInfo(text)
	}
	if r.netInfo != nil {
		text = r.netInfo(text)
	}
	return resp.AppendBulk(dst, text)
}
