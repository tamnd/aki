package shard

import (
	"strconv"

	"github.com/tamnd/aki/f3srv/resp"
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
}

// renderStats formats the summed counters as the INFO bulk reply: one
// name:value line per field, resident memory and value-log accounting first,
// band counts after.
func renderStats(dst []byte, stats []uint64) []byte {
	var text []byte
	text = append(text, "# f3\r\n"...)
	for i := 0; i < NumStats && i < len(stats); i++ {
		text = append(text, statNames[i]...)
		text = append(text, ':')
		text = strconv.AppendUint(text, stats[i], 10)
		text = append(text, '\r', '\n')
	}
	return resp.AppendBulk(dst, text)
}
