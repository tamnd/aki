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
	StatUsedMemory
	StatIndexBytes
	StatArenaLive
	StatChunkedBytes
	StatVlogReads
	StatPromotes
	StatDemotes
	StatBackpressureWaits
	StatBackpressureStalls
	StatVolatileKeys
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
	StatVolatileKeys:       "volatile_keys",
}

// memDoctorFloor is the used-memory figure MEMORY DOCTOR needs to see before it
// will render a health verdict, matching the 5MB floor redis's own doctor uses:
// below it the dataset is too small for any ratio to be meaningful, so the
// honest answer is that there is nothing to diagnose.
const memDoctorFloor = 5 << 20

// renderMemStats formats the summed counters as MEMORY STATS' flat field-value
// array (spec 2064/f3/18): a RESP array of alternating name bulk and integer,
// the shape redis-cli folds into a map. f3 reports the figures it accounts for
// honestly and omits the allocator-internal fields (fragmentation ratios, peak
// and startup allocation, per-db overhead) it does not model, rather than
// inventing numbers a client would read as real. keys.count is the whole-
// keyspace live count, total.allocated the shards' allocator-held bytes (the
// used_memory INFO reports), dataset.bytes the live record charge, and the
// index and value-log figures the f3 memory bar divides against.
func (r *Runtime) renderMemStats(dst []byte, stats []uint64) []byte {
	get := func(i int) uint64 {
		if i < len(stats) {
			return stats[i]
		}
		return 0
	}
	keys := get(StatKeys)
	total := get(StatUsedMemory)
	var perKey uint64
	if keys != 0 {
		perKey = total / keys
	}
	fields := []struct {
		name string
		v    uint64
	}{
		{"keys.count", keys},
		{"total.allocated", total},
		{"dataset.bytes", get(StatArenaLive)},
		{"index.bytes", get(StatIndexBytes)},
		{"vlog.bytes", get(StatVlogBytes)},
		{"keys.bytes-per-key", perKey},
	}
	dst = resp.AppendArrayHeader(dst, len(fields)*2)
	for _, f := range fields {
		dst = resp.AppendBulk(dst, []byte(f.name))
		dst = resp.AppendInt(dst, int64(f.v))
	}
	return dst
}

// renderMemDoctor folds the aggregate used-memory figure into MEMORY DOCTOR's
// bulk verdict. Below the doctor floor the dataset is too small to diagnose, the
// answer redis gives an idle instance; above it f3 reports no issue, since the
// allocator-ratio faults redis's doctor looks for (fragmentation, peak-to-current
// drift, RSS overhead) come from a general-purpose allocator the arena does not
// have. The wording tracks redis so a client that prints the doctor line reads a
// familiar verdict.
func (r *Runtime) renderMemDoctor(dst []byte, stats []uint64) []byte {
	get := func(i int) uint64 {
		if i < len(stats) {
			return stats[i]
		}
		return 0
	}
	var msg string
	if get(StatUsedMemory) < memDoctorFloor {
		msg = "Hi Sam, this instance is empty or is using very little memory, my issues detector can't be used in these conditions. Please, leave this server alone, I can't help you, and go away."
	} else {
		msg = "Sam, I can't find any memory issue in your instance. I can only account for what occurs on this base."
	}
	return resp.AppendBulk(dst, []byte(msg))
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
	// The Keyspace section reports one db line the way redis does, folded from
	// the summed counters: keys is the whole-keyspace live count (StatKeys, every
	// type), expires the count carrying a key-level TTL (StatVolatileKeys). The
	// header always shows, matching redis; the db0 line shows only when the db
	// holds keys, so a fresh server renders the header with no db line, as redis
	// does. avg_ttl stays 0 (redis reports the active-cycle running estimate,
	// which f3's lazy-only expiry does not sample yet) and subexpiry 0 (the
	// per-field HEXPIRE census is owed to a later slice); both are honest zeros a
	// redis client parses without complaint.
	text = append(text, "\r\n# Keyspace\r\n"...)
	if keys := get(StatKeys); keys != 0 {
		text = append(text, "db0:keys="...)
		text = strconv.AppendUint(text, keys, 10)
		text = append(text, ",expires="...)
		text = strconv.AppendUint(text, get(StatVolatileKeys), 10)
		text = append(text, ",avg_ttl=0,subexpiry=0\r\n"...)
	}
	if r.netInfo != nil {
		text = r.netInfo(text)
	}
	return resp.AppendBulk(dst, text)
}
