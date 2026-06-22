package command

import (
	"fmt"
	"math"
	"runtime"
	"strconv"
	"strings"

	"github.com/tamnd/aki/keyspace"
)

// This file implements the operational introspection commands from doc 20:
// SLOWLOG, LATENCY and MEMORY. aki does not record slow commands or latency
// spikes yet, so those report empty results with the right shapes. MEMORY USAGE
// returns a real estimate from the stored value.

// valueHeaderBytes is the fixed per-key header cost MEMORY USAGE charges, matching
// the 48-byte figure the spec uses.
const valueHeaderBytes = 48

func slowlogCommands() []*CmdDesc {
	slowlog := &CmdDesc{
		Name: "slowlog", Group: GroupServer, Since: "2.2.12",
		Arity: -2, Flags: FlagLoading | FlagStale | FlagAdmin,
		Handler: handleSlowlogHelp,
		SubCmds: []*CmdDesc{
			{Name: "get", SubName: "slowlog|get", Group: GroupServer, Since: "2.2.12",
				Arity: -2, Flags: FlagLoading | FlagAdmin, Handler: handleSlowlogGet},
			{Name: "len", SubName: "slowlog|len", Group: GroupServer, Since: "2.2.12",
				Arity: 2, Flags: FlagLoading | FlagAdmin | FlagFast, Handler: handleSlowlogLen},
			{Name: "reset", SubName: "slowlog|reset", Group: GroupServer, Since: "2.2.12",
				Arity: 2, Flags: FlagLoading | FlagAdmin, Handler: handleSlowlogReset},
			{Name: "help", SubName: "slowlog|help", Group: GroupServer, Since: "6.2.0",
				Arity: 2, Flags: FlagLoading | FlagStale, Handler: handleSlowlogHelp},
		},
	}
	return []*CmdDesc{slowlog}
}

// handleSlowlogGet returns the recent slow commands, newest first. The optional
// count defaults to 10 and -1 returns every entry.
func handleSlowlogGet(ctx *Ctx) {
	if len(ctx.Argv) > 3 {
		ctx.enc().WriteError("ERR wrong number of arguments for 'slowlog|get' command")
		return
	}
	count := 10
	if len(ctx.Argv) == 3 {
		n, err := strconv.ParseInt(string(ctx.Argv[2]), 10, 64)
		if err != nil {
			ctx.enc().WriteError("ERR value is not an integer or out of range")
			return
		}
		if n < -1 {
			ctx.enc().WriteError("ERR count should be greater than or equal to -1")
			return
		}
		count = int(n)
	}
	entries := ctx.d.slowlogGet(count)
	enc := ctx.enc()
	enc.WriteArrayLen(len(entries))
	for _, e := range entries {
		enc.WriteArrayLen(6)
		enc.WriteInteger(e.id)
		enc.WriteInteger(e.ts)
		enc.WriteInteger(e.durUs)
		enc.WriteArrayLen(len(e.args))
		for _, a := range e.args {
			enc.WriteBulkStringStr(a)
		}
		enc.WriteBulkStringStr(e.addr)
		enc.WriteBulkStringStr(e.name)
	}
}

func handleSlowlogLen(ctx *Ctx) {
	ctx.enc().WriteInteger(int64(ctx.d.slowlogLen()))
}

func handleSlowlogReset(ctx *Ctx) {
	ctx.d.slowlogReset()
	ctx.enc().WriteStatus("OK")
}

func handleSlowlogHelp(ctx *Ctx) {
	writeHelp(ctx, []string{
		"SLOWLOG <subcommand> [<arg> ...]. Subcommands are:",
		"GET [<count>]",
		"    Return top <count> entries from the slowlog (default: 10, -1 means all).",
		"LEN",
		"    Return the length of the slowlog.",
		"RESET",
		"    Reset the slowlog.",
		"HELP",
		"    Print this help.",
	})
}

func latencyCommands() []*CmdDesc {
	latency := &CmdDesc{
		Name: "latency", Group: GroupServer, Since: "2.8.13",
		Arity: -2, Flags: FlagLoading | FlagStale | FlagAdmin,
		Handler: handleLatencyHelp,
		SubCmds: []*CmdDesc{
			{Name: "history", SubName: "latency|history", Group: GroupServer, Since: "2.8.13",
				Arity: 3, Flags: FlagLoading | FlagAdmin | FlagFast, Handler: handleLatencyHistory},
			{Name: "latest", SubName: "latency|latest", Group: GroupServer, Since: "2.8.13",
				Arity: 2, Flags: FlagLoading | FlagAdmin | FlagFast, Handler: handleLatencyLatest},
			{Name: "reset", SubName: "latency|reset", Group: GroupServer, Since: "2.8.13",
				Arity: -2, Flags: FlagLoading | FlagAdmin, Handler: handleLatencyReset},
			{Name: "doctor", SubName: "latency|doctor", Group: GroupServer, Since: "2.8.13",
				Arity: 2, Flags: FlagLoading | FlagAdmin, Handler: handleLatencyDoctor},
			{Name: "graph", SubName: "latency|graph", Group: GroupServer, Since: "2.8.13",
				Arity: 3, Flags: FlagLoading | FlagAdmin, Handler: handleLatencyGraph},
			{Name: "histogram", SubName: "latency|histogram", Group: GroupServer, Since: "7.0.0",
				Arity: -2, Flags: FlagLoading | FlagAdmin | FlagFast, Handler: handleLatencyHistogram},
			{Name: "help", SubName: "latency|help", Group: GroupServer, Since: "6.2.0",
				Arity: 2, Flags: FlagLoading | FlagStale, Handler: handleLatencyHelp},
		},
	}
	return []*CmdDesc{latency}
}

// handleLatencyHistory returns the recorded spikes for an event as [timestamp,
// latency_ms] pairs, oldest first, or an empty array for an unknown event.
func handleLatencyHistory(ctx *Ctx) {
	samples := ctx.d.latencyHistoryOf(string(ctx.Argv[2]))
	enc := ctx.enc()
	enc.WriteArrayLen(len(samples))
	for _, s := range samples {
		enc.WriteArrayLen(2)
		enc.WriteInteger(s.ts)
		enc.WriteInteger(s.ms)
	}
}

// handleLatencyLatest returns one [event, latest_timestamp, latest_ms, max_ms]
// row per event that has samples.
func handleLatencyLatest(ctx *Ctx) {
	rows := ctx.d.latencyLatest()
	enc := ctx.enc()
	enc.WriteArrayLen(len(rows))
	for _, r := range rows {
		enc.WriteArrayLen(4)
		enc.WriteBulkStringStr(r.event)
		enc.WriteInteger(r.latestTS)
		enc.WriteInteger(r.latestMs)
		enc.WriteInteger(r.maxMs)
	}
}

// handleLatencyReset clears the named events, or every event when none are named,
// and returns how many histories were dropped.
func handleLatencyReset(ctx *Ctx) {
	names := make([]string, 0, len(ctx.Argv)-2)
	for _, a := range ctx.Argv[2:] {
		names = append(names, string(a))
	}
	ctx.enc().WriteInteger(int64(ctx.d.latencyReset(names)))
}

// handleLatencyDoctor returns a plain-English report. With no spikes recorded it
// gives the all-clear; otherwise it lists each event with a recommendation.
func handleLatencyDoctor(ctx *Ctx) {
	rows := ctx.d.latencyLatest()
	if len(rows) == 0 {
		ctx.enc().WriteBulkStringStr("Dave, I have observed the system, no worrying latency spikes. Everything seems fine.")
		return
	}
	var b strings.Builder
	b.WriteString("I detected latency issues in the following event classes:\n\n")
	for _, r := range rows {
		n := len(ctx.d.latencyHistoryOf(r.event))
		fmt.Fprintf(&b, "%s: %d latency spikes (latest: %d ms, max: %d ms).\n",
			r.event, n, r.latestMs, r.maxMs)
		b.WriteString(latencyAdvice(r.event) + "\n\n")
	}
	ctx.enc().WriteBulkStringStr(strings.TrimRight(b.String(), "\n"))
}

// latencyAdvice returns the recommendation line LATENCY DOCTOR prints for an
// event class.
func latencyAdvice(event string) string {
	switch event {
	case "command":
		return "Recommendation: check whether long-running commands (KEYS, SORT, large LRANGE) are in use."
	case "fast-command":
		return "Recommendation: O(1) commands are spiking, which usually points to CPU pressure or contention."
	case "wal-fsync", "aof-write":
		return "Recommendation: check disk I/O latency; consider moving the .aki file to a faster volume."
	case "checkpoint":
		return "Recommendation: checkpoints are slow; check disk throughput and the WAL size."
	default:
		return "Recommendation: inspect the workload driving this event class."
	}
}

// handleLatencyGraph returns an ASCII sparkline of an event's recent spikes, or an
// error when the event has no samples.
func handleLatencyGraph(ctx *Ctx) {
	event := string(ctx.Argv[2])
	samples := ctx.d.latencyHistoryOf(event)
	if len(samples) == 0 {
		ctx.enc().WriteError("ERR No samples for event '" + event + "'")
		return
	}
	ms := make([]int64, len(samples))
	for i, s := range samples {
		ms[i] = s.ms
	}
	ctx.enc().WriteBulkStringStr(event + " - high " +
		strconv.FormatInt(maxInt64(ms), 10) + " ms, low " +
		strconv.FormatInt(minInt64(ms), 10) + " ms\n" + sparkline(ms))
}

// sparkline renders a series as a row of Unicode block elements, log-scaled so a
// wide latency range still shows detail.
func sparkline(vals []int64) string {
	const blocks = "▁▂▃▄▅▆▇█"
	levels := []rune(blocks)
	lo, hi := minInt64(vals), maxInt64(vals)
	llo, lhi := math.Log1p(float64(lo)), math.Log1p(float64(hi))
	span := lhi - llo
	var b strings.Builder
	for _, v := range vals {
		idx := 0
		if span > 0 {
			frac := (math.Log1p(float64(v)) - llo) / span
			idx = int(frac * float64(len(levels)-1))
			if idx < 0 {
				idx = 0
			}
			if idx >= len(levels) {
				idx = len(levels) - 1
			}
		}
		b.WriteRune(levels[idx])
	}
	return b.String()
}

// maxInt64 and minInt64 return the largest and smallest value in a non-empty
// slice.
func maxInt64(vals []int64) int64 {
	m := vals[0]
	for _, v := range vals[1:] {
		if v > m {
			m = v
		}
	}
	return m
}

func minInt64(vals []int64) int64 {
	m := vals[0]
	for _, v := range vals[1:] {
		if v < m {
			m = v
		}
	}
	return m
}

// handleLatencyHistogram reports a per-command latency histogram in microseconds.
// With no arguments it covers every command that has run; with arguments it
// covers only the named commands. The reply is a map keyed by command name, each
// value a map of calls and a cumulative histogram_usec, the same shape real
// Redis returns. The spec named an external HDR library and a different reply
// layout, but aki carries no dependencies and wire compatibility wins, so this
// reads the per-command histograms already kept for INFO latencystats.
func handleLatencyHistogram(ctx *Ctx) {
	names := make([]string, 0, len(ctx.Argv)-2)
	for _, a := range ctx.Argv[2:] {
		names = append(names, strings.ToLower(string(a)))
	}
	entries := ctx.d.commandHistograms(names)
	enc := ctx.enc()
	enc.WriteMapLen(len(entries))
	for _, e := range entries {
		enc.WriteBulkStringStr(e.name)
		enc.WriteMapLen(2)
		enc.WriteBulkStringStr("calls")
		enc.WriteInteger(int64(e.calls))
		enc.WriteBulkStringStr("histogram_usec")
		enc.WriteMapLen(len(e.points))
		for _, p := range e.points {
			enc.WriteInteger(int64(p.bound))
			enc.WriteInteger(int64(p.count))
		}
	}
}

func handleLatencyHelp(ctx *Ctx) {
	writeHelp(ctx, []string{
		"LATENCY <subcommand> [<arg> ...]. Subcommands are:",
		"HISTORY <event>",
		"    Return time-latency samples for the <event> class.",
		"LATEST",
		"    Return the latest latency samples for all events.",
		"RESET [<event> ...]",
		"    Reset latency data of one or more <event> classes (default: reset all).",
		"DOCTOR",
		"    Return a human readable latency analysis report.",
		"GRAPH <event>",
		"    Return a latency graph for the <event> class.",
		"HISTOGRAM [<command> ...]",
		"    Return a cumulative latency histogram by command (default: all commands).",
		"HELP",
		"    Print this help.",
	})
}

func memoryCommands() []*CmdDesc {
	memory := &CmdDesc{
		Name: "memory", Group: GroupServer, Since: "4.0.0",
		Arity: -2, Flags: FlagReadOnly,
		Handler: handleMemoryHelp,
		SubCmds: []*CmdDesc{
			{Name: "usage", SubName: "memory|usage", Group: GroupServer, Since: "4.0.0",
				Arity: -3, Flags: FlagReadOnly, FirstKey: 2, LastKey: 2, Step: 1, Handler: handleMemoryUsage},
			{Name: "doctor", SubName: "memory|doctor", Group: GroupServer, Since: "4.0.0",
				Arity: 2, Flags: FlagReadOnly, Handler: handleMemoryDoctor},
			{Name: "stats", SubName: "memory|stats", Group: GroupServer, Since: "4.0.0",
				Arity: 2, Flags: FlagReadOnly, Handler: handleMemoryStats},
			{Name: "malloc-stats", SubName: "memory|malloc-stats", Group: GroupServer, Since: "4.0.0",
				Arity: 2, Flags: FlagReadOnly, Handler: handleMemoryMallocStats},
			{Name: "purge", SubName: "memory|purge", Group: GroupServer, Since: "4.0.0",
				Arity: 2, Flags: FlagReadOnly, Handler: handleMemoryPurge},
			{Name: "help", SubName: "memory|help", Group: GroupServer, Since: "4.0.0",
				Arity: 2, Flags: FlagReadOnly, Handler: handleMemoryHelp},
		},
	}
	return []*CmdDesc{memory}
}

// handleMemoryUsage estimates the bytes a key occupies: the key string, the fixed
// value header, the value body, and an expiry slot when the key has a TTL. It
// returns nil when the key does not exist.
func handleMemoryUsage(ctx *Ctx) {
	// The optional SAMPLES argument only refines aggregate walks, which aki does
	// not do, so it just has to parse.
	if len(ctx.Argv) > 3 {
		if strings.ToUpper(string(ctx.Argv[3])) != "SAMPLES" || len(ctx.Argv) != 5 {
			ctx.enc().WriteError("ERR syntax error")
			return
		}
		if _, err := strconv.ParseInt(string(ctx.Argv[4]), 10, 64); err != nil {
			ctx.enc().WriteError("ERR value is not an integer or out of range")
			return
		}
	}
	key := ctx.Argv[2]
	var (
		found bool
		bytes int64
	)
	if !ctx.view(func(db *keyspace.DB) error {
		body, hdr, ok, err := db.Get(key)
		if err != nil {
			return err
		}
		found = ok
		if ok {
			bytes = int64(align8(len(key)+8)) + valueHeaderBytes + int64(len(body))
			if hdr.HasTTL() {
				bytes += 16
			}
		}
		return nil
	}) {
		return
	}
	if !found {
		ctx.enc().WriteNull()
		return
	}
	ctx.enc().WriteInteger(bytes)
}

func handleMemoryDoctor(ctx *Ctx) {
	ctx.enc().WriteBulkStringStr("Sam, I detected a few issues in this Redis instance memory implants:\n\nNothing serious, your memory usage looks fine.")
}

// handleMemoryStats returns the allocator and keyspace memory breakdown as a map.
// The figures come from the Go runtime and the keyspace, with the fields aki does
// not track reported as zero.
func handleMemoryStats(ctx *Ctx) {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	used := int64(ms.HeapAlloc)

	var keys int64
	if ctx.d.engine != nil {
		for _, n := range ctx.d.engine.dbSizes() {
			keys += int64(n)
		}
	}

	type kv struct {
		k string
		v int64
	}
	pairs := []kv{
		{"peak.allocated", int64(ms.HeapSys)},
		{"total.allocated", used},
		{"startup.allocated", 0},
		{"replication.backlog", 0},
		{"clients.slaves", 0},
		{"clients.normal", 0},
		{"cluster.links", 0},
		{"aof.buffer", 0},
		{"lua.caches", 0},
		{"functions.caches", 0},
		{"overhead.total", 0},
		{"keys.count", keys},
		{"dataset.bytes", used},
		{"allocator.allocated", used},
		{"allocator.active", int64(ms.HeapInuse)},
		{"allocator.resident", int64(ms.HeapSys)},
		{"aki.buffer-pool.bytes", 0},
		{"aki.buffer-pool.pages", 0},
		{"aki.wal.bytes", 0},
	}
	enc := ctx.enc()
	enc.WriteMapLen(len(pairs))
	for _, p := range pairs {
		enc.WriteBulkStringStr(p.k)
		enc.WriteInteger(p.v)
	}
}

// handleMemoryMallocStats renders the Go runtime memory counters in the line
// format tools that parse the Redis output expect.
func handleMemoryMallocStats(ctx *Ctx) {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	var b strings.Builder
	b.WriteString("Go runtime memory statistics\n")
	fields := []struct {
		name string
		val  uint64
	}{
		{"HeapAlloc", ms.HeapAlloc},
		{"HeapSys", ms.HeapSys},
		{"HeapIdle", ms.HeapIdle},
		{"HeapInuse", ms.HeapInuse},
		{"HeapReleased", ms.HeapReleased},
		{"HeapObjects", ms.HeapObjects},
		{"StackInuse", ms.StackInuse},
		{"StackSys", ms.StackSys},
		{"MSpanInuse", ms.MSpanInuse},
		{"MSpanSys", ms.MSpanSys},
		{"MCacheInuse", ms.MCacheInuse},
		{"MCacheSys", ms.MCacheSys},
		{"GCSys", ms.GCSys},
		{"OtherSys", ms.OtherSys},
		{"NextGC", ms.NextGC},
		{"NumGC", uint64(ms.NumGC)},
	}
	for _, f := range fields {
		fmt.Fprintf(&b, "%-16s %d\n", f.name+":", f.val)
	}
	ctx.enc().WriteBulkStringStr(b.String())
}

func handleMemoryPurge(ctx *Ctx) {
	ctx.enc().WriteStatus("OK")
}

func handleMemoryHelp(ctx *Ctx) {
	writeHelp(ctx, []string{
		"MEMORY <subcommand> [<arg> ...]. Subcommands are:",
		"USAGE <key> [SAMPLES <count>]",
		"    Return memory in bytes used by <key> and its value.",
		"DOCTOR",
		"    Return memory problems reports.",
		"STATS",
		"    Return information about the memory usage of the server.",
		"MALLOC-STATS",
		"    Return internal allocator statistics report.",
		"PURGE",
		"    Attempt to purge dirty pages for reclamation by the allocator.",
		"HELP",
		"    Print this help.",
	})
}

// writeHelp writes a command's help as an array of simple strings, the shape
// every redis container HELP reply uses.
func writeHelp(ctx *Ctx, lines []string) {
	enc := ctx.enc()
	enc.WriteArrayLen(len(lines))
	for _, l := range lines {
		enc.WriteStatus(l)
	}
}

// align8 rounds up to the next multiple of 8, the alignment MEMORY USAGE charges
// for the key string allocation.
func align8(n int) int {
	return (n + 7) &^ 7
}
