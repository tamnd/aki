package command

import (
	"fmt"
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

// handleSlowlogGet returns the recent slow commands, newest first. aki does not
// record them yet, so the list is always empty.
func handleSlowlogGet(ctx *Ctx) {
	ctx.enc().WriteArrayLen(0)
}

func handleSlowlogLen(ctx *Ctx) {
	ctx.enc().WriteInteger(0)
}

func handleSlowlogReset(ctx *Ctx) {
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
			{Name: "help", SubName: "latency|help", Group: GroupServer, Since: "6.2.0",
				Arity: 2, Flags: FlagLoading | FlagStale, Handler: handleLatencyHelp},
		},
	}
	return []*CmdDesc{latency}
}

// handleLatencyHistory returns the latency samples for an event. aki tracks no
// samples yet, so the answer is an empty array for any event.
func handleLatencyHistory(ctx *Ctx) {
	ctx.enc().WriteArrayLen(0)
}

func handleLatencyLatest(ctx *Ctx) {
	ctx.enc().WriteArrayLen(0)
}

func handleLatencyReset(ctx *Ctx) {
	ctx.enc().WriteInteger(0)
}

func handleLatencyDoctor(ctx *Ctx) {
	ctx.enc().WriteBulkStringStr("Dave, I have observed the system, no worrying latency spikes. Everything seems fine.")
}

// handleLatencyGraph errors because there are never samples for an event yet.
func handleLatencyGraph(ctx *Ctx) {
	ctx.enc().WriteError("ERR No samples for event '" + string(ctx.Argv[2]) + "'")
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
