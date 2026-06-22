package command

import (
	"math/bits"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// This file implements per-command statistics, the data behind the INFO
// commandstats, latencystats, and errorstats sections (doc 20 sections 1.9 to
// 1.11) and CONFIG RESETSTAT. Every command that runs records its call count,
// cumulative execution time, and a latency sample; commands rejected before they
// run and commands that return an error are counted separately, and every error
// reply is tallied by its leading error code.

// cmdStat holds the counters for one command name. The fields are atomic so the
// dispatch path can update them without taking a lock, and INFO reads them with
// plain atomic loads.
type cmdStat struct {
	calls    atomic.Uint64
	usec     atomic.Uint64
	rejected atomic.Uint64
	failed   atomic.Uint64
	hist     latencyHist
}

// statsState holds the whole stats table. cmds maps a command name (with its
// subcommand, separated by "|") to its counters. A read-write lock guards the map
// shape; the per-entry counters are atomic so only inserts need the write lock.
// errs tallies error replies by their leading code.
type statsState struct {
	mu   sync.RWMutex
	cmds map[string]*cmdStat
	errs sync.Map // string -> *atomic.Uint64
}

// statsInit allocates the stats table. New calls it once at startup.
func (d *Dispatcher) statsInit() {
	d.stats.cmds = make(map[string]*cmdStat)
}

// statName is the name a command is recorded under: the subcommand-qualified name
// for container commands, the plain name otherwise.
func statName(cmd *CmdDesc) string {
	if cmd.SubName != "" {
		return cmd.SubName
	}
	return cmd.Name
}

// cmdStatFor returns the counter block for a name, creating it on first use.
func (d *Dispatcher) cmdStatFor(name string) *cmdStat {
	d.stats.mu.RLock()
	cs := d.stats.cmds[name]
	d.stats.mu.RUnlock()
	if cs != nil {
		return cs
	}
	d.stats.mu.Lock()
	defer d.stats.mu.Unlock()
	if cs = d.stats.cmds[name]; cs == nil {
		cs = &cmdStat{}
		d.stats.cmds[name] = cs
	}
	return cs
}

// statCall records one successful execution: the call, its microseconds, a
// latency sample, and a failed_calls bump when it returned an error.
func (d *Dispatcher) statCall(cmd *CmdDesc, usec uint64, failed bool) {
	cs := d.cmdStatFor(statName(cmd))
	cs.calls.Add(1)
	cs.usec.Add(usec)
	cs.hist.record(usec)
	if failed {
		cs.failed.Add(1)
	}
}

// statReject records one command rejected before it ran, by ACL, arity, the
// read-only replica guard, or an out-of-memory refusal.
func (d *Dispatcher) statReject(cmd *CmdDesc) {
	d.cmdStatFor(statName(cmd)).rejected.Add(1)
}

// statError tallies an error reply by its leading code, the first token of the
// message up to the first space.
func (d *Dispatcher) statError(msg string) {
	code := msg
	if i := strings.IndexByte(msg, ' '); i >= 0 {
		code = msg[:i]
	}
	if code == "" {
		return
	}
	if v, ok := d.stats.errs.Load(code); ok {
		v.(*atomic.Uint64).Add(1)
		return
	}
	v, _ := d.stats.errs.LoadOrStore(code, new(atomic.Uint64))
	v.(*atomic.Uint64).Add(1)
}

// statResetAll clears every command counter, latency histogram, and error tally.
// CONFIG RESETSTAT calls it.
func (d *Dispatcher) statResetAll() {
	d.stats.mu.Lock()
	d.stats.cmds = make(map[string]*cmdStat)
	d.stats.mu.Unlock()
	d.stats.errs.Range(func(k, _ any) bool {
		d.stats.errs.Delete(k)
		return true
	})
}

// errPrefix reports whether a reply segment is an error and returns its code. The
// segment is the raw bytes a handler wrote, so an error starts with '-'.
func errPrefix(reply []byte) (string, bool) {
	if len(reply) == 0 || reply[0] != '-' {
		return "", false
	}
	end := len(reply)
	for i := 1; i < len(reply); i++ {
		if reply[i] == ' ' || reply[i] == '\r' || reply[i] == '\n' {
			end = i
			break
		}
	}
	return string(reply[1:end]), true
}

// latencyHist is a log-linear histogram of microsecond latencies. Each power of
// two is split into histSub linear sub-buckets, which gives roughly 1/histSub
// relative precision, enough for the p50/p99/p99.9 percentiles INFO reports. The
// bucket counts are atomic so recording needs no lock.
const (
	histSubBits = 3
	histSub     = 1 << histSubBits
	histBuckets = 64 * histSub
)

type latencyHist struct {
	counts [histBuckets]atomic.Uint64
}

// histBucket maps a value to its bucket. Values below histSub land in a linear
// region at the bottom; larger values land in the sub-bucket of their power of
// two.
func histBucket(v uint64) int {
	if v < histSub {
		return int(v)
	}
	msb := bits.Len64(v) - 1
	sub := (v >> (msb - histSubBits)) & (histSub - 1)
	idx := msb*histSub + int(sub)
	if idx >= histBuckets {
		idx = histBuckets - 1
	}
	return idx
}

// histLow returns the low edge of a bucket in microseconds, the value reported
// for a percentile that falls in it.
func histLow(idx int) uint64 {
	if idx < histSub {
		return uint64(idx)
	}
	msb := idx / histSub
	sub := idx % histSub
	return (uint64(1) << msb) + (uint64(sub) << (msb - histSubBits))
}

func (h *latencyHist) record(v uint64) {
	h.counts[histBucket(v)].Add(1)
}

// total returns the number of samples recorded.
func (h *latencyHist) total() uint64 {
	var n uint64
	for i := range h.counts {
		n += h.counts[i].Load()
	}
	return n
}

// percentile returns the latency at the given percentile in microseconds, or 0
// when no samples exist. p is in the range 0 to 100.
func (h *latencyHist) percentile(p float64) uint64 {
	total := h.total()
	if total == 0 {
		return 0
	}
	target := uint64(p / 100 * float64(total))
	if target >= total {
		target = total - 1
	}
	var cum uint64
	for i := range h.counts {
		cum += h.counts[i].Load()
		if cum > target {
			return histLow(i)
		}
	}
	return histLow(histBuckets - 1)
}

// fmtUsec formats a microsecond figure with two decimal places, the form Redis
// uses for usec_per_call and the latency percentiles.
func fmtUsec(v float64) string {
	return strconv.FormatFloat(v, 'f', 2, 64)
}
