package command

import (
	"math/bits"
	"sort"
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
	// calls is striped per CPU because it is the only counter the integrated fast
	// path still writes per command (statCallFast dropped usec and the histogram),
	// so under a saturating single-command load it was the one shared cache line
	// every core fought over. usec, rejected and failed stay plain atomics: they are
	// written only off the timed slow path, never on the fast path, so they carry no
	// saturation contention worth striping.
	calls    stripedUint64
	usec     atomic.Uint64
	rejected atomic.Uint64
	failed   atomic.Uint64
	hist     latencyHist
}

// reset zeroes every counter and the latency histogram in place. CONFIG RESETSTAT
// uses it so the blocks stay at the addresses cmd.stat already points at.
func (cs *cmdStat) reset() {
	cs.calls.Store(0)
	cs.usec.Store(0)
	cs.rejected.Store(0)
	cs.failed.Store(0)
	cs.hist.reset()
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

// statsInit allocates the stats table and links every command descriptor to its
// counter block. New calls it once at startup, after the table is built. Linking
// up front lets statCall and statReject reach the block through cmd.stat with a
// plain atomic bump, instead of taking the stats RWMutex and looking the name up
// in the map on every command. The map keeps the same pointers so INFO,
// LATENCY HISTOGRAM, and the metrics endpoint read the same counters.
func (d *Dispatcher) statsInit() {
	d.stats.cmds = make(map[string]*cmdStat)
	link := func(cmd *CmdDesc) {
		name := statName(cmd)
		cs := d.stats.cmds[name]
		if cs == nil {
			cs = &cmdStat{}
			d.stats.cmds[name] = cs
		}
		cmd.stat = cs
	}
	for _, cmd := range d.table.commands() {
		link(cmd)
		for _, sub := range cmd.SubCmds {
			link(sub)
		}
	}
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
// latency sample, and a failed_calls bump when it returned an error. The counter
// block is reached through cmd.stat, linked once by statsInit, so the hot path
// takes no lock; cmdStatFor is a fallback for the rare descriptor that statsInit
// did not see (none in the built-in table).
func (d *Dispatcher) statCall(cmd *CmdDesc, usec uint64, failed bool) {
	cs := cmd.stat
	if cs == nil {
		cs = d.cmdStatFor(statName(cmd))
	}
	cs.calls.Add(1)
	cs.usec.Add(usec)
	cs.hist.record(usec)
	if failed {
		cs.failed.Add(1)
	}
}

// statCallFast records one successful command on the integrated fast path, which
// does not time its commands. statCall there is always handed usec 0, so its
// cs.usec.Add(0) and cs.hist.record(0) only ever add nothing and drop a bogus
// zero-microsecond sample into the latency histogram, yet both are writes to a
// single shared counter and a single shared histogram bucket that every core
// hammers on every command. Under a saturating GET/SET load those two shared
// cachelines are the per-command cost that stops the fast path scaling cleanly
// across cores, so this records only the call count, the one figure the fast
// path can report honestly, and leaves usec and the histogram to the timed slow
// path. total_commands_processed and commandstats calls stay exact; the only
// change is that fast-path commands no longer log a fabricated 0us latency, which
// the latency histogram is better off without.
func (d *Dispatcher) statCallFast(cmd *CmdDesc) {
	cs := cmd.stat
	if cs == nil {
		cs = d.cmdStatFor(statName(cmd))
	}
	cs.calls.Add(1)
}

// statReject records one command rejected before it ran, by ACL, arity, the
// read-only replica guard, or an out-of-memory refusal.
func (d *Dispatcher) statReject(cmd *CmdDesc) {
	cs := cmd.stat
	if cs == nil {
		cs = d.cmdStatFor(statName(cmd))
	}
	cs.rejected.Add(1)
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
// CONFIG RESETSTAT calls it. The counter blocks are zeroed in place rather than
// replaced, so the cmd.stat pointers linked by statsInit keep pointing at the
// live blocks; a fresh map would orphan them and the hot path would keep writing
// to the old, now-invisible counters.
func (d *Dispatcher) statResetAll() {
	d.stats.mu.RLock()
	for _, cs := range d.stats.cmds {
		cs.reset()
	}
	d.stats.mu.RUnlock()
	d.stats.errs.Range(func(k, _ any) bool {
		d.stats.errs.Delete(k)
		return true
	})
}

// cmdHistogram is one command's latency histogram for LATENCY HISTOGRAM: its
// stat name, total call count, and the cumulative histogram points.
type cmdHistogram struct {
	name   string
	calls  uint64
	points []histPoint
}

// commandHistograms gathers per-command latency histograms for LATENCY
// HISTOGRAM. With names empty it returns every command that has run; otherwise
// it returns only the named commands that have run. Commands with no calls are
// left out, matching Redis, and the result is sorted by name for a stable reply.
func (d *Dispatcher) commandHistograms(names []string) []cmdHistogram {
	d.stats.mu.RLock()
	defer d.stats.mu.RUnlock()
	var out []cmdHistogram
	add := func(name string, cs *cmdStat) {
		if cs == nil {
			return
		}
		calls := cs.calls.Load()
		if calls == 0 {
			return
		}
		out = append(out, cmdHistogram{name: name, calls: calls, points: cs.hist.cumulative()})
	}
	if len(names) > 0 {
		for _, n := range names {
			add(n, d.stats.cmds[n])
		}
	} else {
		for n, cs := range d.stats.cmds {
			add(n, cs)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].name < out[j].name })
	return out
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

// reset zeroes every bucket. CONFIG RESETSTAT calls it through cmdStat.reset.
func (h *latencyHist) reset() {
	for i := range h.counts {
		h.counts[i].Store(0)
	}
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

// histHigh returns the upper edge of a bucket in microseconds: the largest value
// that still falls in it. LATENCY HISTOGRAM reports cumulative counts against
// this upper bound so a point reads as "this many calls at or below this
// latency". Buckets below histSub are the linear region where each bucket holds
// exactly one value, so the upper edge equals the index; taking histLow(idx+1)
// there would cross into the log region and shift by a negative amount.
func histHigh(idx int) uint64 {
	if idx < histSub {
		return uint64(idx)
	}
	if idx >= histBuckets-1 {
		return histLow(idx)
	}
	return histLow(idx+1) - 1
}

// histPoint is one cumulative point of a latency histogram: the upper-bound
// latency in microseconds and the number of calls at or below it.
type histPoint struct {
	bound uint64
	count uint64
}

// cumulative returns the non-empty buckets as cumulative (upper-bound, count)
// points in increasing order, the shape LATENCY HISTOGRAM puts in
// histogram_usec. Empty buckets are skipped, matching how Redis only reports
// boundaries where the running count grows.
func (h *latencyHist) cumulative() []histPoint {
	var out []histPoint
	var cum uint64
	for i := range h.counts {
		c := h.counts[i].Load()
		if c == 0 {
			continue
		}
		cum += c
		out = append(out, histPoint{bound: histHigh(i), count: cum})
	}
	return out
}

// total returns the number of samples recorded.
func (h *latencyHist) total() uint64 {
	var n uint64
	for i := range h.counts {
		n += h.counts[i].Load()
	}
	return n
}

// countLE returns the number of samples at or below le microseconds, the value a
// Prometheus histogram bucket reports. It sums every bucket whose low edge is at
// or below le, which lines up exactly with the power-of-two bucket bounds the
// metrics endpoint uses.
func (h *latencyHist) countLE(le uint64) uint64 {
	var n uint64
	for i := range h.counts {
		c := h.counts[i].Load()
		if c == 0 {
			continue
		}
		if histLow(i) <= le {
			n += c
		}
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
