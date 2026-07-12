package shard

import (
	"runtime"
	"sync/atomic"
	"time"
)

// The constants the M0 labs own. Each one ships with the spec 2064/f3/03
// provisional default and is swept by its lab before the gate run; a lab that
// moves one lands the new value in its own PR with the sweep data attached.
const (
	// batchCap is the command capacity of one hop batch node (doc 03 section
	// 3.2): it covers the P16 gate depth in one node with room to coalesce and
	// keeps the node small. Lab: hop batch cap (sweep {16, 32, 64}, PRED-X8).
	batchCap = 32

	// batchDataCap is a node's steady-state argument-byte capacity: a fuller
	// node splits, and only a single command bigger than the cap grows an
	// empty node's buffer. The M0 transport copies parsed arguments into the
	// node; the zero-copy argument spans into the connection read buffer, with
	// their buffer-generation lifetime rules (doc 03 section 4.4), land with
	// the value-bands work.
	batchDataCap = 8192

	// spanCap bounds a node's argument count across its commands, sized so
	// the point surface never comes near it (SET with a full option run is
	// eight arguments). The multi-key fan-out slice revisits it for MSET.
	spanCap = 8 * batchCap

	// maxCmdBytes bounds one command's total argument bytes, the ceiling on
	// how far a single oversized command may grow an empty node. The chunked
	// band accepts values to the 512MiB proto-max-bulk-len cap, so the node
	// ceiling sits just above it (key and option slack on top of the value);
	// anything bigger cannot succeed downstream, so the dispatcher answers
	// ErrTooBig with an error reply instead of carrying the bytes.
	maxCmdBytes = 512<<20 + 128<<10

	// keepNodeBytes is the largest buffer a recycled node keeps: a node that
	// carried a giant-value command shrinks back on reset instead of pinning
	// hundreds of megabytes on the free list.
	keepNodeBytes = 1 << 20

	// repCap is the starting capacity of a node's reply buffer, sized so the
	// steady path never grows it: every point reply plus an echoed payload fits
	// with headroom. A reply run past the cap grows the buffer once and the
	// node keeps the larger buffer for its next life.
	repCap = batchDataCap + 64*batchCap

	// spinWindow is how long an idle connection writer burns plain loads on
	// its outbound queue before it parks (doc 03 section 9.2, provisional
	// 4us): long enough to catch the gap between pipelined bursts, short
	// enough that a quiet server converges to parked. Lab: spin-before-park
	// window (PRED-X7, labs/f3/m0/03_spin_park).
	spinWindow = 4 * time.Microsecond

	// workerSpinWindow is the shard worker's own spin-before-park window,
	// swept separately from the connection writers' because the workers are
	// the hot consumers. The labs/f3/m0/11_transport sweep froze 0: on a
	// single box the server and its clients share the cores, so every
	// microsecond a worker burns spinning is stolen from the net goroutines
	// that would feed it, and the sweep lost throughput monotonically from 0
	// through 80us in both reps. Parking immediately is cheap once the
	// workers are unpinned (a plain gopark, no locked-M thread handoff); the
	// wake tax issue #542 measured was the pin's, not the park's.
	workerSpinWindow = 0 * time.Microsecond

	// prefetchDepth caps how many of a batch's index buckets stage one touches
	// ahead of execution (doc 03 section 3.4). At the provisional value the
	// whole batch prefetches; the lab decides whether a shorter window beats
	// the memory system's sustainable depth. Lab: prefetch depth (PRED-X6).
	prefetchDepth = 32

	// drainPassCap bounds one worker drain pass: up to this many batches run
	// back to back before the deferred writer wakes go out and the loop
	// returns to its stream pump. Big enough that a saturated inbound queue
	// coalesces wakes well, small enough that no writer waits long for a
	// deferred token.
	drainPassCap = 32

	// timerFireCap bounds one worker timer-fire pass: fireTimers runs at most
	// this many due deadlines back to back before returning to command
	// processing, the sibling of drainPassCap for the deadline heap. It is off
	// the throughput path entirely (a worker fires a timer only when a blocking
	// command's finite timeout elapses), so the cap is not a P1 term; it only
	// caps how long a burst of simultaneous timeouts can hold the loop, and a
	// pass that hits it leaves the rest for the next pass. The lab swept {16, 32,
	// 64, 128}: past 64 the per-pass fire cost stops shrinking the tail delivery
	// and only lengthens the single pass, so 64 is the knee. Lab:
	// labs/f3/m3/05_timer_park.
	timerFireCap = 64

	// replyRing is a connection's pipeline window: the reply reorder ring holds
	// this many in-flight replies, and a producer past the window blocks on the
	// writer's progress. The doc 03 section 4.5 watermarks refine this into
	// per-shard backpressure in the RESP2 slice.
	replyRing = 1024

	// freeListCap bounds a connection's batch-node free list. Nodes past the
	// cap fall to the collector; the steady path recycles well under it.
	freeListCap = 64

	// compactMinDead is the value-log compaction floor: below this many dead
	// bytes a rewrite reclaims too little to pay for reading the live set. The
	// trigger also demands the dead share be at least half the log, so the
	// rewrite cost amortizes against at least its own size in reclaimed space.
	compactMinDead = 1 << 20

	// arenaCompactMinDead is the arena compaction floor at the idle boundary:
	// below this many reclaimable dead bytes the pass is not worth its index
	// walk. The per-segment victim threshold itself is the store's frozen
	// lab constant (labs/f3/m0/10_arena_reclaim); this floor only keeps a
	// quiet shard from re-walking its index over scraps.
	arenaCompactMinDead = 1 << 20
)

// spinIters is spinWindow expressed as iterations of the idle re-check loop,
// calibrated once at package init so the spin does not pay a time.Now per
// turn (the M0 gate profile put that at about 1.4 percent of CPU). The probe
// times a loop of the same shape as the real check, three atomic loads, and
// scales the count to spinWindow. The window only has to be roughly right:
// the lab 3 sweep showed the regime, not the microsecond, is what matters,
// so a calibration run on a busy machine that lands somewhere near 4us is
// fine, and the clamp keeps a bad clock reading from producing a windowless
// or unbounded spin.
var spinIters = spinItersFor(spinWindow)

// workerSpinIters is the worker's window in the same calibrated units. The
// windows differ (see workerSpinWindow), so each gets its own count.
var workerSpinIters = spinItersFor(workerSpinWindow)

// connSpinHighWater is the live-connection count at or above which a connection
// writer stops spinning before it parks and parks at once. Below it a writer
// burns spinWindow to catch the next reply without a futex wake, which pays when
// cores sit idle at low fan-out. At or above it the box is saturated: every
// microsecond a writer spins is a core stolen from the shard workers draining
// the backlog, so it parks immediately and yields the core. The
// labs/f3/m0/22_conn_spin sweep put the crossover near six connections per core
// on the gate box (knee about 80 to 100 connections at GOMAXPROCS 14): the
// median-of-3 verify lifted 512-conn SET from 1.24x (fixed spin) to 1.77x and
// GET from 1.16x to 1.71x vs the slower rival when the writer parked at once,
// while the low-conn cells were unchanged. It scales with GOMAXPROCS so the
// switch tracks the core count rather than a fixed connection number.
var connSpinHighWater = defaultConnSpinHighWater()

func defaultConnSpinHighWater() int {
	n := runtime.GOMAXPROCS(0) * 6
	if n < 1 {
		n = 1
	}
	return n
}

// SetConnSpinHighWater overrides the connection-writer park-immediately
// threshold; the labs/f3/m0/22_conn_spin sweep knob. A value of one parks every
// writer immediately (the always-saturated arm); a very large value restores the
// unconditional spin (the fixed-spin control). Call it only before
// Runtime.Start, the writers read the count with plain loads.
func SetConnSpinHighWater(n int) {
	if n < 1 {
		n = 1
	}
	connSpinHighWater = n
}

// SetWorkerSpinWindow recalibrates the worker spin-before-park window, with 0
// meaning park immediately. It is the labs/f3/m0/11_transport sweep knob;
// call it only before Runtime.Start, the workers read the count with plain
// loads.
func SetWorkerSpinWindow(d time.Duration) {
	workerSpinIters = spinItersFor(d)
}

// spinItersFor turns a window into calibrated iterations; zero and below mean
// no spin at all, which the clamp in the calibration would otherwise round up.
func spinItersFor(window time.Duration) int {
	if window <= 0 {
		return 0
	}
	return calibrateSpinIters(window)
}

func calibrateSpinIters(window time.Duration) int {
	var a, b, c atomic.Uint32
	const probe = 1 << 15
	s := uint32(0)
	start := time.Now()
	for i := 0; i < probe; i++ {
		s += a.Load() + b.Load() + c.Load()
	}
	el := time.Since(start)
	if s != 0 {
		// Never taken (the atomics stay zero), but consuming s keeps the
		// probe loop observable so it cannot fold away.
		return 1 << 12
	}
	if el <= 0 {
		return 1 << 12
	}
	it := int(int64(probe) * int64(window) / int64(el))
	if it < 1<<8 {
		it = 1 << 8
	}
	if it > 1<<22 {
		it = 1 << 22
	}
	return it
}

// DefaultShards is the shard count when the flag is unset: the data plane gets
// about 60 percent of the cores and the net goroutines take the remainder,
// the split the saturating profiles bound (doc 03 section 2.2: parse plus
// syscalls run a third to a half of total CPU). Lab: shard count per box.
func DefaultShards() int {
	n := runtime.GOMAXPROCS(0) * 3 / 5
	if n < 1 {
		n = 1
	}
	return n
}
