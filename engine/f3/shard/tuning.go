package shard

import (
	"runtime"
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

	// spinWindow is how long an idle worker burns plain loads on its inbound
	// queue before it parks (doc 03 section 9.2, provisional 4us): long enough
	// to catch the gap between pipelined bursts, short enough that a quiet
	// server converges to parked. Lab: spin-before-park window (PRED-X7).
	spinWindow = 4 * time.Microsecond

	// prefetchDepth caps how many of a batch's index buckets stage one touches
	// ahead of execution (doc 03 section 3.4). At the provisional value the
	// whole batch prefetches; the lab decides whether a shorter window beats
	// the memory system's sustainable depth. Lab: prefetch depth (PRED-X6).
	prefetchDepth = 32

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
)

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
