package shard

import (
	"errors"
	"sync"
)

// The per-shard I/O worker (spec 2064/f3/06 section 3.4): the one off-owner
// goroutine the LTM tier is allowed, and it is deliberately memoryless. When a
// shard crosses its resident cap the owner stages a drain (phase 1 of the
// migration quantum frames a run of cold-bound records into a pooled buffer),
// then hands the buffer plus its destination offset here; this goroutine
// pwrites the buffer and posts a completion event back onto the owner's control
// queue, where phase 2 runs in owner program order. It never reads or writes
// the index, the arena, the directory, or any other owner-local structure: its
// whole world is a byte buffer and a file offset, so no single-owner invariant
// ever crosses the goroutine boundary (doc 06 section 3.4's four-op contract).
//
// Why a separate goroutine when everything else on the shard is the owner's:
// the pwrite (and later the cold pread and the fsync) is the one blocking
// syscall the owner must not sit inside, because a shard parked in pwrite serves
// no commands. Moving exactly those syscalls off the owner keeps it CPU-bound on
// the data path while the disk runs in parallel, and the completion queue
// re-serializes each result so the owner's view stays single-threaded. This is
// the skeleton: the pwrite drain and the completion round-trip, with the buffer
// drawn from and returned to the cap/4 staging pool (section 10). The cold
// pread, the fsync-on-policy, and the migration quantum that feeds it land with
// slice-1 PR 4; until a producer exists the write seam is nil and the goroutine
// never starts, so a store that never crosses its cap (L9) pays nothing here,
// not even a parked goroutine.

// errNoDrainTarget is the result error when a job reaches the worker before a
// producer has wired the pwrite seam. It cannot happen in production once the
// quantum wires write; it only guards the skeleton, where no code path submits.
var errNoDrainTarget = errors.New("shard: io worker has no drain target")

// ioResult is what a completed I/O op reports to the owner: the byte count the
// syscall moved and the first error. Phase 2 reads it to decide whether the
// drain landed; a failed pwrite leaves the staged records resident and skips the
// flip, so no index pointer ever names a half-written frame (doc 06 section 3.2,
// cold frames immutable).
type ioResult struct {
	n   int
	err error
}

// ioJob is one unit of off-owner work: the staged buffer, the file offset the
// worker pwrites it to, and the owner-side completion posted when the syscall
// returns. buf belongs to the pool; the worker only reads it, and the completion
// returns it to the pool on the owner goroutine so the pool stays single-owner.
// onDone carries the phase-2 work (the flip-list walk in PR 4); it is nil for a
// bare round-trip.
type ioJob struct {
	buf    []byte
	off    int64
	onDone func(cx *Ctx, res ioResult)
}

// ioworker is the shard's off-owner I/O goroutine and the machinery to feed it.
// jobs is the owner-to-worker hand-off, single producer (the owner) and single
// consumer (the goroutine), buffered to the pool bound so a submit the pool
// admitted never blocks. write is the pwrite seam the migration quantum wires to
// the shard's cold region (PR 4); it is nil until then. The goroutine is started
// lazily on the first submit and joined by stop, so a shard that never drains
// keeps this at zero cost.
type ioworker struct {
	owner *worker
	jobs  chan ioJob
	done  chan struct{}
	pool  stagePool
	write func(off int64, b []byte) (int, error)
	begin sync.Once
	up    bool // goroutine started; owner-only, read at stop after the owner joins
}

// init wires the I/O worker to its owner and sizes the staging pool. The jobs
// channel is allocated here but the goroutine is not: submit starts it.
func (io *ioworker) init(owner *worker) {
	io.owner = owner
	io.pool.init(stageBufBytes, stagePoolMax)
	io.jobs = make(chan ioJob, stagePoolMax)
}

// submit hands one job to the I/O worker, starting the goroutine on first use.
// Called on the owner goroutine only (the migration quantum runs there), so the
// pool checkout that produced j.buf and this hand-off share the owner's program
// order. The channel is sized to the pool bound and the worker drains it
// independently of completion draining, so a submit for a pool-admitted buffer
// does not block.
func (io *ioworker) submit(j ioJob) {
	io.begin.Do(func() {
		io.up = true
		io.done = make(chan struct{})
		go io.run()
	})
	io.jobs <- j
}

// run is the off-owner loop: pwrite each staged buffer at its offset and post
// the completion back to the owner. The write happens off the owner goroutine;
// everything the completion touches (onDone, the pool) runs back on the owner
// through the control queue, so this goroutine holds no owner-local state across
// a job. It exits when submit's channel closes (stop).
func (io *ioworker) run() {
	for j := range io.jobs {
		var res ioResult
		if io.write != nil {
			res.n, res.err = io.write(j.off, j.buf)
		} else {
			res.err = errNoDrainTarget
		}
		buf, onDone := j.buf, j.onDone
		io.owner.postCompletion(func(cx *Ctx) {
			if onDone != nil {
				onDone(cx, res)
			}
			io.pool.put(buf)
		})
	}
	close(io.done)
}

// stop shuts the goroutine down after the owner has exited (Runtime.Stop joins
// the owner first), so no submit can race the close. A worker that never drained
// never started the goroutine, and stop is then a plain return.
func (io *ioworker) stop() {
	if !io.up {
		return
	}
	close(io.jobs)
	<-io.done
}

// postCompletion runs fn on this worker's owner goroutine off the intent control
// queue, the same fire-and-forget path PostOwner rides (intent.go). An I/O
// completion is an ordinary owner-queue event: it drains in advanceIntents after
// the current batch, so phase 2 serializes with foreground commands in owner
// program order, and any state the poster published before the post is visible
// to fn. The I/O worker is just another producer on the MPSC, so the wake and
// the ordering are the ones the intent path already proves.
func (w *worker) postCompletion(fn func(*Ctx)) {
	w.postIntent(&intentOp{kind: opAsync, fn: fn})
}

// stagePool is the owner-only free list the drain staging buffers come from (doc
// 06 section 10's cap/4 pool). get and put run only on the owner (phase 1 checks
// a buffer out, the completion returns it), so there is no lock; the I/O worker
// only borrows the slice by reference between the two. out is the count of
// buffers currently checked out, capped at max so the owner cannot start more
// cold drains than the bound, which is the admission control on per-shard cold
// I/O concurrency the doc leans on. Buffers keep their grown capacity across
// reuse so a steady drain regime stops allocating.
type stagePool struct {
	free   [][]byte
	bufcap int
	max    int
	out    int
}

// init sizes the pool: bufcap is a fresh buffer's starting capacity (the drain
// quantum), max the in-flight checkout bound.
func (p *stagePool) init(bufcap, max int) {
	p.bufcap = bufcap
	p.max = max
}

// get checks out a staging buffer, or returns nil when the shard is already at
// its in-flight bound (the caller defers the drain to a later boundary). The
// buffer comes back length zero with at least bufcap capacity, reusing a
// returned buffer's grown capacity when one is free.
func (p *stagePool) get() []byte {
	if p.out >= p.max {
		return nil
	}
	p.out++
	if n := len(p.free); n > 0 {
		b := p.free[n-1]
		p.free[n-1] = nil
		p.free = p.free[:n-1]
		return b[:0]
	}
	return make([]byte, 0, p.bufcap)
}

// put returns a checked-out buffer. A buffer grown past bufcap is kept, because
// its grown capacity is exactly what the next large-record drain wants to reuse;
// the free list never exceeds the checkout bound. A foreign or already-returned
// buffer (out at zero) is dropped without underflowing the checkout count.
func (p *stagePool) put(b []byte) {
	if p.out > 0 {
		p.out--
	}
	if cap(b) >= p.bufcap && len(p.free) < p.max {
		p.free = append(p.free, b[:0])
	}
}
