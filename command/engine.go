package command

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/tamnd/aki/keyspace"
	"github.com/tamnd/aki/pager"
	"github.com/tamnd/aki/rdb"
)

// writeReq carries one write operation to the write worker.
// When done is non-nil, the worker signals it after applying the write and the
// caller waits for that signal before continuing. When done is nil the req is
// fire-and-forget: the caller returns immediately and the worker puts the req
// back in the async pool itself.
//
// shard >= 0 when the write targets a specific keyspace shard and was routed
// to shardQ[shard]. shard == -1 means global (routed to writeCh).
//
// Inline SET fast path: when setKey is non-nil the worker calls SetWithVersion
// directly instead of invoking fn, avoiding a heap-allocated closure per write.
type writeReq struct {
	index int
	shard int
	fn    func(*keyspace.DB) error
	done  chan error // nil for fire-and-forget requests
	// Inline fields for the SET write-behind fast path. When setKey is non-nil,
	// fn is unused and the worker calls db.SetWithVersion with these fields.
	// This lets the hot path avoid a heap-allocated closure per SET.
	setKey  []byte
	setBody []byte
	setTyp  uint8
	setEnc  uint8
	setTTL  int64
	setVer  uint64
	// setDel turns the inline fast path into a delete: when true the worker calls
	// db.DeleteWithVersion(setKey, setVer) instead of SetWithVersion. setKey is the
	// key to remove and setBody is unused. It rides the same setKey-keyed coalescing
	// as SET, so a tombstone and a later same-key SET resolve by version.
	setDel bool
	// next is the intrusive link for the per-shard lock-free hand-off queue
	// (shardQueue). Producers store it under the Treiber compare-and-swap; the
	// single consumer reuses it to hold the reversed first-in-first-out list. It
	// is meaningful only while the req is in a shard queue.
	next atomic.Pointer[writeReq]
}

// writeReqPool amortises writeReq allocations. Each entry already carries a
// preallocated done channel so the hot path never allocates a new one.
var writeReqPool = sync.Pool{
	New: func() any {
		return &writeReq{done: make(chan error, 1)}
	},
}

// asyncReqPool holds writeReq structs for fire-and-forget writes. They have no
// done channel, so the worker returns them to this pool after processing.
var asyncReqPool = sync.Pool{
	New: func() any {
		return &writeReq{}
	},
}

// writeBatchMax is the most requests the write worker drains in a single lock
// hold. A finite bound lets readers and the commit cron interleave with the
// writer rather than starving while the worker works through a deep queue.
const writeBatchMax = 256

// drainScratch is per-shard-worker scratch reused across drains so the coalescing
// pass allocates nothing in steady state. reqs gathers one batch, skip marks the
// superseded SETs in it, and dedup maps a key to the batch slot currently holding
// the highest version seen for it. The map is reused (cleared, not reallocated)
// so its string keys are not re-allocated when the same hot keys repeat batch
// after batch.
type drainScratch struct {
	reqs  []*writeReq
	skip  []bool
	dedup map[string]int
}

func newDrainScratch() *drainScratch {
	return &drainScratch{
		reqs:  make([]*writeReq, 0, writeBatchMax),
		skip:  make([]bool, 0, writeBatchMax),
		dedup: make(map[string]int, 64),
	}
}

// commitPolicy decides when a write makes the .aki file durable. It mirrors the
// Redis appendfsync directive, but it governs the pager checkpoint (aki's real
// durability mechanism) rather than an append log.
type commitPolicy int32

const (
	// commitAlways checkpoints on every write, so a write is durable the moment
	// its reply is sent. This is the v0.1.0 behaviour and the strongest contract.
	commitAlways commitPolicy = iota
	// commitEverySec lets writes mutate the buffer pool and return without an
	// fsync. The cron flushes the pending work about once a second, so a crash
	// loses at most the last second of writes. This is the default.
	commitEverySec
	// commitNo never checkpoints on a timer. Pending writes are flushed only by
	// SAVE, a clean shutdown, or the dirty-page bound below. A crash loses
	// everything written since the last of those events.
	commitNo
)

// defaultDirtyPageLimit bounds how many dirty pages may accumulate before a
// deferred policy forces a checkpoint regardless of the timer. The buffer pool
// can only evict clean pages, so without this an unflushed burst would pin the
// whole working set in memory. At the default 4 KiB page size this caps the
// deferred dirty set near 8 MiB.
const defaultDirtyPageLimit = 2048

// dirtyCheckStride is how often the deferred path consults the pager for the
// live dirty-page count. Checking every write would add a pager-lock round trip
// to the hot path, so we sample once per stride writes instead.
const dirtyCheckStride = 256

// Engine is the command layer's handle on the keyspace. Writes take the write
// lock; reads take the read lock so multiple reads run in parallel. The sharded
// writer model from doc 05 §7 replaces this lock entirely in a later slice.
//
// When the write worker is active (StartWorker has been called), all writes are
// routed through a buffered channel to a single dedicated goroutine, which is
// the only goroutine that ever acquires mu for writing. This eliminates mutex
// contention between the many connection goroutines that would otherwise compete
// for the write lock, and keeps the B-tree pages warm in the CPU cache of the
// worker's core. Reads still use mu.RLock and are not affected.
type Engine struct {
	mu sync.RWMutex
	ks *keyspace.Keyspace
	// onCommit, when set, is called with the duration of each checkpoint commit so
	// the dispatcher can flag slow I/O. The dispatcher installs it at startup.
	onCommit func(op string, dur time.Duration)

	// policy is read on the write hot path without the lock, so it lives in an
	// atomic. setCommitPolicy writes it from CONFIG SET and at startup.
	policy atomic.Int32

	// pendingDirty is set atomically by shard workers when a deferred write lands
	// and cleared under e.mu.Lock by commit. Atomic because N shard workers set
	// it concurrently while holding only e.mu.RLock.
	pendingDirty atomic.Bool
	// pendingWrites counts mutations since the last checkpoint for the dirty-page
	// sampler. Add'd atomically by workers; sampled and reset under e.mu.Lock.
	pendingWrites atomic.Int64
	// lastCommit stamps the previous checkpoint; read and written under e.mu.
	lastCommit time.Time

	// dirtyPageLimit is the early-commit bound described above. Guarded by mu.
	dirtyPageLimit int

	// writeCh is the global input queue for the single global write worker. It
	// handles cross-shard writes, commitAlways writes, and writes where the caller
	// cannot determine the shard. nil when workers are not running.
	writeCh chan *writeReq
	// shardQ are the per-shard input queues. shardQ[s] is served exclusively by
	// one goroutine, so there is zero contention between shards, and it is
	// lock-free on the producer side so the connection goroutines do not
	// serialize on a shared mutex when they all write the same shard. Deferred
	// single-key writes route here by ShardOf(key).
	shardQ [keyspace.NumShards]shardQueue
	// shardsRunning gates the per-shard fast path. It is set true in StartWorker
	// before the workers launch and false at the start of StopWorker, so a
	// producer that sees it false sheds to the synchronous global path instead of
	// pushing to a queue whose worker is draining to exit.
	shardsRunning atomic.Bool
	// rmwLocks serialize write-behind read-modify-write ops. A blind SET can stage
	// and fire its durable write without reading, but an RMW (INCR, LPUSH, SADD,
	// HSET, ZADD) computes its reply from the current value, so two same-key RMW
	// writers must serialize or one loses its update. The connection goroutine holds
	// rmwLocks[rmwStripe(key)] across the read, compute and stage, then fires the
	// async B-tree write and releases it. The shard worker never takes this lock, so
	// the async apply cannot deadlock against it.
	//
	// The stripe is far finer than the eight shards. The lock's only job is per-key
	// read-compute-stage atomicity; the per-key sinks (hot cache, wbPending, the
	// B-tree shard) synchronize themselves, and the shard worker never touches this
	// lock, so two different keys never need the same one. Striping per shard forced
	// every key on a shard to serialize: under the queue and HSET workloads the
	// profile put a third of all CPU in this one mutex (lock2 / semacquire /
	// semrelease). Striping by a 1024-way key hash keeps same-key writers serialized
	// (same key hashes to the same stripe) while letting independent keys run in
	// parallel, which is the actual invariant. 1024 divides the 16384 hash-slot space
	// evenly so the stripes fill uniformly.
	rmwLocks [rmwStripes]sync.Mutex
	// workerStop is closed by StopWorker to signal all write goroutines to drain
	// and exit. workerDone is closed once every goroutine has exited.
	workerStop chan struct{}
	workerDone chan struct{}
	// wg tracks all worker goroutines so workerDone can be closed once every
	// one of them has returned.
	wg sync.WaitGroup
}

// NewEngine wraps a keyspace for use by the dispatcher. It defaults to the
// commitEverySec policy, matching the Redis appendfsync default, and keeps the
// dirty-page bound so deferred writes can never grow memory without limit.
func NewEngine(ks *keyspace.Keyspace) *Engine {
	e := &Engine{ks: ks, dirtyPageLimit: defaultDirtyPageLimit}
	e.policy.Store(int32(commitEverySec))
	return e
}

// setCommitPolicy switches the durability policy. The dispatcher calls it at
// startup and on CONFIG SET appendfsync. Tightening to commitAlways flushes any
// work the looser policy left pending so the stronger contract holds at once.
func (e *Engine) setCommitPolicy(p commitPolicy) error {
	e.policy.Store(int32(p))
	if p != commitAlways {
		return nil
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.pendingDirty.Load() {
		return nil
	}
	return e.commit()
}

// setHashOverlay enables or disables the in-memory hash write overlay under the
// engine write lock, which excludes every shard worker (they hold e.mu.RLock), so
// no absorb or fold races the toggle. Disabling folds every resident copy back into
// its sub-tree and drops the residency maps; that dirties pages, so a checkpoint
// follows to make the folded state durable rather than waiting on the periodic one.
func (e *Engine) setHashOverlay(on bool) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	folded, err := e.ks.SetHashOverlay(on)
	if err != nil {
		return err
	}
	if folded {
		return e.commit()
	}
	return nil
}

// StartWorker starts all write workers: one global worker for cross-shard and
// commitAlways writes, and one dedicated worker per shard for deferred
// single-key writes. The per-shard workers each own their channel exclusively —
// no contention — and hold e.mu.RLock so different shards write in parallel.
// The caller must eventually call StopWorker to clean up.
func (e *Engine) StartWorker() {
	if e.writeCh != nil {
		return // already running
	}
	e.writeCh = make(chan *writeReq, 4096)
	for s := range keyspace.NumShards {
		e.shardQ[s].init()
	}
	e.workerStop = make(chan struct{})
	e.workerDone = make(chan struct{})
	e.wg.Add(1 + keyspace.NumShards)
	e.shardsRunning.Store(true)
	go e.runWriteWorker()
	for s := range keyspace.NumShards {
		go e.runShardWorker(s)
	}
	go func() {
		e.wg.Wait()
		close(e.workerDone)
	}()
}

// StopWorker signals all write workers to drain pending requests and exit,
// then waits until every goroutine has returned. After this, update() falls
// back to the direct lock path. Safe to call when StartWorker was never called.
func (e *Engine) StopWorker() {
	if e.writeCh == nil {
		return
	}
	// Stop new producers from using the shard queues before signalling the
	// workers to drain, so the queues only hold writes staged before shutdown.
	// Closing workerStop wakes any parked shard worker through its park select.
	e.shardsRunning.Store(false)
	close(e.workerStop)
	<-e.workerDone
	e.writeCh = nil
}

// runWriteWorker is the global write worker goroutine. It handles cross-shard
// writes and commitAlways writes via drainBatch, which holds e.mu.Lock or
// e.mu.RLock depending on the active policy.
func (e *Engine) runWriteWorker() {
	defer e.wg.Done()
	for {
		select {
		case req := <-e.writeCh:
			e.drainBatch(req)
		case <-e.workerStop:
			for {
				select {
				case req := <-e.writeCh:
					e.drainBatch(req)
				default:
					return
				}
			}
		}
	}
}

// runShardWorker is the dedicated goroutine for shard s and the single consumer
// of shardQ[s]. No other goroutine ever drains that queue, so the queue and the
// B-tree for shard s are both uncontended on the consumer side. It drains the
// queue in one swap, applies the batch under e.mu.RLock so the shard runs in
// parallel with other shard workers while commits (which take e.mu.Lock) stay
// exclusive, and parks on the queue's doorbell when there is nothing to do.
//
// The park is gated and backstopped: it stores stateParked, re-checks the queue
// once to close the window where a producer enqueued while the worker was still
// running and so skipped the doorbell, then blocks on the doorbell, the stop
// signal, or a short backstop timer. The backstop means a missed wakeup costs at
// most one timer interval of latency, never a hang and never lost work, so
// correctness rests on the queue and the re-check rather than on a perfect
// wakeup.
func (e *Engine) runShardWorker(s int) {
	defer e.wg.Done()
	sc := newDrainScratch()
	q := &e.shardQ[s]
	timer := time.NewTimer(shardParkBackstop)
	timer.Stop()
	for {
		if batch := q.popAll(); batch != nil {
			e.drainShardList(s, batch, sc)
			continue
		}
		// Bounded spin before parking. A worker that spreads a pipeline across all
		// shards (RPUSH+LPOP queue traffic, for example) drains each shard to empty
		// between bursts, and parking on the doorbell every time pays a three-channel
		// select plus a runtime-timer arm per cycle: under that workload the profile
		// put a third of all CPU in runtime.lock2 (the select's sellock) and a quarter
		// in selectgo. The next pipelined batch almost always lands within a few
		// microseconds, so spin-poll for it first and only fall through to the park
		// when the stream has genuinely gone quiet. This keeps a steadily fed worker
		// off the timer heap and the select entirely.
		if batch := spinForBatch(q); batch != nil {
			e.drainShardList(s, batch, sc)
			continue
		}
		q.state.Store(stateParked)
		// Re-check: a producer that pushed while we were still running skipped the
		// doorbell, so we must look once more before blocking on it.
		if batch := q.popAll(); batch != nil {
			q.state.Store(stateRunning)
			e.drainShardList(s, batch, sc)
			continue
		}
		timer.Reset(shardParkBackstop)
		select {
		case <-e.workerStop:
			stopTimer(timer)
			q.state.Store(stateRunning)
			// Drain everything staged before shutdown, then exit.
			for {
				batch := q.popAll()
				if batch == nil {
					return
				}
				e.drainShardList(s, batch, sc)
			}
		case <-q.wake:
			stopTimer(timer)
			q.state.Store(stateRunning)
		case <-timer.C:
			q.state.Store(stateRunning)
		}
	}
}

// rmwStripes is the number of read-modify-write serialization stripes. It is a
// power of two so the index is a mask, and a divisor of the 16384-slot hash space
// so the stripes fill evenly. 1024 is far finer than the eight shards: it keeps
// same-key RMW writers serialized while letting independent keys run in parallel,
// which dropped the rmwLocks mutex from a third of CPU to noise on the write
// workloads (see rmwLocks).
const rmwStripes = 1024

// rmwStripeMask selects the stripe bits from a key's hash slot.
const rmwStripeMask = rmwStripes - 1

// rmwStripe maps a key to its read-modify-write stripe. It reuses the keyspace
// hash slot (the same value ShardOf derives the shard from), so a key always maps
// to one stripe and hash-tagged keys ({tag}) stripe by their tag, consistently
// with shard routing.
func rmwStripe(key []byte) int {
	return int(keyspace.HashSlot(key)) & rmwStripeMask
}

// shardSpinPolls bounds the spin a worker does before it parks: how many times it
// peeks the queue head looking for the next pipelined batch. It is a pure busy
// poll of cheap atomic loads with no runtime.Gosched, so it never touches the
// global scheduler lock (an earlier Gosched-based spin moved the contention there
// instead of removing it). The polls are cheap atomic loads, so even the full budget
// is well under a microsecond and a genuinely idle worker still parks promptly rather
// than burning a core.
//
// 512 is the measured knee. A pipelined producer staging a burst of writes leaves a
// short gap between adjacent batches reaching one shard; a 64-poll budget was too thin
// to bridge that gap, so the worker parked and paid a futex wake on the very next
// write. The wake storm showed in the profile as runtime.usleep / runqgrab / semrelease
// dominating CPU. Raising the budget to 512 lets the worker stay hot across the gap and
// keep draining: on the durable queue workload (pipeline 128, 50 clients) throughput
// rose from ~1.6M to ~2.8M ops/s, flat from 256 up, so 512 sits comfortably in the
// plateau without spinning longer than the gap it needs to cover.
const shardSpinPolls = 512

// spinForBatch polls the queue head for a short bounded window and returns the
// next batch as soon as one is staged, or nil if the stream stayed empty for the
// whole window (the worker should then park). It peeks q.top with a plain load and
// only swaps the stack out once it sees work, so an empty spin never writes the
// contended head cache line that producers compare-and-swap on.
func spinForBatch(q *shardQueue) *writeReq {
	for i := 0; i < shardSpinPolls; i++ {
		if q.top.Load() != nil {
			if batch := q.popAll(); batch != nil {
				return batch
			}
		}
	}
	return nil
}

// drainShardList applies a first-in-first-out list popped from shardQ[s]. It
// works through the list in chunks of at most writeBatchMax under one e.mu.RLock
// each, so a deep queue does not starve readers or the commit cron, which take
// e.mu between chunks. Because only this goroutine drains shard s and only this
// goroutine writes shard s's B-tree, the apply is uncontended.
//
// Before applying, each chunk is coalesced: when several SETs in it target the
// same key (the same-key contention the lockstep harness creates, where every
// client overwrites one key each step), only the highest-version write needs to
// reach the B-tree because the older versions are never committed and the
// hot-value cache already serves the latest. coalesceSets marks the superseded
// SETs so they are recycled instead of upserted, turning N redundant B-tree
// writes into one.
func (e *Engine) drainShardList(s int, head *writeReq, sc *drainScratch) {
	for head != nil {
		reqs := sc.reqs[:0]
		for head != nil && len(reqs) < writeBatchMax {
			next := head.next.Load()
			head.next.Store(nil) // do not carry a stale link into the pool
			reqs = append(reqs, head)
			head = next
		}
		sc.reqs = reqs
		e.coalesceSets(sc)
		e.mu.RLock()
		for i, req := range reqs {
			if sc.skip[i] {
				e.recycleSuperseded(req)
				continue
			}
			e.applyWriteReqDeferred(req)
		}
		e.mu.RUnlock()
	}
}

// coalesceSets fills sc.skip: a SET is superseded (skip = true) when a later SET
// in the same fn-free run targets the same key with a higher version. fn requests
// and fences (setKey == nil) are barriers: a read-modify-write closure may depend
// on the B-tree state a preceding SET to the same key leaves, so coalescing never
// crosses one, and the dedup map is reset at each barrier. The map is keyed by the
// stored key and reused across drains, so the recurring hot keys are not
// re-allocated. Versions decide the winner rather than batch position, because the
// channel can deliver a higher-version write ahead of a lower-version one when the
// producing goroutine is preempted between staging and the channel send.
func (e *Engine) coalesceSets(sc *drainScratch) {
	reqs := sc.reqs
	if cap(sc.skip) < len(reqs) {
		sc.skip = make([]bool, len(reqs))
	}
	sc.skip = sc.skip[:len(reqs)]
	clear(sc.dedup)
	for i, req := range reqs {
		sc.skip[i] = false
		if req.setKey == nil {
			// fn request or fence: a barrier. Anything staged before it must land
			// before it runs, so drop the dedup window.
			clear(sc.dedup)
			continue
		}
		k := string(req.setKey)
		if j, ok := sc.dedup[k]; ok {
			if reqs[j].setVer >= req.setVer {
				// The kept write is newer or equal; this one is superseded.
				sc.skip[i] = true
				continue
			}
			// This write is newer; supersede the previously kept one.
			sc.skip[j] = true
		}
		sc.dedup[k] = i
	}
}

// recycleSuperseded returns a coalesced-away SET or delete request to the async
// pool without touching the B-tree. The winning write for the key advances the
// dirty state and owns the write-behind entry, so a superseded request needs no
// bookkeeping; it is always fire-and-forget (done == nil), since only sendSetAsync
// and sendDeleteAsync produce the setKey requests coalesceSets can skip, and both
// the staged value and the staged tombstone are owned by whichever same-key write
// has the highest version.
func (e *Engine) recycleSuperseded(req *writeReq) {
	req.setKey = nil
	req.setBody = nil
	req.setDel = false
	req.fn = nil
	asyncReqPool.Put(req)
}

// drainBatch applies the first request and then greedily drains additional
// requests from writeCh, up to writeBatchMax, under one lock hold.
//
// Under commitAlways, it group-commits: it applies every request in the drained
// batch and then checkpoints exactly once for the whole batch, so N queued writes
// pay one fsync instead of N. The durability contract still holds because no
// reply is signalled until that single commit has made every write in the batch
// durable. Under load the batch fills on its own: while the worker is inside one
// checkpoint, the other connections' writes queue, so the next drain pays one
// fsync for all of them.
//
// Under deferred policies (commitEverySec, commitNo) it takes e.mu.RLock, which
// lets concurrent workers apply writes to different shards in parallel while still
// blocking commits (which take e.mu.Lock) from running mid-batch. Dirty state is
// recorded atomically; the commit cron flushes it.
func (e *Engine) drainBatch(first *writeReq) {
	if commitPolicy(e.policy.Load()) == commitAlways {
		e.drainBatchAlways(first)
		return
	}
	// Deferred policy: RLock allows concurrent shard writers.
	e.mu.RLock()
	e.applyWriteReqDeferred(first)
	for i := 1; i < writeBatchMax; i++ {
		select {
		case req := <-e.writeCh:
			e.applyWriteReqDeferred(req)
		default:
			e.mu.RUnlock()
			return
		}
	}
	e.mu.RUnlock()
}

// groupCommitWindow is how long the commitAlways worker waits, after draining
// everything already queued, for more concurrent writers to join the group before
// it pays a single checkpoint. A checkpoint is far heavier than this wait, so
// coalescing N writers into one fsync is a large net win. The wait is lock-free
// (the engine lock is taken only to apply and commit the gathered batch), so a
// reader is never blocked by the window. Single-client latency rises by at most
// this window, which is negligible against the checkpoint it amortises.
const groupCommitWindow = 300 * time.Microsecond

// drainBatchAlways group-commits a batch of commitAlways writes. It first gathers
// requests without holding the engine lock: everything already queued, then any
// that arrive within groupCommitWindow. It then takes e.mu.Lock once, applies the
// whole batch, checkpoints exactly once, releases the lock, and finally signals
// each request. So N concurrent writers pay one fsync, not N. The durability
// contract holds because no reply is signalled until the single commit has made
// every write in the batch durable. A request whose own apply failed reports that
// error; the rest report the shared commit result.
func (e *Engine) drainBatchAlways(first *writeReq) {
	var reqs [writeBatchMax]*writeReq
	reqs[0] = first
	n := 1

	// Immediate drain: take everything already in the queue.
	drained := false
	for n < writeBatchMax && !drained {
		select {
		case req := <-e.writeCh:
			reqs[n] = req
			n++
		default:
			drained = true
		}
	}
	// Coalescing window: let stragglers from the just-woken connections rejoin
	// the group before we commit. Lock-free so reads run unblocked meanwhile.
	if n < writeBatchMax {
		timer := time.NewTimer(groupCommitWindow)
		expired := false
		for n < writeBatchMax && !expired {
			select {
			case req := <-e.writeCh:
				reqs[n] = req
				n++
			case <-timer.C:
				expired = true
			}
		}
		timer.Stop()
	}

	// Apply the whole batch and commit once, under a single lock hold.
	var errs [writeBatchMax]error
	e.mu.Lock()
	for i := 0; i < n; i++ {
		errs[i] = e.applyOnly(reqs[i])
	}
	cerr := e.commit()
	e.mu.Unlock()

	for i := 0; i < n; i++ {
		req := reqs[i]
		err := errs[i]
		if err == nil {
			err = cerr
		}
		if req.done != nil {
			req.done <- err
		} else {
			req.setKey = nil
			req.setBody = nil
			req.fn = nil
			asyncReqPool.Put(req)
		}
	}
}

// applyOnly executes one write request against its database without committing
// and without signalling or recycling the request. drainBatchAlways uses it to
// stage every write in a group-commit batch before the single shared checkpoint.
// Caller holds e.mu.Lock. A request with neither fn nor setKey is a no-op (it
// behaves as a fence) and returns nil.
func (e *Engine) applyOnly(req *writeReq) error {
	db, err := e.ks.DB(req.index)
	if err != nil {
		return err
	}
	if req.setKey != nil {
		return db.SetWithVersion(req.setKey, req.setBody, req.setTyp, req.setEnc, req.setTTL, req.setVer)
	}
	if req.fn != nil {
		return req.fn(db)
	}
	return nil
}

// applyWriteReqDeferred executes one write without committing. It records the
// mutation as pending so the commit cron can flush it on the next tick. Caller
// holds e.mu.RLock, which allows other shard workers to run concurrently.
// Unlike commitWrite(), this does not check the dirty-page limit because calling
// commit() under RLock would deadlock.
//
// If both req.fn and req.setKey are nil, the request is a fence: it carries no
// write work and signals req.done without marking the engine dirty. Fences are
// used by FlushShardWrites to wait until all previously queued async writes for
// the shard have been applied.
func (e *Engine) applyWriteReqDeferred(req *writeReq) {
	if req.fn == nil && req.setKey == nil {
		// Fence: no-op, just signal the waiter.
		if req.done != nil {
			req.done <- nil
		}
		return
	}
	db, err := e.ks.DB(req.index)
	if err == nil {
		if req.setKey != nil {
			if req.setDel {
				_, err = db.DeleteWithVersion(req.setKey, req.setVer)
			} else {
				err = db.SetWithVersion(req.setKey, req.setBody, req.setTyp, req.setEnc, req.setTTL, req.setVer)
			}
		} else {
			err = req.fn(db)
		}
	}
	if err == nil {
		e.pendingDirty.Store(true)
		e.pendingWrites.Add(1)
	}
	if req.done != nil {
		req.done <- err
	} else {
		req.setKey = nil
		req.setBody = nil
		req.setDel = false
		req.fn = nil
		asyncReqPool.Put(req)
	}
}

// FlushShardWrites drains all pending async writes from every shard channel by
// sending a synchronous fence request to each shard worker and waiting for it
// to complete. When this returns, every SET that received "+OK" before this call
// has been applied to its shard's B-tree. Commands that read the full keyspace
// (KEYS, SCAN, RANDOMKEY) call this so they never miss a write-behind key.
//
// FlushShardWrites is a no-op when the engine workers are not running.
func (e *Engine) FlushShardWrites() {
	if !e.shardsRunning.Load() {
		return
	}
	reqs := make([]*writeReq, keyspace.NumShards)
	for s := range keyspace.NumShards {
		req := writeReqPool.Get().(*writeReq)
		req.index = 0
		req.shard = s
		req.fn = nil // nil fn + nil setKey = fence signal in applyWriteReqDeferred
		req.setKey = nil
		reqs[s] = req
		e.shardQ[s].push(req)
	}
	for s := range keyspace.NumShards {
		<-reqs[s].done
		req := reqs[s]
		req.fn = nil
		writeReqPool.Put(req)
	}
}

// isDeferred reports whether the active commit policy defers checkpoints, which
// is the precondition for the write-behind fast path. Under commitAlways every
// write must wait for the checkpoint before the reply goes out.
func (e *Engine) isDeferred() bool {
	// The hybrid-log engine has no B-tree hot cache or async write worker, so its
	// writes must take the synchronous db.Set path that routes into the store. Never
	// defer under it.
	if e.ks.HybridLog() {
		return false
	}
	return e.shardsRunning.Load() && commitPolicy(e.policy.Load()) != commitAlways
}

// sendSetAsync enqueues an inline SET write on shardQ[shard] without allocating
// a closure. It stores the SET arguments directly in the writeReq struct
// (pool-allocated) so the shard worker can call SetWithVersion without a heap
// allocation on the hot path. The push is lock-free, so concurrent SETs to the
// same shard do not serialize on a mutex.
//
// Falls back to the synchronous update path when workers are not running, the
// policy is commitAlways, or the shard queue is at its depth bound (the worker
// is behind; the synchronous path then applies backpressure under the lock).
func (e *Engine) sendSetAsync(index, shard int, key, body []byte, typ, enc uint8, ttl int64, ver uint64) error {
	if e.shardsRunning.Load() && commitPolicy(e.policy.Load()) != commitAlways &&
		e.shardQ[shard].length.Load() < shardQueueCap {
		req := asyncReqPool.Get().(*writeReq)
		req.index = index
		req.shard = shard
		req.fn = nil
		req.done = nil
		req.setKey = key
		req.setBody = body
		req.setDel = false
		req.setTyp = typ
		req.setEnc = enc
		req.setTTL = ttl
		req.setVer = ver
		e.shardQ[shard].push(req)
		return nil
	}
	return e.update(index, func(db *keyspace.DB) error {
		return db.SetWithVersion(key, body, typ, enc, ttl, ver)
	})
}

// sendDeleteAsync enqueues a write-behind delete on shardQ[shard], the delete
// twin of sendSetAsync. The worker calls DeleteWithVersion with the pre-assigned
// version, which version-guards the B-tree removal so a reordered older delete
// cannot clobber a newer same-key write. It falls back to the synchronous shard
// path under the same conditions as sendSetAsync (no workers, commitAlways, or the
// shard queue at its depth bound).
func (e *Engine) sendDeleteAsync(index, shard int, key []byte, ver uint64) error {
	if e.shardsRunning.Load() && commitPolicy(e.policy.Load()) != commitAlways &&
		e.shardQ[shard].length.Load() < shardQueueCap {
		req := asyncReqPool.Get().(*writeReq)
		req.index = index
		req.shard = shard
		req.fn = nil
		req.done = nil
		req.setKey = key
		req.setBody = nil
		req.setDel = true
		req.setVer = ver
		e.shardQ[shard].push(req)
		return nil
	}
	return e.update(index, func(db *keyspace.DB) error {
		_, err := db.DeleteWithVersion(key, ver)
		return err
	})
}

// updateShard routes a single-key write to the key's owning shard channel,
// so it can run in parallel with writes to other shards. Falls back to the
// global serial path under commitAlways or when workers are not running.
func (e *Engine) updateShard(index int, key []byte, fn func(*keyspace.DB) error) error {
	if e.shardsRunning.Load() && commitPolicy(e.policy.Load()) != commitAlways {
		s := keyspace.ShardOf(key)
		// Inline fast path: when shard s's hand-off queue is empty, no earlier write
		// is waiting on its worker, so this goroutine applies fn itself instead of
		// handing it off and blocking on the reply. The synchronous round-trip the
		// hand-off costs is two goroutine wakeups per op (producer->worker on the push,
		// worker->producer on the done signal), and on a single hot key, which is
		// exactly what the collection benchmarks drive (redis-benchmark RPUSH/HSET/
		// SADD/ZADD all hammer one fixed key), that wakeup pair is the throughput cap:
		// the profile showed the box only half busy with most of its cycles in
		// pthread_cond_wait/signal and wakep, not in the data path.
		//
		// Correctness rests on the keyspace shard write mutex, not on the worker being
		// the only writer. fn ends in db.set / CollUpdateRouted, both of which take
		// db.shards[s].mu, the same mutex the worker takes, so an inline apply is
		// serialized against the worker and against other inline appliers even though
		// both sides hold only e.mu.RLock (which excludes a checkpoint, matching the
		// worker's drain). The write sinks are version-guarded, so an apply that races
		// a concurrent same-key write resolves by version regardless of order. The
		// length==0 gate keeps an inline apply from jumping ahead of writes already
		// queued for this shard, so a command that must observe an earlier queued write
		// to the same key still hands off and serializes behind it on the worker.
		//
		// Under the single-key collection load every writer serializes on this one
		// shard mutex, so nothing piles up behind a worker and the queue stays empty:
		// the inline path is taken every time and the worker stays parked. Under spread
		// writes the per-shard queues carry real depth, so the gate falls through to the
		// hand-off and the worker keeps batching as before.
		if e.shardQ[s].length.Load() == 0 {
			e.mu.RLock()
			db, err := e.ks.DB(index)
			if err == nil {
				err = fn(db)
			}
			if err == nil {
				e.pendingDirty.Store(true)
				e.pendingWrites.Add(1)
			}
			e.mu.RUnlock()
			return err
		}
		req := writeReqPool.Get().(*writeReq)
		req.index = index
		req.shard = s
		req.fn = fn
		req.setKey = nil
		e.shardQ[s].push(req)
		err := <-req.done
		req.fn = nil
		writeReqPool.Put(req)
		return err
	}
	return e.update(index, fn)
}

// update runs fn against database index under the engine lock and, on success,
// records the change under the active commit policy. A write command goes
// through here.
//
// When the write worker is running, the request is sent to its channel and the
// caller blocks on the reply. When the worker is not running (tests, startup
// before StartWorker), the write lock is acquired directly.
func (e *Engine) update(index int, fn func(*keyspace.DB) error) error {
	if e.writeCh != nil {
		req := writeReqPool.Get().(*writeReq)
		req.index = index
		req.shard = -1
		req.fn = fn
		e.writeCh <- req
		err := <-req.done
		req.fn = nil // don't hold a reference to the closure in the pool
		writeReqPool.Put(req)
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	db, err := e.ks.DB(index)
	if err != nil {
		return err
	}
	if err := fn(db); err != nil {
		return err
	}
	return e.commitWrite()
}

// commitWrite applies the active policy after a write has mutated the buffer
// pool. Under commitAlways it checkpoints immediately. Under deferred policies
// it marks the write pending; if the dirty-page limit is crossed it commits
// early to bound memory use. Caller holds e.mu.Lock (direct path or commitAlways
// worker batch). Do NOT call this under e.mu.RLock — use applyWriteReqDeferred
// for the concurrent shard worker path instead.
func (e *Engine) commitWrite() error {
	if commitPolicy(e.policy.Load()) == commitAlways {
		return e.commit()
	}
	e.pendingDirty.Store(true)
	n := e.pendingWrites.Add(1)
	if e.dirtyPageLimit > 0 && n%dirtyCheckStride == 0 {
		if e.ks.PagerStats().DirtyPages >= e.dirtyPageLimit {
			return e.commit()
		}
	}
	return nil
}

// commit checkpoints the keyspace and reports how long the commit took to the
// latency hook when one is installed. It clears the pending-write bookkeeping so
// the deferred path starts a fresh interval. Caller holds e.mu.Lock.
func (e *Engine) commit() error {
	start := time.Now()
	err := e.ks.Commit()
	if err == nil {
		e.pendingDirty.Store(false)
		e.pendingWrites.Store(0)
		e.lastCommit = start
	}
	if e.onCommit != nil {
		e.onCommit("checkpoint", time.Since(start))
	}
	return err
}

// commitCron flushes pending writes when the everysec interval has elapsed. The
// server cron calls it once per tick. It is a no-op under commitAlways (nothing
// is ever pending) and under commitNo (which flushes only on SAVE, shutdown, or
// the dirty-page bound).
//
// It does a lock-free pre-check on pendingDirty to avoid acquiring the engine
// write lock (which would briefly stall all shard workers) when there is nothing
// to flush.
func (e *Engine) commitCron(now time.Time) error {
	if !e.pendingDirty.Load() {
		return nil
	}
	if commitPolicy(e.policy.Load()) != commitEverySec {
		return nil
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	// Re-check under the lock; a concurrent worker may have already committed.
	if !e.pendingDirty.Load() {
		return nil
	}
	if now.Sub(e.lastCommit) < time.Second {
		return nil
	}
	return e.commit()
}

// ForceCommit flushes any pending writes synchronously. SAVE, BGSAVE, and a
// clean shutdown call it so a deferred policy still lands every acknowledged
// write on disk before the file closes. It is safe to call when nothing is
// pending or no policy deferred anything.
func (e *Engine) ForceCommit() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	// Fold any hash-overlay writes still resident in memory into their sub-trees so
	// the checkpoint persists them; an unfolded write lives only in the residency
	// map and in no page. The fold dirties pages, so commit when it wrote even if
	// nothing else was pending.
	folded, err := e.ks.FoldAllOverlay()
	if err != nil {
		return err
	}
	if !folded && !e.pendingDirty.Load() {
		return nil
	}
	return e.commit()
}

// updateKeyspace runs fn with access to every database under the engine lock and
// commits on success. Cross-database writes like MOVE and COPY go through here.
func (e *Engine) updateKeyspace(fn func(*keyspace.Keyspace) error) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := fn(e.ks); err != nil {
		return err
	}
	return e.commitWrite()
}

// updateKeyspaceDurable runs fn against every database and always checkpoints,
// ignoring the deferred commit policy. Rare administrative metadata writes (ACL
// users, function libraries, cached scripts) go through here so an explicit
// FUNCTION LOAD or ACL SETUSER survives a restart at once, the way a user
// expects, without the data command hot path paying for it.
func (e *Engine) updateKeyspaceDurable(fn func(*keyspace.Keyspace) error) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := fn(e.ks); err != nil {
		return err
	}
	return e.commit()
}

// version returns the write version of a key in database index, and whether the
// key is live. A missing or expired key reports version 0 and exists false. WATCH
// and EXEC use this to detect a change to a watched key.
func (e *Engine) version(index int, key []byte) (uint64, bool, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	db, err := e.ks.DB(index)
	if err != nil {
		return 0, false, err
	}
	_, hdr, found, err := db.Get(key)
	if err != nil || !found {
		return 0, false, err
	}
	return hdr.Version, true, nil
}

// view runs fn against database index under the engine read lock without
// committing. Multiple reads run concurrently; writes take the write lock and
// exclude reads. Lazy expiry is deferred to the next active expiry cycle so the
// read path is free of B-tree writes.
func (e *Engine) view(index int, fn func(*keyspace.DB) error) error {
	e.mu.RLock()
	defer e.mu.RUnlock()
	db, err := e.ks.DB(index)
	if err != nil {
		return err
	}
	return fn(db)
}

// viewHotGet is a lock-free fast path for GET-like commands. It checks the
// hot-value cache for index/key without acquiring the engine read lock. On a
// hit it returns (body, hdr, true); on a miss it returns (nil, _, false) and
// the caller must fall back to view(). The ks.DB lookup is safe without the
// engine lock because the dbs slice is immutable after Open.
func (e *Engine) viewHotGet(index int, key []byte) ([]byte, keyspace.ValueHeader, bool) {
	db, err := e.ks.DB(index)
	if err != nil {
		return nil, keyspace.ValueHeader{}, false
	}
	return db.HotGet(key)
}

// hybridLog reports whether the engine runs its string point path on the v2
// hybrid-log store. GET reads it to pick the hybrid read fast path.
func (e *Engine) hybridLog() bool { return e.ks.HybridLog() }

// viewHybridGet reads a key straight off the hybrid-log store. It is the hybrid
// analogue of viewHotGet: the hybrid engine never populates the hot-value cache,
// so probing it is wasted work, and the store carries its own per-shard locks so
// the read needs no engine-level read lock. The ks.DB lookup is safe lock-free
// because the dbs slice is immutable after Open, and GetUncached routes to the
// store, which loads its handle through an atomic pointer. So a hybrid GET costs
// one DB lookup and the store read, with none of the per-command atomics the
// general view path pays.
func (e *Engine) viewHybridGet(index int, key []byte) ([]byte, keyspace.ValueHeader, bool, error) {
	db, err := e.ks.DB(index)
	if err != nil {
		return nil, keyspace.ValueHeader{}, false, err
	}
	return db.GetUncached(key)
}

// activeExpireCycle runs one background expiry pass over every database, deleting
// volatile keys whose TTL has passed and committing the removals so they are
// durable. The expired keys land in the log for the caller to notify on.
func (e *Engine) activeExpireCycle() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	n, err := e.ks.ActiveExpireCycle()
	if err != nil {
		return err
	}
	if n == 0 {
		return nil
	}
	return e.commitWrite()
}

// takeExpired drains the keys the active expiry cycle removed since the last
// call. TakeExpired holds expiredMu internally, so no engine lock is needed.
func (e *Engine) takeExpired() []keyspace.ExpiredKey {
	return e.ks.TakeExpired()
}

// snapshotAll copies every live key in every database into an rdb.Snapshot under
// the engine read lock. The copy is taken in memory; BGSAVE writes it from a
// background goroutine while new writes proceed.
func (e *Engine) snapshotAll() (rdb.Snapshot, error) {
	// The write lock is taken rather than the read lock because the overlay fold
	// below mutates sub-trees. A resident hash's newest elements live only in the
	// residency map; without the fold the snapshot would walk a stale sub-tree and
	// miss them. BGSAVE copies in memory under the lock, so the pause is bounded by
	// the copy, not by writing the file.
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, err := e.ks.FoldAllOverlay(); err != nil {
		return rdb.Snapshot{}, err
	}
	return SnapshotKeyspace(e.ks)
}

// setLFUParams pushes the lfu-log-factor and lfu-decay-time knobs down to the
// keyspace, which the eviction sampler reads when it scores LFU candidates.
func (e *Engine) setLFUParams(logFactor, decayTime int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.ks.SetLFUParams(logFactor, decayTime)
}

// usedMemory returns the live-data estimate the maxmemory check compares against.
func (e *Engine) usedMemory() int64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.ks.UsedMemory()
}

// sampleForEviction returns up to n eviction candidates, restricted to volatile
// keys when volatileOnly is set.
func (e *Engine) sampleForEviction(n int, volatileOnly bool) []keyspace.EvictionCandidate {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.ks.SampleForEviction(n, volatileOnly)
}

// evict deletes one key for the eviction loop and commits so the removal is
// durable. It reports whether a key was actually removed.
func (e *Engine) evict(dbIndex int, key []byte) (bool, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	db, err := e.ks.DB(dbIndex)
	if err != nil {
		return false, err
	}
	ok, err := db.Delete(key)
	if err != nil || !ok {
		return false, err
	}
	return true, e.commitWrite()
}

// dbSizes returns the key count of every database, indexed by database number.
// INFO's keyspace section reads it.
func (e *Engine) dbSizes() []uint64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	n := e.ks.DBCount()
	out := make([]uint64, n)
	for i := range n {
		db, err := e.ks.DB(i)
		if err == nil {
			out[i] = db.Len()
		}
	}
	return out
}

// fileStats returns the pager counters for the file-growth INFO fields.
func (e *Engine) fileStats() pager.Stats {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.ks.PagerStats()
}

// filePath returns the path of the .aki file backing the engine, empty for an
// in-memory backing.
func (e *Engine) filePath() string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.ks.PagerName()
}

// updateWriteBehind stages an inline SET write in the hot-value cache and the
// write-behind pending table synchronously, then fires the B-tree write to the
// write worker without blocking. The connection goroutine never waits for the
// B-tree write to complete, so it can reply and read the next command while the
// write worker applies the change in the background.
//
// key and body must be heap-owned copies of the command arguments; the caller
// must not pass slices that alias the connection read buffer (qbuf), because
// qbuf may be reused before the async B-tree write runs. len(body) must be at
// most keyspace.MaxInlineBody; the caller must verify this before calling.
//
// This function does not take the engine lock: ks.DB() reads an immutable
// slice, NextVersion() is an atomic increment, and PrepareWriteBehind uses its
// own per-shard mutexes. The async B-tree write is queued with sendSetAsync,
// which stores the arguments directly in a pooled writeReq (no closure
// allocation) and routes the request to the shard-owned channel.
//
// If the channel is full or the policy is commitAlways, it falls back to the
// synchronous update path. The caller must have confirmed e.isDeferred().
func (ctx *Ctx) updateWriteBehind(key, body []byte, typ, enc uint8, ttlMs int64) bool {
	if ctx.d.engine == nil {
		ctx.enc().WriteError("ERR this server has no keyspace")
		return false
	}
	e := ctx.d.engine
	db, dbErr := e.ks.DB(ctx.Conn.DB())
	if dbErr != nil {
		ctx.enc().WriteError("ERR " + dbErr.Error())
		return false
	}
	version := e.ks.NextVersion()
	hdr := keyspace.ValueHeader{
		Type:     typ,
		Encoding: enc,
		TTLms:    ttlMs,
		Version:  version,
		BodyLen:  uint32(len(body)),
		RefCount: 1,
		Flags:    keyspace.FlagInlineBody,
	}
	if ttlMs >= 0 {
		hdr.Flags |= keyspace.FlagHasTTL
	}
	db.PrepareWriteBehind(key, body, hdr)
	// Route the async B-tree write directly to the shard channel using the
	// inline SET fast path. sendSetAsync stores the arguments in the pooled
	// writeReq struct so no closure allocation is needed on this hot path.
	shard := keyspace.ShardOf(key)
	if asyncErr := e.sendSetAsync(ctx.Conn.DB(), shard, key, body, typ, enc, ttlMs, version); asyncErr != nil {
		ctx.enc().WriteError("ERR " + asyncErr.Error())
		return false
	}
	ctx.d.persist.markDirty()
	return true
}

// rmwResult is what an rmwWriteBehind compute closure produces: the new whole
// body to store and its header fields, or write=false to leave the key
// unchanged (the handler has already decided its own reply, for instance a wrong
// type or overflow error). body must be heap-owned and must not alias the
// connection read buffer, because the durable write runs asynchronously.
type rmwResult struct {
	body  []byte
	typ   uint8
	enc   uint8
	ttlMs int64
	write bool
	// fallback asks the helper to run the synchronous path instead of staging an
	// inline write-behind cell. A compute closure sets it when the new value is
	// not a simple inline whole-body write: a btree-backed collection element
	// update, a listpack to quicklist promotion, or any case its syncFn handles
	// but the fast path cannot. write is ignored when fallback is set.
	fallback bool
	// del asks the helper to stage a delete (tombstone) instead of a value write,
	// for a read-modify-write whose result is to remove the key: an LPOP or RPOP
	// that empties its list, for instance. body/typ/enc/ttl are ignored and write
	// is treated as false. fallback takes precedence over del.
	del bool
}

// rmwWriteBehind runs a whole-body read-modify-write for key with the durable
// B-tree write deferred to the async write-behind path, the read-modify-write
// analogue of updateWriteBehind. It is the fast path for INCR, LPUSH, SADD,
// HSET and ZADD in their small whole-body form: the reply depends on the current
// value, so it cannot blind-write like SET, but it need not block the reply on
// the B-tree either.
//
// compute receives the current body, header and presence (body is nil when the
// key is absent) and returns the new value to store. It runs exactly once under
// rmwLocks[ShardOf(key)], which serializes same-key RMW writers so no update is
// lost. After compute the new body is staged in the hot cache with a fresh
// version and the durable write is fired asynchronously, mirroring SET.
//
// Returns false only when an engine error has already been written to the wire.
// A compute that declines to write (write=false) still returns true; the caller
// inspects its own captured state to write the reply.
//
// syncFn is the synchronous fallback closure. When it is nil the helper rebuilds
// the write from compute (read, compute, db.Set), which is all a plain whole-body
// RMW like INCR needs. A caller whose write is not a plain whole-body Set (a
// collection that may be in or promote to the btree-backed element form) passes
// its own full closure here and has compute return fallback=true for the cases
// only that closure can handle.
//
// Fallbacks match the SET fast path: when the policy is not deferred (or no
// workers run) the whole RMW runs synchronously under the global update lock, a
// body larger than keyspace.MaxInlineBody falls back because the inline
// write-behind cell cannot hold it, and a compute that returns fallback=true
// runs syncFn under the shard RMW lock so no concurrent fast-path stage on the
// same shard can interleave with it.
func (ctx *Ctx) rmwWriteBehind(key []byte, compute func(cur []byte, hdr keyspace.ValueHeader, found bool) rmwResult, syncFn func(*keyspace.DB) error) bool {
	if ctx.d.engine == nil {
		ctx.enc().WriteError("ERR this server has no keyspace")
		return false
	}
	e := ctx.d.engine
	index := ctx.Conn.DB()
	db, dbErr := e.ks.DB(index)
	if dbErr != nil {
		ctx.enc().WriteError("ERR " + dbErr.Error())
		return false
	}

	// runSync runs the write synchronously through the key's own shard worker. It
	// is the path for commitAlways, the no-workers case, oversized bodies, and any
	// compute that asked to fall back. With a syncFn it runs that closure verbatim;
	// otherwise it rebuilds the write from compute, which is pure given the current
	// value.
	//
	// It must route through updateShard (shardQ[ShardOf(key)]), not the global
	// update channel: the fast path stages and fires its durable write on that same
	// per-shard channel, so a fallback for the same key has to queue behind those
	// in-flight async writes on the same worker to see them. The global update
	// channel is a separate goroutine with no ordering against the per-shard async
	// writes, which would let a fallback read a stale body and drop a staged update.
	runSync := func() bool {
		fn := syncFn
		if fn == nil {
			fn = func(db *keyspace.DB) error {
				cur, hdr, found, err := db.Get(key)
				if err != nil {
					return err
				}
				r := compute(cur, hdr, found)
				if !r.write {
					return nil
				}
				return db.Set(key, r.body, r.typ, r.enc, r.ttlMs)
			}
		}
		if err := e.updateShard(index, key, fn); err != nil {
			ctx.enc().WriteError("ERR " + err.Error())
			return false
		}
		ctx.d.persist.markDirty()
		ctx.fireExpired()
		return true
	}

	if !e.isDeferred() {
		return runSync()
	}

	shard := keyspace.ShardOf(key)
	stripe := rmwStripe(key)
	e.rmwLocks[stripe].Lock()
	cur, hdr, found, err := db.Get(key)
	if err != nil {
		e.rmwLocks[stripe].Unlock()
		ctx.enc().WriteError("ERR " + err.Error())
		return false
	}
	r := compute(cur, hdr, found)
	if r.fallback {
		// compute hit a case the fast path cannot stage (a btree-backed collection
		// or a promotion). Run the synchronous closure while still holding this key's
		// RMW stripe so a concurrent fast-path stage on the same key cannot interleave.
		ok := runSync()
		e.rmwLocks[stripe].Unlock()
		return ok
	}
	if r.del {
		// The RMW removes the key (an LPOP/RPOP that emptied its list). Stage a
		// tombstone under the lock so a concurrent same-key writer cannot read the
		// old value, then fire the version-guarded delete asynchronously, mirroring
		// the staged-SET path. The key copy outlives this command, so it must not
		// alias the connection read buffer.
		keyCopy := append([]byte(nil), key...)
		version := e.ks.NextVersion()
		db.PrepareDeleteBehind(keyCopy, version)
		e.rmwLocks[stripe].Unlock()
		if asyncErr := e.sendDeleteAsync(index, shard, keyCopy, version); asyncErr != nil {
			ctx.enc().WriteError("ERR " + asyncErr.Error())
			return false
		}
		ctx.d.persist.markDirty()
		return true
	}
	if !r.write {
		e.rmwLocks[stripe].Unlock()
		return true
	}
	// The staged and async writes outlive this command, so the key must be a
	// heap-owned copy rather than a slice of the connection read buffer. compute's
	// body is already a fresh allocation. Only this path allocates; the error and
	// no-write paths above do not.
	keyCopy := append([]byte(nil), key...)
	version := e.ks.NextVersion()
	// A body over MaxInlineBody rides the overflow chain in the B-tree, so the
	// staged header must not claim FlagInlineBody for it. The staged copy in the
	// hot cache and wbPending always carries the full body and the read paths
	// return that body directly without consulting the flag (only the B-tree read
	// follows FlagInlineBody/BodyRef, and the durable SetWithVersion re-derives the
	// correct flag and writes the overflow chain there). So an over-size RMW write
	// stages and fires asynchronously just like a small one, instead of dropping to
	// the synchronous shard round-trip; this is the LPUSH and large-HSET fast path,
	// where the list or hash blob grows past the inline cap after a handful of
	// elements and every later element would otherwise block the reply on a worker
	// round-trip.
	flags := uint8(keyspace.FlagInlineBody)
	if len(r.body) > keyspace.MaxInlineBody {
		flags = 0
	}
	nhdr := keyspace.ValueHeader{
		Type:     r.typ,
		Encoding: r.enc,
		TTLms:    r.ttlMs,
		Version:  version,
		BodyLen:  uint32(len(r.body)),
		RefCount: 1,
		Flags:    flags,
	}
	if r.ttlMs >= 0 {
		nhdr.Flags |= keyspace.FlagHasTTL
	}
	db.PrepareWriteBehind(keyCopy, r.body, nhdr)
	e.rmwLocks[stripe].Unlock()
	// The durable hand-off does not need the RMW lock. The lock's job is to make
	// the read-modify-stage atomic so two pushes to one key cannot both read the
	// old value and lose an update; once PrepareWriteBehind has staged this write
	// under the lock, the staged value is the authoritative one every reader sees
	// (hot cache, then wbPending), and the version is already assigned. The B-tree
	// hand-off is version-guarded at every sink: the shard worker applies in
	// version order and SetWithVersion rejects an older version, and removeWBPending
	// only clears a pending entry whose version matches, so a send that reorders
	// past a newer same-key send cannot clobber it. This is the same guarantee the
	// blind SET path already relies on, which also sends with no per-key lock.
	// Moving the send out lets the fifty connections that hit one shard in lockstep
	// run the queue push concurrently instead of serializing it behind the lock,
	// and the shorter critical section in turn cuts the contention on the lock
	// itself, which the INCR-family profile showed as its largest single cost.
	asyncErr := e.sendSetAsync(index, shard, keyCopy, r.body, r.typ, r.enc, r.ttlMs, version)
	if asyncErr != nil {
		ctx.enc().WriteError("ERR " + asyncErr.Error())
		return false
	}
	ctx.d.persist.markDirty()
	return true
}

// updateShard routes a single-key write for key to the key's owning shard
// channel. Under a deferred commit policy this lets it run in parallel with
// writes to other shards. Under commitAlways or when workers are not running
// it falls back to the global serial path via update().
func (ctx *Ctx) updateShard(key []byte, fn func(*keyspace.DB) error) bool {
	if ctx.d.engine == nil {
		ctx.enc().WriteError("ERR this server has no keyspace")
		return false
	}
	if err := ctx.d.engine.updateShard(ctx.Conn.DB(), key, fn); err != nil {
		ctx.enc().WriteError("ERR " + err.Error())
		return false
	}
	ctx.d.persist.markDirty()
	ctx.fireExpired()
	return true
}

// update routes a write to the current connection's database. It reports false
// and writes an error reply when no engine is configured.
func (ctx *Ctx) update(fn func(*keyspace.DB) error) bool {
	if ctx.d.engine == nil {
		ctx.enc().WriteError("ERR this server has no keyspace")
		return false
	}
	if err := ctx.d.engine.update(ctx.Conn.DB(), fn); err != nil {
		ctx.enc().WriteError("ERR " + err.Error())
		return false
	}
	ctx.d.persist.markDirty()
	ctx.fireExpired()
	return true
}

// updateKeyspace routes a multi-database write through the engine, mirroring
// update. It reports false and writes an error reply when no engine is set.
func (ctx *Ctx) updateKeyspace(fn func(*keyspace.Keyspace) error) bool {
	if ctx.d.engine == nil {
		ctx.enc().WriteError("ERR this server has no keyspace")
		return false
	}
	if err := ctx.d.engine.updateKeyspace(fn); err != nil {
		ctx.enc().WriteError("ERR " + err.Error())
		return false
	}
	ctx.d.persist.markDirty()
	ctx.fireExpired()
	return true
}

// view routes a read to the current connection's database, mirroring update.
func (ctx *Ctx) view(fn func(*keyspace.DB) error) bool {
	if ctx.d.engine == nil {
		ctx.enc().WriteError("ERR this server has no keyspace")
		return false
	}
	if err := ctx.d.engine.view(ctx.Conn.DB(), fn); err != nil {
		ctx.enc().WriteError("ERR " + err.Error())
		return false
	}
	ctx.fireExpired()
	return true
}

// fireExpired drains the keys lazy expiry removed during the access just made and
// fires the "expired" keyspace event for each. It runs after the engine call
// returns, so the event fires outside the keyspace lock, the same ordering the
// type-event notifications use.
func (ctx *Ctx) fireExpired() { ctx.d.drainExpired() }

// drainExpired empties the lazy-expiry log and fires the "expired" keyspace event
// for each key, on the database the key lived in. Both the command wrappers and
// the WATCH version check call it after touching the keyspace.
func (d *Dispatcher) drainExpired() {
	if d.engine == nil {
		return
	}
	for _, ek := range d.engine.takeExpired() {
		d.notifyKeyspaceEvent(ek.DB, notifyExpired, "expired", string(ek.Key))
		d.trackingInvalidateKey(ek.Key, 0)
	}
}
