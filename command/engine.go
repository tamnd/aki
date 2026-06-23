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
// to writeChs[shard]. shard == -1 means global (routed to writeCh).
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
	// writeChs are the per-shard input queues. writeChs[s] is served exclusively
	// by one goroutine, so there is zero channel contention between shards.
	// Deferred single-key writes route here by ShardOf(key).
	writeChs [keyspace.NumShards]chan *writeReq
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
		e.writeChs[s] = make(chan *writeReq, 4096)
	}
	e.workerStop = make(chan struct{})
	e.workerDone = make(chan struct{})
	e.wg.Add(1 + keyspace.NumShards)
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
	close(e.workerStop)
	<-e.workerDone
	e.writeCh = nil
	for s := range keyspace.NumShards {
		e.writeChs[s] = nil
	}
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

// runShardWorker is a dedicated goroutine for shard s. It reads exclusively
// from writeChs[s] — no other goroutine ever reads that channel — so there is
// zero channel-lock contention between shards. It holds e.mu.RLock during a
// batch so the shard runs in parallel with other shard workers while commits
// (which take e.mu.Lock) remain fully exclusive.
func (e *Engine) runShardWorker(s int) {
	defer e.wg.Done()
	for {
		select {
		case req := <-e.writeChs[s]:
			e.drainShardBatch(s, req)
		case <-e.workerStop:
			for {
				select {
				case req := <-e.writeChs[s]:
					e.drainShardBatch(s, req)
				default:
					return
				}
			}
		}
	}
}

// drainShardBatch processes a batch from writeChs[s] under e.mu.RLock.
// Because only one goroutine reads writeChs[s], and keyspace shard s is also
// written exclusively by this goroutine, both the channel and the B-tree are
// uncontended. Batching amortises the RLock acquire/release cost.
func (e *Engine) drainShardBatch(s int, first *writeReq) {
	e.mu.RLock()
	e.applyWriteReqDeferred(first)
	for i := 1; i < writeBatchMax; i++ {
		select {
		case req := <-e.writeChs[s]:
			e.applyWriteReqDeferred(req)
		default:
			e.mu.RUnlock()
			return
		}
	}
	e.mu.RUnlock()
}

// drainBatch applies the first request and then greedily drains additional
// requests from writeCh, up to writeBatchMax, under one lock hold.
//
// Under commitAlways, it takes e.mu.Lock so each write can checkpoint inside
// applyWriteReq. Under deferred policies (commitEverySec, commitNo) it takes
// e.mu.RLock, which lets concurrent workers apply writes to different shards in
// parallel while still blocking commits (which take e.mu.Lock) from running
// mid-batch. Dirty state is recorded atomically; the commit cron flushes it.
func (e *Engine) drainBatch(first *writeReq) {
	if commitPolicy(e.policy.Load()) == commitAlways {
		e.mu.Lock()
		e.applyWriteReq(first)
		for i := 1; i < writeBatchMax; i++ {
			select {
			case req := <-e.writeCh:
				e.applyWriteReq(req)
			default:
				e.mu.Unlock()
				return
			}
		}
		e.mu.Unlock()
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
			err = db.SetWithVersion(req.setKey, req.setBody, req.setTyp, req.setEnc, req.setTTL, req.setVer)
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
	if e.writeCh == nil {
		return
	}
	reqs := make([]*writeReq, keyspace.NumShards)
	for s := range keyspace.NumShards {
		req := writeReqPool.Get().(*writeReq)
		req.index = 0
		req.shard = s
		req.fn = nil    // nil fn + nil setKey = fence signal in applyWriteReqDeferred
		req.setKey = nil
		reqs[s] = req
		e.writeChs[s] <- req // may block briefly if channel is at capacity
	}
	for s := range keyspace.NumShards {
		<-reqs[s].done
		req := reqs[s]
		req.fn = nil
		writeReqPool.Put(req)
	}
}

// applyWriteReq executes one write request under the already-held write lock.
// For sync requests (done != nil) it signals the caller via the done channel;
// the caller is responsible for returning the req to writeReqPool afterward.
// For fire-and-forget requests (done == nil) it returns the req to asyncReqPool
// itself because the caller already returned without waiting for a signal.
// Caller holds e.mu.
func (e *Engine) applyWriteReq(req *writeReq) {
	db, err := e.ks.DB(req.index)
	if err == nil {
		if req.setKey != nil {
			err = db.SetWithVersion(req.setKey, req.setBody, req.setTyp, req.setEnc, req.setTTL, req.setVer)
		} else {
			err = req.fn(db)
		}
	}
	if err == nil {
		err = e.commitWrite()
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

// isDeferred reports whether the active commit policy defers checkpoints, which
// is the precondition for the write-behind fast path. Under commitAlways every
// write must wait for the checkpoint before the reply goes out.
func (e *Engine) isDeferred() bool {
	return e.writeCh != nil && commitPolicy(e.policy.Load()) != commitAlways
}

// sendSetAsync enqueues an inline SET write on writeChs[shard] without
// allocating a closure. It stores the SET arguments directly in the writeReq
// struct (pool-allocated) so the shard worker can call SetWithVersion without
// a heap allocation on the hot path.
//
// Falls back to the synchronous update path when workers are not running or
// the policy is commitAlways.
func (e *Engine) sendSetAsync(index, shard int, key, body []byte, typ, enc uint8, ttl int64, ver uint64) error {
	if e.writeChs[shard] != nil && commitPolicy(e.policy.Load()) != commitAlways {
		req := asyncReqPool.Get().(*writeReq)
		req.index = index
		req.shard = shard
		req.fn = nil
		req.done = nil
		req.setKey = key
		req.setBody = body
		req.setTyp = typ
		req.setEnc = enc
		req.setTTL = ttl
		req.setVer = ver
		select {
		case e.writeChs[shard] <- req:
			return nil
		default:
			asyncReqPool.Put(req)
		}
	}
	return e.update(index, func(db *keyspace.DB) error {
		return db.SetWithVersion(key, body, typ, enc, ttl, ver)
	})
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
	if !e.pendingDirty.Load() {
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
	e.mu.RLock()
	defer e.mu.RUnlock()
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
