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
type writeReq struct {
	index int
	fn    func(*keyspace.DB) error
	done  chan error // nil for fire-and-forget requests
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

	// pendingDirty is true when at least one write has mutated the buffer pool
	// since the last checkpoint. pendingWrites counts those writes for the
	// dirty-page sampler. lastCommit stamps the previous checkpoint so the cron
	// can hold the everysec interval. All three are guarded by mu.
	pendingDirty  bool
	pendingWrites int
	lastCommit    time.Time

	// dirtyPageLimit is the early-commit bound described above. Guarded by mu.
	dirtyPageLimit int

	// writeCh is the input queue for the write worker. nil when the worker is not
	// running; update() falls back to a direct lock acquire in that case.
	writeCh chan *writeReq
	// workerStop is closed by StopWorker to signal the write goroutine to drain
	// and exit. workerDone is closed by the goroutine once it has exited.
	workerStop chan struct{}
	workerDone chan struct{}
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
	if !e.pendingDirty {
		return nil
	}
	return e.commit()
}

// StartWorker starts the single-goroutine write worker. All calls to update()
// after this point route through a buffered channel to the worker instead of
// acquiring the write lock directly, eliminating contention between connection
// goroutines. The caller must eventually call StopWorker to clean up.
func (e *Engine) StartWorker() {
	if e.writeCh != nil {
		return // already running
	}
	e.writeCh = make(chan *writeReq, 4096)
	e.workerStop = make(chan struct{})
	e.workerDone = make(chan struct{})
	go e.runWriteWorker()
}

// StopWorker signals the write worker to drain any pending requests and exit,
// then waits until it has. After this returns, update() falls back to the
// direct lock path. Safe to call when StartWorker was never called.
func (e *Engine) StopWorker() {
	if e.writeCh == nil {
		return
	}
	close(e.workerStop)
	<-e.workerDone
	e.writeCh = nil
}

// runWriteWorker is the write worker goroutine body. It drains write requests
// from writeCh, applies them in batches under a single lock acquisition, and
// signals each sync requester via its done channel. Fire-and-forget requests
// (done == nil) are returned to asyncReqPool after processing. On workerStop
// it drains any remaining requests before exiting so no sync caller blocks
// forever on its done channel.
func (e *Engine) runWriteWorker() {
	defer close(e.workerDone)
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

// drainBatch applies the first request and then greedily drains any additional
// requests already sitting in writeCh, up to writeBatchMax, under one lock hold.
// Holding the lock across a batch keeps the B-tree pages warm in the CPU cache
// of the worker's core and amortises the lock acquire/release cost.
func (e *Engine) drainBatch(first *writeReq) {
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
		err = req.fn(db)
	}
	if err == nil {
		err = e.commitWrite()
	}
	if req.done != nil {
		req.done <- err
	} else {
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

// updateAsync enqueues a fire-and-forget write when the write worker is running
// and the commit policy is deferred. The caller returns nil immediately; the
// write worker applies fn to the B-tree in the background without signaling the
// caller. fn must capture only heap-owned copies of any data derived from the
// connection read buffer (qbuf), because qbuf may be reused before fn runs.
//
// If the worker channel is full or the policy is commitAlways, updateAsync
// falls back to the synchronous update path to apply back-pressure and preserve
// the durability contract.
func (e *Engine) updateAsync(index int, fn func(*keyspace.DB) error) error {
	if e.writeCh != nil && commitPolicy(e.policy.Load()) != commitAlways {
		req := asyncReqPool.Get().(*writeReq)
		req.index = index
		req.fn = fn
		req.done = nil
		select {
		case e.writeCh <- req:
			return nil
		default:
			// Channel full: fall back to sync to apply back-pressure.
			asyncReqPool.Put(req)
		}
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

// commitWrite applies the active policy to a write that has already mutated the
// buffer pool. Under commitAlways it checkpoints now. Under a deferred policy it
// marks the work pending and only checkpoints when the dirty-page bound is hit,
// leaving the cron to flush on its timer. Caller holds e.mu.
func (e *Engine) commitWrite() error {
	if commitPolicy(e.policy.Load()) == commitAlways {
		return e.commit()
	}
	e.pendingDirty = true
	e.pendingWrites++
	if e.dirtyPageLimit > 0 && e.pendingWrites%dirtyCheckStride == 0 {
		if e.ks.PagerStats().DirtyPages >= e.dirtyPageLimit {
			return e.commit()
		}
	}
	return nil
}

// commit checkpoints the keyspace and reports how long the commit took to the
// latency hook when one is installed. It clears the pending-write bookkeeping so
// the deferred path starts a fresh interval. Caller holds e.mu.
func (e *Engine) commit() error {
	start := time.Now()
	err := e.ks.Commit()
	if err == nil {
		e.pendingDirty = false
		e.pendingWrites = 0
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
func (e *Engine) commitCron(now time.Time) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.pendingDirty {
		return nil
	}
	if commitPolicy(e.policy.Load()) != commitEverySec {
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
	if !e.pendingDirty {
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
// B-tree write to complete, so it can send the reply and read the next command
// while the write worker applies the change in the background.
//
// key and body must be heap-owned copies of the command arguments; the caller
// must not pass slices that alias the connection read buffer (qbuf), because
// qbuf may be reused before the async B-tree write runs. len(body) must be at
// most keyspace.MaxInlineBody; the caller must verify this before calling.
//
// updateWriteBehind assigns the write version atomically under the engine read
// lock and stores it in the hot-value cache and the write-behind pending table
// so any subsequent read sees the new value immediately, even on a cache miss.
// If the write worker channel is full or the policy is commitAlways, it falls
// back to the synchronous update path to preserve durability and apply
// back-pressure. The caller must have confirmed e.isDeferred() before calling.
func (ctx *Ctx) updateWriteBehind(key, body []byte, typ, enc uint8, ttlMs int64) bool {
	if ctx.d.engine == nil {
		ctx.enc().WriteError("ERR this server has no keyspace")
		return false
	}
	e := ctx.d.engine
	var version uint64
	var err error
	e.mu.RLock()
	db, dbErr := e.ks.DB(ctx.Conn.DB())
	if dbErr == nil {
		version = e.ks.NextVersion()
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
	} else {
		err = dbErr
	}
	e.mu.RUnlock()
	if err != nil {
		ctx.enc().WriteError("ERR " + err.Error())
		return false
	}
	// Capture copies so the closure is safe after qbuf is compacted.
	k, v, t, en, ttl, ver := key, body, typ, enc, ttlMs, version
	if asyncErr := e.updateAsync(ctx.Conn.DB(), func(db *keyspace.DB) error {
		return db.SetWithVersion(k, v, t, en, ttl, ver)
	}); asyncErr != nil {
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
