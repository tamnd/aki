package command

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/tamnd/aki/keyspace"
	"github.com/tamnd/aki/pager"
	"github.com/tamnd/aki/rdb"
)

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

// Engine is the command layer's handle on the keyspace. It serializes every
// access with a single mutex, which is the one-writer assumption the keyspace
// makes at this milestone. The sharded writer model from doc 05 §7 replaces this
// global lock in a later slice.
type Engine struct {
	mu sync.Mutex
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

// update runs fn against database index under the engine lock and, on success,
// records the change under the active commit policy. A write command goes
// through here.
func (e *Engine) update(index int, fn func(*keyspace.DB) error) error {
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
// and EXEC use this to detect a change to a watched key. The read goes through the
// lock and may delete an expired key as a side effect, the same lazy expiry any
// read does.
func (e *Engine) version(index int, key []byte) (uint64, bool, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
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

// view runs fn against database index under the engine lock without committing.
// A read command goes through here. Lazy expiry inside a read may delete a key;
// that deletion is left in the buffer pool and folds into the next commit.
func (e *Engine) view(index int, fn func(*keyspace.DB) error) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	db, err := e.ks.DB(index)
	if err != nil {
		return err
	}
	return fn(db)
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

// takeExpired drains the keys lazy expiry removed since the last call. It holds
// the engine lock so it does not race a concurrent access appending to the log.
func (e *Engine) takeExpired() []keyspace.ExpiredKey {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.ks.TakeExpired()
}

// snapshotAll copies every live key in every database into an rdb.Snapshot under
// the engine lock. The copy is taken in memory so the lock is held only for the
// scan, not for the disk write that follows: BGSAVE writes the returned snapshot
// from a background goroutine while new writes proceed.
func (e *Engine) snapshotAll() (rdb.Snapshot, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
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
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.ks.UsedMemory()
}

// sampleForEviction returns up to n eviction candidates, restricted to volatile
// keys when volatileOnly is set.
func (e *Engine) sampleForEviction(n int, volatileOnly bool) []keyspace.EvictionCandidate {
	e.mu.Lock()
	defer e.mu.Unlock()
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
// INFO's keyspace section reads it. The read takes the engine lock so it does
// not race a concurrent write.
func (e *Engine) dbSizes() []uint64 {
	e.mu.Lock()
	defer e.mu.Unlock()
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

// fileStats returns the pager counters for the file-growth INFO fields. It takes
// the engine lock so the read does not race a commit changing the page count.
func (e *Engine) fileStats() pager.Stats {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.ks.PagerStats()
}

// filePath returns the path of the .aki file backing the engine, empty for an
// in-memory backing.
func (e *Engine) filePath() string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.ks.PagerName()
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
