package command

import (
	"sync"

	"github.com/tamnd/aki/keyspace"
	"github.com/tamnd/aki/rdb"
)

// Engine is the command layer's handle on the keyspace. It serializes every
// access with a single mutex, which is the one-writer assumption the keyspace
// makes at this milestone. The sharded writer model from doc 05 §7 replaces this
// global lock in a later slice.
type Engine struct {
	mu sync.Mutex
	ks *keyspace.Keyspace
}

// NewEngine wraps a keyspace for use by the dispatcher.
func NewEngine(ks *keyspace.Keyspace) *Engine { return &Engine{ks: ks} }

// update runs fn against database index under the engine lock and, on success,
// commits the change so it is durable. A write command goes through here.
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
	return e.ks.Commit()
}

// updateKeyspace runs fn with access to every database under the engine lock and
// commits on success. Cross-database writes like MOVE and COPY go through here.
func (e *Engine) updateKeyspace(fn func(*keyspace.Keyspace) error) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := fn(e.ks); err != nil {
		return err
	}
	return e.ks.Commit()
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
	return e.ks.Commit()
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
	snap := rdb.Snapshot{}
	for i := range e.ks.DBCount() {
		db, err := e.ks.DB(i)
		if err != nil {
			return rdb.Snapshot{}, err
		}
		entries, err := reloadEntries(db)
		if err != nil {
			return rdb.Snapshot{}, err
		}
		if len(entries) > 0 {
			snap.DBs = append(snap.DBs, rdb.DBData{Index: i, Entries: entries})
		}
	}
	return snap, nil
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
	return true, e.ks.Commit()
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
	}
}
