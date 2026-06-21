package command

import (
	"sync"

	"github.com/tamnd/aki/keyspace"
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
	return true
}
