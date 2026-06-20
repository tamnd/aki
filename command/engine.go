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
