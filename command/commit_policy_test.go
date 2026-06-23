package command

import (
	"testing"
	"time"

	"github.com/tamnd/aki/keyspace"
	"github.com/tamnd/aki/pager"
	"github.com/tamnd/aki/vfs"
)

// reopenCount writes through dispatcher d over fs/name, runs after to take the
// durability action under test, closes the pager, reopens it, and returns the
// number of keys in DB 0. It is the shared body of the policy round-trip tests.
func reopenCount(t *testing.T, name string, write func(*Engine) error, after func(*Dispatcher), policy commitPolicy) int {
	t.Helper()
	fs := vfs.NewMem()
	p, err := pager.Create(fs, name, pager.Options{PageSize: 4096, DBCount: 16})
	if err != nil {
		t.Fatalf("create pager: %v", err)
	}
	ks, err := keyspace.Open(p)
	if err != nil {
		t.Fatalf("open keyspace: %v", err)
	}
	d := New(Config{Databases: 16, Engine: NewEngine(ks)})
	d.engine.policy.Store(int32(policy))

	if err := write(d.engine); err != nil {
		t.Fatalf("write: %v", err)
	}
	if after != nil {
		after(d)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	p2, err := pager.Open(fs, name, pager.Options{})
	if err != nil {
		t.Fatalf("reopen pager: %v", err)
	}
	defer func() { _ = p2.Close() }()
	ks2, err := keyspace.Open(p2)
	if err != nil {
		t.Fatalf("reopen keyspace: %v", err)
	}
	db, err := ks2.DB(0)
	if err != nil {
		t.Fatalf("db: %v", err)
	}
	return int(db.Len())
}

func putKey(e *Engine, key string) error {
	return e.update(0, func(db *keyspace.DB) error {
		return db.Set([]byte(key), []byte("v"), keyspace.TypeString, keyspace.EncRaw, -1)
	})
}

// TestCommitAlwaysIsDurableWithoutShutdown proves the always policy lands a write
// on disk the moment it returns, with no flush step before the file closes.
func TestCommitAlwaysIsDurableWithoutShutdown(t *testing.T) {
	got := reopenCount(t, "always.aki", func(e *Engine) error {
		return putKey(e, "k1")
	}, nil, commitAlways)
	if got != 1 {
		t.Fatalf("key count after reopen = %d want 1 (always must be durable on return)", got)
	}
}

// TestCommitEverySecLosesWithoutFlush proves the everysec policy defers: a write
// followed by an abrupt close (no cron tick, no shutdown) is not on disk yet.
func TestCommitEverySecLosesWithoutFlush(t *testing.T) {
	got := reopenCount(t, "everysec-lose.aki", func(e *Engine) error {
		return putKey(e, "k1")
	}, nil, commitEverySec)
	if got != 0 {
		t.Fatalf("key count after abrupt close = %d want 0 (everysec defers the commit)", got)
	}
}

// TestCommitEverySecCronFlush proves the cron flush makes a deferred write
// durable once the one-second interval has elapsed.
func TestCommitEverySecCronFlush(t *testing.T) {
	got := reopenCount(t, "everysec-cron.aki", func(e *Engine) error {
		return putKey(e, "k1")
	}, func(d *Dispatcher) {
		// Backdate the last commit so the interval gate opens, then run the cron.
		d.engine.mu.Lock()
		d.engine.lastCommit = time.Now().Add(-2 * time.Second)
		d.engine.mu.Unlock()
		d.runCommitCron()
	}, commitEverySec)
	if got != 1 {
		t.Fatalf("key count after cron flush = %d want 1", got)
	}
}

// TestCommitEverySecShutdownFlush proves a clean shutdown flushes deferred work,
// so no acknowledged write is lost on an orderly stop.
func TestCommitEverySecShutdownFlush(t *testing.T) {
	got := reopenCount(t, "everysec-shutdown.aki", func(e *Engine) error {
		return putKey(e, "k1")
	}, func(d *Dispatcher) {
		d.StopBackground()
	}, commitEverySec)
	if got != 1 {
		t.Fatalf("key count after shutdown = %d want 1", got)
	}
}

// TestCommitNoIgnoresCron proves the no policy does not flush on the timer: only
// SAVE, shutdown, or the dirty-page bound commits it.
func TestCommitNoIgnoresCron(t *testing.T) {
	got := reopenCount(t, "no-cron.aki", func(e *Engine) error {
		return putKey(e, "k1")
	}, func(d *Dispatcher) {
		d.engine.mu.Lock()
		d.engine.lastCommit = time.Now().Add(-10 * time.Second)
		d.engine.mu.Unlock()
		d.runCommitCron()
	}, commitNo)
	if got != 0 {
		t.Fatalf("key count after cron under no policy = %d want 0", got)
	}
}

// TestCommitNoForceCommit proves ForceCommit (the SAVE and shutdown path) flushes
// even under the no policy.
func TestCommitNoForceCommit(t *testing.T) {
	got := reopenCount(t, "no-force.aki", func(e *Engine) error {
		return putKey(e, "k1")
	}, func(d *Dispatcher) {
		if err := d.engine.ForceCommit(); err != nil {
			t.Fatalf("force commit: %v", err)
		}
	}, commitNo)
	if got != 1 {
		t.Fatalf("key count after force commit = %d want 1", got)
	}
}

// TestSetCommitPolicyTightenFlushes proves switching to always flushes whatever a
// looser policy left pending, so the stronger contract holds at once.
func TestSetCommitPolicyTightenFlushes(t *testing.T) {
	got := reopenCount(t, "tighten.aki", func(e *Engine) error {
		return putKey(e, "k1")
	}, func(d *Dispatcher) {
		if err := d.engine.setCommitPolicy(commitAlways); err != nil {
			t.Fatalf("tighten: %v", err)
		}
	}, commitEverySec)
	if got != 1 {
		t.Fatalf("key count after tighten to always = %d want 1", got)
	}
}

// TestDirtyPageBoundForcesCommit proves a deferred policy still checkpoints when
// the dirty-page bound is crossed, so a write burst cannot pin memory without end.
func TestDirtyPageBoundForcesCommit(t *testing.T) {
	fs := vfs.NewMem()
	p, err := pager.Create(fs, "bound.aki", pager.Options{PageSize: 4096, DBCount: 16})
	if err != nil {
		t.Fatalf("create pager: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	ks, err := keyspace.Open(p)
	if err != nil {
		t.Fatalf("open keyspace: %v", err)
	}
	d := New(Config{Databases: 16, Engine: NewEngine(ks)})
	d.engine.policy.Store(int32(commitNo))
	// A tiny bound so a handful of keys trips it without writing thousands.
	d.engine.mu.Lock()
	d.engine.dirtyPageLimit = 1
	d.engine.mu.Unlock()

	// Exactly one stride of writes, so the sampler fires on the last write and the
	// trip leaves no pending work behind.
	for i := 0; i < dirtyCheckStride; i++ {
		if err := putKey(d.engine, "k"+itoa(int64(i))); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	d.engine.mu.Lock()
	pending := d.engine.pendingDirty
	d.engine.mu.Unlock()
	if pending {
		t.Fatal("dirty-page bound did not force a checkpoint under the no policy")
	}
}
