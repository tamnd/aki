package command

import "testing"

// aofDispatcherForFsync builds a dispatcher with appendonly on and the AOF files
// initialized so appendAOF has an open incr file to write into.
func aofDispatcherForFsync(t *testing.T) *Dispatcher {
	t.Helper()
	d := newMetricsDispatcher(t)
	if err := d.SetConfig("dir", t.TempDir()); err != nil {
		t.Fatalf("set dir: %v", err)
	}
	if err := d.SetConfig("appendonly", "yes"); err != nil {
		t.Fatalf("set appendonly: %v", err)
	}
	d.initAOF()
	return d
}

func writeAOF(d *Dispatcher) {
	d.appendAOF(0, [][]byte{[]byte("SET"), []byte("k"), []byte("v")})
}

// TestAppendFsyncAlways checks the always policy fsyncs the incr file inline so a
// write leaves nothing pending.
func TestAppendFsyncAlways(t *testing.T) {
	d := aofDispatcherForFsync(t)
	if err := d.SetConfig("appendfsync", "always"); err != nil {
		t.Fatalf("set appendfsync: %v", err)
	}

	writeAOF(d)

	d.aof.mu.Lock()
	pending := d.aof.pendingSync
	zero := d.aof.lastSync.IsZero()
	d.aof.mu.Unlock()
	if pending {
		t.Fatal("always policy left the write pending, want it synced inline")
	}
	if zero {
		t.Fatal("always policy did not record a sync time")
	}
}

// TestAppendFsyncEverysec checks the everysec policy leaves a write pending until
// syncAOFCron runs, and that a second sync inside the same second is throttled.
func TestAppendFsyncEverysec(t *testing.T) {
	d := aofDispatcherForFsync(t)
	if err := d.SetConfig("appendfsync", "everysec"); err != nil {
		t.Fatalf("set appendfsync: %v", err)
	}

	writeAOF(d)
	d.aof.mu.Lock()
	pending := d.aof.pendingSync
	d.aof.mu.Unlock()
	if !pending {
		t.Fatal("everysec policy did not mark the write pending")
	}

	// The first cron pass syncs because no sync has happened yet.
	d.syncAOFCron()
	d.aof.mu.Lock()
	pending = d.aof.pendingSync
	d.aof.mu.Unlock()
	if pending {
		t.Fatal("syncAOFCron did not fsync the first pending write")
	}

	// A second write inside the same second stays pending: the throttle holds the
	// fsync to about once per second.
	writeAOF(d)
	d.syncAOFCron()
	d.aof.mu.Lock()
	pending = d.aof.pendingSync
	d.aof.mu.Unlock()
	if !pending {
		t.Fatal("syncAOFCron synced again within one second, want it throttled")
	}
}

// TestAppendFsyncNo checks the no policy never fsyncs from the cron, leaving the
// write pending for the OS to flush.
func TestAppendFsyncNo(t *testing.T) {
	d := aofDispatcherForFsync(t)
	if err := d.SetConfig("appendfsync", "no"); err != nil {
		t.Fatalf("set appendfsync: %v", err)
	}

	writeAOF(d)
	d.syncAOFCron()

	d.aof.mu.Lock()
	pending := d.aof.pendingSync
	d.aof.mu.Unlock()
	if !pending {
		t.Fatal("no policy fsynced from the cron, want it left to the OS")
	}
}

// TestAppendFsyncNoSyncOnRewrite checks that no-appendfsync-on-rewrite holds off
// the inline always fsync while a rewrite is in progress.
func TestAppendFsyncNoSyncOnRewrite(t *testing.T) {
	d := aofDispatcherForFsync(t)
	if err := d.SetConfig("appendfsync", "always"); err != nil {
		t.Fatalf("set appendfsync: %v", err)
	}
	if err := d.SetConfig("no-appendfsync-on-rewrite", "yes"); err != nil {
		t.Fatalf("set no-appendfsync-on-rewrite: %v", err)
	}

	d.aof.mu.Lock()
	d.aof.rewriteInProgress = true
	d.aof.mu.Unlock()

	writeAOF(d)

	d.aof.mu.Lock()
	pending := d.aof.pendingSync
	d.aof.mu.Unlock()
	if !pending {
		t.Fatal("always fsync ran during a rewrite with no-appendfsync-on-rewrite set")
	}
}
