package command

import (
	"testing"

	"github.com/tamnd/aki/networking"
)

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

// TestAppendFsyncAlwaysBatchSyncsOnBatchComplete checks that an online pipeline
// under the always policy buffers its records per session and that OnBatchComplete
// group-commits the whole batch with one fsync. serve() runs OnBatchComplete before
// it flushes the replies, so this is what keeps the batch durable before its replies
// leave the socket while paying one fsync instead of one per command.
func TestAppendFsyncAlwaysBatchSyncsOnBatchComplete(t *testing.T) {
	d := aofDispatcherForFsync(t)
	if err := d.SetConfig("appendfsync", "always"); err != nil {
		t.Fatalf("set appendfsync: %v", err)
	}

	conn := networking.NewOfflineConn()
	sess := &session{authenticated: true, aofBufDB: -1}
	conn.SetSession(sess)

	// Two writes buffered into the session, the way an online pipeline batches them.
	// Buffering alone must not sync: the records sit in the session buffer until the
	// batch completes.
	d.bufferAOFRecord(sess, 0, [][]byte{[]byte("SET"), []byte("a"), []byte("1")})
	d.bufferAOFRecord(sess, 0, [][]byte{[]byte("SET"), []byte("b"), []byte("2")})

	d.aof.mu.Lock()
	syncedBefore := d.aof.syncedSeq
	bufferedBytes := len(sess.aofBuf)
	d.aof.mu.Unlock()
	if bufferedBytes == 0 {
		t.Fatal("records were not buffered into the session")
	}

	d.OnBatchComplete(conn)

	d.aof.mu.Lock()
	pending := d.aof.pendingSync
	syncedThrough := d.aof.syncedSeq >= d.aof.writeSeq && d.aof.writeSeq > syncedBefore
	leftover := len(sess.aofBuf)
	d.aof.mu.Unlock()
	if pending {
		t.Fatal("always policy left the batch pending after OnBatchComplete")
	}
	if !syncedThrough {
		t.Fatal("OnBatchComplete did not fsync the batch through under always")
	}
	if leftover != 0 {
		t.Fatalf("session buffer not spliced, %d bytes left", leftover)
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
