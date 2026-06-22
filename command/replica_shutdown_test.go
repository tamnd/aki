package command

import (
	"testing"
	"time"

	"github.com/tamnd/aki/keyspace"
	"github.com/tamnd/aki/networking"
	"github.com/tamnd/aki/pager"
	"github.com/tamnd/aki/vfs"
)

// newReplicaDispatcher builds a data-backed dispatcher and its pager without a
// network server, so a test can drive replication lifecycle directly.
func newReplicaDispatcher(t *testing.T) (*Dispatcher, *pager.Pager) {
	t.Helper()
	fs := vfs.NewMem()
	p, err := pager.Create(fs, "data.aki", pager.Options{PageSize: 4096, DBCount: 16})
	if err != nil {
		t.Fatalf("create pager: %v", err)
	}
	ks, err := keyspace.Open(p)
	if err != nil {
		t.Fatalf("open keyspace: %v", err)
	}
	return New(Config{Engine: NewEngine(ks)}), p
}

// TestStopReplicationJoinsLoop points a replica at a dead address so its client
// loop keeps retrying, then checks StopReplication returns once the goroutine has
// exited. The pager must still be open when StopReplication returns, so a close
// after it is safe.
func TestStopReplicationJoinsLoop(t *testing.T) {
	d, p := newReplicaDispatcher(t)

	ctx := &Ctx{Conn: networking.NewOfflineConn()}
	// 127.0.0.1:1 refuses connections, so the loop sits in its retry cycle.
	ctx.Argv = [][]byte{[]byte("REPLICAOF"), []byte("127.0.0.1"), []byte("1")}
	d.handleReplicaOf(ctx)

	done := make(chan struct{})
	go func() {
		d.StopReplication()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("StopReplication did not return")
	}

	// The goroutine is joined, so closing the pager cannot race the apply loop.
	if err := p.Close(); err != nil {
		t.Fatalf("pager close: %v", err)
	}
}

// TestStopReplicationIdempotent checks StopReplication is safe on a master with
// no link and safe to call more than once.
func TestStopReplicationIdempotent(t *testing.T) {
	d, p := newReplicaDispatcher(t)
	defer func() { _ = p.Close() }()

	d.StopReplication()
	d.StopReplication()
}
