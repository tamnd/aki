package drivers

import (
	"bufio"
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/tamnd/aki/engine/obs1"
	"github.com/tamnd/aki/engine/obs1/shard"
	"github.com/tamnd/aki/engine/obs1/sim"
)

// startLaggedServer is startLoggedServer with the flush knobs pinned so
// one oversized SET raises the flush lag and nothing brings it down on
// its own: the cap is tiny, the size trigger sits far above it, and the
// age trigger sits at an hour. The release lever is wl.SetFlushAge,
// which lets the age flush fire, advance FlushCount, and clear the
// flag. The chain is never gated because flushes must succeed the
// moment the age allows them.
func startLaggedServer(t *testing.T) (*obs1.WriteLog, net.Conn, *bufio.Reader, func() (net.Conn, *bufio.Reader)) {
	t.Helper()
	const node = uint64(0xE2)
	store := sim.New(sim.Config{})
	fold := obs1.NewLeaseFold()
	ap, err := obs1.NewChainAppender(store, "p", 0, node, 1, obs1.ChainPos{}, fold)
	if err != nil {
		t.Fatal(err)
	}
	recs := make([]obs1.ChainRecord, 0, shard.DefaultSlotGroups)
	for g := range shard.DefaultSlotGroups {
		recs = append(recs, obs1.GrantRecord{Group: uint16(g), Node: node, Epoch: 1})
	}
	if _, err := ap.Append(context.Background(), recs); err != nil {
		t.Fatal(err)
	}
	wl, err := obs1.NewWriteLog(obs1.WriteLogConfig{
		Store: store, Prefix: "p", Node: node, Chain: ap, Fold: fold,
		Groups: shard.DefaultSlotGroups, MapKey: clusterMapKey,
		FlushSize: 1 << 20, FlushAge: time.Hour, CapBytes: 64,
	})
	if err != nil {
		t.Fatal(err)
	}
	srv, err := Listen(Options{
		Addr: "127.0.0.1:0", Shards: 2, ArenaBytes: 4 << 20, SegBytes: 1 << 18,
		ConnShape: testConnShape(), NetDriver: testNetDriver(),
		WriteLog: wl, WALInfo: wl.AppendInfo,
	})
	if err != nil {
		t.Fatal(err)
	}
	for g := range shard.DefaultSlotGroups {
		wl.SetGroup(uint16(g), 1, 1)
	}
	// Registered before any dial so the LIFO cleanup order closes every
	// connection first: Close waits for the read loops to drain.
	t.Cleanup(func() {
		srv.Close()
		// The hour age would strand the dirty bytes; let Close drain.
		wl.SetFlushAge(time.Millisecond)
		if err := wl.Close(); err != nil {
			t.Errorf("write log close: %v", err)
		}
	})
	go srv.Serve()
	dial := func() (net.Conn, *bufio.Reader) {
		c, err := net.Dial("tcp", srv.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { c.Close() })
		return c, br(c)
	}
	nc, r := dial()
	return wl, nc, r, dial
}

// TestFlushlagParksWritesEndToEnd drives the flushlag gate through the
// whole server: an oversized SET leaves the WAL buffer over cap, the
// next write parks before its handler runs and its round stays silent,
// reads and INFO keep flowing on another connection with the flushlag
// wait counted, and retuning the flush age completes a flush that
// clears the lag and delivers the held reply.
func TestFlushlagParksWritesEndToEnd(t *testing.T) {
	wl, nc, r, dial := startLaggedServer(t)

	big := strings.Repeat("x", 512)
	send(t, nc, "SET", "big", big)
	expect(t, r, "+OK\r\n")
	if !wl.FlushLagged() {
		t.Fatal("lag flag down after an over-cap append")
	}

	// The next write parks pre-execution: no reply while the lag holds.
	send(t, nc, "SET", "held", "v")
	if err := nc.SetReadDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Peek(1); err == nil {
		t.Fatal("write replied while the flush lagged")
	}
	if err := nc.SetReadDeadline(time.Time{}); err != nil {
		t.Fatal(err)
	}

	// Reads and INFO flow on a second connection while the write is
	// parked, and the park registered under the flushlag reason.
	nc2, r2 := dial()
	send(t, nc2, "GET", "big")
	expectBulk(t, r2, []byte(big))
	info := readInfo(t, nc2, r2)
	if info["backpressure_waits_flushlag"] == 0 {
		t.Fatal("backpressure_waits_flushlag = 0 with a write parked on the lag")
	}
	if info["wal_stall_errors"] != 0 {
		t.Fatalf("wal_stall_errors = %d, want 0: the cap is not an error anymore", info["wal_stall_errors"])
	}

	// Release: the age flush fires, the lag clears, and the worker's
	// retry runs the held write for the first time.
	wl.SetFlushAge(time.Millisecond)
	expect(t, r, "+OK\r\n")
	if wl.FlushLagged() {
		t.Fatal("lag flag still up after the flush completed")
	}
	if wl.FlushCount() == 0 {
		t.Fatal("FlushCount = 0 after the release flush")
	}
	send(t, nc, "GET", "held")
	expectBulk(t, r, []byte("v"))
}
