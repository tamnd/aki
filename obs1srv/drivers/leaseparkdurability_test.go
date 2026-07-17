package drivers

import (
	"bufio"
	"context"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/tamnd/aki/engine/obs1"
	"github.com/tamnd/aki/engine/obs1/shard"
	"github.com/tamnd/aki/engine/obs1/sim"
)

// startLeasedServer is startLoggedServer with the lease gate wired on both
// ends: WriteLogConfig.Gate so committed appends renew groups, and
// Options.LeaseView so the workers consult the gate before write handlers.
// Every group is granted on the chain and renewed at boot, so writes flow
// until a test backdates or demotes a group; the flush age sits at an hour
// so the only appends are the ones a test releases with SetFlushAge, which
// makes the renewal moment deterministic.
func startLeasedServer(t *testing.T) (*obs1.WriteLog, *obs1.LeaseGate, net.Conn, *bufio.Reader, func() (net.Conn, *bufio.Reader)) {
	t.Helper()
	const node = uint64(0xE3)
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
	gate := obs1.NewLeaseGate(time.Hour, time.Minute)
	wl, err := obs1.NewWriteLog(obs1.WriteLogConfig{
		Store: store, Prefix: "p", Node: node, Chain: ap, Fold: fold,
		Groups: shard.DefaultSlotGroups, MapKey: ClusterMapKey,
		FlushAge: time.Hour, Gate: gate,
	})
	if err != nil {
		t.Fatal(err)
	}
	srv, err := Listen(Options{
		Addr: "127.0.0.1:0", Shards: 2, ArenaBytes: 4 << 20, SegBytes: 1 << 18,
		ConnShape: testConnShape(), NetDriver: testNetDriver(),
		WriteLog: wl, WALInfo: wl.AppendInfo, LeaseView: gate,
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	for g := range shard.DefaultSlotGroups {
		wl.SetGroup(uint16(g), 1, 1)
		gate.Renewed(uint16(g), now)
	}
	// Registered before any dial so the LIFO cleanup order closes every
	// connection first: Close waits for the read loops to drain.
	t.Cleanup(func() {
		srv.Close()
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
	return wl, gate, nc, r, dial
}

// TestLeaseParkAndRenewEndToEnd drives the suspend-park-renew cycle over
// the socket: a write dirties its group, the group's lease belief is
// backdated past the TTL so the gate suspends it, the next write parks
// round-silent while reads and INFO flow with the lease wait counted, and
// releasing the flush lets the dirty frame commit, which renews exactly
// that group through the committer hook and delivers the held reply. The
// progress signal literally is the node's own chain append.
func TestLeaseParkAndRenewEndToEnd(t *testing.T) {
	wl, gate, nc, r, dial := startLeasedServer(t)
	_, group := ClusterMapKey([]byte("k1"))

	// The first write flows (every group renewed at boot) and leaves one
	// dirty frame in group k1's WAL buffer, the fuel for the release.
	send(t, nc, "SET", "k1", "a")
	expect(t, r, "+OK\r\n")

	// Run the group's belief down: renewed two hours ago, TTL one hour,
	// so the gate suspends it and the next write parks before its handler.
	gate.Renewed(group, time.Now().Add(-2*time.Hour))
	send(t, nc, "SET", "k1", "b")
	if err := nc.SetReadDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Peek(1); err == nil {
		t.Fatal("write replied while its group was suspended")
	}
	if err := nc.SetReadDeadline(time.Time{}); err != nil {
		t.Fatal(err)
	}

	// Reads keep serving the suspended group on a second connection, and
	// the park registered under the lease reason.
	nc2, r2 := dial()
	send(t, nc2, "GET", "k1")
	expectBulk(t, r2, []byte("a"))
	info := readInfo(t, nc2, r2)
	if info["backpressure_waits_lease"] == 0 {
		t.Fatal("backpressure_waits_lease = 0 with a write parked on a suspended group")
	}

	// Release: the age flush fires, the commit lands, the committer hook
	// renews the group the batch carried, and the worker's retry runs the
	// held write for the first time.
	before := gate.Renewals()
	wl.SetFlushAge(time.Millisecond)
	expect(t, r, "+OK\r\n")
	if gate.Renewals() <= before {
		t.Fatal("the releasing commit did not move the renewal count")
	}
	if gate.Suspended(group, time.Now().UnixMilli()) {
		t.Fatal("group still suspended after its own append committed")
	}
	send(t, nc, "GET", "k1")
	expectBulk(t, r, []byte("b"))
}

// TestLeaseDemotedRedirectsEndToEnd pins the doc 07 redirect over the
// socket: a write for a demoted group takes MOVED with the key's cluster
// slot and the taker's endpoint, immediately and without parking, while a
// key in a still-held group keeps writing.
func TestLeaseDemotedRedirectsEndToEnd(t *testing.T) {
	_, gate, nc, r, _ := startLeasedServer(t)
	slot, group := ClusterMapKey([]byte("k2"))
	gate.Demote(group, "10.0.0.9:7000")

	send(t, nc, "SET", "k2", "x")
	expect(t, r, "-MOVED "+strconv.Itoa(int(slot))+" 10.0.0.9:7000\r\n")

	// A key in another group still writes; find one mapping elsewhere.
	other := "k1"
	if _, g := ClusterMapKey([]byte(other)); g == group {
		t.Fatalf("fixture keys k1 and k2 share group %d", g)
	}
	send(t, nc, "SET", other, "y")
	expect(t, r, "+OK\r\n")
}
