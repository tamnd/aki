package drivers

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/tamnd/aki/engine/obs1"
	"github.com/tamnd/aki/engine/obs1/shard"
	"github.com/tamnd/aki/engine/obs1/sim"
)

// gateChain holds every chain append until released, so a test can
// prove a reply arrived before any commit was possible.
type gateChain struct {
	inner obs1.ChainWriter
	gate  chan struct{}
}

func (g *gateChain) Append(ctx context.Context, recs []obs1.ChainRecord) (obs1.ChainPos, error) {
	<-g.gate
	return g.inner.Append(ctx, recs)
}

// clusterMapKey is the production route: cluster hash slot to slot
// group, the same mapping dispatch uses.
func clusterMapKey(key []byte) (uint16, uint16) {
	slot := shard.HashSlot(key)
	return uint16(slot), uint16(shard.GroupOfSlot(slot, shard.DefaultSlotGroups))
}

// startLoggedServer is startServer with the durability pipeline wired:
// a write log over the sim store and a chain appender, every group
// granted to this node at epoch 1, the INFO hook registered. With
// gated true the chain is blocked until the returned release is
// called, which the cleanup also does so Close can drain.
func startLoggedServer(t *testing.T, gated bool) (*obs1.WriteLog, *sim.Sim, net.Conn, *bufio.Reader, func()) {
	t.Helper()
	const node = uint64(0xE1)
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
	var chain obs1.ChainWriter = ap
	gate := make(chan struct{})
	if gated {
		chain = &gateChain{inner: ap, gate: gate}
	} else {
		close(gate)
	}
	var once sync.Once
	release := func() {
		if gated {
			once.Do(func() { close(gate) })
		}
	}
	wl, err := obs1.NewWriteLog(obs1.WriteLogConfig{
		Store: store, Prefix: "p", Node: node, Chain: chain, Fold: fold,
		Groups: shard.DefaultSlotGroups, MapKey: clusterMapKey,
		FlushAge: time.Hour,
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
	go srv.Serve()
	nc, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		srv.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		nc.Close()
		srv.Close()
		release()
		if err := wl.Close(); err != nil {
			t.Errorf("write log close: %v", err)
		}
	})
	return wl, store, nc, br(nc), release
}

func br(nc net.Conn) *bufio.Reader { return bufio.NewReader(nc) }

// walFrames collects every flushed frame across all of the node's WAL
// objects, keyed nothing, in object then section then frame order.
func walFrames(t *testing.T, store *sim.Sim, node uint64) []obs1.WALFrame {
	t.Helper()
	var frames []obs1.WALFrame
	for walSeq := uint64(1); ; walSeq++ {
		body, _, err := store.Get(context.Background(), fmt.Sprintf("p/wal/%016x/%016d", node, walSeq))
		if err != nil {
			return frames
		}
		secs, _, err := obs1.ParseWAL(body)
		if err != nil {
			t.Fatalf("ParseWAL %d: %v", walSeq, err)
		}
		for _, sec := range secs {
			frames = append(frames, sec.Frames...)
		}
	}
}

// TestDurabilityRoundTrip drives the string write surface over the
// socket and checks the flushed frames carry post-decision effects:
// the INCR result as counter-laddered int text, the whole string after
// APPEND and SETRANGE, the deadline a KEEPTTL write carried, and a
// keydel only for a delete that removed something.
func TestDurabilityRoundTrip(t *testing.T) {
	wl, store, nc, r, _ := startLoggedServer(t, false)
	const node = uint64(0xE1)

	// Track the seq each emission takes on its group, in send order,
	// by replaying the production route client-side.
	seqs := map[uint16]uint64{}
	emit := func(key string) {
		_, g := clusterMapKey([]byte(key))
		seqs[g]++
	}

	send(t, nc, "SET", "k", "v")
	expect(t, r, "+OK\r\n")
	emit("k")
	send(t, nc, "SET", "pxk", "v2", "PX", "100000")
	expect(t, r, "+OK\r\n")
	emit("pxk")
	send(t, nc, "SET", "pxk", "v3", "KEEPTTL")
	expect(t, r, "+OK\r\n")
	emit("pxk")
	send(t, nc, "MSET", "a", "1", "b", "2")
	expect(t, r, "+OK\r\n")
	emit("a")
	emit("b")
	send(t, nc, "INCR", "ctr")
	expect(t, r, ":1\r\n")
	emit("ctr")
	send(t, nc, "INCRBY", "ctr", "41")
	expect(t, r, ":42\r\n")
	emit("ctr")
	send(t, nc, "APPEND", "s", "he")
	expect(t, r, ":2\r\n")
	emit("s")
	send(t, nc, "SETRANGE", "s", "1", "ello")
	expect(t, r, ":5\r\n")
	emit("s")
	send(t, nc, "INCRBYFLOAT", "f", "1.5")
	expect(t, r, "$3\r\n1.5\r\n")
	emit("f")
	send(t, nc, "DEL", "k")
	expect(t, r, ":1\r\n")
	emit("k")
	// A miss emits nothing: the frame records an effect, and there was
	// none.
	send(t, nc, "DEL", "nosuch")
	expect(t, r, ":0\r\n")

	wl.Barrier()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for g, last := range seqs {
		if err := wl.Marks().Wait(ctx, g, last); err != nil {
			t.Fatalf("Wait group %d seq %d: %v", g, last, err)
		}
	}

	byKey := map[string][]obs1.Op{}
	for _, f := range walFrames(t, store, node) {
		op, err := obs1.DecodeOp(f)
		if err != nil {
			t.Fatalf("DecodeOp seq %d: %v", f.Seq, err)
		}
		byKey[string(f.Key)] = append(byKey[string(f.Key)], op)
	}

	total := 0
	for _, ops := range byKey {
		total += len(ops)
	}
	if total != 11 {
		t.Fatalf("%d frames flushed, want 11: %v", total, byKey)
	}
	if ops := byKey["k"]; len(ops) != 2 {
		t.Fatalf("k ops = %v", ops)
	} else {
		if ss := ops[0].(obs1.StrSet); string(ss.Value) != "v" || ss.ExpiryMS != 0 || ss.Ladder != 0 {
			t.Fatalf("SET frame = %+v", ss)
		}
		if _, ok := ops[1].(obs1.KeyDel); !ok {
			t.Fatalf("DEL frame = %+v", ops[1])
		}
	}
	if ops := byKey["pxk"]; len(ops) != 2 {
		t.Fatalf("pxk ops = %v", ops)
	} else {
		first := ops[0].(obs1.StrSet)
		second := ops[1].(obs1.StrSet)
		if string(first.Value) != "v2" || first.ExpiryMS == 0 {
			t.Fatalf("PX frame = %+v, want an absolute deadline", first)
		}
		if string(second.Value) != "v3" || second.ExpiryMS != first.ExpiryMS {
			t.Fatalf("KEEPTTL frame = %+v, want the carried deadline %d", second, first.ExpiryMS)
		}
	}
	if ss := byKey["a"][0].(obs1.StrSet); string(ss.Value) != "1" {
		t.Fatalf("MSET a frame = %+v", ss)
	}
	if ss := byKey["b"][0].(obs1.StrSet); string(ss.Value) != "2" {
		t.Fatalf("MSET b frame = %+v", ss)
	}
	if ops := byKey["ctr"]; len(ops) != 2 {
		t.Fatalf("ctr ops = %v", ops)
	} else {
		first := ops[0].(obs1.StrSet)
		second := ops[1].(obs1.StrSet)
		if string(first.Value) != "1" || first.Ladder != obs1.LadderCounter {
			t.Fatalf("INCR frame = %+v", first)
		}
		if string(second.Value) != "42" || second.Ladder != obs1.LadderCounter {
			t.Fatalf("INCRBY frame = %+v", second)
		}
	}
	if ops := byKey["s"]; len(ops) != 2 {
		t.Fatalf("s ops = %v", ops)
	} else {
		if ss := ops[0].(obs1.StrSet); string(ss.Value) != "he" {
			t.Fatalf("APPEND frame = %+v", ss)
		}
		if ss := ops[1].(obs1.StrSet); string(ss.Value) != "hello" || ss.Ladder != 0 {
			t.Fatalf("SETRANGE frame = %+v, want the whole resulting string", ss)
		}
	}
	if ss := byKey["f"][0].(obs1.StrSet); string(ss.Value) != "1.5" || ss.Ladder != 0 {
		t.Fatalf("INCRBYFLOAT frame = %+v", ss)
	}
	if _, ok := byKey["nosuch"]; ok {
		t.Fatal("a no-effect DEL flushed a frame")
	}

	stats := readInfo(t, nc, r)
	for _, name := range []string{"wal_flushes", "wal_barrier_flushes", "wal_flushed_bytes", "chain_commit_batches", "chain_commit_records"} {
		if stats[name] == 0 {
			t.Fatalf("INFO %s = 0 after a barrier flush (%v)", name, stats)
		}
	}
	for _, name := range []string{"wal_encode_errors", "wal_stall_errors", "wal_fatal_errors", "wal_epoch_errors"} {
		if v, ok := stats[name]; !ok || v != 0 {
			t.Fatalf("INFO %s = %d ok=%v on the clean path", name, v, ok)
		}
	}
}

// TestDurabilityRelaxedAck proves the ack point: with the chain gated
// so no commit can happen, and the flusher idle so no flush has
// happened either, SET still answers OK, because the relaxed contract
// acks on the frame reaching the group buffer.
func TestDurabilityRelaxedAck(t *testing.T) {
	wl, _, nc, r, release := startLoggedServer(t, true)

	send(t, nc, "SET", "k", "v")
	expect(t, r, "+OK\r\n")

	_, g := clusterMapKey([]byte("k"))
	if got := wl.Marks().Committed(g); got != 0 {
		t.Fatalf("Committed = %d at ack time with the chain gated", got)
	}
	if stats := readInfo(t, nc, r); stats["wal_flushes"] != 0 {
		t.Fatalf("wal_flushes = %d at ack time, the ack must not wait for a flush", stats["wal_flushes"])
	}

	wl.Barrier()
	release()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := wl.Marks().Wait(ctx, g, 1); err != nil {
		t.Fatalf("Wait after release: %v", err)
	}
}
