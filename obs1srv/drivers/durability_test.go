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

// TestDurabilityStrictAckCoversCommit proves the strict contract's
// positive half with no gating: once the reply is on the wire, the
// group's committed watermark covers the write's frame. The harness runs
// with a one-hour flush age, so the reply arriving at all also proves the
// pending strict ack demanded the barrier flush; without that demand the
// SET would sit in the buffer for the hour and the test would time out.
func TestDurabilityStrictAckCoversCommit(t *testing.T) {
	wl, _, nc, r, _ := startLoggedServer(t, false)

	send(t, nc, "AKI.DURABILITY", "strict")
	expect(t, r, "+OK\r\n")
	send(t, nc, "SET", "k", "v")
	expect(t, r, "+OK\r\n")
	_, g := clusterMapKey([]byte("k"))
	if got := wl.Marks().Committed(g); got < 1 {
		t.Fatalf("Committed = %d at strict ack time, the ack must cover the commit", got)
	}
	// The keydel frame rides the same contract.
	send(t, nc, "DEL", "k")
	expect(t, r, ":1\r\n")
	if got := wl.Marks().Committed(g); got < 2 {
		t.Fatalf("Committed = %d after the strict DEL ack", got)
	}
	// A write that emits nothing has nothing to wait for, strict or not.
	send(t, nc, "DEL", "nosuch")
	expect(t, r, ":0\r\n")
}

// TestDurabilityStrictHoldsPipeline gates the chain and proves the
// negative half over the socket: the strict write executes (a second
// connection reads its value) but its reply and everything pipelined
// behind it stay unanswered until the commit lands, then arrive in order.
func TestDurabilityStrictHoldsPipeline(t *testing.T) {
	wl, _, nc, r, release := startLoggedServer(t, true)

	send(t, nc, "AKI.DURABILITY")
	expectBulk(t, r, []byte("relaxed"))
	send(t, nc, "AKI.DURABILITY", "strict")
	expect(t, r, "+OK\r\n")
	send(t, nc, "AKI.DURABILITY")
	expectBulk(t, r, []byte("strict"))

	send(t, nc, "SET", "k", "v")
	send(t, nc, "GET", "k")

	// A second, relaxed connection watches from outside the held pipeline:
	// the strict ack's barrier demand flushes the frame (the age trigger is
	// an hour away), and the write is in RAM already, only its output waits.
	nc2, err := net.Dial("tcp", nc.RemoteAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer nc2.Close()
	r2 := br(nc2)
	deadline := time.Now().Add(10 * time.Second)
	for readInfo(t, nc2, r2)["wal_barrier_flushes"] == 0 {
		if time.Now().After(deadline) {
			t.Fatal("no barrier flush while a strict ack was pending")
		}
		time.Sleep(5 * time.Millisecond)
	}
	send(t, nc2, "GET", "k")
	expectBulk(t, r2, []byte("v"))
	if _, g := clusterMapKey([]byte("k")); wl.Marks().Committed(g) != 0 {
		t.Fatal("the gated chain committed")
	}
	if err := nc.SetReadDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Peek(1); err == nil {
		t.Fatal("a strict reply arrived with the chain gated")
	}
	if err := nc.SetReadDeadline(time.Time{}); err != nil {
		t.Fatal(err)
	}

	release()
	expect(t, r, "+OK\r\n")
	expectBulk(t, r, []byte("v"))
}

// TestDurabilityStrictFanCoversCommit proves the fan half's positive
// contract with no gating: a multi-key write's single reply arrives only
// once every touched group's committed watermark covers its frames. Same
// one-hour flush age as the point test, so the replies arriving at all
// prove the held gather demanded the barrier flush.
func TestDurabilityStrictFanCoversCommit(t *testing.T) {
	wl, _, nc, r, _ := startLoggedServer(t, false)

	send(t, nc, "AKI.DURABILITY", "strict")
	expect(t, r, "+OK\r\n")

	seqs := map[uint16]uint64{}
	emit := func(key string) {
		_, g := clusterMapKey([]byte(key))
		seqs[g]++
	}
	send(t, nc, "MSET", "a", "1", "b", "2")
	expect(t, r, "+OK\r\n")
	emit("a")
	emit("b")
	for g, want := range seqs {
		if got := wl.Marks().Committed(g); got < want {
			t.Fatalf("Committed group %d = %d at strict MSET ack time, want at least %d", g, got, want)
		}
	}
	// The fan keydel frames ride the same contract.
	send(t, nc, "DEL", "a", "b")
	expect(t, r, ":2\r\n")
	emit("a")
	emit("b")
	for g, want := range seqs {
		if got := wl.Marks().Committed(g); got < want {
			t.Fatalf("Committed group %d = %d after the strict DEL ack, want at least %d", g, got, want)
		}
	}
	// A fan write with no effect emits nothing and has nothing to wait for.
	send(t, nc, "DEL", "nosuch", "nosuch2")
	expect(t, r, ":0\r\n")
}

// TestDurabilityStrictFanHoldsPipeline gates the chain and proves the fan
// half's negative contract over the socket: the MSET executes on every
// shard (a second connection reads both values) but the gathered reply and
// everything pipelined behind it stay unanswered until the commit lands,
// then arrive in order.
func TestDurabilityStrictFanHoldsPipeline(t *testing.T) {
	wl, _, nc, r, release := startLoggedServer(t, true)

	send(t, nc, "AKI.DURABILITY", "strict")
	expect(t, r, "+OK\r\n")
	send(t, nc, "MSET", "a", "1", "b", "2")
	send(t, nc, "GET", "a")

	nc2, err := net.Dial("tcp", nc.RemoteAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer nc2.Close()
	r2 := br(nc2)
	deadline := time.Now().Add(10 * time.Second)
	for readInfo(t, nc2, r2)["wal_barrier_flushes"] == 0 {
		if time.Now().After(deadline) {
			t.Fatal("no barrier flush while a strict fan ack was pending")
		}
		time.Sleep(5 * time.Millisecond)
	}
	send(t, nc2, "GET", "a")
	expectBulk(t, r2, []byte("1"))
	send(t, nc2, "GET", "b")
	expectBulk(t, r2, []byte("2"))
	if _, g := clusterMapKey([]byte("a")); wl.Marks().Committed(g) != 0 {
		t.Fatal("the gated chain committed")
	}
	if err := nc.SetReadDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Peek(1); err == nil {
		t.Fatal("a strict fan reply arrived with the chain gated")
	}
	if err := nc.SetReadDeadline(time.Time{}); err != nil {
		t.Fatal(err)
	}

	release()
	expect(t, r, "+OK\r\n")
	expectBulk(t, r, []byte("1"))
}

// TestDurabilityVolatileNode proves the toggle's guard rails on a server
// with no pipeline: strict is refused (a strict write would hang, not be
// stricter), relaxed is accepted, and the argument grammar answers in
// band.
func TestDurabilityVolatileNode(t *testing.T) {
	_, nc, r := startServer(t)

	send(t, nc, "AKI.DURABILITY")
	expectBulk(t, r, []byte("relaxed"))
	send(t, nc, "AKI.DURABILITY", "STRICT")
	expect(t, r, "-ERR DURABILITY STRICT is not available on a volatile node\r\n")
	send(t, nc, "AKI.DURABILITY", "RELAXED")
	expect(t, r, "+OK\r\n")
	send(t, nc, "AKI.DURABILITY", "bogus")
	expect(t, r, "-ERR DURABILITY mode must be STRICT or RELAXED\r\n")
	send(t, nc, "AKI.DURABILITY", "strict", "extra")
	expect(t, r, "-ERR wrong number of arguments for 'aki.durability' command\r\n")
	// The refused connection keeps serving, relaxed.
	send(t, nc, "SET", "k", "v")
	expect(t, r, "+OK\r\n")
}
