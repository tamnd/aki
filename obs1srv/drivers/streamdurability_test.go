package drivers

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/tamnd/aki/engine/obs1"
)

// TestStreamDurabilityRoundTrip drives the stream entry surface over the
// socket and checks the flushed frames carry post-decision effects: a
// creating XADD leads with a collnew, a trimming XADD rides its xadd and
// the trim clause's xtrim as one command, XTRIM frames the count it
// removed, XDEL frames only the ids that actually removed, XSETID frames
// the three resulting values, no stream ever frames a colldrop (an
// emptied stream persists and the next XADD proves it by framing no
// collnew), and a parked XREAD's serve frames nothing beyond the waking
// XADD, since serves are pure reads.
func TestStreamDurabilityRoundTrip(t *testing.T) {
	wl, store, nc, r, _ := startLoggedServer(t, false)
	const node = uint64(0xE1)

	seqs := map[uint16]uint64{}
	emit := func(key string, n uint64) {
		_, g := clusterMapKey([]byte(key))
		seqs[g] += n
	}

	// The creating XADD frames collnew then the entry at its explicit id;
	// the follow-up frames the entry alone.
	send(t, nc, "XADD", "s1", "1-1", "f1", "v1")
	expectBulk(t, r, []byte("1-1"))
	emit("s1", 2) // collnew, xadd
	send(t, nc, "XADD", "s1", "2-2", "a", "b", "c", "d")
	expectBulk(t, r, []byte("2-2"))
	emit("s1", 1)
	// A NOMKSTREAM miss creates nothing and frames nothing; a rejected id
	// is a client error and frames nothing.
	send(t, nc, "XADD", "nos", "NOMKSTREAM", "*", "f", "v")
	expect(t, r, "$-1\r\n")
	send(t, nc, "XADD", "s1", "1-1", "f", "v")
	expect(t, r, "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n")

	// A trimming XADD: three entries live, MAXLEN 2 drops the oldest, so
	// the command frames its xadd and an xtrim of 1 as one run.
	send(t, nc, "XADD", "s1", "MAXLEN", "2", "3-0", "f", "v")
	expectBulk(t, r, []byte("3-0"))
	emit("s1", 2) // xadd, xtrim
	send(t, nc, "XADD", "s1", "4-0", "f", "v")
	expectBulk(t, r, []byte("4-0"))
	emit("s1", 1)

	// A bare XTRIM frames the count it removed; a no-effect trim frames
	// nothing.
	send(t, nc, "XTRIM", "s1", "MAXLEN", "1")
	expect(t, r, ":2\r\n")
	emit("s1", 1)
	send(t, nc, "XTRIM", "s1", "MAXLEN", "5")
	expect(t, r, ":0\r\n")

	// XDEL frames only the id that actually removed, argument order; the
	// miss half of the pair never hits the WAL, and emptying the stream
	// frames no colldrop. A whole-miss XDEL frames nothing.
	send(t, nc, "XDEL", "s1", "4-0", "9-9")
	expect(t, r, ":1\r\n")
	emit("s1", 1)
	send(t, nc, "XLEN", "s1")
	expect(t, r, ":0\r\n")
	send(t, nc, "XDEL", "s1", "9-9")
	expect(t, r, ":0\r\n")
	// The emptied stream persists: the next XADD frames no collnew.
	send(t, nc, "XADD", "s1", "5-0", "f", "v")
	expectBulk(t, r, []byte("5-0"))
	emit("s1", 1)

	// XSETID frames the three resulting values after the merge, and the
	// bare form carries the unchanged counters forward unconditionally. A
	// missing key is a client error and frames nothing.
	send(t, nc, "XSETID", "s1", "10-0", "ENTRIESADDED", "42", "MAXDELETEDID", "6-6")
	expect(t, r, "+OK\r\n")
	emit("s1", 1)
	send(t, nc, "XSETID", "s1", "11-1")
	expect(t, r, "+OK\r\n")
	emit("s1", 1)
	send(t, nc, "XSETID", "nos", "1-1")
	expect(t, r, "-ERR The XSETID command requires the key to exist.\r\n")

	// A parked XREAD served by a later XADD: the waking command frames its
	// collnew and xadd, the serve frames nothing, and the delivered reply
	// proves the serve ran.
	nc2, err := net.Dial("tcp", nc.RemoteAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer nc2.Close()
	r2 := br(nc2)
	send(t, nc2, "XREAD", "BLOCK", "30000", "STREAMS", "bws", "$")
	time.Sleep(50 * time.Millisecond) // let the XREAD park
	send(t, nc, "XADD", "bws", "1-1", "f", "v")
	expectBulk(t, r, []byte("1-1"))
	expect(t, r2, "*1\r\n*2\r\n$3\r\nbws\r\n*1\r\n*2\r\n$3\r\n1-1\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n")
	emit("bws", 2) // collnew, xadd; the serve adds nothing

	wl.Barrier()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for g, last := range seqs {
		if err := wl.Marks().Wait(ctx, g, last); err != nil {
			t.Fatalf("Wait group %d seq %d: %v", g, last, err)
		}
	}

	byKey := map[string][]obs1.Op{}
	total := 0
	for _, f := range walFrames(t, store, node) {
		op, err := obs1.DecodeOp(f)
		if err != nil {
			t.Fatalf("DecodeOp seq %d: %v", f.Seq, err)
		}
		byKey[string(f.Key)] = append(byKey[string(f.Key)], op)
		total++
	}
	if total != 13 {
		t.Fatalf("%d frames flushed, want 13: %v", total, byKey)
	}
	xadd := func(op obs1.Op) obs1.XAdd {
		xa, ok := op.(obs1.CollDelta).Sub.(obs1.XAdd)
		if !ok {
			t.Fatalf("op %+v, want an xadd", op)
		}
		return xa
	}
	xtrim := func(op obs1.Op) uint64 {
		xt, ok := op.(obs1.CollDelta).Sub.(obs1.XTrim)
		if !ok {
			t.Fatalf("op %+v, want an xtrim", op)
		}
		return xt.Count
	}
	isColl := func(op obs1.Op) bool {
		cn, ok := op.(obs1.CollNew)
		return ok && cn.Type == obs1.CollStream && len(cn.Hints) == 0
	}

	s1 := byKey["s1"]
	if len(s1) != 11 {
		t.Fatalf("s1 ops = %v", s1)
	}
	if !isColl(s1[0]) {
		t.Fatalf("s1 frame 1 = %+v, want a hintless stream collnew", s1[0])
	}
	if xa := xadd(s1[1]); xa.IDMs != 1 || xa.IDSeq != 1 || len(xa.Pairs) != 1 || string(xa.Pairs[0].Field) != "f1" || string(xa.Pairs[0].Value) != "v1" {
		t.Fatalf("s1 frame 2 = %+v", s1[1])
	}
	if xa := xadd(s1[2]); xa.IDMs != 2 || xa.IDSeq != 2 || len(xa.Pairs) != 2 || string(xa.Pairs[1].Field) != "c" {
		t.Fatalf("s1 frame 3 = %+v", s1[2])
	}
	// The trimming XADD's run: the entry then the trim clause's count.
	if xa := xadd(s1[3]); xa.IDMs != 3 || xa.IDSeq != 0 {
		t.Fatalf("s1 frame 4 = %+v", s1[3])
	}
	if n := xtrim(s1[4]); n != 1 {
		t.Fatalf("s1 frame 5 count = %d, want the trim clause's 1", n)
	}
	if xa := xadd(s1[5]); xa.IDMs != 4 {
		t.Fatalf("s1 frame 6 = %+v", s1[5])
	}
	if n := xtrim(s1[6]); n != 2 {
		t.Fatalf("s1 frame 7 count = %d, want XTRIM's 2", n)
	}
	xd, ok := s1[7].(obs1.CollDelta).Sub.(obs1.XDel)
	if !ok || len(xd.IDMs) != 1 || xd.IDMs[0] != 4 || xd.IDSeq[0] != 0 {
		t.Fatalf("s1 frame 8 = %+v, want the one removed id", s1[7])
	}
	// No colldrop after the emptying XDEL, and no collnew on the re-add.
	if xa := xadd(s1[8]); xa.IDMs != 5 || xa.IDSeq != 0 {
		t.Fatalf("s1 frame 9 = %+v, want the re-add without a collnew", s1[8])
	}
	xs, ok := s1[9].(obs1.CollDelta).Sub.(obs1.XSetID)
	if !ok || xs.LastMs != 10 || xs.LastSeq != 0 || xs.EntriesAdded != 42 || xs.MaxDelMs != 6 || xs.MaxDelSeq != 6 {
		t.Fatalf("s1 frame 10 = %+v", s1[9])
	}
	xs, ok = s1[10].(obs1.CollDelta).Sub.(obs1.XSetID)
	if !ok || xs.LastMs != 11 || xs.LastSeq != 1 || xs.EntriesAdded != 42 || xs.MaxDelMs != 6 || xs.MaxDelSeq != 6 {
		t.Fatalf("s1 frame 11 = %+v, want the unchanged counters carried forward", s1[10])
	}

	bws := byKey["bws"]
	if len(bws) != 2 || !isColl(bws[0]) {
		t.Fatalf("bws ops = %v, want the waking XADD's two frames alone", bws)
	}
	if xa := xadd(bws[1]); xa.IDMs != 1 || xa.IDSeq != 1 {
		t.Fatalf("bws frame 2 = %+v", bws[1])
	}
}

// TestStreamDurabilityStrictAck proves an XADD on a strict connection
// holds its reply until the chain commits: with the chain gated the
// entry is readable in RAM through a second connection while the
// writer's reply stays unanswered, and releasing the gate lands it.
func TestStreamDurabilityStrictAck(t *testing.T) {
	wl, _, nc, r, release := startLoggedServer(t, true)

	send(t, nc, "AKI.DURABILITY", "strict")
	expect(t, r, "+OK\r\n")
	send(t, nc, "XADD", "sk", "1-1", "f", "v")

	nc2, err := net.Dial("tcp", nc.RemoteAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer nc2.Close()
	r2 := br(nc2)
	// The relaxed reader sees the entry at once: the write ran in RAM and
	// only the strict writer's output waits on the gated chain.
	send(t, nc2, "XLEN", "sk")
	expect(t, r2, ":1\r\n")

	deadline := time.Now().Add(10 * time.Second)
	for readInfo(t, nc2, r2)["wal_barrier_flushes"] == 0 {
		if time.Now().After(deadline) {
			t.Fatal("no barrier flush while a strict XADD was pending")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if _, g := clusterMapKey([]byte("sk")); wl.Marks().Committed(g) != 0 {
		t.Fatal("the gated chain committed")
	}
	if err := nc.SetReadDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Peek(1); err == nil {
		t.Fatal("a strict XADD's reply arrived with the chain gated")
	}
	if err := nc.SetReadDeadline(time.Time{}); err != nil {
		t.Fatal(err)
	}

	release()
	expectBulk(t, r, []byte("1-1"))
}
