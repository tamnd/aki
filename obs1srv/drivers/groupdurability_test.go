package drivers

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/tamnd/aki/engine/obs1"
)

// TestGroupDurabilityRoundTrip drives the consumer-group surface over the
// socket and checks the flushed frames carry post-decision effects: an
// MKSTREAM-less create frames the cursor it resolved to, a NOACK delivery
// still frames because the cursor moves, XACK frames only the ids that
// left the PEL, a claim of an XDEL'd pending entry frames its drop as a
// gack, XAUTOCLAIM frames its claim and drop halves as one command,
// XNACK frames the unowned shape with the epoch-reset clock, and the
// misses (BUSYGROUP, an existing CREATECONSUMER, an empty read, a
// DELCONSUMER or DESTROY of nothing) frame nothing.
func TestGroupDurabilityRoundTrip(t *testing.T) {
	wl, store, nc, r, _ := startLoggedServer(t, false)
	const node = uint64(0xE1) // startLoggedServer's fixed node id

	seqs := map[uint16]uint64{}
	emit := func(key string, n uint64) {
		_, g := clusterMapKey([]byte(key))
		seqs[g] += n
	}

	// Three entries to deliver, the creating XADD leading with a collnew.
	send(t, nc, "XADD", "gs", "1-1", "f", "v")
	expectBulk(t, r, []byte("1-1"))
	emit("gs", 2) // collnew, xadd
	send(t, nc, "XADD", "gs", "2-1", "f", "v")
	expectBulk(t, r, []byte("2-1"))
	emit("gs", 1)
	send(t, nc, "XADD", "gs", "3-1", "f", "v")
	expectBulk(t, r, []byte("3-1"))
	emit("gs", 1)

	// A create at an explicit id frames that cursor; the duplicate is
	// BUSYGROUP and frames nothing; a $ create frames the tail and the
	// live lag basis.
	send(t, nc, "XGROUP", "CREATE", "gs", "g1", "0")
	expect(t, r, "+OK\r\n")
	emit("gs", 1)
	send(t, nc, "XGROUP", "CREATE", "gs", "g1", "0")
	expect(t, r, "-BUSYGROUP Consumer Group name already exists\r\n")
	send(t, nc, "XGROUP", "CREATE", "gs", "g2", "$")
	expect(t, r, "+OK\r\n")
	emit("gs", 1)

	// SETID frames the merged result; the NOACK read that follows frames
	// its delivery even though no pending entry is recorded, because the
	// cursor and lag basis still move.
	send(t, nc, "XGROUP", "SETID", "gs", "g2", "1-1", "ENTRIESREAD", "7")
	expect(t, r, "+OK\r\n")
	emit("gs", 1)
	send(t, nc, "XREADGROUP", "GROUP", "g2", "nc1", "COUNT", "1", "NOACK", "STREAMS", "gs", ">")
	expect(t, r, "*1\r\n*2\r\n$2\r\ngs\r\n*1\r\n*2\r\n$3\r\n2-1\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n")
	emit("gs", 1)

	// CREATECONSUMER frames the creation once; the second is a miss and
	// frames nothing.
	send(t, nc, "XGROUP", "CREATECONSUMER", "gs", "g1", "c1")
	expect(t, r, ":1\r\n")
	emit("gs", 1)
	send(t, nc, "XGROUP", "CREATECONSUMER", "gs", "g1", "c1")
	expect(t, r, ":0\r\n")

	// Two deliveries move the cursor through the stream; the drained read
	// answers the null array and frames nothing.
	send(t, nc, "XREADGROUP", "GROUP", "g1", "c1", "COUNT", "2", "STREAMS", "gs", ">")
	expect(t, r, "*1\r\n*2\r\n$2\r\ngs\r\n*2\r\n*2\r\n$3\r\n1-1\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n*2\r\n$3\r\n2-1\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n")
	emit("gs", 1)
	send(t, nc, "XREADGROUP", "GROUP", "g1", "c1", "STREAMS", "gs", ">")
	expect(t, r, "*1\r\n*2\r\n$2\r\ngs\r\n*1\r\n*2\r\n$3\r\n3-1\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n")
	emit("gs", 1)
	send(t, nc, "XREADGROUP", "GROUP", "g1", "c1", "STREAMS", "gs", ">")
	expect(t, r, "*-1\r\n")

	// XACK frames only the id that left the PEL, never the miss.
	send(t, nc, "XACK", "gs", "g1", "1-1", "9-9")
	expect(t, r, ":1\r\n")
	emit("gs", 1)

	// XCLAIM moves 2-1 to c2, bumping its delivery count to 2.
	send(t, nc, "XCLAIM", "gs", "g1", "c2", "0", "2-1")
	expect(t, r, "*1\r\n*2\r\n$3\r\n2-1\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n")
	emit("gs", 1)

	// Deleting 3-1's log entry and then claiming it drops the pending
	// entry instead: an empty claim reply whose frame is the drop's gack.
	send(t, nc, "XDEL", "gs", "3-1")
	expect(t, r, ":1\r\n")
	emit("gs", 1)
	send(t, nc, "XCLAIM", "gs", "g1", "c2", "0", "3-1")
	expect(t, r, "*0\r\n")
	emit("gs", 1)

	// XNACK releases 2-1 unowned with the epoch-reset clock; FAIL leaves
	// the delivery count where the claim put it.
	send(t, nc, "XNACK", "gs", "g1", "FAIL", "IDS", "1", "2-1")
	expect(t, r, ":1\r\n")
	emit("gs", 1)

	// A second pending entry for c1, then its log entry removed, so the
	// autoclaim pass claims 2-1 for c3 and drops 4-1, one command, one
	// run.
	send(t, nc, "XADD", "gs", "4-1", "f", "v")
	expectBulk(t, r, []byte("4-1"))
	emit("gs", 1)
	send(t, nc, "XREADGROUP", "GROUP", "g1", "c1", "STREAMS", "gs", ">")
	expect(t, r, "*1\r\n*2\r\n$2\r\ngs\r\n*1\r\n*2\r\n$3\r\n4-1\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n")
	emit("gs", 1)
	send(t, nc, "XDEL", "gs", "4-1")
	expect(t, r, ":1\r\n")
	emit("gs", 1)
	send(t, nc, "XAUTOCLAIM", "gs", "g1", "c3", "0", "0")
	expect(t, r, "*3\r\n$3\r\n0-0\r\n*1\r\n*2\r\n$3\r\n2-1\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n*1\r\n$3\r\n4-1\r\n")
	emit("gs", 2) // gclaim, gack

	// DELCONSUMER frames only when the consumer existed; DESTROY only when
	// a group was removed.
	send(t, nc, "XGROUP", "DELCONSUMER", "gs", "g1", "c9")
	expect(t, r, ":0\r\n")
	send(t, nc, "XGROUP", "DELCONSUMER", "gs", "g1", "c3")
	expect(t, r, ":1\r\n")
	emit("gs", 1)
	send(t, nc, "XGROUP", "DESTROY", "gs", "g9")
	expect(t, r, ":0\r\n")
	send(t, nc, "XGROUP", "DESTROY", "gs", "g1")
	expect(t, r, ":1\r\n")
	emit("gs", 1)

	// A parked XREADGROUP served by a later XADD: the delivery's gdeliver
	// frames behind the waking xadd, because the XADD handler emits before
	// it serves.
	send(t, nc, "XADD", "bgs", "1-1", "f", "v")
	expectBulk(t, r, []byte("1-1"))
	emit("bgs", 2) // collnew, xadd
	send(t, nc, "XGROUP", "CREATE", "bgs", "bg", "$")
	expect(t, r, "+OK\r\n")
	emit("bgs", 1)
	nc2, err := net.Dial("tcp", nc.RemoteAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer nc2.Close()
	r2 := br(nc2)
	send(t, nc2, "XREADGROUP", "GROUP", "bg", "bc", "BLOCK", "30000", "STREAMS", "bgs", ">")
	time.Sleep(50 * time.Millisecond) // let the read park
	send(t, nc, "XADD", "bgs", "2-1", "f", "v")
	expectBulk(t, r, []byte("2-1"))
	expect(t, r2, "*1\r\n*2\r\n$3\r\nbgs\r\n*1\r\n*2\r\n$3\r\n2-1\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n")
	emit("bgs", 2) // xadd, then the serve's gdeliver

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
	if total != 28 {
		t.Fatalf("%d frames flushed, want 28: %v", total, byKey)
	}
	sub := func(op obs1.Op) obs1.GroupSub {
		gd, ok := op.(obs1.GroupDelta)
		if !ok {
			t.Fatalf("op %+v, want a groupdelta", op)
		}
		return gd.Sub
	}

	gs := byKey["gs"]
	if len(gs) != 23 {
		t.Fatalf("gs ops = %v, want 23", gs)
	}
	// Frames 1-4 are the stream surface's: collnew and the three adds.
	gn := sub(gs[4]).(obs1.GNew)
	if string(gn.Group) != "g1" || gn.LastMs != 0 || gn.LastSeq != 0 || gn.EntriesRead != 0 || !gn.ReadValid {
		t.Fatalf("gs frame 5 = %+v, want g1 at the explicit 0-0", gn)
	}
	gn = sub(gs[5]).(obs1.GNew)
	if string(gn.Group) != "g2" || gn.LastMs != 3 || gn.LastSeq != 1 || gn.EntriesRead != 3 || !gn.ReadValid {
		t.Fatalf("gs frame 6 = %+v, want g2 at the $ tail", gn)
	}
	gsid := sub(gs[6]).(obs1.GSetID)
	if string(gsid.Group) != "g2" || gsid.LastMs != 1 || gsid.LastSeq != 1 || gsid.EntriesRead != 7 || !gsid.ReadValid {
		t.Fatalf("gs frame 7 = %+v", gsid)
	}
	gd := sub(gs[7]).(obs1.GDeliver)
	if string(gd.Group) != "g2" || string(gd.Consumer) != "nc1" || !gd.NoAck || gd.TimeMs <= 0 || len(gd.IDMs) != 1 || gd.IDMs[0] != 2 || gd.IDSeq[0] != 1 {
		t.Fatalf("gs frame 8 = %+v, want the NOACK delivery of 2-1", gd)
	}
	cnew := sub(gs[8]).(obs1.GConsumerNew)
	if string(cnew.Group) != "g1" || string(cnew.Consumer) != "c1" || cnew.SeenMs <= 0 {
		t.Fatalf("gs frame 9 = %+v", cnew)
	}
	gd = sub(gs[9]).(obs1.GDeliver)
	if string(gd.Group) != "g1" || string(gd.Consumer) != "c1" || gd.NoAck || len(gd.IDMs) != 2 || gd.IDMs[0] != 1 || gd.IDMs[1] != 2 {
		t.Fatalf("gs frame 10 = %+v, want the two-entry delivery", gd)
	}
	gd = sub(gs[10]).(obs1.GDeliver)
	if len(gd.IDMs) != 1 || gd.IDMs[0] != 3 || gd.IDSeq[0] != 1 {
		t.Fatalf("gs frame 11 = %+v, want the delivery of 3-1", gd)
	}
	ga := sub(gs[11]).(obs1.GAck)
	if string(ga.Group) != "g1" || len(ga.IDMs) != 1 || ga.IDMs[0] != 1 || ga.IDSeq[0] != 1 {
		t.Fatalf("gs frame 12 = %+v, want the one acked id", ga)
	}
	gc := sub(gs[12]).(obs1.GClaim)
	if string(gc.Consumer) != "c2" || gc.Unowned || len(gc.IDMs) != 1 || gc.IDMs[0] != 2 || gc.TimeMs[0] <= 0 || gc.Counts[0] != 2 {
		t.Fatalf("gs frame 13 = %+v, want 2-1 claimed by c2 at count 2", gc)
	}
	if xd, ok := gs[13].(obs1.CollDelta).Sub.(obs1.XDel); !ok || xd.IDMs[0] != 3 {
		t.Fatalf("gs frame 14 = %+v, want the xdel of 3-1", gs[13])
	}
	ga = sub(gs[14]).(obs1.GAck)
	if len(ga.IDMs) != 1 || ga.IDMs[0] != 3 || ga.IDSeq[0] != 1 {
		t.Fatalf("gs frame 15 = %+v, want the dropped 3-1 as a gack", ga)
	}
	gc = sub(gs[15]).(obs1.GClaim)
	if !gc.Unowned || len(gc.Consumer) != 0 || gc.IDMs[0] != 2 || gc.TimeMs[0] != 0 || gc.Counts[0] != 2 {
		t.Fatalf("gs frame 16 = %+v, want the unowned nack at the epoch clock", gc)
	}
	// Frames 17-19: the second add, its delivery, its xdel.
	gd = sub(gs[17]).(obs1.GDeliver)
	if len(gd.IDMs) != 1 || gd.IDMs[0] != 4 {
		t.Fatalf("gs frame 18 = %+v, want the delivery of 4-1", gd)
	}
	// The autoclaim pass: gclaim then gack, one command.
	gc = sub(gs[19]).(obs1.GClaim)
	if string(gc.Consumer) != "c3" || gc.Unowned || gc.IDMs[0] != 2 || gc.TimeMs[0] <= 0 || gc.Counts[0] != 3 {
		t.Fatalf("gs frame 20 = %+v, want 2-1 autoclaimed by c3 at count 3", gc)
	}
	ga = sub(gs[20]).(obs1.GAck)
	if len(ga.IDMs) != 1 || ga.IDMs[0] != 4 || ga.IDSeq[0] != 1 {
		t.Fatalf("gs frame 21 = %+v, want the autoclaim's dropped 4-1", ga)
	}
	cdel := sub(gs[21]).(obs1.GConsumerDel)
	if string(cdel.Group) != "g1" || string(cdel.Consumer) != "c3" {
		t.Fatalf("gs frame 22 = %+v", cdel)
	}
	if dr := sub(gs[22]).(obs1.GDrop); string(dr.Group) != "g1" {
		t.Fatalf("gs frame 23 = %+v", dr)
	}

	// The blocking hand-off: the gdeliver rides behind the waking xadd.
	bgs := byKey["bgs"]
	if len(bgs) != 5 {
		t.Fatalf("bgs ops = %v, want 5", bgs)
	}
	if gn := sub(bgs[2]).(obs1.GNew); string(gn.Group) != "bg" || gn.LastMs != 1 || gn.LastSeq != 1 {
		t.Fatalf("bgs frame 3 = %+v", gn)
	}
	if xa, ok := bgs[3].(obs1.CollDelta).Sub.(obs1.XAdd); !ok || xa.IDMs != 2 || xa.IDSeq != 1 {
		t.Fatalf("bgs frame 4 = %+v, want the waking xadd", bgs[3])
	}
	gd = sub(bgs[4]).(obs1.GDeliver)
	if string(gd.Group) != "bg" || string(gd.Consumer) != "bc" || gd.NoAck || len(gd.IDMs) != 1 || gd.IDMs[0] != 2 || gd.IDSeq[0] != 1 {
		t.Fatalf("bgs frame 5 = %+v, want the served delivery behind the xadd", gd)
	}
}

// TestGroupDurabilityStrictAck proves an XGROUP CREATE on a strict
// connection holds its reply until the chain commits: with the chain
// gated the group is live in RAM through a second connection while the
// creator's reply stays unanswered, and releasing the gate lands it.
func TestGroupDurabilityStrictAck(t *testing.T) {
	wl, _, nc, r, release := startLoggedServer(t, true)

	send(t, nc, "AKI.DURABILITY", "strict")
	expect(t, r, "+OK\r\n")
	send(t, nc, "XGROUP", "CREATE", "gk", "g", "0", "MKSTREAM")

	nc2, err := net.Dial("tcp", nc.RemoteAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer nc2.Close()
	r2 := br(nc2)
	// The relaxed reader sees the group while the strict creator's output
	// waits on the gated chain. The creator's reply is held, so nothing
	// orders nc2 behind the create; on a slow box nc2's command can reach
	// the shard first and land in the NOGROUP window, so retry through it.
	deadline := time.Now().Add(10 * time.Second)
	for {
		send(t, nc2, "XGROUP", "CREATECONSUMER", "gk", "g", "c")
		line, err := r2.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		if line == ":1\r\n" {
			break
		}
		if !strings.HasPrefix(line, "-NOGROUP") || time.Now().After(deadline) {
			t.Fatalf("createconsumer reply = %q", line)
		}
		time.Sleep(time.Millisecond)
	}

	deadline = time.Now().Add(10 * time.Second)
	for readInfo(t, nc2, r2)["wal_barrier_flushes"] == 0 {
		if time.Now().After(deadline) {
			t.Fatal("no barrier flush while a strict XGROUP CREATE was pending")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if _, g := clusterMapKey([]byte("gk")); wl.Marks().Committed(g) != 0 {
		t.Fatal("the gated chain committed")
	}
	if err := nc.SetReadDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Peek(1); err == nil {
		t.Fatal("a strict XGROUP CREATE's reply arrived with the chain gated")
	}
	if err := nc.SetReadDeadline(time.Time{}); err != nil {
		t.Fatal(err)
	}

	release()
	expect(t, r, "+OK\r\n")
}
