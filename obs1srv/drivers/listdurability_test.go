package drivers

import (
	"context"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/tamnd/aki/engine/obs1"
	"github.com/tamnd/aki/engine/obs1/shard"
)

// TestListDurabilityRoundTrip drives the list write surface over the
// socket and checks the flushed frames carry post-decision effects: the
// pushes carry only what applied (a PUSHX miss frames nothing), the pops
// record decided counts with the emptying colldrop behind the last, LTRIM
// frames as the end pops it resolved to or a bare colldrop on a
// clamp-fail, LREM records the pre-removal positions it matched, LINSERT
// the resolved resulting index, and the move family frames per side with
// the destination push ahead of the source pop on every route: the
// same-key rotation, the co-located pair, the cross-shard hop, and the
// deferred waiter serves behind BLPOP and a cross BLMOVE.
func TestListDurabilityRoundTrip(t *testing.T) {
	wl, store, nc, r, _ := startLoggedServer(t, false)
	const node = uint64(0xE1)

	seqs := map[uint16]uint64{}
	emit := func(key string, n uint64) {
		_, g := ClusterMapKey([]byte(key))
		seqs[g] += n
	}

	// The push family: RPUSH creates, LPUSH prepends, the PUSHX forms
	// frame only on a hit.
	send(t, nc, "RPUSH", "l1", "a", "b", "c")
	expect(t, r, ":3\r\n")
	emit("l1", 2) // collnew, rpush
	send(t, nc, "LPUSH", "l1", "z")
	expect(t, r, ":4\r\n")
	emit("l1", 1)
	send(t, nc, "LPUSHX", "l1", "y")
	expect(t, r, ":5\r\n")
	emit("l1", 1)
	send(t, nc, "RPUSHX", "nol", "x")
	expect(t, r, ":0\r\n")
	// LSET frames the validated index; the list is y z a b c.
	send(t, nc, "LSET", "l1", "0", "Y")
	expect(t, r, "+OK\r\n")
	emit("l1", 1)
	// The pops record the decided count, values never (replay pops the
	// same ends deterministically).
	send(t, nc, "LPOP", "l1")
	expectBulk(t, r, []byte("Y"))
	emit("l1", 1)
	send(t, nc, "RPOP", "l1", "2")
	expect(t, r, "*2\r\n$1\r\nc\r\n$1\r\nb\r\n")
	emit("l1", 1)
	// LREM frames the pre-removal positions; the list is z a.
	send(t, nc, "LREM", "l1", "0", "z")
	expect(t, r, ":1\r\n")
	emit("l1", 1)
	// LINSERT frames the resolved resulting index, BEFORE then AFTER
	// over the one-element list a; a missing pivot frames nothing.
	send(t, nc, "LINSERT", "l1", "BEFORE", "a", "first")
	expect(t, r, ":2\r\n")
	emit("l1", 1)
	send(t, nc, "LINSERT", "l1", "AFTER", "a", "last")
	expect(t, r, ":3\r\n")
	emit("l1", 1)
	send(t, nc, "LINSERT", "l1", "BEFORE", "nosuch", "v")
	expect(t, r, ":-1\r\n")
	// LTRIM 1 1 over first a last drops one head and one tail element;
	// the full-range trim moves nothing and frames nothing.
	send(t, nc, "LTRIM", "l1", "1", "1")
	expect(t, r, "+OK\r\n")
	emit("l1", 2) // lpop, rpop
	send(t, nc, "LTRIM", "l1", "0", "-1")
	expect(t, r, "+OK\r\n")
	// The emptying pop drags the colldrop behind it.
	send(t, nc, "LPOP", "l1")
	expectBulk(t, r, []byte("a"))
	emit("l1", 2) // lpop, colldrop

	// LREM by sign over x y x y x: a positive count scans from the head,
	// a negative from the tail, zero takes all; positions are always the
	// pre-removal ascending ones.
	send(t, nc, "RPUSH", "lr", "x", "y", "x", "y", "x")
	expect(t, r, ":5\r\n")
	emit("lr", 2)
	send(t, nc, "LREM", "lr", "2", "x")
	expect(t, r, ":2\r\n")
	emit("lr", 1)
	send(t, nc, "LREM", "lr", "-1", "x")
	expect(t, r, ":1\r\n")
	emit("lr", 1)
	send(t, nc, "LREM", "lr", "0", "nosuch")
	expect(t, r, ":0\r\n")
	send(t, nc, "LREM", "lr", "0", "y")
	expect(t, r, ":2\r\n")
	emit("lr", 2) // lrem, colldrop

	// A clamp-fail LTRIM empties the list in one decision: a bare
	// colldrop, no pop run.
	send(t, nc, "RPUSH", "rc", "a", "b")
	expect(t, r, ":2\r\n")
	emit("rc", 2)
	send(t, nc, "LTRIM", "rc", "5", "10")
	expect(t, r, "+OK\r\n")
	emit("rc", 1)

	// The same-key rotation frames as the pop-first run it is.
	send(t, nc, "RPUSH", "rot", "a", "b", "c")
	expect(t, r, ":3\r\n")
	emit("rot", 2)
	send(t, nc, "LMOVE", "rot", "rot", "LEFT", "RIGHT")
	expectBulk(t, r, []byte("a"))
	emit("rot", 2) // lpop, rpush

	// The co-located distinct pair, one tag: the destination push frames
	// ahead of the source pop, and the emptying move drags the source
	// colldrop.
	send(t, nc, "RPUSH", "{m}a", "one", "two")
	expect(t, r, ":2\r\n")
	emit("{m}a", 2)
	send(t, nc, "LMOVE", "{m}a", "{m}b", "LEFT", "RIGHT")
	expectBulk(t, r, []byte("one"))
	emit("{m}b", 2) // collnew, rpush
	emit("{m}a", 1) // lpop
	send(t, nc, "RPOPLPUSH", "{m}a", "{m}b")
	expectBulk(t, r, []byte("two"))
	emit("{m}b", 1) // lpush
	emit("{m}a", 2) // rpop, colldrop

	// The cross-shard move, placements computed, never guessed.
	shardOf := func(key string) int {
		return shard.GroupOfSlot(shard.HashSlot([]byte(key)), shard.DefaultSlotGroups) % 2
	}
	keyOn := func(sh int, prefix string) string {
		for i := 0; ; i++ {
			k := prefix + strconv.Itoa(i)
			if shardOf(k) == sh {
				return k
			}
		}
	}
	csrc := keyOn(0, "csrc")
	cdst := keyOn(1, "cdst")
	send(t, nc, "RPUSH", csrc, "m1", "m2")
	expect(t, r, ":2\r\n")
	emit(csrc, 2)
	send(t, nc, "LMOVE", csrc, cdst, "LEFT", "LEFT")
	expectBulk(t, r, []byte("m1"))
	emit(cdst, 2) // collnew, lpush on the destination hop
	emit(csrc, 1) // lpop on the source hop

	// LMPOP drains the first non-empty key (co-located under one tag)
	// and the full draw drops it; an all-empty key list frames nothing.
	send(t, nc, "RPUSH", "{q}f", "p1", "p2")
	expect(t, r, ":2\r\n")
	emit("{q}f", 2)
	send(t, nc, "LMPOP", "2", "{q}e", "{q}f", "LEFT", "COUNT", "5")
	expect(t, r, "*2\r\n$4\r\n{q}f\r\n*2\r\n$2\r\np1\r\n$2\r\np2\r\n")
	emit("{q}f", 2) // lpop, colldrop
	send(t, nc, "LMPOP", "2", "{q}e", "{q}g", "LEFT")
	expect(t, r, "*-1\r\n")

	// An immediately served BLPOP frames the pop it is.
	send(t, nc, "RPUSH", "bi", "el")
	expect(t, r, ":1\r\n")
	emit("bi", 2)
	send(t, nc, "BLPOP", "bi", "0")
	expect(t, r, "*2\r\n$2\r\nbi\r\n$2\r\nel\r\n")
	emit("bi", 2) // lpop, colldrop

	// A parked BLPOP served by a later push: the push frames ahead of
	// the serve's pop, the same four frames whichever side lands first.
	nc2, err := net.Dial("tcp", nc.RemoteAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer nc2.Close()
	r2 := br(nc2)
	send(t, nc2, "BLPOP", "bw", "30")
	time.Sleep(50 * time.Millisecond) // let the BLPOP park
	send(t, nc, "LPUSH", "bw", "served")
	expect(t, r, ":1\r\n")
	expect(t, r2, "*2\r\n$2\r\nbw\r\n$6\r\nserved\r\n")
	emit("bw", 4) // collnew, lpush, then the serve's lpop, colldrop

	// A parked cross-shard BLMOVE served by a later push runs the
	// coordinator: the destination hop frames its push, the final source
	// hop its pop, and reading the reply proves both hops finished.
	bms := keyOn(0, "bms")
	bmd := keyOn(1, "bmd")
	send(t, nc2, "BLMOVE", bms, bmd, "LEFT", "LEFT", "30")
	time.Sleep(50 * time.Millisecond) // let the BLMOVE park
	send(t, nc, "RPUSH", bms, "mv")
	expect(t, r, ":1\r\n")
	expectBulk(t, r2, []byte("mv"))
	emit(bms, 4) // collnew, rpush, then the coordinator's lpop, colldrop
	emit(bmd, 2) // collnew, lpush on the destination hop

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
	if total != 58 {
		t.Fatalf("%d frames flushed, want 58: %v", total, byKey)
	}
	pushVals := func(op obs1.Op, front bool) []string {
		cd, ok := op.(obs1.CollDelta)
		if !ok {
			t.Fatalf("op %+v, want a push delta", op)
		}
		var vals [][]byte
		if front {
			p, ok := cd.Sub.(obs1.LPush)
			if !ok {
				t.Fatalf("op %+v, want an lpush", op)
			}
			vals = p.Values
		} else {
			p, ok := cd.Sub.(obs1.RPush)
			if !ok {
				t.Fatalf("op %+v, want an rpush", op)
			}
			vals = p.Values
		}
		out := make([]string, len(vals))
		for i, v := range vals {
			out[i] = string(v)
		}
		return out
	}
	popCount := func(op obs1.Op, front bool) uint32 {
		cd, ok := op.(obs1.CollDelta)
		if !ok {
			t.Fatalf("op %+v, want a pop delta", op)
		}
		if front {
			p, ok := cd.Sub.(obs1.LPop)
			if !ok {
				t.Fatalf("op %+v, want an lpop", op)
			}
			return p.Count
		}
		p, ok := cd.Sub.(obs1.RPop)
		if !ok {
			t.Fatalf("op %+v, want an rpop", op)
		}
		return p.Count
	}
	indices := func(op obs1.Op) []uint32 {
		lr, ok := op.(obs1.CollDelta).Sub.(obs1.LRem)
		if !ok {
			t.Fatalf("op %+v, want an lrem", op)
		}
		return lr.Indices
	}
	same := func(got []string, want ...string) bool {
		if len(got) != len(want) {
			return false
		}
		for i := range got {
			if got[i] != want[i] {
				return false
			}
		}
		return true
	}
	sameIdx := func(got []uint32, want ...uint32) bool {
		if len(got) != len(want) {
			return false
		}
		for i := range got {
			if got[i] != want[i] {
				return false
			}
		}
		return true
	}
	isColl := func(op obs1.Op) bool {
		cn, ok := op.(obs1.CollNew)
		return ok && cn.Type == obs1.CollList && len(cn.Hints) == 0
	}
	isDrop := func(op obs1.Op) bool {
		_, ok := op.(obs1.CollDrop)
		return ok
	}

	l1 := byKey["l1"]
	if len(l1) != 14 {
		t.Fatalf("l1 ops = %v", l1)
	}
	if !isColl(l1[0]) {
		t.Fatalf("l1 frame 1 = %+v, want a hintless list collnew", l1[0])
	}
	if !same(pushVals(l1[1], false), "a", "b", "c") {
		t.Fatalf("l1 frame 2 = %+v", l1[1])
	}
	if !same(pushVals(l1[2], true), "z") || !same(pushVals(l1[3], true), "y") {
		t.Fatalf("l1 frames 3-4 = %+v %+v", l1[2], l1[3])
	}
	if ls, ok := l1[4].(obs1.CollDelta).Sub.(obs1.LSet); !ok || ls.Index != 0 || string(ls.Value) != "Y" {
		t.Fatalf("l1 frame 5 = %+v, want the lset at the validated index", l1[4])
	}
	if popCount(l1[5], true) != 1 || popCount(l1[6], false) != 2 {
		t.Fatalf("l1 frames 6-7 = %+v %+v", l1[5], l1[6])
	}
	if !sameIdx(indices(l1[7]), 0) {
		t.Fatalf("l1 frame 8 = %+v, want the matched head position", l1[7])
	}
	li := l1[8].(obs1.CollDelta).Sub.(obs1.LIns)
	if li.Index != 0 || string(li.Value) != "first" {
		t.Fatalf("l1 frame 9 = %+v, want the BEFORE insert at 0", li)
	}
	li = l1[9].(obs1.CollDelta).Sub.(obs1.LIns)
	if li.Index != 2 || string(li.Value) != "last" {
		t.Fatalf("l1 frame 10 = %+v, want the AFTER insert at 2", li)
	}
	if popCount(l1[10], true) != 1 || popCount(l1[11], false) != 1 {
		t.Fatalf("l1 frames 11-12 = %+v %+v, want the trim's end pops", l1[10], l1[11])
	}
	if popCount(l1[12], true) != 1 || !isDrop(l1[13]) {
		t.Fatalf("l1 frames 13-14 = %+v %+v, want the emptying pop and colldrop", l1[12], l1[13])
	}

	lr := byKey["lr"]
	if len(lr) != 6 || !isColl(lr[0]) {
		t.Fatalf("lr ops = %v", lr)
	}
	if !sameIdx(indices(lr[2]), 0, 2) {
		t.Fatalf("lr frame 3 = %+v, want the head-scan positions", lr[2])
	}
	if !sameIdx(indices(lr[3]), 2) {
		t.Fatalf("lr frame 4 = %+v, want the tail-scan position ascending", lr[3])
	}
	if !sameIdx(indices(lr[4]), 0, 1) || !isDrop(lr[5]) {
		t.Fatalf("lr frames 5-6 = %+v %+v", lr[4], lr[5])
	}

	rc := byKey["rc"]
	if len(rc) != 3 || !isDrop(rc[2]) {
		t.Fatalf("rc ops = %v, want the clamp-fail colldrop alone", rc)
	}

	rot := byKey["rot"]
	if len(rot) != 4 {
		t.Fatalf("rot ops = %v", rot)
	}
	if popCount(rot[2], true) != 1 || !same(pushVals(rot[3], false), "a") {
		t.Fatalf("rot frames 3-4 = %+v %+v, want the pop-first rotation run", rot[2], rot[3])
	}

	ma, mb := byKey["{m}a"], byKey["{m}b"]
	if len(ma) != 5 || len(mb) != 3 {
		t.Fatalf("{m}a ops = %v, {m}b ops = %v", ma, mb)
	}
	if !isColl(mb[0]) || !same(pushVals(mb[1], false), "one") || !same(pushVals(mb[2], true), "two") {
		t.Fatalf("{m}b ops = %v, want the destination pushes", mb)
	}
	if popCount(ma[2], true) != 1 || popCount(ma[3], false) != 1 || !isDrop(ma[4]) {
		t.Fatalf("{m}a ops = %v, want the source pops and the emptying colldrop", ma)
	}

	cs, cd := byKey[csrc], byKey[cdst]
	if len(cs) != 3 || popCount(cs[2], true) != 1 {
		t.Fatalf("%s ops = %v", csrc, cs)
	}
	if len(cd) != 2 || !isColl(cd[0]) || !same(pushVals(cd[1], true), "m1") {
		t.Fatalf("%s ops = %v, want the destination hop's push", cdst, cd)
	}

	qf := byKey["{q}f"]
	if len(qf) != 4 || popCount(qf[2], true) != 2 || !isDrop(qf[3]) {
		t.Fatalf("{q}f ops = %v", qf)
	}

	bi := byKey["bi"]
	if len(bi) != 4 || popCount(bi[2], true) != 1 || !isDrop(bi[3]) {
		t.Fatalf("bi ops = %v, want the immediate serve's pop", bi)
	}

	bw := byKey["bw"]
	if len(bw) != 4 {
		t.Fatalf("bw ops = %v", bw)
	}
	if !isColl(bw[0]) || !same(pushVals(bw[1], true), "served") {
		t.Fatalf("bw frames 1-2 = %v, want the waking push ahead of the serve", bw)
	}
	if popCount(bw[2], true) != 1 || !isDrop(bw[3]) {
		t.Fatalf("bw frames 3-4 = %v, want the serve's pop and colldrop", bw)
	}

	bs, bd := byKey[bms], byKey[bmd]
	if len(bs) != 4 || !same(pushVals(bs[1], false), "mv") || popCount(bs[2], true) != 1 || !isDrop(bs[3]) {
		t.Fatalf("%s ops = %v, want the waking push then the coordinator's pop", bms, bs)
	}
	if len(bd) != 2 || !isColl(bd[0]) || !same(pushVals(bd[1], true), "mv") {
		t.Fatalf("%s ops = %v, want the coordinator's destination push", bmd, bd)
	}
}

// TestListDurabilityStrictWaiterHold gates the chain and proves a served
// waiter rides the strict contract: a strict connection's parked BLPOP is
// woken by a relaxed push, the serve executes in RAM (the pusher's reply
// and a second connection's LLEN say so), the pending strict ack demands
// the barrier flush, and the waiter's reply stays unanswered until the
// commit lands.
func TestListDurabilityStrictWaiterHold(t *testing.T) {
	wl, _, nc, r, release := startLoggedServer(t, true)

	send(t, nc, "AKI.DURABILITY", "strict")
	expect(t, r, "+OK\r\n")
	send(t, nc, "BLPOP", "bk", "0")
	time.Sleep(50 * time.Millisecond) // let the BLPOP park

	nc2, err := net.Dial("tcp", nc.RemoteAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer nc2.Close()
	r2 := br(nc2)
	// The relaxed pusher acks on the buffer; the serve already ran under
	// the same execution, so the list is drained in RAM and only the
	// strict waiter's output waits.
	send(t, nc2, "LPUSH", "bk", "v")
	expect(t, r2, ":1\r\n")
	send(t, nc2, "LLEN", "bk")
	expect(t, r2, ":0\r\n")

	deadline := time.Now().Add(10 * time.Second)
	for readInfo(t, nc2, r2)["wal_barrier_flushes"] == 0 {
		if time.Now().After(deadline) {
			t.Fatal("no barrier flush while a served strict waiter was pending")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if _, g := ClusterMapKey([]byte("bk")); wl.Marks().Committed(g) != 0 {
		t.Fatal("the gated chain committed")
	}
	if err := nc.SetReadDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Peek(1); err == nil {
		t.Fatal("a served strict waiter's reply arrived with the chain gated")
	}
	if err := nc.SetReadDeadline(time.Time{}); err != nil {
		t.Fatal(err)
	}

	release()
	expect(t, r, "*2\r\n$2\r\nbk\r\n$1\r\nv\r\n")
}
