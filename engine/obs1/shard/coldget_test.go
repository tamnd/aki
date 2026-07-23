package shard

import (
	"fmt"
	"strconv"
	"testing"
)

// The cold read seam (coldget.go): a hot miss consults the plan, a keymap
// miss stays a zero-GET definitive miss, a hit parks the command and the
// completion delivers through the CompleteBlocked loopback on the owner,
// with the demoted-group check at delivery. The plan here is a hand-driven
// fake; the real adapter over the engine ColdReader is the server's.

// coldPlanFake scripts the seam: keys in hits launch and hand their done to
// the test through pending, everything else is a definitive miss.
type coldPlanFake struct {
	hits    map[string]bool
	planned int
	pending chan func(ColdHit, error)
}

func newColdPlanFake(hits ...string) *coldPlanFake {
	f := &coldPlanFake{hits: map[string]bool{}, pending: make(chan func(ColdHit, error), 8)}
	for _, k := range hits {
		f.hits[k] = true
	}
	return f
}

func (f *coldPlanFake) plan(key []byte) (ColdLaunch, bool) {
	f.planned++
	if !f.hits[string(key)] {
		return nil, false
	}
	return func(done func(ColdHit, error)) { f.pending <- done }, true
}

// newColdRuntime builds a single-shard runtime with the fake plan wired and
// one extra op: a GET-shaped handler that misses the empty hot store and
// falls through to ColdGet, replying null on a definitive miss.
func newColdRuntime(fake *coldPlanFake) (*Runtime, byte) {
	handlers := testHandlers()
	coldOp := byte(len(handlers))
	handlers = append(handlers, func(cx *Ctx, a [][]byte, r Reply) {
		v, ok := cx.St.Get(a[0], cx.Val)
		cx.Val = v
		if ok {
			r.Bulk(v)
			return
		}
		if cx.ColdGet(a[0], r) {
			return
		}
		r.Null()
	})
	rt := New(1, testArena, testSeg)
	rt.Use(handlers)
	if fake != nil {
		rt.SetColdPlan(fake.plan)
	}
	return rt, coldOp
}

func drainReplies(c *Conn) []string {
	var got []string
	c.DrainReplies(func(rep []byte) { got = append(got, string(rep)) })
	return got
}

// TestColdGetNoPlanFallsThrough: a runtime with no seam wired answers a hot
// miss null on the spot, the pre-cold behavior unchanged.
func TestColdGetNoPlanFallsThrough(t *testing.T) {
	rt, coldOp := newColdRuntime(nil)
	w := rt.workers[0]
	c := rt.NewConn()
	if err := c.Do(coldOp, true, args("k")); err != nil {
		t.Fatal(err)
	}
	c.Flush()
	for w.drainAndExecute() > 0 {
	}
	if got := drainReplies(c); len(got) != 1 || got[0] != "$-1\r\n" {
		t.Fatalf("replies %q, want one null", got)
	}
	if w.cx.ColdParks() != 0 {
		t.Fatalf("parked %d with no plan", w.cx.ColdParks())
	}
}

// TestColdGetDefinitiveMiss: the plan consulted and refusing is a zero-GET
// null right in the handler, no park, no launch.
func TestColdGetDefinitiveMiss(t *testing.T) {
	fake := newColdPlanFake()
	rt, coldOp := newColdRuntime(fake)
	w := rt.workers[0]
	c := rt.NewConn()
	if err := c.Do(coldOp, true, args("nope")); err != nil {
		t.Fatal(err)
	}
	c.Flush()
	for w.drainAndExecute() > 0 {
	}
	if got := drainReplies(c); len(got) != 1 || got[0] != "$-1\r\n" {
		t.Fatalf("replies %q, want one null", got)
	}
	if fake.planned != 1 {
		t.Fatalf("plan consulted %d times", fake.planned)
	}
	if w.cx.ColdParks() != 0 || len(fake.pending) != 0 {
		t.Fatalf("miss parked or launched: parks %d pending %d", w.cx.ColdParks(), len(fake.pending))
	}
}

// TestColdGetServesInOrder: a keymap hit parks, the pipeline stalls at its
// slot, and the completion delivers the value at the parked sequence with
// the commands behind it following in order.
func TestColdGetServesInOrder(t *testing.T) {
	fake := newColdPlanFake("cold")
	rt, coldOp := newColdRuntime(fake)
	w := rt.workers[0]
	c := rt.NewConn()

	if err := c.Do(opSet, true, args("a", "hot")); err != nil { // seq 0
		t.Fatal(err)
	}
	if err := c.Do(coldOp, true, args("cold")); err != nil { // seq 1
		t.Fatal(err)
	}
	c.ArmBlock()
	if err := c.Do(opGet, true, args("a")); err != nil { // seq 2
		t.Fatal(err)
	}
	c.Flush()
	for w.drainAndExecute() > 0 {
	}

	got := drainReplies(c)
	if len(got) != 1 || got[0] != "+OK\r\n" {
		t.Fatalf("before completion %q, want only the SET", got)
	}
	if w.cx.ColdParks() != 1 {
		t.Fatalf("parks %d", w.cx.ColdParks())
	}

	done := <-fake.pending
	done(ColdHit{Found: true, Value: []byte("v9")}, nil)
	w.advanceIntents()
	got = append(got, drainReplies(c)...)

	want := []string{"+OK\r\n", "$2\r\nv9\r\n", "$3\r\nhot\r\n"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("replies %q, want %q", got, want)
	}
	if w.cx.ColdServes() != 1 || w.cx.ColdErrs() != 0 {
		t.Fatalf("serves %d errs %d", w.cx.ColdServes(), w.cx.ColdErrs())
	}
}

// TestColdGetAbsentAndError: a completion without the record (the collision
// case, or a tombstone) is a null, and a failed fetch is the loud taxonomy
// error, both at the parked slot.
func TestColdGetAbsentAndError(t *testing.T) {
	fake := newColdPlanFake("gone", "bad")
	rt, coldOp := newColdRuntime(fake)
	w := rt.workers[0]
	c := rt.NewConn()

	if err := c.Do(coldOp, true, args("gone")); err != nil {
		t.Fatal(err)
	}
	c.Flush()
	for w.drainAndExecute() > 0 {
	}
	(<-fake.pending)(ColdHit{}, nil)
	w.advanceIntents()
	if got := drainReplies(c); len(got) != 1 || got[0] != "$-1\r\n" {
		t.Fatalf("absent completion %q, want null", got)
	}

	if err := c.Do(coldOp, true, args("bad")); err != nil {
		t.Fatal(err)
	}
	c.Flush()
	for w.drainAndExecute() > 0 {
	}
	(<-fake.pending)(ColdHit{}, fmt.Errorf("boom"))
	w.advanceIntents()
	if got := drainReplies(c); len(got) != 1 || got[0] != "-ERR store: cold read failed\r\n" {
		t.Fatalf("failed completion %q", got)
	}
	if w.cx.ColdErrs() != 1 {
		t.Fatalf("errs %d", w.cx.ColdErrs())
	}
}

// TestColdGetDemotedDrops is the epoch-retirement contract at delivery: the
// group demoted while the GET flew, so the value is dropped and the client
// fails over with the doc 07 MOVED redirect.
func TestColdGetDemotedDrops(t *testing.T) {
	fake := newColdPlanFake("cold")
	rt, coldOp := newColdRuntime(fake)
	fv := newFakeLeases()
	fv.gated = false
	rt.UseLeaseView(fv)
	w := rt.workers[0]
	c := rt.NewConn()

	if err := c.Do(coldOp, true, args("cold")); err != nil {
		t.Fatal(err)
	}
	c.Flush()
	for w.drainAndExecute() > 0 {
	}
	done := <-fake.pending

	// The handoff finishes while the GET is in flight.
	fv.demoted[leaseGroup("cold")] = "10.0.0.9:7000"
	done(ColdHit{Found: true, Value: []byte("stale")}, nil)
	w.advanceIntents()

	want := "-MOVED " + strconv.Itoa(HashSlot([]byte("cold"))) + " 10.0.0.9:7000\r\n"
	if got := drainReplies(c); len(got) != 1 || got[0] != want {
		t.Fatalf("demoted completion %q, want %q", got, want)
	}
	if w.cx.ColdDropped() != 1 || w.cx.ColdServes() != 0 {
		t.Fatalf("dropped %d serves %d", w.cx.ColdDropped(), w.cx.ColdServes())
	}
}
