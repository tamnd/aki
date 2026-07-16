package shard

import (
	"fmt"
	"testing"

	"github.com/tamnd/aki/engine/obs1/store"
)

// The O1a policy slice (2064/obs1 doc 11 section 2, "SIEVE demotion +
// doorkeeper promotion"): the machinery arrived with the store and shard
// copies, so what these tests pin is the two claims the milestone row makes
// about it. Wired: through the real shard wiring, demotion selects and sheds
// under pressure (drain_test.go proves that leg) and a cold read promotes only
// on its second sighting, the doorkeeper leg proven here. Inert: a diskless
// runtime, the O1a persistence-off serving shape, never engages either side,
// so the policy costs nothing until fold gives it a target.

// TestDoorkeeperPromotesThroughShard drives the two-touch promotion discipline
// end to end at the shard level: flood a cold-configured worker past its
// resident cap, drain records cold through the real async migrator, then read
// one cold key twice. The first sighting serves the frame and leaves the key
// cold (a one-hit wonder never costs a bring-up); the second promotes it back
// to the arena. The store-level colddoor tests prove the filter; this proves
// the policy is reachable through the same wiring the serving layer will use.
func TestDoorkeeperPromotesThroughShard(t *testing.T) {
	s := drainStore(t, 1<<20)
	w := newWorker(0, s)

	const n = 40000
	for i := 0; i < n; i++ {
		k := fmt.Appendf(nil, "k:%07d", i)
		v := fmt.Appendf(nil, "v-%d", i)
		if err := s.Set(k, v); err != nil {
			t.Fatalf("set %d: %v", i, err)
		}
	}
	if !s.NeedsColdDrain() {
		t.Fatal("fixture did not cross the cap")
	}
	for pass := 0; pass < 32 && s.NeedsColdDrain(); pass++ {
		w.drainCold()
		w.advanceIntents()
	}
	w.io.stop()
	for i := 0; i < 64 && w.io.pool.out > 0; i++ {
		if w.advanceIntents() == 0 {
			break
		}
	}
	if s.Cold().Records == 0 {
		t.Fatal("no records migrated cold, nothing to promote")
	}

	// Scan for a key the drain moved cold: for a resident key neither read
	// changes the cold census, for a cold key the first read must leave it
	// unchanged (mark only) and the second must drop it by one (bring-up).
	var dst []byte
	for i := 0; i < n; i++ {
		k := fmt.Appendf(nil, "k:%07d", i)
		want := fmt.Sprintf("v-%d", i)
		before := s.Cold().Records

		got, ok := s.GetString(k, 0, dst)
		if !ok || string(got) != want {
			t.Fatalf("first read of key %d = %q,%v, want %q", i, got, ok, want)
		}
		afterFirst := s.Cold().Records
		if afterFirst != before {
			t.Fatalf("first sighting of key %d moved the cold census %d -> %d, want mark only", i, before, afterFirst)
		}

		got, ok = s.GetString(k, 0, dst)
		if !ok || string(got) != want {
			t.Fatalf("second read of key %d = %q,%v, want %q", i, got, ok, want)
		}
		if s.Cold().Records == before-1 {
			// The doorkeeper admitted on second sight; the promoted key now
			// answers from the arena.
			return
		}
	}
	t.Fatal("no cold key promoted on its second sighting")
}

// TestPolicyInertDiskless pins the other half of the milestone row: a runtime
// built the O1a serving way, memory-only stores with persistence off, never
// engages demotion or promotion no matter the traffic. The cold tier is
// unconfigured, MaybeDemote declines at its first check, and every residency
// and cold counter stays zero, so the diskless binary carries the policy at
// zero cost until fold exists.
func TestPolicyInertDiskless(t *testing.T) {
	rt := New(1, testArena, testSeg)
	rt.Use(testHandlers())
	c := rt.NewConn()
	w := rt.workers[0]

	// A workload with values on both sides of the inline ceiling, so the
	// separated band (the residency hand's whole diet) is populated.
	big := make([]byte, 2048)
	for i := range big {
		big[i] = byte('a' + i%26)
	}
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("k%03d", i)
		val := fmt.Sprintf("v%d", i)
		if err := c.Do(opSet, true, args(key, val)); err != nil {
			t.Fatal(err)
		}
		if err := c.Do(opSet, true, [][]byte{[]byte(key + ":big"), big}); err != nil {
			t.Fatal(err)
		}
		if err := c.Do(opGet, true, args(key)); err != nil {
			t.Fatal(err)
		}
		c.Flush()
		w.drainAndExecute()
		collect(t, c, 3)
	}

	if w.st.ColdConfigured() {
		t.Fatal("a diskless store reports a configured cold tier")
	}
	if got := w.st.MaybeDemote(); got != 0 {
		t.Fatalf("MaybeDemote demoted %d on a diskless store, want 0", got)
	}
	if got := w.st.Resid(); got != (store.ResidStats{}) {
		t.Fatalf("residency counters moved on a diskless store: %+v", got)
	}
	if got := w.st.Cold(); got != (store.ColdStats{}) {
		t.Fatalf("cold counters moved on a diskless store: %+v", got)
	}
	if w.st.NeedsColdDrain() {
		t.Fatal("a diskless store asks for a cold drain")
	}
}
