package sqlo1b

import (
	"fmt"
	"testing"
)

func TestExpClassFor(t *testing.T) {
	now := int64(1_000_000)
	cases := []struct {
		delta int64
		want  uint8
	}{
		{-5000, ExpClassNear}, // already expired is the most due there is
		{0, ExpClassNear},
		{expNearHorizonMS, ExpClassNear},
		{expNearHorizonMS + 1, ExpClassMid},
		{expMidHorizonMS, ExpClassMid},
		{expMidHorizonMS + 1, ExpClassFar},
		{365 * 24 * 60 * 60 * 1000, ExpClassFar},
	}
	for _, c := range cases {
		if got := expClassFor(uint64(now+c.delta), now); got != c.want {
			t.Errorf("delta %d: class %d, want %d", c.delta, got, c.want)
		}
	}
}

// TestSampleExpiryClasses drives the full slice: classes assigned at
// insert from the write-time clock, the sampler tallying them, exact
// probes on the due-plausible classes only, and an overwrite
// refreshing a class as its deadline approaches.
func TestSampleExpiryClasses(t *testing.T) {
	r := newStoreRig(t)
	base := r.now
	for i := range 8 {
		r.apply(t, putOp(fmt.Sprintf("none%d", i), []byte("v"), 0))
		r.apply(t, putOp(fmt.Sprintf("near%d", i), []byte("v"), base+60_000))
		r.apply(t, putOp(fmt.Sprintf("mid%d", i), []byte("v"), base+24*60*60*1000))
		r.apply(t, putOp(fmt.Sprintf("far%d", i), []byte("v"), base+30*24*60*60*1000))
	}
	sm, err := r.s.SampleExpiry(1 << 20)
	if err != nil {
		t.Fatal(err)
	}
	for cls, want := range map[uint8]int64{ExpClassNone: 8, ExpClassNear: 8, ExpClassMid: 8, ExpClassFar: 8} {
		if sm[cls].Entries != want {
			t.Errorf("class %d entries %d, want %d", cls, sm[cls].Entries, want)
		}
	}
	if sm[ExpClassNone].Probed != 0 || sm[ExpClassFar].Probed != 0 {
		t.Errorf("sampler probed a skip class: none %d, far %d", sm[ExpClassNone].Probed, sm[ExpClassFar].Probed)
	}
	if sm[ExpClassNear].Probed != 8 || sm[ExpClassMid].Probed != 8 {
		t.Errorf("due classes probed near %d mid %d, want 8 and 8", sm[ExpClassNear].Probed, sm[ExpClassMid].Probed)
	}
	if sm[ExpClassNear].Expired != 0 || sm[ExpClassMid].Expired != 0 {
		t.Errorf("nothing is due yet: near expired %d, mid expired %d", sm[ExpClassNear].Expired, sm[ExpClassMid].Expired)
	}

	// Two hours on: the 60-second keys are dead, the sampler sees it
	// exactly, and the classes themselves have not moved because no
	// write touched the entries.
	r.now = base + 2*60*60*1000
	sm, err = r.s.SampleExpiry(1 << 20)
	if err != nil {
		t.Fatal(err)
	}
	if sm[ExpClassNear].Expired != 8 {
		t.Errorf("near expired %d, want 8", sm[ExpClassNear].Expired)
	}
	if sm[ExpClassMid].Expired != 0 {
		t.Errorf("mid expired %d, want 0", sm[ExpClassMid].Expired)
	}

	// One hour before the mid deadline, an overwrite refreshes the
	// entry's class: the same expiry now reads as near.
	r.now = base + 23*60*60*1000
	r.apply(t, putOp("mid0", []byte("v2"), base+24*60*60*1000))
	sm, err = r.s.SampleExpiry(1 << 20)
	if err != nil {
		t.Fatal(err)
	}
	if sm[ExpClassMid].Entries != 7 || sm[ExpClassNear].Entries != 9 {
		t.Errorf("after refresh: mid %d near %d, want 7 and 9", sm[ExpClassMid].Entries, sm[ExpClassNear].Entries)
	}

	// Stats reports the cached sample verbatim.
	if got := r.s.Stats().ExpiryClasses; got != sm {
		t.Errorf("Stats sample %+v, want %+v", got, sm)
	}
	r.verify(t)
}

// TestSampleExpiryBudget holds the sampler to its chunk budget: a
// keyspace spread over many chunks is not covered in one bounded
// pass, and successive passes resume instead of rescanning the same
// front.
func TestSampleExpiryBudget(t *testing.T) {
	r := newStoreRig(t)
	const n = 500
	for i := range n {
		r.apply(t, putOp(fmt.Sprintf("k%04d", i), []byte("v"), 0))
	}
	sm1, err := r.s.SampleExpiry(1)
	if err != nil {
		t.Fatal(err)
	}
	var seen int64
	for _, c := range sm1 {
		seen += c.Entries
	}
	if seen == 0 || seen >= n {
		t.Fatalf("one-chunk pass saw %d entries, want a bounded nonzero slice of %d", seen, n)
	}
	cur := r.s.expCursor
	if _, err := r.s.SampleExpiry(1); err != nil {
		t.Fatal(err)
	}
	if r.s.expCursor == cur {
		t.Fatalf("cursor stayed at %d across passes", cur)
	}
	// Enough passes to lap the table: the cursor wraps instead of
	// running off the bucket count.
	for range int(NumBuckets(r.s.level, r.s.split)) + 4 {
		if _, err := r.s.SampleExpiry(1); err != nil {
			t.Fatal(err)
		}
	}
	if r.s.expCursor >= NumBuckets(r.s.level, r.s.split) {
		t.Fatalf("cursor %d past bucket count %d", r.s.expCursor, NumBuckets(r.s.level, r.s.split))
	}
}
