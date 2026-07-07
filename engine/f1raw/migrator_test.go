package f1raw

import (
	"fmt"
	"testing"
	"time"
)

// TestMigrateDownReclaimsToLowWater is the M3 slice-3 gate for the fill trigger and segment
// selection (doc 21 section 6, D14 and D15), driven synchronously so the trigger math and the
// emptiest-segment pick are checked without goroutine timing. It fills several segments past the
// high-water mark, then runs one migrateDown pass and asserts the resident live-byte total fell
// below the low-water mark, every migrated key still reads its exact value from cold, and the cold
// region grew. This is the trigger deciding to drain and the selection picking segments to drain,
// composed with the slice-2 drain loop.
func TestMigrateDownReclaimsToLowWater(t *testing.T) {
	s := churnSegColdStore(t, 8)
	s.migHiNum = defaultMigHiNum
	s.migLoNum = defaultMigLoNum

	// Fill segments until the resident live-byte total is above the high-water mark, recording every
	// key so the post-drain read check can verify exact values. Leave the last-filled segment as the
	// current one (not full), so the drain has full non-current segments to choose from.
	budget := uint64(len(s.segs)) * s.segSize
	hi := budget * s.migHiNum / 100
	var keys [][]byte
	for round := 0; s.liveBytes() <= hi; round++ {
		_, ks := fillSegBig(t, s, fmt.Sprintf("r%d-", round))
		keys = append(keys, ks...)
		if round > len(s.segs)+2 {
			t.Fatalf("live bytes never crossed high-water after %d segment fills", round)
		}
	}
	if s.liveBytes() <= hi {
		t.Fatalf("setup did not cross high-water: live=%d hi=%d", s.liveBytes(), hi)
	}
	coldBefore, _ := s.ColdRecords()

	s.migrateDown()

	lo := budget * s.migLoNum / 100
	if got := s.liveBytes(); got >= lo {
		t.Fatalf("after migrateDown live=%d, want below low-water %d", got, lo)
	}
	if coldAfter, _ := s.ColdRecords(); coldAfter <= coldBefore {
		t.Fatalf("cold region did not grow: %d -> %d", coldBefore, coldAfter)
	}
	for _, k := range keys {
		if _, ok := s.Get(k, nil); !ok {
			t.Fatalf("key %q lost after migrateDown", k)
		}
	}
	// Spot-check a handful against their exact expected value, decoding the prefix/index the key
	// carries so the check does not depend on remembering every value.
	for _, k := range keys {
		var round, i int
		if _, err := fmt.Sscanf(string(k), "r%d-%06d", &round, &i); err != nil {
			continue
		}
		v, ok := s.Get(k, nil)
		if !ok || string(v) != string(churnVal(fmt.Sprintf("r%d-", round), i)) {
			t.Fatalf("key %q = %q,%v; want its exact value", k, v, ok)
		}
	}
}

// TestMigratorServesBeyondArena is the M3 slice-3 end-to-end gate: the whole point of the migrator
// is that a bounded arena serves a dataset larger than itself by continually sinking full segments to
// the single file. It engages the background migrator, then writes far more distinct records than the
// arena can hold at once. Without the migrator the arena fills and Set returns ErrFull after a few
// hundred keys; with it, the migrator frees segments as they fill so every write eventually lands, the
// resident footprint stays bounded near the arena, and every key reads back its exact value, most of
// them from cold. A write that momentarily finds the arena full waits for the migrator and retries,
// standing in for the D12 backpressure a later slice moves into the write path itself.
func TestMigratorServesBeyondArena(t *testing.T) {
	s := churnSegColdStore(t, 6)
	s.EnableMigrator()

	// Write roughly four arenas' worth of distinct records. Each is churnValLen bytes, a segment
	// holds tens of them, and the arena holds six segments, so this forces continual draining.
	perSeg := int(s.segSize / align8(recSize(12, churnValLen)))
	total := perSeg * len(s.segs) * 4
	for i := 0; i < total; i++ {
		k := []byte(fmt.Sprintf("k%08d", i))
		v := churnVal("k", i)
		if !setWithMigratorRetry(t, s, k, v) {
			t.Fatalf("Set %q never succeeded even after waiting for the migrator (i=%d/%d)", k, i, total)
		}
	}

	// The resident footprint must have stayed bounded: the arena never held the whole dataset, so
	// most records are cold now. Confirm the cold region carries far more than one arena of records.
	if coldTotal, _ := s.ColdRecords(); coldTotal < s.cap {
		t.Fatalf("cold region holds %d bytes, want more than one arena (%d): records did not sink", coldTotal, s.cap)
	}

	// Every distinct key reads back its exact value, whether it ended up resident or cold.
	for i := 0; i < total; i++ {
		k := []byte(fmt.Sprintf("k%08d", i))
		v, ok := s.Get(k, nil)
		if !ok || string(v) != string(churnVal("k", i)) {
			t.Fatalf("key %q = %q,%v; want its exact value (i=%d)", k, v, ok, i)
		}
	}
}

// TestBackpressureServesBeyondArena is the M3 slice-4 gate for the write-path backpressure (D12):
// with the migrator engaged, a plain Set that momentarily finds the arena full must wait on the
// migrator and succeed on its own, not return ErrFull, so a caller writing a dataset larger than
// the arena never sees a spurious full error. It is TestMigratorServesBeyondArena without the
// external retry helper: every write goes straight through Set, and any ErrFull is a failure of the
// backpressure to wait. Every distinct key still reads back its exact value, most from cold.
func TestBackpressureServesBeyondArena(t *testing.T) {
	s := churnSegColdStore(t, 6)
	s.EnableMigrator()

	perSeg := int(s.segSize / align8(recSize(12, churnValLen)))
	total := perSeg * len(s.segs) * 4
	for i := 0; i < total; i++ {
		k := []byte(fmt.Sprintf("k%08d", i))
		if err := s.Set(k, churnVal("k", i)); err != nil {
			t.Fatalf("Set %q returned %v with the migrator engaged; backpressure did not wait (i=%d/%d)", k, err, i, total)
		}
	}

	if coldTotal, _ := s.ColdRecords(); coldTotal < s.cap {
		t.Fatalf("cold region holds %d bytes, want more than one arena (%d): records did not sink", coldTotal, s.cap)
	}
	for i := 0; i < total; i++ {
		k := []byte(fmt.Sprintf("k%08d", i))
		v, ok := s.Get(k, nil)
		if !ok || string(v) != string(churnVal("k", i)) {
			t.Fatalf("key %q = %q,%v; want its exact value (i=%d)", k, v, ok, i)
		}
	}
}

// TestMigratorLeavesCollectionRecordsResident is the M3 slice-6a gate for the string-only migration
// restriction (doc 21 section 9, deferring collection tiering to D8/D20). A collection element
// record still has resident addresses cached in its type's secondary structures, so the migrator
// must not sink it: only string records, which have no secondary structure and a fully tier-aware
// path, may go cold. It fills one segment with string records plus a single collection-kind record,
// drains that segment, and asserts the strings went cold while the collection record stayed
// resident and readable, so the segment kept a nonzero live total and did not retire.
func TestMigratorLeavesCollectionRecordsResident(t *testing.T) {
	s := churnSegColdStore(t, 4)
	const collKind = byte(1)

	// Lay the collection record down first so it lands at the base of the still-empty target
	// segment, then fill the rest of that segment with string records.
	target := s.curSeg.Load()
	collKey := []byte("coll-elem")
	if _, err := s.PutKind(collKey, []byte("coll-value"), collKind); err != nil {
		t.Fatalf("PutKind: %v", err)
	}
	var strKeys [][]byte
	for i := 0; s.curSeg.Load() == target; i++ {
		k := []byte(fmt.Sprintf("s%06d", i))
		if err := s.Set(k, churnVal("s", i)); err != nil {
			t.Fatalf("Set %q: %v", k, err)
		}
		if s.curSeg.Load() != target {
			s.Delete(k) // spilled into the next segment; keep the tracked set within target
			break
		}
		strKeys = append(strKeys, k)
	}
	if len(strKeys) == 0 {
		t.Fatal("no string records landed in the target segment")
	}
	coldBefore, _ := s.ColdRecords()

	s.drainSegment(target)

	// Every string record in the segment sank cold and still reads its exact value.
	for i, k := range strKeys {
		if !s.entryIsCold(t, k, stringKind) {
			t.Fatalf("string key %q stayed resident; migrator did not sink it", k)
		}
		v, ok := s.Get(k, nil)
		if !ok || string(v) != string(churnVal("s", i)) {
			t.Fatalf("Get %q = %q,%v; want its exact value", k, v, ok)
		}
	}
	if coldAfter, _ := s.ColdRecords(); coldAfter <= coldBefore {
		t.Fatalf("cold region did not grow draining strings: %d -> %d", coldBefore, coldAfter)
	}

	// The collection record must have stayed resident: its type's secondary structures still hold
	// its resident address, so a cold flip would dangle them until D8/D20.
	if s.entryIsCold(t, collKey, collKind) {
		t.Fatal("collection record was migrated cold; the string-only gate did not hold")
	}
	if v, ok := s.GetKind(collKey, nil, collKind); !ok || string(v) != "coll-value" {
		t.Fatalf("GetKind coll-elem = %q,%v; want coll-value,true", v, ok)
	}
	// With one record still live, the segment kept a nonzero live total and did not retire.
	if live := s.segs[target].live.Load(); live <= 0 {
		t.Fatalf("target segment live=%d; want > 0 while the collection record remains", live)
	}
}

// TestPickDrainTargetSkipsPinnedSegment is the regression gate for the pinned-segment drain
// livelock. A non-migratable record (a collection header row) pins its segment: the migrator can
// never move it, so that segment can never retire and keeps a small live residue. Once its
// migratable records have sunk cold it becomes the emptiest full segment, so the emptiest-first
// pickDrainTarget kept re-selecting it and re-walking a segment that can never free space, starving
// the string-full segments that could. The fix records the futile-drain residue (segment.stuck) and
// skips a segment while its live still equals that mark. This drives the two code paths directly and
// synchronously: fill seg0 with a pinning record plus strings and seg1 with strings only, drain seg0
// so it stuck-pins, then assert the next pickDrainTarget steps over seg0 to the drainable seg1.
// Without the fix pickDrainTarget returns seg0 (the emptiest) forever and seg1 never drains.
func TestPickDrainTargetSkipsPinnedSegment(t *testing.T) {
	s := churnSegColdStore(t, 5)
	// Leave migratableKind at its nil default: only string records migrate, so the collection-kind
	// record below is exactly the non-migratable resident that pins its segment.
	const collKind = byte(1)

	// seg0 gets one non-migratable collection record at its base, then strings fill the rest. seg1
	// gets strings only. After this the current segment is seg2, so seg0 and seg1 are both full,
	// non-current drain candidates.
	seg0 := s.curSeg.Load()
	pinKey := []byte("pinned-coll-header")
	if _, err := s.PutKind(pinKey, []byte("header-value"), collKind); err != nil {
		t.Fatalf("PutKind pin: %v", err)
	}
	fillSegBig(t, s, "a") // fills the rest of seg0 with strings
	seg1, _ := fillSegBig(t, s, "b")
	if seg1 == seg0 {
		t.Fatalf("seg1 %d did not advance past seg0 %d", seg1, seg0)
	}

	// Drain seg0: its strings sink cold, the collection record cannot move, so seg0 keeps a nonzero
	// live residue, does not retire, and records that residue as its stuck mark.
	s.drainSegment(seg0)
	live := s.segs[seg0].live.Load()
	if live <= 0 {
		t.Fatalf("seg0 live=%d after drain; want the pinning record's bytes to remain", live)
	}
	if stuck := s.segs[seg0].stuck.Load(); stuck != live {
		t.Fatalf("seg0 stuck=%d after a futile drain; want it to equal the residue live=%d", stuck, live)
	}
	// seg0 now has the fewest live bytes (just the pinning record), so the plain emptiest-first pick
	// would choose it. The stuck skip must step over it to seg1, which can actually drain and free.
	if seg1Live := s.segs[seg1].live.Load(); seg1Live <= live {
		t.Fatalf("seg1 live=%d is not above seg0 residue %d; test would not exercise the skip", seg1Live, live)
	}
	got, ok := s.pickDrainTarget()
	if !ok {
		t.Fatal("pickDrainTarget found no target; want seg1")
	}
	if got == seg0 {
		t.Fatal("pickDrainTarget returned the stuck-pinned seg0; the skip did not engage, so field segments starve")
	}
	if got != seg1 {
		t.Fatalf("pickDrainTarget returned seg %d; want the drainable seg1 %d", got, seg1)
	}

	// Draining the chosen segment frees space, closing the loop: seg1 retires while seg0 stays pinned.
	s.drainSegment(got)
	if l := s.segs[seg1].live.Load(); l != 0 {
		t.Fatalf("seg1 live=%d after drain; want it fully drained and retired", l)
	}

	// A delete that lowers seg0's live below its stuck mark must re-enable it as a target, so the
	// residue is skipped only while it is genuinely unretireable, not permanently blacklisted.
	s.DeleteKind(pinKey, collKind)
	if l := s.segs[seg0].live.Load(); l >= live {
		t.Fatalf("seg0 live=%d after deleting the pin; want it below the old residue %d", l, live)
	}
}

// setWithMigratorRetry writes one record, waiting on the migrator when the arena is momentarily full.
// The migrator runs on its own goroutine, so a burst of writes can outrun its draining and hit
// ErrFull before a segment frees; this signals the migrator and retries with a short backoff, which
// is exactly the wait the D12 write-path backpressure will internalize in a later slice. It returns
// false only if the arena stays full across the whole backoff budget, which flags a genuine stall.
func setWithMigratorRetry(t *testing.T, s *Store, k, v []byte) bool {
	t.Helper()
	for attempt := 0; attempt < 2000; attempt++ {
		if err := s.Set(k, v); err == nil {
			return true
		} else if err != ErrFull {
			t.Fatalf("Set %q: %v", k, err)
		}
		s.signalMigrator()
		time.Sleep(time.Millisecond)
	}
	return false
}
