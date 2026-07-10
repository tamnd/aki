package store

import (
	"bytes"
	"fmt"
	"testing"
)

// sepVal builds a separated-band value: over strInlineMax, under strChunkMin.
func sepVal(c byte, n int) []byte {
	return bytes.Repeat([]byte{c}, n)
}

// TestSetSepReplaceInPlace pins the separated-band full-replace path: a SET
// over a separated record whose new value fits the run's reserved capacity
// rewrites the run in place, keeping the record, the run, and the arena fill
// exactly where they were. This is what makes sustained same-size overwrite
// steady-state instead of an arena-full death.
func TestSetSepReplaceInPlace(t *testing.T) {
	s := testStore(t, 4)
	key := []byte("sep")
	if err := s.Set(key, sepVal('a', 4096)); err != nil {
		t.Fatal(err)
	}
	_, addr, _ := s.findEntry(Hash(key), key)
	if addr == 0 {
		t.Fatal("key missing after SET")
	}
	word, _, vcap := s.readPtr(s.valueStart(addr))
	fill := s.arena.used()

	for i := 0; i < 100; i++ {
		if err := s.Set(key, sepVal(byte('b'+i%20), 4096)); err != nil {
			t.Fatalf("overwrite %d: %v", i, err)
		}
	}
	_, addr2, _ := s.findEntry(Hash(key), key)
	if addr2 != addr {
		t.Fatalf("record moved on same-size replace: %d -> %d", addr, addr2)
	}
	w2, _, c2 := s.readPtr(s.valueStart(addr))
	if w2 != word || c2 != vcap {
		t.Fatalf("run moved on same-size replace: %x/%d -> %x/%d", word, vcap, w2, c2)
	}
	if got := s.arena.used(); got != fill {
		t.Fatalf("arena fill grew from %d to %d across in-place replaces", fill, got)
	}
	mustGet(t, s, key, string(sepVal(byte('b'+99%20), 4096)))
}

// TestSetSepReplaceGrows pins the other half: a replace past the run's
// capacity keeps the record, swaps in a fresh run, and charges the old run's
// bytes dead in its segment.
func TestSetSepReplaceGrows(t *testing.T) {
	s := testStore(t, 4)
	key := []byte("sep")
	if err := s.Set(key, sepVal('a', 4096)); err != nil {
		t.Fatal(err)
	}
	_, addr, _ := s.findEntry(Hash(key), key)
	live := s.arena.live()
	if err := s.Set(key, sepVal('b', 8192)); err != nil {
		t.Fatal(err)
	}
	_, addr2, _ := s.findEntry(Hash(key), key)
	if addr2 != addr {
		t.Fatalf("record republished on a run-only grow: %d -> %d", addr, addr2)
	}
	mustGet(t, s, key, string(sepVal('b', 8192)))
	// The ledger swapped a 4096 charge for an 8192 one; the difference is the
	// exact growth, meaning the old run was charged back where it lay.
	if got := s.arena.live(); got != live+4096 {
		t.Fatalf("live charge %d, want %d", got, live+4096)
	}
}

// TestSetSepReplaceTTL pins the deadline rules across the in-place separated
// replace: KEEPTTL carries the deadline, a plain SET on a slotted record
// clears it in place, and a SET with a deadline on a slotless record still
// republishes into a slotted one.
func TestSetSepReplaceTTL(t *testing.T) {
	s := testStore(t, 4)
	key := []byte("sep")
	now := int64(1_000)
	if err := s.SetString(key, sepVal('a', 2048), now, now+60_000, false); err != nil {
		t.Fatal(err)
	}
	_, addr, _ := s.findEntry(Hash(key), key)

	// KEEPTTL replace stays in place and keeps the deadline.
	if err := s.SetString(key, sepVal('b', 2048), now, 0, true); err != nil {
		t.Fatal(err)
	}
	_, addr2, _ := s.findEntry(Hash(key), key)
	if addr2 != addr {
		t.Fatal("KEEPTTL replace republished the record")
	}
	if at := s.expireAt(addr2); at != now+60_000 {
		t.Fatalf("deadline %d, want %d", at, now+60_000)
	}

	// A plain SET clears the deadline in the slot, still in place.
	if err := s.SetString(key, sepVal('c', 2048), now, 0, false); err != nil {
		t.Fatal(err)
	}
	_, addr3, _ := s.findEntry(Hash(key), key)
	if addr3 != addr {
		t.Fatal("deadline-clearing replace republished the record")
	}
	if at := s.expireAt(addr3); at != 0 {
		t.Fatalf("deadline survived a plain SET: %d", at)
	}

	// A slotless record cannot take a deadline in place: fresh key without a
	// TTL, then SET with one must republish into a slotted record.
	k2 := []byte("sep2")
	if err := s.Set(k2, sepVal('a', 2048)); err != nil {
		t.Fatal(err)
	}
	_, b1, _ := s.findEntry(Hash(k2), k2)
	if err := s.SetString(k2, sepVal('b', 2048), now, now+5_000, false); err != nil {
		t.Fatal(err)
	}
	_, b2, _ := s.findEntry(Hash(k2), k2)
	if b2 == b1 {
		t.Fatal("slotless record took a deadline without republishing")
	}
	if at := s.expireAt(b2); at != now+5_000 {
		t.Fatalf("deadline %d, want %d", at, now+5_000)
	}
	mustGet(t, s, k2, string(sepVal('b', 2048)))
}

// TestSetSepReplaceBandChange pins that a full replace which leaves the
// separated band still republishes and re-selects from scratch.
func TestSetSepReplaceBandChange(t *testing.T) {
	s := testStore(t, 4)
	key := []byte("sep")
	if err := s.Set(key, sepVal('a', 2048)); err != nil {
		t.Fatal(err)
	}
	_, addr, _ := s.findEntry(Hash(key), key)
	if err := s.Set(key, []byte("small")); err != nil {
		t.Fatal(err)
	}
	_, addr2, _ := s.findEntry(Hash(key), key)
	if addr2 == addr {
		t.Fatal("band change reused the separated record")
	}
	mustGet(t, s, key, "small")
	st := s.Stats()
	if st.Embedded != 1 || st.Separated != 0 {
		t.Fatalf("band census emb=%d sep=%d, want 1/0", st.Embedded, st.Separated)
	}
}

// fillSepSeg writes separated-band records under prefix-numbered keys until
// the current segment advances, returning the keys whose record and run both
// landed in the segment that was current when it started.
func fillSepSeg(t *testing.T, s *Store, prefix string, vlen int) (startSeg uint64, keys [][]byte) {
	t.Helper()
	startSeg = s.arena.cur
	for i := 0; s.arena.cur == startSeg; i++ {
		k := []byte(fmt.Sprintf("%s%06d", prefix, i))
		if err := s.Set(k, sepVal(byte('a'+i%26), vlen)); err != nil {
			t.Fatalf("Set %q: %v", k, err)
		}
		if s.arena.cur != startSeg {
			s.Delete(k)
			break
		}
		keys = append(keys, k)
	}
	if len(keys) == 0 {
		t.Fatal("filled no keys before the segment advanced")
	}
	return startSeg, keys
}

// TestDeleteHeavyReclaim deletes every record in the first segments and pins
// path 4, the whole-segment drop: CompactArena frees the fully dead segments
// with no relocation and the arena fill drops back to the survivors.
func TestDeleteHeavyReclaim(t *testing.T) {
	s := testStore(t, 6)
	seg0, keysA := fillSepSeg(t, s, "a", 4096)
	seg1, keysB := fillSepSeg(t, s, "b", 4096)
	_, keysC := fillSepSeg(t, s, "c", 4096)
	for _, k := range append(keysA, keysB...) {
		if !s.Delete(k) {
			t.Fatalf("Delete %q: missing", k)
		}
	}
	freed := s.CompactArena()
	if freed < 2 {
		t.Fatalf("CompactArena freed %d segments, want at least 2", freed)
	}
	if f := s.arena.fillOf(seg0); f != 0 {
		t.Fatalf("segment %d still holds %d bytes", seg0, f)
	}
	if f := s.arena.fillOf(seg1); f != 0 {
		t.Fatalf("segment %d still holds %d bytes", seg1, f)
	}
	for i, k := range keysC {
		mustGet(t, s, k, string(sepVal(byte('a'+i%26), 4096)))
	}
}

// TestCompactSurvivors forces a dead-fraction compaction and checks the
// survivors: keys readable at their new addresses, values intact byte for
// byte, deadline flag and value preserved, and the victim segment freed.
func TestCompactSurvivors(t *testing.T) {
	s := testStore(t, 6)
	seg0, keys := fillSepSeg(t, s, "a", 4096)

	// One survivor carries a deadline: republish it with a slot.
	surv := keys[3]
	if err := s.SetString(surv, sepVal('T', 4096), 1_000, 99_000, false); err != nil {
		t.Fatal(err)
	}
	// And one embedded survivor, so both tenant kinds move.
	emb := keys[5]
	if err := s.Set(emb, []byte("embedded-survivor")); err != nil {
		t.Fatal(err)
	}

	// The republishes above may have advanced the cursor; make sure seg0 is
	// not current, then kill everything else in it.
	if s.arena.cur == seg0 {
		fillSepSeg(t, s, "pad", 4096)
	}
	for _, k := range keys {
		if string(k) == string(surv) || string(k) == string(emb) {
			continue
		}
		s.Delete(k)
	}

	fill := s.arena.fillOf(seg0)
	dead := s.arena.deadOf(seg0)
	if dead*s.segDeadDen < fill*s.segDeadNum {
		t.Fatalf("segment %d dead/fill %d/%d under the threshold; test setup broken", seg0, dead, fill)
	}
	_, oldSurv, _ := s.findEntry(Hash(surv), surv)
	if si, _ := s.arena.segOf(oldSurv); si != seg0 {
		// The TTL republish moved the survivor off the victim; that is fine,
		// the embedded one is still there.
		if si2, _ := s.arena.segOf(func() uint64 { _, a, _ := s.findEntry(Hash(emb), emb); return a }()); si2 != seg0 {
			t.Skip("no survivor left in the victim segment; sizing changed")
		}
	}

	if freed := s.CompactArena(); freed < 1 {
		t.Fatalf("CompactArena freed %d segments, want at least 1", freed)
	}
	if f := s.arena.fillOf(seg0); f != 0 {
		t.Fatalf("victim segment %d still holds %d bytes", seg0, f)
	}
	mustGet(t, s, surv, string(sepVal('T', 4096)))
	mustGet(t, s, emb, "embedded-survivor")
	_, addr, _ := s.findEntry(Hash(surv), surv)
	if at := s.expireAt(addr); at != 99_000 {
		t.Fatalf("survivor deadline %d after the move, want 99000", at)
	}
	if si, _ := s.arena.segOf(addr); si == seg0 {
		t.Fatal("survivor still addresses the freed segment")
	}
}

// TestCompactChunkedSurvivor moves a chunked record, its directory, and its
// arena chunks across a forced compaction and reads the value back whole.
func TestCompactChunkedSurvivor(t *testing.T) {
	s := testStore(t, 12)
	val := make([]byte, 3*strChunkMin/2) // two chunks, one partial
	for i := range val {
		val[i] = byte(i * 31)
	}
	if err := s.Set([]byte("big"), val); err != nil {
		t.Fatal(err)
	}
	// Surround with churn so the chunks' segments go mostly dead.
	_, keys := fillSepSeg(t, s, "pad", 4096)
	for _, k := range keys {
		s.Delete(k)
	}
	// Mark every touched non-current segment a victim by tuning the
	// threshold to zero, so the chunked tenant is guaranteed to move.
	s.TuneArenaReclaim(0, 1)
	if freed := s.CompactArena(); freed < 1 {
		t.Fatalf("CompactArena freed %d segments, want at least 1", freed)
	}
	got, ok := s.Get([]byte("big"), nil)
	if !ok || !bytes.Equal(got, val) {
		t.Fatalf("chunked value corrupt after compaction: ok=%v len=%d", ok, len(got))
	}
}

// TestOpenStreamPinsArena pins the stream rule: while a ChunkStream is open
// the compactor refuses to run, and after Release it runs again.
func TestOpenStreamPinsArena(t *testing.T) {
	s := testStore(t, 12)
	val := make([]byte, strChunkMin)
	if err := s.Set([]byte("big"), val); err != nil {
		t.Fatal(err)
	}
	_, cs, ok := s.GetStream([]byte("big"), 0, nil)
	if !ok || cs == nil {
		t.Fatal("GetStream did not return a stream")
	}
	_, keys := fillSepSeg(t, s, "pad", 4096)
	for _, k := range keys {
		s.Delete(k)
	}
	s.TuneArenaReclaim(0, 1)
	if freed := s.CompactArena(); freed != 0 {
		t.Fatalf("CompactArena ran under an open stream, freed %d", freed)
	}
	cs.Release()
	cs.Release() // idempotent
	if freed := s.CompactArena(); freed < 1 {
		t.Fatal("CompactArena did not run after the stream released")
	}
}

// TestOverwriteSteadyState is the gate scenario in miniature: a small arena,
// separated-band values, overwrite forever with varying sizes so runs churn,
// compacting at simulated drain boundaries. The arena must never report full
// and the fill must stay bounded under its total.
func TestOverwriteSteadyState(t *testing.T) {
	s := testStore(t, 8)
	const nKeys = 32
	sizes := []int{1536, 2048, 3072, 4096, 6144}
	for i := 0; i < 20_000; i++ {
		k := []byte(fmt.Sprintf("key%03d", i%nKeys))
		v := sepVal(byte('a'+i%26), sizes[(i*7)%len(sizes)])
		if err := s.Set(k, v); err != nil {
			t.Fatalf("op %d: %v", i, err)
		}
		if i%256 == 0 && s.ArenaTight() {
			s.CompactArena()
		}
	}
	used, total := s.ArenaBytes()
	if used > total {
		t.Fatalf("arena fill %d over total %d", used, total)
	}
	for i := 0; i < nKeys; i++ {
		k := []byte(fmt.Sprintf("key%03d", i))
		if _, ok := s.Get(k, nil); !ok {
			t.Fatalf("key %d missing after churn", i)
		}
	}
}

// TestArenaFullBackstop pins the write path's synchronous reclaim: with the
// free list empty and a fully dead segment sitting there, a SET that would
// have reported arena full frees it mid-command and succeeds.
func TestArenaFullBackstop(t *testing.T) {
	s := testStore(t, 2)
	_, keysA := fillSepSeg(t, s, "a", 4096)
	for _, k := range keysA {
		s.Delete(k)
	}
	// Segment 0 is fully dead but on nobody's free list. Now write until the
	// remaining segments would run out; the backstop must free segment 0
	// instead of surfacing ErrFull.
	var keysB [][]byte
	for i := 0; i < 3*len(keysA); i++ {
		k := []byte(fmt.Sprintf("b%06d", i))
		if err := s.Set(k, sepVal('b', 4096)); err != nil {
			t.Fatalf("op %d: %v", i, err)
		}
		keysB = append(keysB, k)
		if len(keysB) > len(keysA) {
			s.Delete(keysB[0])
			keysB = keysB[1:]
		}
	}
	for _, k := range keysB {
		mustGet(t, s, k, string(sepVal('b', 4096)))
	}
}

// TestArenaGenuinelyFull keeps the error path honest: when the live bytes
// exceed what the arena can hold, ErrFull still surfaces and the store stays
// readable.
func TestArenaGenuinelyFull(t *testing.T) {
	s := testStore(t, 2)
	var keys [][]byte
	var sawFull bool
	for i := 0; i < 10_000; i++ {
		k := []byte(fmt.Sprintf("k%06d", i))
		err := s.Set(k, sepVal('x', 4096))
		if err == ErrFull {
			sawFull = true
			break
		}
		if err != nil {
			t.Fatalf("op %d: %v", i, err)
		}
		keys = append(keys, k)
	}
	if !sawFull {
		t.Fatal("live fill never reported ErrFull")
	}
	for _, k := range keys {
		mustGet(t, s, k, string(sepVal('x', 4096)))
	}
}
