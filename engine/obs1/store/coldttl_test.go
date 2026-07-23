package store

import (
	"bytes"
	"encoding/binary"
	"testing"
)

// The cold plane's TTL projection (spec 2064/obs1 doc 08): a record with a
// deadline demotes with the deadline as the frame's trailing expiry word, the
// bring-up restores the expiry slot, and the cold read path reaps a fired
// frame lazily, the doc 09 rule extended to the cold tier.

// TestColdFrameTTLRoundTrip frames a TTL record and holds the codec facts: the
// expiry word rides the tail, the value region excludes it, a TTL-free frame
// is byte-identical to the pre-projection layout, and a truncation through the
// expiry word refuses to decode.
func TestColdFrameTTLRoundTrip(t *testing.T) {
	s := coldStore(t)
	const at = int64(1 << 41)
	if err := s.SetString([]byte("ttl"), []byte("value-bytes"), 1, at, false); err != nil {
		t.Fatal(err)
	}
	frame := s.frameRecord(addrOf(s, "ttl"), nil)
	fr, n, err := decodeColdFrame(frame)
	if err != nil || n != len(frame) {
		t.Fatalf("decode: %v, consumed %d of %d", err, n, len(frame))
	}
	if fr.flags&flagHasTTL == 0 || fr.exp != uint64(at) {
		t.Fatalf("frame flags %#x exp %d, want the TTL flag and deadline %d", fr.flags, fr.exp, at)
	}
	if string(fr.key) != "ttl" || string(fr.value) != "value-bytes" {
		t.Fatalf("frame carries key %q value %q; the expiry word leaked into a region", fr.key, fr.value)
	}
	if got := binary.LittleEndian.Uint64(frame[len(frame)-coldExpSize:]); got != uint64(at) {
		t.Fatalf("expiry word at the tail reads %d, want %d", got, at)
	}

	// Truncating into the expiry word must refuse, same as any torn tail.
	if _, _, err := decodeColdFrame(frame[:len(frame)-1]); err != errColdShort {
		t.Fatalf("torn expiry word decoded with err %v, want errColdShort", err)
	}
	// A TTL total too small to hold the word must refuse rather than alias.
	bad := append([]byte(nil), frame...)
	binary.LittleEndian.PutUint32(bad[0:], coldHdr+3+coldExpSize-1) // klen is 3
	if _, _, err := decodeColdFrame(bad[:coldHdr+3+coldExpSize-1]); err != errColdShort {
		t.Fatalf("undersized TTL total decoded with err %v, want errColdShort", err)
	}

	// A TTL-free record's frame has no tail word: byte-identical layout.
	if err := s.Set([]byte("plain"), []byte("value-bytes")); err != nil {
		t.Fatal(err)
	}
	pf := s.frameRecord(addrOf(s, "plain"), nil)
	want := appendColdFrame(nil, kindString, 0, 11, []byte("plain"), []byte("value-bytes"), 0)
	if !bytes.Equal(pf, want) {
		t.Fatal("a TTL-free frame is not byte-identical to the pre-projection layout")
	}
}

// TestColdTTLDemoteServeBringUp demotes a TTL key and drives the three answers
// a live deadline must keep giving: the cold read serves the value before the
// deadline, ExpireAt reads the deadline from the frame, and a bring-up
// restores the expiry slot so the resident record fires on time.
func TestColdTTLDemoteServeBringUp(t *testing.T) {
	s := coldStore(t)
	const at = int64(5000)
	if err := s.SetString([]byte("k"), []byte("v"), 1, at, false); err != nil {
		t.Fatal(err)
	}
	if !s.DemoteKey([]byte("k")) {
		t.Fatal("a TTL key refused to demote")
	}
	if !s.slotIsCold([]byte("k")) {
		t.Fatal("the key is not cold after DemoteKey")
	}
	if v, ok := s.GetString([]byte("k"), at-1, nil); !ok || string(v) != "v" {
		t.Fatalf("cold read before the deadline: %q %v, want v", v, ok)
	}
	if got := s.ExpireAt([]byte("k"), at-1); got != at {
		t.Fatalf("cold ExpireAt = %d, want %d", got, at)
	}

	h := Hash([]byte("k"))
	slot, off, _ := s.findLive(h, []byte("k"), at-1)
	if noff := s.bringUp(h, slot, off); noff == off || slotCold(*slot) {
		t.Fatal("bring-up left the key cold")
	}
	if got := s.ExpireAt([]byte("k"), at-1); got != at {
		t.Fatalf("resident ExpireAt after bring-up = %d, want %d", got, at)
	}
	if _, ok := s.GetString([]byte("k"), at, nil); ok {
		t.Fatal("the brought-up record survived its deadline")
	}
	assertCensus(t, s)
}

// TestColdTTLExpiredReadReaps holds the lazy-expiry rule on the cold tier: a
// read past the deadline answers absent, drops the index entry, and leaves the
// frame unreferenced for the region's compaction.
func TestColdTTLExpiredReadReaps(t *testing.T) {
	s := coldStore(t)
	const at = int64(5000)
	if err := s.SetString([]byte("k"), []byte("v"), 1, at, false); err != nil {
		t.Fatal(err)
	}
	if !s.DemoteKey([]byte("k")) {
		t.Fatal("a TTL key refused to demote")
	}
	before := s.Len()
	if _, ok := s.GetString([]byte("k"), at, nil); ok {
		t.Fatal("a fired cold record served its value")
	}
	if s.Exists([]byte("k"), at) {
		t.Fatal("a fired cold record still exists")
	}
	if s.Len() != before-1 {
		t.Fatalf("count %d after the reap, want %d", s.Len(), before-1)
	}
	if s.Cold().Records != 0 {
		t.Fatalf("cold census %d after the reap, want 0", s.Cold().Records)
	}
	assertCensus(t, s)
}

// TestStagedTTLFrameCarriesExp drives the staged drain over TTL records,
// embedded and separated band, and reads the deadlines back through the fold
// walker: the frames the fold tap hears carry the expiry word, which is what
// the folder's per-chunk bounds are built from.
func TestStagedTTLFrameCarriesExp(t *testing.T) {
	const cap = 1 << 20
	s := migratorStore(t, cap)
	const at = int64(1 << 41)
	if err := s.SetString([]byte("ttl-emb"), []byte("small"), 1, at, false); err != nil {
		t.Fatal(err)
	}
	if err := s.SetString([]byte("ttl-sep"), bytes.Repeat([]byte("z"), 4096), 1, at+1, false); err != nil {
		t.Fatal(err)
	}
	fillSmall(t, s, 40000)
	if s.arena.live() <= cap {
		t.Fatal("fixture did not cross the cap")
	}
	deep := 0
	exps := map[string]uint64{}
	for {
		buf := make([]byte, 0, 1<<20)
		d := s.StageColdDrainDeep(buf)
		if d == nil || len(d.flips) == 0 {
			break
		}
		if err := WalkStagedFrames(d.buf, func(f FoldFrame) error {
			if f.Exp != 0 {
				exps[string(f.Key)] = f.Exp
			}
			if f.Flags&flagHasTTL != 0 && f.Exp == 0 {
				t.Errorf("key %q staged with the TTL flag but no deadline", f.Key)
			}
			return nil
		}); err != nil {
			t.Fatalf("walk: %v", err)
		}
		if nw, err := s.ColdWriteAt(d.Off(), d.Buf()); err != nil || nw != len(d.Buf()) {
			t.Fatalf("cold write: n=%d err=%v", nw, err)
		}
		s.CompleteColdDrain(d, true)
		if deep++; deep > 200 {
			t.Fatal("the deep drain did not converge")
		}
	}
	if exps["ttl-emb"] != uint64(at) || exps["ttl-sep"] != uint64(at+1) {
		t.Fatalf("staged deadlines %v, want ttl-emb=%d ttl-sep=%d", exps, at, at+1)
	}
	// The separated bearer staged its resolved bytes; its frame must read back
	// whole with the deadline intact.
	if v, ok := s.GetString([]byte("ttl-sep"), at, nil); !ok || len(v) != 4096 {
		t.Fatalf("staged separated TTL key read %d bytes %v, want 4096", len(v), ok)
	}
	if got := s.ExpireAt([]byte("ttl-sep"), at); got != at+1 {
		t.Fatalf("staged separated TTL key deadline %d, want %d", got, at+1)
	}
	assertCensus(t, s)
}
