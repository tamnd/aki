package sqlo1b

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"testing"

	"github.com/tamnd/aki/engine/sqlo1"
)

// storeRig drives a Store next to a shadow map so every test asserts
// against the same oracle: shadow keys must Get their latest value,
// absent keys must miss, Scan must deliver exactly the live set.
type storeRig struct {
	s    *Store
	path string
	sh   map[string]sqlo1.Record
	seq  int64
	now  int64
}

func newStoreRig(t *testing.T) *storeRig {
	t.Helper()
	path := filepath.Join(t.TempDir(), "b.aki")
	s, err := CreateStore(path, 1<<16)
	if err != nil {
		t.Fatal(err)
	}
	r := &storeRig{s: s, path: path, sh: map[string]sqlo1.Record{}, now: 1_000_000}
	s.nowMS = func() int64 { return r.now }
	t.Cleanup(func() { s.Close() })
	return r
}

func (r *storeRig) reopen(t *testing.T) {
	t.Helper()
	if err := r.s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err := OpenStore(r.path, 1<<16)
	if err != nil {
		t.Fatal(err)
	}
	s.nowMS = func() int64 { return r.now }
	r.s = s
	t.Cleanup(func() { s.Close() })
}

func (r *storeRig) apply(t *testing.T, ops ...sqlo1.Op) {
	t.Helper()
	r.seq++
	if err := r.s.ApplyBatch(context.Background(), &sqlo1.DrainBatch{Seq: r.seq, Ops: ops}); err != nil {
		t.Fatalf("batch %d: %v", r.seq, err)
	}
	for _, op := range ops {
		if op.Del {
			delete(r.sh, string(op.Rec.Key))
			continue
		}
		r.sh[string(op.Rec.Key)] = sqlo1.Record{
			Key:      bytes.Clone(op.Rec.Key),
			Value:    bytes.Clone(op.Rec.Value),
			ExpireMs: op.Rec.ExpireMs,
			Gen:      op.Rec.Gen,
		}
	}
}

func putOp(key string, val []byte, expireMS int64) sqlo1.Op {
	return sqlo1.Op{Rec: sqlo1.Record{Key: []byte(key), Value: val, ExpireMs: expireMS}}
}

func delOp(key string) sqlo1.Op {
	return sqlo1.Op{Del: true, Rec: sqlo1.Record{Key: []byte(key)}}
}

// verify checks every shadow key through Get, one guaranteed miss,
// and the stats counters.
func (r *storeRig) verify(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	for k, want := range r.sh {
		got, err := r.s.Get(ctx, []byte(k))
		if want.ExpireMs != 0 && want.ExpireMs <= r.now {
			if !errors.Is(err, sqlo1.ErrNotFound) {
				t.Fatalf("expired %q: got %v, want ErrNotFound", k, err)
			}
			continue
		}
		if err != nil {
			t.Fatalf("get %q: %v", k, err)
		}
		if !bytes.Equal(got.Value, want.Value) || got.ExpireMs != want.ExpireMs || got.Gen != want.Gen {
			t.Fatalf("get %q: got (%d bytes, exp %d, gen %d), want (%d bytes, exp %d, gen %d)",
				k, len(got.Value), got.ExpireMs, got.Gen, len(want.Value), want.ExpireMs, want.Gen)
		}
	}
	if _, err := r.s.Get(ctx, []byte("no such key anywhere")); !errors.Is(err, sqlo1.ErrNotFound) {
		t.Fatalf("miss: got %v, want ErrNotFound", err)
	}
	st := r.s.Stats()
	if st.Keys != int64(len(r.sh)) {
		t.Fatalf("stats keys %d, shadow holds %d", st.Keys, len(r.sh))
	}
	if st.HighWater != r.seq {
		t.Fatalf("stats high-water %d, want %d", st.HighWater, r.seq)
	}
}

// liveShadow is the shadow minus expired records: what Scan owes.
func (r *storeRig) liveShadow() map[string][]byte {
	out := map[string][]byte{}
	for k, v := range r.sh {
		if v.ExpireMs != 0 && v.ExpireMs <= r.now {
			continue
		}
		out[k] = v.Value
	}
	return out
}

func (r *storeRig) scanAll(t *testing.T) map[string][]byte {
	t.Helper()
	got := map[string][]byte{}
	cur, err := r.s.Scan(context.Background(), nil, func(rec sqlo1.Record) bool {
		got[string(rec.Key)] = bytes.Clone(rec.Value)
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
	if cur != nil {
		t.Fatalf("full scan returned cursor %v", cur)
	}
	return got
}

func (r *storeRig) verifyScan(t *testing.T) {
	t.Helper()
	got := r.scanAll(t)
	want := r.liveShadow()
	if len(got) != len(want) {
		t.Fatalf("scan delivered %d records, want %d", len(got), len(want))
	}
	for k, v := range want {
		if !bytes.Equal(got[k], v) {
			t.Fatalf("scan %q: %d bytes, want %d", k, len(got[k]), len(v))
		}
	}
}

func TestStoreRoundtrip(t *testing.T) {
	r := newStoreRig(t)
	ctx := context.Background()
	blob := bytes.Repeat([]byte("blobby"), 1500) // 9000 bytes, past BlobThreshold
	sub := bytes.Repeat([]byte{0xAB}, SubkeySize)
	r.apply(t,
		putOp("alpha", []byte("one"), 0),
		putOp("keeps", []byte("until later"), r.now+60_000),
		putOp("gone", []byte("already dead"), r.now-1),
		sqlo1.Op{Rec: sqlo1.Record{Key: sub, Value: []byte("segment payload"), Gen: 7}},
		putOp("big", blob, 0),
	)
	r.verify(t)

	got, err := r.s.Get(ctx, sub)
	if err != nil {
		t.Fatal(err)
	}
	if got.Gen != 7 || !bytes.Equal(got.Value, []byte("segment payload")) {
		t.Fatalf("segment record came back %+v", got)
	}

	out, err := r.s.BatchGet(ctx, [][]byte{[]byte("alpha"), []byte("missing"), []byte("big"), []byte("gone")})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out[0].Value, []byte("one")) || !bytes.Equal(out[2].Value, blob) {
		t.Fatal("batchget hits came back wrong")
	}
	if out[1].Key != nil || out[3].Key != nil {
		t.Fatal("batchget misses must have nil keys")
	}

	// A nonzero Gen on anything but a 16-byte subkey fails envelope
	// validation before any frame is emitted, so the store stays
	// usable and the high-water does not move.
	err = r.s.ApplyBatch(ctx, &sqlo1.DrainBatch{Seq: r.seq + 1, Ops: []sqlo1.Op{
		{Rec: sqlo1.Record{Key: []byte("short"), Value: []byte("x"), Gen: 3}},
	}})
	if err == nil {
		t.Fatal("gen record with a 5-byte key was accepted")
	}
	r.verify(t)
}

func TestStoreExactlyOnce(t *testing.T) {
	r := newStoreRig(t)
	ctx := context.Background()
	r.apply(t, putOp("k", []byte("v1"), 0))
	r.apply(t, putOp("k", []byte("v2"), 0))
	r.apply(t, putOp("k", []byte("v3"), 0))
	// Re-delivered batches at or below the high-water must be
	// swallowed whole, out of order included.
	for _, seq := range []int64{2, 1, 3} {
		err := r.s.ApplyBatch(ctx, &sqlo1.DrainBatch{Seq: seq, Ops: []sqlo1.Op{
			{Rec: sqlo1.Record{Key: []byte("k"), Value: []byte("stale")}},
		}})
		if err != nil {
			t.Fatalf("replayed batch %d: %v", seq, err)
		}
	}
	got, err := r.s.Get(ctx, []byte("k"))
	if err != nil || !bytes.Equal(got.Value, []byte("v3")) {
		t.Fatalf("got %q err %v, want v3", got.Value, err)
	}
	if hw := r.s.Stats().HighWater; hw != 3 {
		t.Fatalf("high-water %d after replays, want 3", hw)
	}
}

func TestStoreOverwriteDelete(t *testing.T) {
	r := newStoreRig(t)
	ctx := context.Background()
	r.apply(t, putOp("k", []byte("v1"), 0), putOp("j", []byte("other"), 0))
	r.apply(t, putOp("k", []byte("v2"), 0))
	r.verify(t)
	r.apply(t, delOp("k"))
	if _, err := r.s.Get(ctx, []byte("k")); !errors.Is(err, sqlo1.ErrNotFound) {
		t.Fatalf("deleted key: %v", err)
	}
	// Absent-key deletes are seam no-ops, and a key can come back in
	// the same batch that re-deletes it.
	r.apply(t, delOp("k"), delOp("never-there"), putOp("k", []byte("v3"), 0))
	r.verify(t)
	if st := r.s.Stats(); st.Keys != 2 {
		t.Fatalf("stats keys %d, want 2", st.Keys)
	}
}

func TestStoreExpiry(t *testing.T) {
	r := newStoreRig(t)
	ctx := context.Background()
	r.apply(t, putOp("fades", []byte("soon"), r.now+1000), putOp("stays", []byte("forever"), 0))
	if _, err := r.s.Get(ctx, []byte("fades")); err != nil {
		t.Fatalf("pre-expiry get: %v", err)
	}
	r.now += 1001
	if _, err := r.s.Get(ctx, []byte("fades")); !errors.Is(err, sqlo1.ErrNotFound) {
		t.Fatalf("post-expiry get: %v", err)
	}
	r.verifyScan(t)
	// Overwriting an expired record revives the key.
	r.apply(t, putOp("fades", []byte("again"), r.now+5000))
	got, err := r.s.Get(ctx, []byte("fades"))
	if err != nil || !bytes.Equal(got.Value, []byte("again")) {
		t.Fatalf("revived key: %q err %v", got.Value, err)
	}
	r.verifyScan(t)
}

// mixedTraffic drives n keyed puts through the rig in batches, with
// every fifth key overwritten, every seventh deleted, and every
// hundredth a blob. Deterministic, so shapes reproduce.
func mixedTraffic(t *testing.T, r *storeRig, base, n, batch int) {
	t.Helper()
	rng := rand.New(rand.NewPCG(uint64(base)+7, 11))
	var ops []sqlo1.Op
	flush := func() {
		if len(ops) > 0 {
			r.apply(t, ops...)
			ops = nil
		}
	}
	for i := base; i < base+n; i++ {
		var val []byte
		if i%100 == 99 {
			val = make([]byte, 4200+rng.IntN(3000))
		} else {
			val = make([]byte, 20+rng.IntN(20))
		}
		for j := range val {
			val[j] = byte(rng.IntN(256))
		}
		ops = append(ops, putOp(fmt.Sprintf("key%06d", i), val, 0))
		if i%5 == 4 && i-2 >= base {
			ops = append(ops, putOp(fmt.Sprintf("key%06d", i-2), []byte("rewritten"), 0))
		}
		if i%7 == 6 && i-3 >= base {
			ops = append(ops, delOp(fmt.Sprintf("key%06d", i-3)))
		}
		if len(ops) >= batch {
			flush()
		}
	}
	flush()
}

// TestStoreSplitsOracle runs enough keys to push linear hashing past
// level 9, where SplitBucket's refresh path rebases windows, with a
// checkpoint in the middle so both the dirty-over-cold and the
// all-cold shapes serve the same oracle.
func TestStoreSplitsOracle(t *testing.T) {
	r := newStoreRig(t)
	mixedTraffic(t, r, 0, 12_000, 400)
	if err := r.s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if len(r.s.dirty) != 0 {
		t.Fatalf("checkpoint left %d dirty buckets", len(r.s.dirty))
	}
	r.verify(t) // all cold
	mixedTraffic(t, r, 12_000, 13_000, 400)
	if nb := NumBuckets(r.s.level, r.s.split); nb <= 512 {
		t.Fatalf("only %d buckets, the refresh path needs more than 512", nb)
	}
	r.verify(t) // dirty over cold
	r.verifyScan(t)
	if err := r.s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	r.verify(t)
	r.verifyScan(t)
}

func TestStoreScanPaged(t *testing.T) {
	r := newStoreRig(t)
	mixedTraffic(t, r, 0, 3_000, 250)
	got := map[string]bool{}
	var cur sqlo1.Cursor
	pages := 0
	for {
		n := 0
		next, err := r.s.Scan(context.Background(), cur, func(rec sqlo1.Record) bool {
			got[string(rec.Key)] = true
			n++
			return n < 500
		})
		if err != nil {
			t.Fatal(err)
		}
		pages++
		if next == nil {
			break
		}
		cur = next
	}
	if pages < 2 {
		t.Fatalf("scan finished in %d pages, the paging path never ran", pages)
	}
	want := r.liveShadow()
	if len(got) != len(want) {
		t.Fatalf("paged scan delivered %d keys, want %d", len(got), len(want))
	}
	for k := range want {
		if !got[k] {
			t.Fatalf("paged scan missed %q", k)
		}
	}
}

// TestStoreReopen walks the durable path: checkpointed base plus a
// replayed WAL tail, twice.
func TestStoreReopen(t *testing.T) {
	r := newStoreRig(t)
	mixedTraffic(t, r, 0, 3_000, 250)
	r.apply(t, putOp("expiring", []byte("live"), r.now+90_000),
		sqlo1.Op{Rec: sqlo1.Record{Key: bytes.Repeat([]byte{0xCD}, SubkeySize), Value: []byte("seg"), Gen: 12}})
	if err := r.s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	mixedTraffic(t, r, 3_000, 500, 200) // tail past the checkpoint
	r.reopen(t)
	r.verify(t)
	r.verifyScan(t)
	mixedTraffic(t, r, 3_500, 500, 200)
	if err := r.s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	r.reopen(t)
	r.verify(t)
	r.verifyScan(t)
}

// TestStoreReopenNoCheckpoint rebuilds everything from a pure WAL
// replay: no checkpoint ever committed, the superblock is still the
// creation image.
func TestStoreReopenNoCheckpoint(t *testing.T) {
	r := newStoreRig(t)
	mixedTraffic(t, r, 0, 600, 150)
	r.reopen(t)
	r.verify(t)
	r.verifyScan(t)
}

// TestStoreCheckpointCrash aborts a checkpoint at every step
// boundary. The store must keep serving from RAM, accept traffic, be
// able to checkpoint again, and reopen correctly from whichever
// superblock survived on disk.
func TestStoreCheckpointCrash(t *testing.T) {
	for step := 1; step <= 5; step++ {
		t.Run(fmt.Sprintf("crash-after-step-%d", step), func(t *testing.T) {
			r := newStoreRig(t)
			mixedTraffic(t, r, 0, 800, 200)
			r.s.ckptCrash = func(s int) error {
				if s == step {
					return errBoom
				}
				return nil
			}
			if err := r.s.Checkpoint(); !errors.Is(err, errBoom) {
				t.Fatalf("crashed checkpoint returned %v", err)
			}
			r.s.ckptCrash = nil
			r.verify(t) // RAM still authoritative
			r.apply(t, putOp("post-crash", []byte("still writing"), 0))
			if err := r.s.Checkpoint(); err != nil {
				t.Fatalf("checkpoint after the crashed one: %v", err)
			}
			r.verify(t)
			r.reopen(t)
			r.verify(t)
			r.verifyScan(t)
		})
	}
}

// TestStoreCheckpointCrashThenReopen crashes the checkpoint and goes
// straight to recovery without a second checkpoint, so the reopened
// state comes from the old superblock plus replay (steps 1..4) or
// the new superblock plus the post-freeze tail (step 5).
func TestStoreCheckpointCrashThenReopen(t *testing.T) {
	for step := 1; step <= 5; step++ {
		t.Run(fmt.Sprintf("crash-after-step-%d", step), func(t *testing.T) {
			r := newStoreRig(t)
			mixedTraffic(t, r, 0, 800, 200)
			r.s.ckptCrash = func(s int) error {
				if s == step {
					return errBoom
				}
				return nil
			}
			if err := r.s.Checkpoint(); !errors.Is(err, errBoom) {
				t.Fatalf("crashed checkpoint returned %v", err)
			}
			r.s.ckptCrash = nil
			r.apply(t, putOp("post-crash", []byte("tail write"), 0))
			r.reopen(t)
			r.verify(t)
			r.verifyScan(t)
		})
	}
}

// faultStoreFile plugs a FaultFile under the store; Truncate has no
// torn-write story to model, so it passes through to the base file.
type faultStoreFile struct {
	*FaultFile
	base *os.File
}

func (f faultStoreFile) Truncate(n int64) error { return f.base.Truncate(n) }

// TestStoreTornDataWrites cuts power on the data file: every write
// after the last checkpoint vanishes (KeepNone), the real-file WAL
// survives, and recovery must rebuild every acknowledged batch.
func TestStoreTornDataWrites(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "b.aki")
	base, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer base.Close()
	ff := NewFaultFile(base)
	s, err := CreateStoreOn(faultStoreFile{ff, base}, sqlo1.WALPath(path), 1<<16)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	sh := map[string][]byte{}
	seq := int64(0)
	apply := func(ops ...sqlo1.Op) {
		seq++
		if err := s.ApplyBatch(ctx, &sqlo1.DrainBatch{Seq: seq, Ops: ops}); err != nil {
			t.Fatalf("batch %d: %v", seq, err)
		}
		for _, op := range ops {
			if op.Del {
				delete(sh, string(op.Rec.Key))
			} else {
				sh[string(op.Rec.Key)] = bytes.Clone(op.Rec.Value)
			}
		}
	}

	apply(putOp("base1", []byte("checkpointed"), 0), putOp("base2", bytes.Repeat([]byte("L"), 5000), 0))
	apply(putOp("base3", []byte("also checkpointed"), 0))
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	apply(putOp("tail1", []byte("acknowledged"), 0), delOp("base3"))
	apply(putOp("tail2", bytes.Repeat([]byte("M"), 4500), 0), putOp("base1", []byte("rewritten"), 0))

	// Power cut: unsynced data-file writes are gone, the sidecar is
	// not (it syncs per batch on its own file).
	if err := ff.Crash(KeepNone); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := OpenStore(path, 1<<16)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	for k, want := range sh {
		got, err := s2.Get(ctx, []byte(k))
		if err != nil {
			t.Fatalf("get %q after torn writes: %v", k, err)
		}
		if !bytes.Equal(got.Value, want) {
			t.Fatalf("get %q: %d bytes, want %d", k, len(got.Value), len(want))
		}
	}
	if _, err := s2.Get(ctx, []byte("base3")); !errors.Is(err, sqlo1.ErrNotFound) {
		t.Fatalf("deleted key survived the replay: %v", err)
	}
	if hw := s2.Stats().HighWater; hw != seq {
		t.Fatalf("high-water %d after recovery, want %d", hw, seq)
	}
}

// TestStorePutDelPayloadCodecs pins the doc 03 section 12.2 payload
// shapes through their roundtrips and rejects the truncations.
func TestStorePutDelPayloadCodecs(t *testing.T) {
	rec := &Record{
		RType:    RecSeg,
		RFlags:   RFlagExpiry | RFlagRootgen,
		Key:      bytes.Repeat([]byte{0x11}, SubkeySize),
		Value:    []byte("segment"),
		ExpireMS: 123456,
		Rootgen:  9,
	}
	pay, err := EncodePutPayload(rec)
	if err != nil {
		t.Fatal(err)
	}
	back, err := DecodePutPayload(pay)
	if err != nil {
		t.Fatal(err)
	}
	if back.RType != rec.RType || back.ExpireMS != rec.ExpireMS || back.Rootgen != rec.Rootgen ||
		!bytes.Equal(back.Key, rec.Key) || !bytes.Equal(back.Value, rec.Value) {
		t.Fatalf("PUT roundtrip: %+v", back)
	}
	if _, err := DecodePutPayload(pay[:len(pay)-1]); err == nil {
		t.Fatal("truncated PUT payload decoded")
	}

	dp, err := EncodeDelPayload([]byte("some key"))
	if err != nil {
		t.Fatal(err)
	}
	key, err := DecodeDelPayload(dp)
	if err != nil || !bytes.Equal(key, []byte("some key")) {
		t.Fatalf("DEL roundtrip: %q err %v", key, err)
	}
	if _, err := DecodeDelPayload(dp[:len(dp)-2]); err == nil {
		t.Fatal("truncated DEL payload decoded")
	}

	mark, err := EncodeMarkPayload(42)
	if err != nil {
		t.Fatal(err)
	}
	mrec, err := DecodePutPayload(mark)
	if err != nil {
		t.Fatal(err)
	}
	seq, isMark, err := MarkSeq(mrec)
	if err != nil || !isMark || seq != 42 {
		t.Fatalf("mark roundtrip: seq %d mark %v err %v", seq, isMark, err)
	}
	if _, _, err := MarkSeq(&Record{RType: RecMeta, Key: []byte("mystery"), Value: []byte("x")}); err == nil {
		t.Fatal("unknown meta key accepted")
	}
}
