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
	gens map[uint64]uint32
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
	r := &storeRig{s: s, path: path, sh: map[string]sqlo1.Record{}, gens: map[uint64]uint32{}, now: 1_000_000}
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
			Root:     op.Rec.Root,
		}
	}
}

func putOp(key string, val []byte, expireMS int64) sqlo1.Op {
	return sqlo1.Op{Rec: sqlo1.Record{Key: []byte(key), Value: val, ExpireMs: expireMS}}
}

func delOp(key string) sqlo1.Op {
	return sqlo1.Op{Del: true, Rec: sqlo1.Record{Key: []byte(key)}}
}

// applyBumps is apply with seam Bumps riding the batch, shadowed into
// the rig's generation table the same way genbump shadows GenBump.
func (r *storeRig) applyBumps(t *testing.T, ops []sqlo1.Op, bumps ...sqlo1.Bump) {
	t.Helper()
	r.seq++
	if err := r.s.ApplyBatch(context.Background(), &sqlo1.DrainBatch{Seq: r.seq, Ops: ops, Bumps: bumps}); err != nil {
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
			Root:     op.Rec.Root,
		}
	}
	for _, bp := range bumps {
		if bp.NewGen > r.gens[bp.Rooth] {
			r.gens[bp.Rooth] = bp.NewGen
		}
	}
}

// genbump mirrors GenBump into the rig's shadow generation table so
// verify can account for the generation records in Stats.Keys.
func (r *storeRig) genbump(t *testing.T, rooth uint64, newgen uint32) {
	t.Helper()
	if err := r.s.GenBump(context.Background(), rooth, newgen); err != nil {
		t.Fatalf("genbump %#x to %d: %v", rooth, newgen, err)
	}
	if newgen > r.gens[rooth] {
		r.gens[rooth] = newgen
	}
}

func (r *storeRig) checkLive(t *testing.T, rooth uint64, rootgen uint32, want bool) {
	t.Helper()
	got, err := r.s.RootLive(rooth, rootgen)
	if err != nil {
		t.Fatalf("rootlive %#x gen %d: %v", rooth, rootgen, err)
	}
	if got != want {
		t.Fatalf("rootlive %#x gen %d: got %v, want %v", rooth, rootgen, got, want)
	}
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
		if !bytes.Equal(got.Value, want.Value) || got.ExpireMs != want.ExpireMs || got.Gen != want.Gen || got.Root != want.Root {
			t.Fatalf("get %q: got (%d bytes, exp %d, gen %d, root %v), want (%d bytes, exp %d, gen %d, root %v)",
				k, len(got.Value), got.ExpireMs, got.Gen, got.Root, len(want.Value), want.ExpireMs, want.Gen, want.Root)
		}
	}
	if _, err := r.s.Get(ctx, []byte("no such key anywhere")); !errors.Is(err, sqlo1.ErrNotFound) {
		t.Fatalf("miss: got %v, want ErrNotFound", err)
	}
	st := r.s.Stats()
	if st.Keys != int64(len(r.sh)+len(r.gens)) {
		t.Fatalf("stats keys %d, shadow holds %d plus %d generation records", st.Keys, len(r.sh), len(r.gens))
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

// TestStoreGenbumpPayloadCodec pins the doc 03 section 12.2 op 4
// shape: klen u16, newgen u32, key, with generation 0 rejected on
// both sides.
func TestStoreGenbumpPayloadCodec(t *testing.T) {
	key := GenKey(0x1122334455667788)
	pay, err := EncodeGenbumpPayload(key, 9)
	if err != nil {
		t.Fatal(err)
	}
	back, gen, err := DecodeGenbumpPayload(pay)
	if err != nil || gen != 9 || !bytes.Equal(back, key) {
		t.Fatalf("GENBUMP roundtrip: key %x gen %d err %v", back, gen, err)
	}
	if _, _, err := DecodeGenbumpPayload(pay[:len(pay)-1]); err == nil {
		t.Fatal("truncated GENBUMP payload decoded")
	}
	if _, _, err := DecodeGenbumpPayload(pay[:5]); err == nil {
		t.Fatal("headerless GENBUMP payload decoded")
	}
	if _, err := EncodeGenbumpPayload(key, 0); err == nil {
		t.Fatal("GENBUMP to generation 0 encoded")
	}
	if _, err := EncodeGenbumpPayload(nil, 1); err == nil {
		t.Fatal("GENBUMP with an empty key encoded")
	}
	zeroed := bytes.Clone(pay)
	for i := 2; i < 6; i++ {
		zeroed[i] = 0
	}
	if _, _, err := DecodeGenbumpPayload(zeroed); err == nil {
		t.Fatal("GENBUMP frame carrying generation 0 decoded")
	}
}

// TestStoreGenBump drives the liveness model of doc 03 section 6.3:
// a rooth with no generation record is live at any rootgen, a bump
// kills everything below it, bumps are monotonic, and generation
// records never leak through Get, BatchGet, or Scan.
func TestStoreGenBump(t *testing.T) {
	r := newStoreRig(t)
	ctx := context.Background()
	r1, err := MintRooth(0, 1)
	if err != nil {
		t.Fatal(err)
	}
	r2, err := MintRooth(0, 2)
	if err != nil {
		t.Fatal(err)
	}
	seg := func(rooth uint64, segid uint64) []byte {
		sk, err := NewSubkey(rooth, SubkindSeg, segid)
		if err != nil {
			t.Fatal(err)
		}
		return sk.Encode()
	}
	r.apply(t,
		sqlo1.Op{Rec: sqlo1.Record{Key: seg(r1, 1), Value: []byte("r1 seg"), Gen: 1}},
		sqlo1.Op{Rec: sqlo1.Record{Key: seg(r2, 1), Value: []byte("r2 seg"), Gen: 1}},
		putOp("plain", []byte("v"), 0),
	)
	r.checkLive(t, r1, 1, true) // never bumped

	r.genbump(t, r1, 2)
	r.checkLive(t, r1, 1, false)
	r.checkLive(t, r1, 2, true)
	r.checkLive(t, r2, 1, true)

	// Re-delivering the same bump and delivering a lower one are the
	// monotonic no-ops replay depends on.
	r.genbump(t, r1, 2)
	r.genbump(t, r1, 1)
	r.checkLive(t, r1, 1, false)
	r.checkLive(t, r1, 2, true)

	if err := r.s.GenBump(ctx, r1, 0); err == nil {
		t.Fatal("bump to generation 0 accepted")
	}

	// The generation record is index state, not seam state.
	if _, err := r.s.Get(ctx, GenKey(r1)); !errors.Is(err, sqlo1.ErrNotFound) {
		t.Fatalf("get of a generation record: %v", err)
	}
	out, err := r.s.BatchGet(ctx, [][]byte{GenKey(r1), []byte("plain")})
	if err != nil {
		t.Fatal(err)
	}
	if out[0].Key != nil {
		t.Fatal("batchget delivered a generation record")
	}
	if !bytes.Equal(out[1].Value, []byte("v")) {
		t.Fatal("batchget hit next to the hidden record came back wrong")
	}
	r.verify(t)
	r.verifyScan(t) // scanAll == liveShadow proves Scan skips it

	// Bumps interleaved with real traffic, across splits.
	mixedTraffic(t, r, 0, 2_000, 300)
	r.genbump(t, r2, 5)
	r.checkLive(t, r2, 4, false)
	r.checkLive(t, r2, 5, true)
	r.checkLive(t, r1, 2, true)
	r.verify(t)
	r.verifyScan(t)
}

// TestStoreGenBumpReopen carries generation records across recovery:
// once from a pure WAL replay, once from a checkpointed base with
// bumps in the tail, exercising the cold-load found path.
func TestStoreGenBumpReopen(t *testing.T) {
	r := newStoreRig(t)
	r1, err := MintRooth(1, 1)
	if err != nil {
		t.Fatal(err)
	}
	r2, err := MintRooth(1, 2)
	if err != nil {
		t.Fatal(err)
	}
	mixedTraffic(t, r, 0, 600, 150)
	r.genbump(t, r1, 3)

	r.reopen(t) // no checkpoint: the bump survives on WAL replay alone
	r.verify(t)
	r.verifyScan(t)
	r.checkLive(t, r1, 2, false)
	r.checkLive(t, r1, 3, true)
	r.checkLive(t, r2, 7, true)

	if err := r.s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	r.genbump(t, r1, 4) // found path through a cold-loaded chain
	r.genbump(t, r2, 2) // fresh insert past the checkpoint
	mixedTraffic(t, r, 600, 300, 100)

	r.reopen(t) // checkpointed base plus a tail holding both bumps
	r.verify(t)
	r.verifyScan(t)
	r.checkLive(t, r1, 3, false)
	r.checkLive(t, r1, 4, true)
	r.checkLive(t, r2, 1, false)
	r.checkLive(t, r2, 2, true)

	if err := r.s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	r.reopen(t) // generation records ride the checkpoint like any record
	r.verify(t)
	r.verifyScan(t)
	r.checkLive(t, r1, 4, true)
	r.checkLive(t, r2, 2, true)
}

// TestStoreRootRecords drives the seam Root flag end to end: a Root op
// lands as an rtype-2 root image whose entry meta carries the root bit,
// reads back with the flag through Get and Scan, survives a reopen, and
// a Root op smuggling a seam gen rejects its whole batch at plan time,
// before the WAL sees a frame, leaving the store usable.
func TestStoreRootRecords(t *testing.T) {
	r := newStoreRig(t)
	ctx := context.Background()
	r.apply(t,
		sqlo1.Op{Rec: sqlo1.Record{Key: []byte("wide"), Value: []byte("root payload bytes"), Root: true}},
		putOp("plain", []byte("v"), 0),
	)
	got, err := r.s.Get(ctx, []byte("wide"))
	if err != nil {
		t.Fatal(err)
	}
	if !got.Root || got.Gen != 0 {
		t.Fatalf("root read back root=%v gen=%d", got.Root, got.Gen)
	}
	if plain, err := r.s.Get(ctx, []byte("plain")); err != nil || plain.Root {
		t.Fatalf("plain read back root=%v err=%v", plain.Root, err)
	}
	rootSeen := false
	if _, err := r.s.Scan(ctx, nil, func(rec sqlo1.Record) bool {
		if string(rec.Key) == "wide" {
			rootSeen = rec.Root
		} else if rec.Root {
			t.Fatalf("scan flagged %q as root", rec.Key)
		}
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if !rootSeen {
		t.Fatal("scan dropped the root flag")
	}

	bad := &sqlo1.DrainBatch{Seq: r.seq + 1, Ops: []sqlo1.Op{
		{Rec: sqlo1.Record{Key: []byte("bad"), Value: []byte("p"), Root: true, Gen: 3}},
	}}
	if err := r.s.ApplyBatch(ctx, bad); err == nil {
		t.Fatal("root op with a seam gen applied")
	}
	if _, err := r.s.Get(ctx, []byte("bad")); !errors.Is(err, sqlo1.ErrNotFound) {
		t.Fatalf("rejected op left a record: %v", err)
	}
	bare := &sqlo1.DrainBatch{Seq: r.seq + 1, Ops: []sqlo1.Op{
		{Rec: sqlo1.Record{Key: []byte("bare"), Value: []byte("p"), Delta: true}},
	}}
	if err := r.s.ApplyBatch(ctx, bare); err == nil {
		t.Fatal("delta op without the root flag applied")
	}
	if _, err := r.s.Get(ctx, []byte("bare")); !errors.Is(err, sqlo1.ErrNotFound) {
		t.Fatalf("rejected delta op left a record: %v", err)
	}
	// A delta root frames and stores like any root until the W3
	// reconciliation earns the elision.
	r.apply(t, sqlo1.Op{Rec: sqlo1.Record{Key: []byte("wide2"), Value: []byte("delta image"), Root: true, Delta: true}})
	if got, err := r.s.Get(ctx, []byte("wide2")); err != nil || !got.Root {
		t.Fatalf("delta root read back root=%v err=%v", got.Root, err)
	}
	r.apply(t, putOp("after", []byte("v2"), 0))
	r.verify(t)

	r.reopen(t)
	got, err = r.s.Get(ctx, []byte("wide"))
	if err != nil || !got.Root {
		t.Fatalf("after reopen: root=%v err=%v", got.Root, err)
	}
	r.verify(t)
}

// TestBatchBumps drives seam Bumps through ApplyBatch: GENBUMP frames
// share the batch's ops and its one durability point, stay monotonic,
// replay exactly once under a reused Seq, survive both a WAL-tail
// reopen and a checkpoint, and a zero-generation bump rejects its
// whole batch at plan time, leaving the store usable.
func TestBatchBumps(t *testing.T) {
	r := newStoreRig(t)
	ctx := context.Background()
	r1, err := MintRooth(3, 1)
	if err != nil {
		t.Fatal(err)
	}
	r2, err := MintRooth(3, 2)
	if err != nil {
		t.Fatal(err)
	}

	r.applyBumps(t,
		[]sqlo1.Op{
			{Rec: sqlo1.Record{Key: []byte("wide"), Value: []byte("root image"), Root: true}},
			putOp("plain", []byte("v"), 0),
		},
		sqlo1.Bump{Rooth: r1, NewGen: 2}, sqlo1.Bump{Rooth: r2, NewGen: 3},
	)
	r.checkLive(t, r1, 1, false)
	r.checkLive(t, r1, 2, true)
	r.checkLive(t, r2, 2, false)
	r.checkLive(t, r2, 3, true)
	r.verify(t)
	r.verifyScan(t) // generation records stay invisible to the seam

	// Lower and equal bumps riding a later batch are monotonic no-ops.
	r.applyBumps(t, []sqlo1.Op{putOp("more", []byte("w"), 0)},
		sqlo1.Bump{Rooth: r1, NewGen: 1}, sqlo1.Bump{Rooth: r2, NewGen: 3})
	r.checkLive(t, r1, 2, true)
	r.checkLive(t, r2, 3, true)

	// A replayed Seq applies nothing, bumps included.
	replay := &sqlo1.DrainBatch{Seq: r.seq, Bumps: []sqlo1.Bump{{Rooth: r1, NewGen: 9}}}
	if err := r.s.ApplyBatch(ctx, replay); err != nil {
		t.Fatal(err)
	}
	r.checkLive(t, r1, 2, true)

	// A zero-generation bump rejects at plan time: no WAL frame, no op
	// applied, store still usable.
	bad := &sqlo1.DrainBatch{
		Seq:   r.seq + 1,
		Ops:   []sqlo1.Op{putOp("bad", []byte("x"), 0)},
		Bumps: []sqlo1.Bump{{Rooth: r1, NewGen: 0}},
	}
	if err := r.s.ApplyBatch(ctx, bad); err == nil {
		t.Fatal("bump to generation 0 accepted")
	}
	if _, err := r.s.Get(ctx, []byte("bad")); !errors.Is(err, sqlo1.ErrNotFound) {
		t.Fatalf("rejected batch left an op behind: %v", err)
	}
	r.verify(t)

	r.reopen(t) // GENBUMP frames replay from the WAL tail
	r.checkLive(t, r1, 1, false)
	r.checkLive(t, r1, 2, true)
	r.checkLive(t, r2, 3, true)
	r.verify(t)
	r.verifyScan(t)

	if err := r.s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	r.reopen(t) // batch-carried generation records ride the checkpoint
	r.checkLive(t, r1, 2, true)
	r.checkLive(t, r2, 3, true)
	r.verify(t)
}

// TestMintLease covers the Minter capability end to end: disjoint
// ranges, seam invisibility of the lease record, a rejected lease
// leaving the store usable, and the mark surviving both a WAL-tail
// reopen and a reopen after the post-crash mint.
func TestMintLease(t *testing.T) {
	r := newStoreRig(t)
	ctx := context.Background()
	start, err := r.s.MintLease(ctx, 100)
	if err != nil || start != 0 {
		t.Fatalf("first lease: start %d, err %v", start, err)
	}
	r.apply(t, putOp("user", []byte("v"), 0))
	start, err = r.s.MintLease(ctx, 50)
	if err != nil || start != 100 {
		t.Fatalf("second lease: start %d, err %v, want 100", start, err)
	}
	if _, err := r.s.Get(ctx, leaseKey); !errors.Is(err, sqlo1.ErrNotFound) {
		t.Fatalf("lease record leaked through Get: %v", err)
	}
	if got := r.scanAll(t); len(got) != 1 || got["user"] == nil {
		t.Fatalf("scan sees %d records, want the one user key", len(got))
	}
	if _, err := r.s.MintLease(ctx, 0); err == nil {
		t.Fatal("zero-counter lease accepted")
	}
	r.reopen(t)
	start, err = r.s.MintLease(ctx, 1)
	if err != nil || start != 150 {
		t.Fatalf("lease after reopen: start %d, err %v, want 150", start, err)
	}
	r.reopen(t)
	start, err = r.s.MintLease(ctx, 1)
	if err != nil || start != 151 {
		t.Fatalf("lease after second reopen: start %d, err %v, want 151", start, err)
	}
	if rec, err := r.s.Get(ctx, []byte("user")); err != nil || !bytes.Equal(rec.Value, []byte("v")) {
		t.Fatalf("user record after reopens: %v", err)
	}
	if _, err := r.s.MintLease(ctx, 1<<48); err == nil {
		t.Fatal("lease past the counter space accepted")
	}
}
