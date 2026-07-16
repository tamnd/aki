package sqlo1

import (
	"bytes"
	"context"
	"fmt"
	"testing"
)

// recordingStore wraps a MemStore, deep-copying every applied batch
// (ApplyBatch ops alias hot-tier arenas, so a shallow copy would rot)
// and counting puts by record class. The recorded batches let a test
// replay any crash prefix into a fresh store, which is the strongest
// crash statement available above the seam: per the Store contract a
// crash keeps a batch prefix, so every prefix must read consistently.
type recordingStore struct {
	*MemStore
	batches   []DrainBatch
	chunkPuts int // segment-subkey put ops
	rootPuts  int // Root-flagged put ops
	plainPuts int // everything else
}

func newRecordingStore() *recordingStore {
	return &recordingStore{MemStore: NewMemStore()}
}

func (r *recordingStore) ApplyBatch(ctx context.Context, b *DrainBatch) error {
	if err := r.MemStore.ApplyBatch(ctx, b); err != nil {
		return err
	}
	cp := DrainBatch{Seq: b.Seq, Bumps: append([]Bump(nil), b.Bumps...)}
	for _, op := range b.Ops {
		rec := Record{
			Key:      append([]byte(nil), op.Rec.Key...),
			Value:    append([]byte(nil), op.Rec.Value...),
			ExpireMs: op.Rec.ExpireMs,
			Gen:      op.Rec.Gen,
			Root:     op.Rec.Root,
		}
		cp.Ops = append(cp.Ops, Op{Del: op.Del, Rec: rec})
		switch {
		case op.Del:
		case op.Rec.Root:
			r.rootPuts++
		case len(op.Rec.Key) == SubkeySize && op.Rec.Key[8] == chunkKind:
			r.chunkPuts++
		default:
			r.plainPuts++
		}
	}
	r.batches = append(r.batches, cp)
	return nil
}

// replayPrefix builds a fresh MemStore holding the first n recorded
// batches, the state a crash after batch n recovers to.
func (r *recordingStore) replayPrefix(t *testing.T, n int) *MemStore {
	t.Helper()
	ms := NewMemStore()
	for i := range n {
		b := r.batches[i]
		if err := ms.ApplyBatch(context.Background(), &b); err != nil {
			t.Fatalf("replay batch %d: %v", i, err)
		}
	}
	return ms
}

// strRig is a Str over a Tiered over a recording MemStore, with the
// promotion coin disabled so reads are deterministic.
type strRig struct {
	t  *testing.T
	rs *recordingStore
	tr *Tiered
	s  *Str
	// legal chunk config for most tests: 1 KiB chunks, 8 KiB boundary
	cfg StrConfig
}

func newStrRig(t *testing.T) *strRig {
	t.Helper()
	rs := newRecordingStore()
	tr := NewTiered(rs, TieredConfig{
		Budget:   Budget{Entries: 4096, Arenas: 64 << 20},
		PromoteP: -1,
		Seed:     7,
		NowMs:    func() int64 { return 1 << 41 },
	})
	cfg := StrConfig{RopeMin: 8 << 10, Log2Chunk: 10}
	s, err := NewStr(tr, cfg)
	if err != nil {
		t.Fatalf("NewStr: %v", err)
	}
	return &strRig{t: t, rs: rs, tr: tr, s: s, cfg: cfg}
}

// reopen builds a fresh Tiered and Str over the same store, the
// cold-cache view a restart would see.
func (r *strRig) reopen() *Str {
	r.t.Helper()
	tr := NewTiered(r.rs, TieredConfig{
		Budget:   Budget{Entries: 4096, Arenas: 64 << 20},
		PromoteP: -1,
		Seed:     8,
		NowMs:    func() int64 { return 1 << 41 },
	})
	s, err := NewStr(tr, r.cfg)
	if err != nil {
		r.t.Fatalf("NewStr over reopened tier: %v", err)
	}
	return s
}

func (r *strRig) set(key string, val []byte) {
	r.t.Helper()
	if err := r.s.Set(context.Background(), []byte(key), val); err != nil {
		r.t.Fatalf("Set(%q, %d bytes): %v", key, len(val), err)
	}
}

func (r *strRig) get(s *Str, key string) ([]byte, bool) {
	r.t.Helper()
	v, ok, err := s.Get(context.Background(), []byte(key))
	if err != nil {
		r.t.Fatalf("Get(%q): %v", key, err)
	}
	return v, ok
}

func (r *strRig) flush() {
	r.t.Helper()
	if err := r.tr.Flush(context.Background()); err != nil {
		r.t.Fatalf("Flush: %v", err)
	}
}

func (r *strRig) want(s *Str, key string, want []byte) {
	r.t.Helper()
	got, ok := r.get(s, key)
	if !ok {
		r.t.Fatalf("Get(%q): missing, want %d bytes", key, len(want))
	}
	if !bytes.Equal(got, want) {
		r.t.Fatalf("Get(%q): %d bytes, want %d, first diff at %d", key, len(got), len(want), firstDiff(got, want))
	}
}

func firstDiff(a, b []byte) int {
	n := min(len(a), len(b))
	for i := range n {
		if a[i] != b[i] {
			return i
		}
	}
	return n
}

// pat builds a deterministic non-repeating byte pattern.
func pat(n int, seed byte) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i)*31 ^ seed ^ byte(i>>9)
	}
	return b
}

// storedRoot reads key's record straight from the store and decodes
// it as a rope root.
func (r *strRig) storedRoot(key string) (ropeRoot, bool) {
	r.t.Helper()
	rec, err := r.rs.MemStore.Get(context.Background(), []byte(key))
	if err != nil {
		return ropeRoot{}, false
	}
	if !rec.Root {
		return ropeRoot{}, false
	}
	root, err := decodeRopeRoot(rec.Value)
	if err != nil {
		r.t.Fatalf("stored root under %q does not decode: %v", key, err)
	}
	return root, true
}

// TestStrLadderRoundtrip walks every rung and both sides of the rope
// boundary, checking hot reads, drained reads, and a cold restart.
func TestStrLadderRoundtrip(t *testing.T) {
	r := newStrRig(t)
	sizes := []int{0, 1, 10, 1023, 1024, 4096, 8<<10 - 1, 8 << 10, 8<<10 + 1, 12<<10 + 300, 32 << 10}
	for i, n := range sizes {
		key := fmt.Sprintf("k%02d", i)
		val := pat(n, byte(i))
		r.set(key, val)
		r.want(r.s, key, val)
	}
	r.flush()
	for i, n := range sizes {
		key := fmt.Sprintf("k%02d", i)
		r.want(r.s, key, pat(n, byte(i)))
		// The stored representation matches the ladder: rope at and
		// past the boundary, one plain record below it.
		_, isRope := r.storedRoot(key)
		if want := n >= r.cfg.RopeMin; isRope != want {
			t.Errorf("%s (%d bytes): stored as rope=%v, want %v", key, n, isRope, want)
		}
	}
	cold := r.reopen()
	for i, n := range sizes {
		r.want(cold, fmt.Sprintf("k%02d", i), pat(n, byte(i)))
	}
}

// TestStrUpgradeBoundaryOnce is the milestone's one-time O(value)
// boundary test: APPEND-driven growth pays the full plane build in
// exactly one window, and every window after it touches at most the
// tail chunk, one new chunk, and the root.
func TestStrUpgradeBoundaryOnce(t *testing.T) {
	r := newStrRig(t)
	ctx := context.Background()
	key := []byte("grow")
	const piece = 512
	const pieces = 24 // crosses the 8 KiB boundary at piece 16
	oracle := pat(piece*pieces, 9)
	fullPlane := r.cfg.RopeMin >> r.cfg.Log2Chunk // chunks in the boundary-crossing build

	planeBuilds := 0
	for i := range pieces {
		before := r.rs.chunkPuts
		suffix := oracle[i*piece : (i+1)*piece]
		n, err := r.s.Append(ctx, key, suffix)
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		if want := int64((i + 1) * piece); n != want {
			t.Fatalf("append %d: length %d, want %d", i, n, want)
		}
		r.flush()
		window := r.rs.chunkPuts - before
		grown := (i + 1) * piece
		switch {
		case grown < r.cfg.RopeMin:
			if window != 0 {
				t.Errorf("append %d (inline, %d bytes): %d chunk puts, want 0", i, grown, window)
			}
		case grown == r.cfg.RopeMin:
			if window != fullPlane {
				t.Errorf("boundary append %d: %d chunk puts, want the full plane %d", i, window, fullPlane)
			}
			planeBuilds++
		default:
			if window > 2 {
				t.Errorf("append %d (rope, %d bytes): %d chunk puts, want at most 2", i, grown, window)
			}
			if window >= fullPlane {
				planeBuilds++
			}
		}
		r.want(r.s, string(key), oracle[:grown])
	}
	if planeBuilds != 1 {
		t.Errorf("plane built %d times, want exactly once", planeBuilds)
	}
	r.want(r.reopen(), string(key), oracle)
}

// TestStrRewriteCrashPrefix drives the full plane lifecycle and then
// replays every batch prefix, the state a crash at that point
// recovers to. Every prefix must read as a complete former or new
// value, never a mix, and a readable new value implies its
// predecessor's plane is already retired (the bump rode the same
// batch as the root image).
func TestStrRewriteCrashPrefix(t *testing.T) {
	r := newStrRig(t)
	key := "k"
	val1 := pat(12<<10, 1) // rope, 12 chunks
	val2 := pat(20<<10, 2) // rope, 20 chunks
	val3 := pat(100, 3)    // back to plain

	r.set(key, val1)
	r.flush()
	root1, ok := r.storedRoot(key)
	if !ok {
		t.Fatal("val1 did not store as a rope")
	}
	r.set(key, val2)
	r.flush()
	root2, ok := r.storedRoot(key)
	if !ok {
		t.Fatal("val2 did not store as a rope")
	}
	if root2.rooth == root1.rooth {
		t.Fatal("rewrite reused the old plane's rooth; in-place chunk overwrite tears crash prefixes")
	}
	r.set(key, val3)
	r.flush()
	if _, err := r.s.Del(context.Background(), []byte(key)); err != nil {
		t.Fatalf("Del: %v", err)
	}
	r.flush()

	legal := [][]byte{val1, val2, val3}
	sawStates := map[int]bool{}
	for p := 0; p <= len(r.rs.batches); p++ {
		ms := r.rs.replayPrefix(t, p)
		tr := NewTiered(ms, TieredConfig{
			Budget:   Budget{Entries: 1024, Arenas: 64 << 20},
			PromoteP: -1,
			Seed:     uint64(p) + 100,
			NowMs:    func() int64 { return 1 << 41 },
		})
		s, err := NewStr(tr, r.cfg)
		if err != nil {
			t.Fatal(err)
		}
		got, ok, err := s.Get(context.Background(), []byte(key))
		if err != nil {
			t.Fatalf("prefix %d: Get: %v", p, err)
		}
		state := -1
		if !ok {
			state = 3 // missing: legal only before val1 or after the delete
		} else {
			for i, v := range legal {
				if bytes.Equal(got, v) {
					state = i
					break
				}
			}
		}
		if state == -1 {
			t.Fatalf("prefix %d: torn read, %d bytes matching no complete value", p, len(got))
		}
		sawStates[state] = true
		// Atomicity of retire-with-replace: the moment a successor is
		// readable, the predecessor's plane is dead. (A missing key says
		// nothing: prefixes before val1's root land carry chunks of a
		// live-but-unreferenced plane, the accepted crash state.)
		if state == 1 || state == 2 {
			if live, _ := ms.RootLive(root1.rooth, root1.rootgen); live {
				t.Errorf("prefix %d: val%d readable but plane 1 still live", p, state+1)
			}
		}
		if state == 2 {
			if live, _ := ms.RootLive(root2.rooth, root2.rootgen); live {
				t.Errorf("prefix %d: val%d readable but plane 2 still live", p, state+1)
			}
		}
	}
	// The full replay is the post-delete state: key gone, both planes dead.
	full := r.rs.replayPrefix(t, len(r.rs.batches))
	if live, _ := full.RootLive(root1.rooth, root1.rootgen); live {
		t.Error("plane 1 live after the full lifecycle")
	}
	if live, _ := full.RootLive(root2.rooth, root2.rootgen); live {
		t.Error("plane 2 live after the full lifecycle")
	}
	for i := range 4 {
		if !sawStates[i] {
			t.Errorf("no prefix hit state %d; the walk should visit every lifecycle state", i)
		}
	}
}

// TestStrAppendCrashPrefix forces one-op batches so the append's
// chunk writes and root write land in separate batches, then checks
// that every prefix reads as exactly the old or the new value: the
// root's total_len is the commit point.
func TestStrAppendCrashPrefix(t *testing.T) {
	r := newStrRig(t)
	key := "k"
	old := pat(2560, 4) // 2.5 chunks: short tail to read-modify
	r.set(key, old)
	r.flush()

	r.tr.dr.maxOps = 1
	suffix := pat(1200, 5)
	if _, err := r.s.Append(context.Background(), []byte(key), suffix); err != nil {
		t.Fatalf("Append: %v", err)
	}
	r.flush()

	grown := append(append([]byte(nil), old...), suffix...)
	for p := 0; p <= len(r.rs.batches); p++ {
		ms := r.rs.replayPrefix(t, p)
		tr := NewTiered(ms, TieredConfig{
			Budget:   Budget{Entries: 1024, Arenas: 64 << 20},
			PromoteP: -1,
			Seed:     uint64(p) + 200,
			NowMs:    func() int64 { return 1 << 41 },
		})
		s, err := NewStr(tr, r.cfg)
		if err != nil {
			t.Fatal(err)
		}
		got, ok, err := s.Get(context.Background(), []byte(key))
		if err != nil {
			t.Fatalf("prefix %d: Get: %v", p, err)
		}
		switch {
		case !ok && p == 0:
		case !ok:
			t.Fatalf("prefix %d: key missing", p)
		case bytes.Equal(got, old), bytes.Equal(got, grown):
		default:
			t.Fatalf("prefix %d: torn read of %d bytes (old %d, new %d)", p, len(got), len(old), len(grown))
		}
	}
}

// TestStrRopeDelete checks DEL of a rope is a tombstone plus a bump:
// the value disappears, the plane dies, and a later value under the
// same key builds a fresh plane.
func TestStrRopeDelete(t *testing.T) {
	r := newStrRig(t)
	ctx := context.Background()
	key := "k"
	val := pat(10<<10, 6)
	r.set(key, val)
	r.flush()
	root, ok := r.storedRoot(key)
	if !ok {
		t.Fatal("value did not store as a rope")
	}

	existed, err := r.s.Del(ctx, []byte(key))
	if err != nil || !existed {
		t.Fatalf("Del: existed=%v err=%v, want true nil", existed, err)
	}
	if _, ok := r.get(r.s, key); ok {
		t.Fatal("key readable after Del")
	}
	r.flush()
	if live, _ := r.rs.RootLive(root.rooth, root.rootgen); live {
		t.Fatal("plane still live after the delete drained")
	}
	if _, ok := r.get(r.reopen(), key); ok {
		t.Fatal("key readable cold after Del")
	}

	existed, err = r.s.Del(ctx, []byte(key))
	if err != nil || existed {
		t.Fatalf("Del of missing key: existed=%v err=%v, want false nil", existed, err)
	}

	val2 := pat(9<<10, 7)
	r.set(key, val2)
	r.flush()
	root2, ok := r.storedRoot(key)
	if !ok {
		t.Fatal("re-created value did not store as a rope")
	}
	if root2.rooth == root.rooth {
		t.Fatal("re-created rope reused the deleted plane's rooth")
	}
	r.want(r.s, key, val2)
}

// TestStrRopeToInline checks a small SET over a rope: the record
// under the key flips back to a plain value and the plane dies in
// the batch that lands the replacement.
func TestStrRopeToInline(t *testing.T) {
	r := newStrRig(t)
	key := "k"
	big := pat(16<<10, 8)
	small := pat(64, 9)
	r.set(key, big)
	r.flush()
	root, ok := r.storedRoot(key)
	if !ok {
		t.Fatal("big value did not store as a rope")
	}
	r.set(key, small)
	r.want(r.s, key, small)
	r.flush()
	if _, isRope := r.storedRoot(key); isRope {
		t.Fatal("small overwrite left a root record")
	}
	if live, _ := r.rs.RootLive(root.rooth, root.rootgen); live {
		t.Fatal("plane still live after the inline overwrite drained")
	}
	r.want(r.reopen(), key, small)
}

// TestStrLazyZeroRead pins the assembly semantics the range surface
// will lean on: absent chunks and short chunks read as zeros.
func TestStrLazyZeroRead(t *testing.T) {
	r := newStrRig(t)
	ctx := context.Background()

	// A rope of 4 chunks with chunk 1 absent and chunk 2 short,
	// planted below the tier the way a lazy SETRANGE would leave it.
	root := ropeRoot{log2chunk: 10, rootgen: 1, rooth: 42, totalLen: 3<<10 + 100, chunkCount: 4}
	c0, c2, c3 := pat(1024, 10), pat(400, 11), pat(100, 12)
	var k0, k2, k3 [SubkeySize]byte
	putChunkKey(k0[:], root.rooth, 0)
	putChunkKey(k2[:], root.rooth, 2)
	putChunkKey(k3[:], root.rooth, 3)
	b := &DrainBatch{Seq: 1, Ops: []Op{
		{Rec: Record{Key: []byte("k"), Value: appendRopeRoot(nil, root), Root: true}},
		{Rec: Record{Key: k0[:], Value: c0, Gen: 1}},
		{Rec: Record{Key: k2[:], Value: c2, Gen: 1}},
		{Rec: Record{Key: k3[:], Value: c3, Gen: 1}},
	}}
	if err := r.rs.MemStore.ApplyBatch(ctx, b); err != nil {
		t.Fatal(err)
	}

	want := make([]byte, root.totalLen)
	copy(want, c0)
	copy(want[2048:], c2) // chunk 1 stays zero, chunk 2's tail stays zero
	copy(want[3072:], c3)
	r.want(r.reopen(), "k", want)
}

// TestStrNeedsRope pins the ladder condition, including the blob
// ceiling clause that keeps giant-key records inside one extent.
func TestStrNeedsRope(t *testing.T) {
	rig := newStrRig(t)
	tr := rig.tr
	s, err := NewStr(tr, StrConfig{})
	if err != nil {
		t.Fatal(err)
	}
	smallKey := []byte("k")
	// inlineCap sits below DefaultRopeMin, so the ceiling clause is the
	// binding one for a small key: the largest inline value is the one
	// whose record exactly fills the cap.
	ceiling := inlineCap - len(smallKey) - recEnvelopeMax
	if s.needsRope(smallKey, 100) || s.needsRope(smallKey, ceiling) {
		t.Error("inline values under both boundaries routed to rope")
	}
	if !s.needsRope(smallKey, ceiling+1) {
		t.Error("record past the blob ceiling not routed to rope")
	}
	if !s.needsRope(smallKey, DefaultRopeMin) {
		t.Error("boundary value not routed to rope")
	}
	bigKey := bytes.Repeat([]byte("K"), 60<<10)
	if !s.needsRope(bigKey, inlineCap-len(bigKey)) {
		t.Error("giant-key record past the blob ceiling not routed to rope")
	}
}

// TestTieredLookupAndSetGen covers the two seam doors this slice
// added: Lookup's root bit hot and cold, and SetGen's generation
// riding the drain into the store record.
func TestTieredLookupAndSetGen(t *testing.T) {
	ctx := context.Background()
	r := newTieredRig(t, 64, -1, 3)

	if err := r.t.Set(ctx, []byte("plain"), []byte("v"), TagString); err != nil {
		t.Fatal(err)
	}
	if err := r.t.Set(ctx, []byte("root"), []byte("payload"), TagString|TagRoot); err != nil {
		t.Fatal(err)
	}
	if err := r.t.SetGen(ctx, []byte("seg"), []byte("chunk"), TagString, 5); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		key  string
		root bool
	}{{"plain", false}, {"root", true}} {
		_, root, ok, err := r.t.Lookup(ctx, []byte(tc.key))
		if err != nil || !ok || root != tc.root {
			t.Errorf("hot Lookup(%q): root=%v ok=%v err=%v, want root=%v ok=true", tc.key, root, ok, err, tc.root)
		}
	}
	r.flush(t)
	rec, err := r.ms.Get(ctx, []byte("seg"))
	if err != nil {
		t.Fatal(err)
	}
	if rec.Gen != 5 {
		t.Errorf("drained segment gen %d, want 5", rec.Gen)
	}
	if rec, err = r.ms.Get(ctx, []byte("root")); err != nil || !rec.Root {
		t.Errorf("drained root record Root=%v err=%v, want true nil", rec.Root, err)
	}

	// Cold: a fresh tier reads the bits back from the store.
	tr := NewTiered(r.ms, TieredConfig{
		Budget:   Budget{Entries: 64, Arenas: 4 << 20},
		PromoteP: -1,
		Seed:     4,
		NowMs:    func() int64 { return 1 << 41 },
	})
	_, root, ok, err := tr.Lookup(ctx, []byte("root"))
	if err != nil || !ok || !root {
		t.Errorf("cold Lookup(root): root=%v ok=%v err=%v, want true true nil", root, ok, err)
	}
	_, root, ok, err = tr.Lookup(ctx, []byte("plain"))
	if err != nil || !ok || root {
		t.Errorf("cold Lookup(plain): root=%v ok=%v err=%v, want false true nil", root, ok, err)
	}
	if _, _, ok, err = tr.Lookup(ctx, []byte("nope")); ok || err != nil {
		t.Errorf("cold Lookup(missing): ok=%v err=%v, want false nil", ok, err)
	}
}
