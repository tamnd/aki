package sqlo1

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"hash/maphash"
	"math"
	"strconv"
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
	chunkPuts int // chunk-subkey put ops
	pcPuts    int // popcount-segment put ops
	rootPuts  int // Root-flagged put ops
	plainPuts int // everything else
	// Cold reads by key class: only misses the hot tier forwards land
	// here, which is what the S-I3 read-cost asserts want to see.
	chunkReads int
	pcReads    int
	otherReads int
}

func (r *recordingStore) BatchGet(ctx context.Context, keys [][]byte) ([]Record, error) {
	for _, k := range keys {
		switch {
		case len(k) == SubkeySize && k[8] == chunkKind:
			r.chunkReads++
		case len(k) == SubkeySize && k[8] == pcKind:
			r.pcReads++
		default:
			r.otherReads++
		}
	}
	return r.MemStore.BatchGet(ctx, keys)
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
			Delta:    op.Rec.Delta,
		}
		cp.Ops = append(cp.Ops, Op{Del: op.Del, Rec: rec})
		switch {
		case op.Del:
		case op.Rec.Root:
			r.rootPuts++
		case len(op.Rec.Key) == SubkeySize && op.Rec.Key[8] == chunkKind:
			r.chunkPuts++
		case len(op.Rec.Key) == SubkeySize && op.Rec.Key[8] == pcKind:
			r.pcPuts++
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

// oracleSetRange is the eager reference: zero-extend, overlay.
func oracleSetRange(o []byte, off int, patch []byte) []byte {
	if end := off + len(patch); end > len(o) {
		o = append(o, make([]byte, end-len(o))...)
	}
	copy(o[off:], patch)
	return o
}

// TestStrSetRangeOracle walks one key through every SetRange shape the
// ladder has: create, overwrite, grow inside plain, the sparse write
// that crosses the rope boundary, blind interior chunks, partial-chunk
// merges, a merge against an absent gap chunk, tail growth, and far
// growth, each checked against the eager oracle.
func TestStrSetRangeOracle(t *testing.T) {
	r := newStrRig(t)
	ctx := context.Background()
	key := []byte("k")

	var want []byte
	seed := byte(0)
	apply := func(off, n int) {
		t.Helper()
		seed++
		p := pat(n, seed)
		got, err := r.s.SetRange(ctx, key, int64(off), p)
		if err != nil {
			t.Fatalf("SetRange(%d, %d bytes): %v", off, n, err)
		}
		want = oracleSetRange(want, off, p)
		if got != int64(len(want)) {
			t.Fatalf("SetRange(%d, %d bytes) = %d, want %d", off, n, got, len(want))
		}
		r.want(r.s, "k", want)
		sl, _, err := r.s.Strlen(ctx, key)
		if err != nil || sl != int64(len(want)) {
			t.Fatalf("Strlen = %d, %v, want %d", sl, err, len(want))
		}
	}

	apply(3, 3)      // create plain: zeros then patch
	apply(0, 2)      // overwrite the head
	apply(4090, 20)  // grow inside plain
	apply(8100, 200) // sparse cross of the rope boundary, gap chunks absent
	if _, isRope := func() (ropeRoot, bool) { r.flush(); return r.storedRoot("k") }(); !isRope {
		t.Fatal("boundary cross did not store a rope")
	}
	apply(2048, 2048) // blind interior chunks
	apply(1500, 100)  // partial merge inside one chunk
	apply(5500, 100)  // merge against an absent gap chunk
	apply(1030, 10)   // partial with first chunk == last chunk
	apply(8250, 2000) // grow across the old tail
	apply(20000, 50)  // far grow, a fresh gap
	apply(0, 1024)    // blind write of exactly the first chunk

	// An empty patch reports the length and writes nothing.
	if n, err := r.s.SetRange(ctx, key, 1<<30, nil); err != nil || n != int64(len(want)) {
		t.Fatalf("empty patch = %d, %v, want %d", n, err, len(want))
	}
	r.want(r.s, "k", want)

	// Cold pass: a fresh tier over the drained store sees the same
	// bytes, and cold RMW paths keep matching the oracle.
	r.flush()
	s2 := r.reopen()
	r.want(s2, "k", want)
	for _, op := range [][2]int{{1025, 200}, {12000, 100}, {30000, 30}} {
		seed++
		p := pat(op[1], seed)
		if _, err := s2.SetRange(ctx, key, int64(op[0]), p); err != nil {
			t.Fatalf("cold SetRange(%d): %v", op[0], err)
		}
		want = oracleSetRange(want, op[0], p)
		r.want(s2, "k", want)
	}
}

// TestStrSetRangeLazyChunks pins S-I1 for the range surface: a far
// offset on a missing key creates only the chunks the patch
// addresses, never the gap's.
func TestStrSetRangeLazyChunks(t *testing.T) {
	r := newStrRig(t)
	ctx := context.Background()

	n, err := r.s.SetRange(ctx, []byte("far"), 100<<10, pat(500, 3))
	if err != nil {
		t.Fatal(err)
	}
	if wantLen := int64(100<<10 + 500); n != wantLen {
		t.Fatalf("length = %d, want %d", n, wantLen)
	}
	r.flush()
	// 100 KiB of gap at 1 KiB chunks would be 100 eager chunks; the
	// lazy plane writes exactly the one chunk the patch lands in.
	if r.rs.chunkPuts != 1 || r.rs.rootPuts != 1 {
		t.Fatalf("drained %d chunk puts and %d root puts, want 1 and 1", r.rs.chunkPuts, r.rs.rootPuts)
	}
	root, ok := r.storedRoot("far")
	if !ok {
		t.Fatal("far write did not store a rope")
	}
	if root.totalLen != 100<<10+500 || root.chunkCount != 101 {
		t.Fatalf("root = %d bytes over %d chunks, want %d over 101", root.totalLen, root.chunkCount, 100<<10+500)
	}
	want := oracleSetRange(nil, 100<<10, pat(500, 3))
	r.want(r.reopen(), "far", want)
}

// TestStrRangeWritesKeepTTL pins the restamp rule: Append and SetRange
// on a cold, unpromoted key keep its expiry. PutGen only preserves the
// stamp when the key already sits hot; without the restamp, the fresh
// header these writes create would drain with expire_ms zero and the
// TTL would silently die.
func TestStrRangeWritesKeepTTL(t *testing.T) {
	r := newStrRig(t)
	ctx := context.Background()
	at := int64(1<<41) + 60_000 // rig clock is a constant 1<<41

	r.set("p", pat(64, 1))
	r.set("r", pat(16<<10, 2))
	for _, k := range []string{"p", "r"} {
		if _, err := r.s.ExpireAt(ctx, []byte(k), at); err != nil {
			t.Fatalf("ExpireAt(%q): %v", k, err)
		}
	}
	r.flush()

	// A fresh tier reads everything cold, and PromoteP -1 means the
	// lookups inside Append and SetRange never promote.
	s2 := r.reopen()
	if _, err := s2.Append(ctx, []byte("p"), []byte("tail")); err != nil {
		t.Fatal(err)
	}
	if _, err := s2.SetRange(ctx, []byte("r"), 17<<10, pat(100, 3)); err != nil {
		t.Fatal(err)
	}
	if err := s2.t.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"p", "r"} {
		rec, err := r.rs.MemStore.Get(ctx, []byte(k))
		if err != nil {
			t.Fatalf("store Get(%q): %v", k, err)
		}
		if rec.ExpireMs != at {
			t.Fatalf("%q drained with expire_ms %d, want %d", k, rec.ExpireMs, at)
		}
	}
}

// TestStrIncrShadowByteWriterInvalidation is the T1 milestone's
// invalidation-by-byte-writer test: once INCR arms the int shadow,
// every byte-level writer must kill it, and the next INCR must answer
// from the bytes the writer left, never the stale cached integer. The
// shadow itself is observed through the hot-tier door so the test
// fails loudly if a writer leaves a stale entry behind, not just if
// the stale value happens to surface.
func TestStrIncrShadowByteWriterInvalidation(t *testing.T) {
	r := newStrRig(t)
	ctx := context.Background()
	k := []byte("n")

	shadow := func() (int64, bool) {
		t.Helper()
		n, _, ok := r.tr.ht.intShadowOf(k)
		return n, ok
	}
	incr := func(delta, want int64) {
		t.Helper()
		got, err := r.s.IncrBy(ctx, k, delta)
		if err != nil {
			t.Fatalf("IncrBy(%d): %v", delta, err)
		}
		if got != want {
			t.Fatalf("IncrBy(%d) = %d, want %d", delta, got, want)
		}
		if n, ok := shadow(); !ok || n != want {
			t.Fatalf("shadow after IncrBy = (%d, %v), want (%d, true)", n, ok, want)
		}
	}

	// INCR on a missing key counts from zero and arms the shadow.
	incr(1, 1)
	incr(41, 42)

	// Set: the shadow dies and INCR reads the new bytes.
	r.set("n", []byte("100"))
	if _, ok := shadow(); ok {
		t.Fatal("shadow survived Set")
	}
	incr(1, 101)

	// SetRange: patching the first digit is exactly the byte-level
	// desync the shadow must never survive.
	if _, err := r.s.SetRange(ctx, k, 0, []byte("9")); err != nil {
		t.Fatal(err)
	}
	if _, ok := shadow(); ok {
		t.Fatal("shadow survived SetRange")
	}
	incr(1, 902) // "901" + 1, not 102

	// Append: "902" grows to "9020".
	if _, err := r.s.Append(ctx, k, []byte("0")); err != nil {
		t.Fatal(err)
	}
	if _, ok := shadow(); ok {
		t.Fatal("shadow survived Append")
	}
	incr(1, 9021)

	// Del: the tombstone drops the shadow and a revive counts fresh.
	if _, err := r.s.Del(ctx, k); err != nil {
		t.Fatal(err)
	}
	if _, ok := shadow(); ok {
		t.Fatal("shadow survived Del")
	}
	incr(5, 5)

	// TTL edits touch no value bytes, so the shadow survives them.
	if _, err := r.s.ExpireAt(ctx, k, int64(1<<41)+60_000); err != nil {
		t.Fatal(err)
	}
	if n, ok := shadow(); !ok || n != 5 {
		t.Fatalf("shadow after ExpireAt = (%d, %v), want (5, true)", n, ok)
	}

	// Drain cools dirty to resident with the value untouched, so the
	// shadow survives a flush too.
	r.flush()
	if n, ok := shadow(); !ok || n != 5 {
		t.Fatalf("shadow after flush = (%d, %v), want (5, true)", n, ok)
	}
	incr(1, 6)

	// Eviction retires the slot through removeSlot, which must drop
	// the map entry with it; the next INCR parses cold.
	r.flush()
	s, ok := r.tr.ht.lookup(maphash.Bytes(r.tr.ht.seed, k), k)
	if !ok {
		t.Fatal("key not hot after flush")
	}
	if !r.tr.ht.evict(s, false) {
		t.Fatal("evict refused a resident slot")
	}
	if _, ok := shadow(); ok {
		t.Fatal("shadow survived eviction")
	}
	if len(r.tr.ht.intShadow) != 0 {
		t.Fatalf("shadow map holds %d stale entries after eviction", len(r.tr.ht.intShadow))
	}
	incr(1, 7)
}

// TestStrIncrValues walks INCR semantics against an int64 oracle and
// the whole rejection matrix: non-canonical bytes, ropes, overflow in
// both directions with the value left standing.
func TestStrIncrValues(t *testing.T) {
	r := newStrRig(t)
	ctx := context.Background()

	var oracle int64
	for _, d := range []int64{1, -1, 100, -42, 1 << 40, -(1 << 39), 7} {
		oracle += d
		got, err := r.s.IncrBy(ctx, []byte("c"), d)
		if err != nil {
			t.Fatalf("IncrBy(%d): %v", d, err)
		}
		if got != oracle {
			t.Fatalf("IncrBy(%d) = %d, want %d", d, got, oracle)
		}
	}
	// The stored bytes are the canonical decimal string.
	r.want(r.s, "c", []byte(strconv.FormatInt(oracle, 10)))

	// Values Redis's string2ll rejects are not integers here either.
	for _, bad := range []string{"", "abc", "1.5", "0123", "+1", "-0", " 1", "1 ", "9223372036854775808", "-9223372036854775809", "123456789012345678901"} {
		r.set("bad", []byte(bad))
		if _, err := r.s.IncrBy(ctx, []byte("bad"), 1); !errors.Is(err, ErrNotInt) {
			t.Fatalf("IncrBy on %q: err = %v, want ErrNotInt", bad, err)
		}
	}
	// The int64 poles themselves are canonical and reachable.
	r.set("edge", []byte("9223372036854775806"))
	if got, err := r.s.IncrBy(ctx, []byte("edge"), 1); err != nil || got != math.MaxInt64 {
		t.Fatalf("IncrBy to MaxInt64 = (%d, %v)", got, err)
	}
	if _, err := r.s.IncrBy(ctx, []byte("edge"), 1); !errors.Is(err, ErrOverflow) {
		t.Fatalf("IncrBy past MaxInt64: err = %v, want ErrOverflow", err)
	}
	r.want(r.s, "edge", []byte("9223372036854775807"))
	r.set("edge", []byte("-9223372036854775808"))
	if _, err := r.s.IncrBy(ctx, []byte("edge"), -1); !errors.Is(err, ErrOverflow) {
		t.Fatalf("IncrBy past MinInt64: err = %v, want ErrOverflow", err)
	}
	r.want(r.s, "edge", []byte("-9223372036854775808"))

	// A rope is never an integer, whatever its bytes hold.
	r.set("rope", bytes.Repeat([]byte("1"), 9<<10))
	if _, err := r.s.IncrBy(ctx, []byte("rope"), 1); !errors.Is(err, ErrNotInt) {
		t.Fatalf("IncrBy on rope: err = %v, want ErrNotInt", err)
	}

	// Cold pass: a reopened tier parses from the store and counts on.
	r.flush()
	s2 := r.reopen()
	if got, err := s2.IncrBy(ctx, []byte("c"), 3); err != nil || got != oracle+3 {
		t.Fatalf("cold IncrBy = (%d, %v), want %d", got, err, oracle+3)
	}
}

// TestStrIncrKeepsTTL pins the restamp rule on the INCR family: a
// cold key's expiry survives IncrBy and IncrByFloat, a hot armed
// key's expiry survives repeated INCR through a drain, and the metaOf
// fix keeps a cold rope's expiry through a full-value Set.
func TestStrIncrKeepsTTL(t *testing.T) {
	r := newStrRig(t)
	ctx := context.Background()
	at := int64(1<<41) + 60_000

	r.set("i", []byte("10"))
	r.set("f", []byte("1.5"))
	r.set("rope", pat(16<<10, 2))
	for _, k := range []string{"i", "f", "rope"} {
		if _, err := r.s.ExpireAt(ctx, []byte(k), at); err != nil {
			t.Fatalf("ExpireAt(%q): %v", k, err)
		}
	}
	r.flush()

	s2 := r.reopen()
	if got, err := s2.IncrBy(ctx, []byte("i"), 5); err != nil || got != 15 {
		t.Fatalf("cold IncrBy = (%d, %v)", got, err)
	}
	if v, err := s2.IncrByFloat(ctx, []byte("f"), 0.25); err != nil || string(v) != "1.75" {
		t.Fatalf("cold IncrByFloat = (%q, %v)", v, err)
	}
	// The metaOf fix: Set over a cold rope carries the root's expiry.
	if err := s2.Set(ctx, []byte("rope"), []byte("small")); err != nil {
		t.Fatal(err)
	}
	if err := s2.t.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"i", "f", "rope"} {
		rec, err := r.rs.MemStore.Get(ctx, []byte(k))
		if err != nil {
			t.Fatalf("store Get(%q): %v", k, err)
		}
		if rec.ExpireMs != at {
			t.Fatalf("%q drained with expire_ms %d, want %d", k, rec.ExpireMs, at)
		}
	}

	// Hot armed path: INCR under an armed shadow through a drain.
	if _, err := r.s.IncrBy(ctx, []byte("hot"), 1); err != nil {
		t.Fatal(err)
	}
	if _, err := r.s.ExpireAt(ctx, []byte("hot"), at); err != nil {
		t.Fatal(err)
	}
	r.flush()
	if _, err := r.s.IncrBy(ctx, []byte("hot"), 1); err != nil {
		t.Fatal(err)
	}
	r.flush()
	rec, err := r.rs.MemStore.Get(ctx, []byte("hot"))
	if err != nil {
		t.Fatal(err)
	}
	if rec.ExpireMs != at {
		t.Fatalf("hot INCR drained with expire_ms %d, want %d", rec.ExpireMs, at)
	}
}

// TestStrIncrByFloat pins the float path: arithmetic against Redis
// replies, formatting with no exponent and no trailing zeros, and the
// rejection matrix including the Inf and NaN doors.
func TestStrIncrByFloat(t *testing.T) {
	r := newStrRig(t)
	ctx := context.Background()

	steps := []struct {
		delta float64
		want  string
	}{
		{10.5, "10.5"},
		{0.1, "10.6"},
		{-5.6, "5"},
		{2, "7"},
		{0.25, "7.25"},
	}
	for _, st := range steps {
		got, err := r.s.IncrByFloat(ctx, []byte("f"), st.delta)
		if err != nil {
			t.Fatalf("IncrByFloat(%v): %v", st.delta, err)
		}
		if string(got) != st.want {
			t.Fatalf("IncrByFloat(%v) = %q, want %q", st.delta, got, st.want)
		}
	}
	// Exponent-form and int-shaped current values both parse.
	r.set("e", []byte("3.0e3"))
	if got, err := r.s.IncrByFloat(ctx, []byte("e"), 200); err != nil || string(got) != "3200" {
		t.Fatalf("IncrByFloat over 3.0e3 = (%q, %v)", got, err)
	}

	for _, bad := range []string{"", "abc", "1..2", " 1.5", "nan"} {
		r.set("bad", []byte(bad))
		if _, err := r.s.IncrByFloat(ctx, []byte("bad"), 1); !errors.Is(err, ErrNotFloat) {
			t.Fatalf("IncrByFloat on %q: err = %v, want ErrNotFloat", bad, err)
		}
	}
	r.set("rope", bytes.Repeat([]byte("1"), 9<<10))
	if _, err := r.s.IncrByFloat(ctx, []byte("rope"), 1); !errors.Is(err, ErrNotFloat) {
		t.Fatalf("IncrByFloat on rope: err = %v, want ErrNotFloat", err)
	}

	// An inf current value parses (Redis accepts it) but any result
	// that lands on NaN or Inf is refused and the value stands.
	r.set("inf", []byte("inf"))
	if _, err := r.s.IncrByFloat(ctx, []byte("inf"), 1); !errors.Is(err, ErrNaNInf) {
		t.Fatalf("IncrByFloat on inf: err = %v, want ErrNaNInf", err)
	}
	r.set("big", []byte("1.7e308"))
	if _, err := r.s.IncrByFloat(ctx, []byte("big"), 1.7e308); !errors.Is(err, ErrNaNInf) {
		t.Fatalf("IncrByFloat overflow to Inf: err = %v, want ErrNaNInf", err)
	}
	r.want(r.s, "big", []byte("1.7e308"))
}

// TestParseCanonicalInt pins the string2ll shape: exactly the strings
// strconv round-trips are accepted, nothing else.
func TestParseCanonicalInt(t *testing.T) {
	good := []int64{0, 1, -1, 9, 10, -10, math.MaxInt64, math.MinInt64, 1 << 40, -(1 << 52)}
	for _, n := range good {
		s := strconv.FormatInt(n, 10)
		got, ok := parseCanonicalInt([]byte(s))
		if !ok || got != n {
			t.Fatalf("parseCanonicalInt(%q) = (%d, %v), want (%d, true)", s, got, ok, n)
		}
	}
	bad := []string{"", "-", "+1", "01", "-0", "-01", "1a", "a1", " 1", "1 ", "1.0",
		"9223372036854775808", "-9223372036854775809", "99999999999999999999", "-99999999999999999999", "123456789012345678901"}
	for _, s := range bad {
		if n, ok := parseCanonicalInt([]byte(s)); ok {
			t.Fatalf("parseCanonicalInt(%q) = (%d, true), want rejection", s, n)
		}
	}
}

// TestStrMGet pins the batch read door: emission order and values
// across hot, cold, missing, empty, and rope keys, the one-round
// coalescing claim for cold plain misses, and the copy-before-rope
// rule that keeps cold plain values alive when ropes in the same
// batch recycle the read buffers.
func TestStrMGet(t *testing.T) {
	r := newStrRig(t)
	ctx := context.Background()

	mget := func(s *Str, keys ...string) ([][]byte, []bool) {
		t.Helper()
		bs := make([][]byte, len(keys))
		for i, k := range keys {
			bs[i] = []byte(k)
		}
		var vals [][]byte
		var oks []bool
		err := s.MGet(ctx, bs, func(v []byte, ok bool) {
			// The value is valid only inside emit, so the test copies,
			// exactly like the reply builder consumes it.
			vals = append(vals, append([]byte(nil), v...))
			oks = append(oks, ok)
		})
		if err != nil {
			t.Fatalf("MGet(%q): %v", keys, err)
		}
		if len(vals) != len(keys) {
			t.Fatalf("MGet emitted %d values for %d keys", len(vals), len(keys))
		}
		return vals, oks
	}

	rope1 := pat(9<<10, 1)
	rope2 := pat(12<<10+37, 2)
	r.set("a", []byte("alpha"))
	r.set("empty", nil)
	r.set("r1", rope1)
	r.set("b", pat(600, 3))
	r.set("r2", rope2)
	r.set("c", []byte("charlie"))

	// Hot pass, ropes on both sides of plain values and a missing key
	// in the middle.
	vals, oks := mget(r.s, "a", "r1", "missing", "b", "r2", "empty", "c")
	wantVals := [][]byte{[]byte("alpha"), rope1, nil, pat(600, 3), rope2, {}, []byte("charlie")}
	wantOks := []bool{true, true, false, true, true, true, true}
	for i := range wantVals {
		if oks[i] != wantOks[i] || !bytes.Equal(vals[i], wantVals[i]) {
			t.Fatalf("hot MGet[%d]: (%d bytes, %v), want (%d bytes, %v)", i, len(vals[i]), oks[i], len(wantVals[i]), wantOks[i])
		}
	}

	// Cold pass: same answers through the store, and the plain misses
	// coalesce into exactly one BatchGet round. The ropes cost their
	// own assembly rounds on top, so the delta is pinned against a
	// plain-only batch.
	r.flush()
	s2 := r.reopen()
	before := s2.t.Stats().BatchReads
	vals, oks = mget(s2, "a", "missing", "b", "empty", "c")
	if got := s2.t.Stats().BatchReads - before; got != 1 {
		t.Fatalf("cold plain MGET cost %d BatchGet rounds, want 1", got)
	}
	wantVals = [][]byte{[]byte("alpha"), nil, pat(600, 3), {}, []byte("charlie")}
	wantOks = []bool{true, false, true, true, true}
	for i := range wantVals {
		if oks[i] != wantOks[i] || !bytes.Equal(vals[i], wantVals[i]) {
			t.Fatalf("cold MGet[%d]: (%d bytes, %v), want (%d bytes, %v)", i, len(vals[i]), oks[i], len(wantVals[i]), wantOks[i])
		}
	}

	// Cold pass with ropes interleaved: the rope assemblies recycle
	// the round's buffers, so correct plain values here prove the
	// copy-before-assembly rule.
	s3 := r.reopen()
	vals, oks = mget(s3, "c", "r1", "a", "r2", "b")
	wantVals = [][]byte{[]byte("charlie"), rope1, []byte("alpha"), rope2, pat(600, 3)}
	for i := range wantVals {
		if !oks[i] || !bytes.Equal(vals[i], wantVals[i]) {
			t.Fatalf("cold rope MGet[%d]: (%d bytes, %v), want %d bytes", i, len(vals[i]), oks[i], len(wantVals[i]))
		}
	}

	// An expired key is a miss mid-batch.
	r.set("dying", []byte("x"))
	if _, err := r.s.ExpireAt(ctx, []byte("dying"), 1<<41-1); err != nil {
		t.Fatal(err)
	}
	vals, oks = mget(r.s, "a", "dying", "c")
	if oks[0] != true || oks[1] != false || oks[2] != true || vals[1] != nil {
		t.Fatalf("expired key mid-batch: oks %v", oks)
	}
}

// TestStrMSet pins the batch write door: values across the ladder,
// the one-round meta prefetch, TTL survival on cold overwrites, rope
// plane retirement, and the repeated-key rule that rereads a meta the
// batch's own earlier write made stale.
func TestStrMSet(t *testing.T) {
	r := newStrRig(t)
	ctx := context.Background()

	keys := func(ss ...string) [][]byte {
		bs := make([][]byte, len(ss))
		for i, s := range ss {
			bs[i] = []byte(s)
		}
		return bs
	}

	// Create across the ladder in one call.
	if err := r.s.MSet(ctx, keys("a", "r", "empty"), [][]byte{[]byte("one"), pat(9<<10, 1), nil}); err != nil {
		t.Fatal(err)
	}
	r.want(r.s, "a", []byte("one"))
	r.want(r.s, "r", pat(9<<10, 1))
	r.want(r.s, "empty", []byte{})

	// Cold overwrite: metas prefetch in one round, TTLs survive the
	// Str layer (the command layer owns MSET's discard), and the rope
	// to plain shrink retires the plane.
	at := int64(1<<41) + 60_000
	for _, k := range []string{"a", "r"} {
		if _, err := r.s.ExpireAt(ctx, []byte(k), at); err != nil {
			t.Fatal(err)
		}
	}
	r.flush()
	s2 := r.reopen()
	before := s2.t.Stats().BatchReads
	if err := s2.MSet(ctx, keys("a", "r", "fresh"), [][]byte{[]byte("two"), []byte("small"), []byte("new")}); err != nil {
		t.Fatal(err)
	}
	if got := s2.t.Stats().BatchReads - before; got != 1 {
		t.Fatalf("cold MSET meta prefetch cost %d BatchGet rounds, want 1", got)
	}
	if err := s2.t.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	for k, want := range map[string]int64{"a": at, "r": at, "fresh": 0} {
		rec, err := r.rs.MemStore.Get(ctx, []byte(k))
		if err != nil {
			t.Fatalf("store Get(%q): %v", k, err)
		}
		if rec.ExpireMs != want {
			t.Fatalf("%q drained with expire_ms %d, want %d", k, rec.ExpireMs, want)
		}
		if rec.Root {
			t.Fatalf("%q still a root after plain MSET overwrite", k)
		}
	}

	// A key repeated in one MSET: the first write builds a rope, the
	// second shrinks it back, and the stale prefetched meta must not
	// leak the plane or resurrect the rope.
	if err := r.s.MSet(ctx, keys("dup", "dup"), [][]byte{pat(10<<10, 4), []byte("final")}); err != nil {
		t.Fatal(err)
	}
	r.want(r.s, "dup", []byte("final"))
	r.flush()
	if _, isRoot := r.storedRoot("dup"); isRoot {
		t.Fatal("dup key drained as a rope root after the shrink")
	}
	r.want(r.reopen(), "dup", []byte("final"))
}
