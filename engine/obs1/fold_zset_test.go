package obs1_test

import (
	"encoding/binary"
	"fmt"
	"math"
	"sort"
	"testing"

	"github.com/tamnd/aki/engine/obs1"
	"github.com/tamnd/aki/engine/obs1/store"
)

// The zset dual projection on the fold plane (spec 2064/obs1 doc 08 section
// 5): the demoter's two chunk kinds fold into one segment under one
// collection key, ZSCORE floors the member-hash projection through the
// kind-aware field reader, the score plane floors by its own coordinate, and
// the two projections cross-check to the identical pair multiset (T-I3). The
// frames are built with the demoter's own codec and coordinates, so the byte
// stream under test is the demote tap's.

// The two projection kinds, format like kindSetChunk above: 0x02 is the
// member-hash projection, 0x06 the score runs.
const (
	kindZsetMemberChunk = 0x02
	kindZsetScoreChunk  = 0x06
)

// zsetScoreKey mirrors the demoter's IEEE-754 total-order lift, the score
// coordinate the run discs lead with.
func zsetScoreKey(s float64) uint64 {
	b := math.Float64bits(s)
	if b&(1<<63) != 0 {
		return ^b
	}
	return b | 1<<63
}

// zsetPair is one (member, score) of the fixture zset.
type zsetPair struct {
	member string
	score  float64
}

// zsetDualFrames packs the pairs into both projections' demoter-shaped
// frames: score runs sorted by (score key, member) with the composite disc,
// member chunks sorted by Disc with the 8-byte member coordinate. Values are
// the raw score bits in both, the self-describing payload the slice ships.
func zsetDualFrames(key string, pairs []zsetPair, perChunk int) []byte {
	var buf []byte
	var pk store.ChunkPacker

	runs := append([]zsetPair(nil), pairs...)
	sort.Slice(runs, func(i, j int) bool {
		ki, kj := zsetScoreKey(runs[i].score), zsetScoreKey(runs[j].score)
		if ki != kj {
			return ki < kj
		}
		return runs[i].member < runs[j].member
	})
	for i := 0; i < len(runs); i += perChunk {
		end := min(i+perChunk, len(runs))
		pk.Reset()
		for _, p := range runs[i:end] {
			var v [8]byte
			binary.BigEndian.PutUint64(v[:], math.Float64bits(p.score))
			pk.Add([]byte(p.member), v[:], 0)
		}
		payload, flags := pk.Finish()
		var disc [8]byte
		binary.BigEndian.PutUint64(disc[:], zsetScoreKey(runs[i].score))
		d := append(disc[:], []byte(runs[i].member)...)
		buf = store.AppendRunChunk(buf, kindZsetScoreChunk|store.ChunkKindBit, flags, uint16(pk.Count()), []byte(key), d, payload)
	}

	hash := append([]zsetPair(nil), pairs...)
	sort.Slice(hash, func(i, j int) bool {
		return obs1.Disc([]byte(hash[i].member)) < obs1.Disc([]byte(hash[j].member))
	})
	for i := 0; i < len(hash); i += perChunk {
		end := min(i+perChunk, len(hash))
		pk.Reset()
		for _, p := range hash[i:end] {
			var v [8]byte
			binary.BigEndian.PutUint64(v[:], math.Float64bits(p.score))
			pk.Add([]byte(p.member), v[:], 0)
		}
		payload, flags := pk.Finish()
		var disc [8]byte
		binary.BigEndian.PutUint64(disc[:], obs1.Disc([]byte(hash[i].member)))
		buf = store.AppendRunChunk(buf, kindZsetMemberChunk|store.ChunkKindBit, flags, uint16(pk.Count()), []byte(key), disc[:], payload)
	}
	return buf
}

func zsetFixture(n int) []zsetPair {
	out := make([]zsetPair, n)
	for i := range out {
		out[i] = zsetPair{member: fmt.Sprintf("player%03d", i), score: float64(i) * 1.5}
	}
	return out
}

// fetchFieldKindWait is fetchFieldWait through the kind-restricted read, the
// ZSCORE plan that must land on the member projection.
func fetchFieldKindWait(t *testing.T, cr *obs1.ColdReader, key string, loc obs1.KeyLoc, field string, kind uint8, nowMs int64) (obs1.ColdField, error) {
	t.Helper()
	var (
		cf   obs1.ColdField
		rerr error
		done = make(chan struct{})
	)
	cr.FetchFieldKind(3, []byte(key), loc, []byte(field), kind, nowMs, func(f obs1.ColdField, err error) {
		cf, rerr = f, err
		close(done)
	})
	<-done
	return cf, rerr
}

// TestFolderZsetDualProjection folds one zset's dual frames and drives the
// reads: ZSCORE through the kind-restricted field reader answers every
// member's score bits off the member projection, a stranger misses, the
// score-plane floor resolves score-run chunks by the score coordinate, and
// the projection cross-check decodes both kinds to the identical multiset.
func TestFolderZsetDualProjection(t *testing.T) {
	fx, km, dir := newFoldDirFixture(t)
	const nowMs = int64(1_700_000_000_000)
	pairs := zsetFixture(60)

	fx.folder.Add(zsetDualFrames("z1", pairs, 16))
	fx.folder.Flush()
	waitFor(t, "publish", func() bool { return len(fx.folder.Ledger()) == 1 })
	led := fx.folder.Ledger()[0]

	fp := obs1.Fingerprint([]byte("z1"))
	loc, ok := km.Lookup(fp)
	if !ok || loc.Seg != uint32(led.SegSeq) {
		t.Fatalf("z1 locator %+v ok=%v, want seg %d", loc, ok, led.SegSeq)
	}

	cr, err := obs1.NewColdReader(obs1.ColdReadConfig{
		Store: fx.sim,
		Dir:   func(group uint16) *obs1.Directory { return dir },
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cr.Close)

	// ZSCORE: every member answers its raw score bits off the member-hash
	// projection; the kind restriction is what keeps the floor off the score
	// runs, whose discs live in a different coordinate space.
	memberKind := uint8(kindZsetMemberChunk | store.ChunkKindBit)
	for _, p := range pairs {
		cf, err := fetchFieldKindWait(t, cr, "z1", loc, p.member, memberKind, nowMs)
		if err != nil {
			t.Fatalf("%s: %v", p.member, err)
		}
		if !cf.Found || len(cf.Value) != 8 {
			t.Fatalf("%s = %+v, want found with the 8 score bytes", p.member, cf)
		}
		if got := math.Float64frombits(binary.BigEndian.Uint64(cf.Value)); got != p.score {
			t.Fatalf("%s score %v, want %v", p.member, got, p.score)
		}
	}
	cf, err := fetchFieldKindWait(t, cr, "z1", loc, "stranger", memberKind, nowMs)
	if err != nil || cf.Found {
		t.Fatalf("stranger = %+v err %v, want a clean miss", cf, err)
	}

	// The score plane floors by its own coordinate: a kind-restricted resolve
	// at a mid-band score key lands on a score-run chunk, never a member one.
	scoreKind := uint8(kindZsetScoreChunk | store.ChunkKindBit)
	ref, ok := dir.ResolveFieldKind(loc, fp, zsetScoreKey(pairs[len(pairs)/2].score), scoreKind)
	if !ok {
		t.Fatal("score-plane floor resolved nothing")
	}
	if ref.ChunkKind != scoreKind {
		t.Fatalf("score-plane floor landed on kind 0x%02x, want 0x%02x", ref.ChunkKind, scoreKind)
	}

	// T-I3: walk every collection chunk, split by kind caller-side, and hold
	// the two projections to the identical pair multiset.
	refs := dir.CollChunks(loc, fp)
	if len(refs) < 4 {
		t.Fatalf("planned %d chunks, want both projections' packs split", len(refs))
	}
	proj := map[uint8]map[string]uint64{
		memberKind: {},
		scoreKind:  {},
	}
	ctx := t.Context()
	for _, r := range refs {
		got, ok := proj[r.ChunkKind]
		if !ok {
			t.Fatalf("planned chunk of kind 0x%02x, want a zset projection kind", r.ChunkKind)
		}
		off, n := r.Block.BlockSpan()
		raw, _, err := fx.sim.GetRange(ctx, r.ObjKey, off, n)
		if err != nil {
			t.Fatalf("fetch block: %v", err)
		}
		data, err := obs1.ParseSegmentBlock(raw, r.Block)
		if err != nil {
			t.Fatalf("parse block: %v", err)
		}
		err = obs1.WalkColdFields(data, r.OffInBlock, nowMs, func(p store.PackedPair) error {
			got[string(p.Field)] = binary.BigEndian.Uint64(p.Value)
			return nil
		})
		if err != nil {
			t.Fatalf("walk chunk: %v", err)
		}
	}
	for _, m := range proj {
		if len(m) != len(pairs) {
			t.Fatalf("a projection decodes %d pairs, want %d", len(m), len(pairs))
		}
	}
	for _, p := range pairs {
		want := math.Float64bits(p.score)
		if proj[memberKind][p.member] != want || proj[scoreKind][p.member] != want {
			t.Fatalf("%s bits %d/%d across projections, want %d", p.member,
				proj[memberKind][p.member], proj[scoreKind][p.member], want)
		}
	}
}
