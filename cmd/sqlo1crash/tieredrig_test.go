package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/cespare/xxhash/v2"
	"github.com/tamnd/aki/engine/sqlo1"
	"github.com/tamnd/aki/engine/sqlo1b"
)

// The B3 exit-gate crash rig (spec 2064/sqlo1, milestone B3): the full
// Track B runtime composite, Tiered over a real sqlo1b store, killed
// mid-flight while drain, compaction, and eviction are all running.
// The worker half drives a seeded SET/DEL/GET stream over a bounded
// keyset with deliberately tiny budgets, so the hot tier evicts and
// force-drains constantly, overwrites pile garbage into sealed vlog
// extents, and the byte cap keeps extent pressure positive so the
// debt controller compacts while traffic runs. The parent half
// recovers the killed image on the bare store and holds it against
// invariants recomputed from the seed alone.
//
// What the suite claims, honestly: durability lands at ApplyBatch
// (the seam's WAL sync), so hot-tier dirty data that never drained is
// legitimately lost at a kill. The kill arm therefore checks prefix
// consistency and no-corruption, not zero loss: every surviving
// record must be byte-exact f(key, version) for a version the seeded
// stream really assigned to that key, recovery must be repeatable,
// and the recovered high-water mark can never fall below one the
// worker reported durable. The clean-shutdown control arm is where
// zero loss is asserted: after Flush plus Checkpoint, the store must
// hold exactly the stream's final state.

const (
	tieredDataFile = "t.aki"
	// tieredSegSize follows the shed test in tiered_b_test.go: one
	// full drain batch of a few-KiB values must fit one WAL segment.
	tieredSegSize = 8 << 20
	// tieredKeys bounds the keyset so the SET-heavy stream is mostly
	// overwrites, which is what feeds the compaction debt controller.
	tieredKeys = 384
	// Values come in two bands: a slotted band wide enough to produce
	// every group occupancy, several records per group through lone
	// big records right under BlobThreshold, and a true blob band, so
	// both record homes and the borderline between them all ride the
	// stream. This exact spread is what caught the untrimmed slot
	// slice misroute in compaction (fixed in #816): this preflight,
	// seed 4000001, poisoned the store within a few thousand ops when
	// a lone record's padded slice crossed the threshold.
	tieredValSlotMin  = 256
	tieredValSlotSpan = 3700
	tieredValBlobMin  = 4000
	tieredValBlobSpan = 352
	tieredBlobPct     = 30
	// The hot tier holds a third of the keyset, so refusals, forced
	// drains, and evictions never stop.
	tieredHotEntries = 128
	tieredHotArenas  = 8 << 20
	// tieredMaxBytes keeps the free-extent gauge positive once the
	// file grows past the reserve, which is what promotes compaction
	// to the foreground on every ladder step.
	tieredMaxBytes = 32 << 20
	// tieredCkptBytes is the WAL cadence: a checkpoint about every
	// couple of drain cycles, taken by the regular Tick.
	tieredCkptBytes = 256 << 10
	tieredTickEvery = 64
	// tieredProgressEvery paces the worker's PROGRESS markers; the
	// parent's op-count bound rides the last marker it read, and the
	// slack covers everything the worker could run past it.
	tieredProgressEvery = 512
	tieredBoundSlack    = 1 << 16
	// Warmup runs until drain, eviction, and compaction have all
	// demonstrably fired (never less than the min, never past the
	// cap), so every kill window opens on a steady-state composite.
	tieredWarmupMin = 2000
	tieredWarmupCap = 60000
	tieredValMagic  = 0x31767473 // "stv1"
)

// tieredKey names key idx; parseTieredKey inverts it strictly.
func tieredKey(idx int) []byte {
	return fmt.Appendf(nil, "tk%05d", idx)
}

func parseTieredKey(k []byte) (int, bool) {
	s := string(k)
	if !strings.HasPrefix(s, "tk") {
		return 0, false
	}
	idx, err := strconv.Atoi(s[2:])
	if err != nil || idx < 0 || idx >= tieredKeys {
		return 0, false
	}
	if !bytes.Equal(k, tieredKey(idx)) {
		return 0, false
	}
	return idx, true
}

// tieredValue is the self-describing record body: magic, key index,
// version, and total length up front, seeded filler in the middle,
// and an xxhash64 of everything before it at the tail. The whole
// value is a deterministic function of (seed, key, version), so the
// parent classifies any stored value with no journal: parse the
// version, regenerate, and compare bytes.
func tieredValue(seed uint64, key, ver uint32) []byte {
	rng := rand.New(rand.NewPCG(seed^0x7469657265647631, uint64(key)<<32|uint64(ver)))
	n := tieredValSlotMin + rng.IntN(tieredValSlotSpan)
	if rng.IntN(100) < tieredBlobPct {
		n = tieredValBlobMin + rng.IntN(tieredValBlobSpan)
	}
	b := make([]byte, n)
	binary.LittleEndian.PutUint32(b[0:], tieredValMagic)
	binary.LittleEndian.PutUint32(b[4:], key)
	binary.LittleEndian.PutUint32(b[8:], ver)
	binary.LittleEndian.PutUint32(b[12:], uint32(n))
	for i := 16; i < n-8; i++ {
		b[i] = byte(rng.UintN(256))
	}
	binary.LittleEndian.PutUint64(b[n-8:], xxhash.Sum64(b[:n-8]))
	return b
}

// parseTieredValue verifies a stored value against the key it sits
// under and returns the version it embeds. The checks are layered
// for error clarity, but the regeneration compare at the end is the
// authority: a torn or invented value cannot reproduce the exact
// bytes of any (key, version) the generator makes.
func parseTieredValue(seed uint64, key uint32, val []byte) (uint32, error) {
	if len(val) < tieredValSlotMin {
		return 0, fmt.Errorf("value is %d bytes, below the generator minimum", len(val))
	}
	if got := binary.LittleEndian.Uint32(val[0:]); got != tieredValMagic {
		return 0, fmt.Errorf("value magic %#x", got)
	}
	if got := binary.LittleEndian.Uint32(val[4:]); got != key {
		return 0, fmt.Errorf("value claims key %d but is stored under key %d", got, key)
	}
	ver := binary.LittleEndian.Uint32(val[8:])
	if ver == 0 {
		return 0, fmt.Errorf("value claims version 0, the stream starts at 1")
	}
	if got := binary.LittleEndian.Uint32(val[12:]); got != uint32(len(val)) {
		return 0, fmt.Errorf("value claims %d bytes, holds %d", got, len(val))
	}
	tail := len(val) - 8
	if got, want := xxhash.Sum64(val[:tail]), binary.LittleEndian.Uint64(val[tail:]); got != want {
		return 0, fmt.Errorf("value checksum %#x, tail holds %#x", got, want)
	}
	if !bytes.Equal(val, tieredValue(seed, key, ver)) {
		return 0, fmt.Errorf("value does not regenerate from (key %d, version %d)", key, ver)
	}
	return ver, nil
}

// The op stream: 60 percent SET, 15 percent DEL, 25 percent GET over
// the bounded keyset, everything drawn from one seeded PCG so the
// parent replays it bit-exactly. SET versions are per-key counters
// assigned whether or not the write later lands, so a shed write just
// leaves a hole the verifier's one-sided bound tolerates.
const (
	tieredOpSet = iota
	tieredOpDel
	tieredOpGet
)

type tieredOp struct {
	kind int
	key  int
	ver  uint32
}

type tieredStream struct {
	rng *rand.Rand
	ver []uint32
}

func newTieredStream(seed uint64) *tieredStream {
	return &tieredStream{
		rng: rand.New(rand.NewPCG(seed, 0x424354726b427233)),
		ver: make([]uint32, tieredKeys),
	}
}

func (s *tieredStream) next() tieredOp {
	k := s.rng.IntN(tieredKeys)
	switch p := s.rng.IntN(100); {
	case p < 60:
		s.ver[k]++
		return tieredOp{kind: tieredOpSet, key: k, ver: s.ver[k]}
	case p < 75:
		return tieredOp{kind: tieredOpDel, key: k}
	default:
		return tieredOp{kind: tieredOpGet, key: k}
	}
}

// simulateTiered replays the stream for n ops with no store: landed
// is the final version per key (0 means absent), skipping SETs whose
// op index is in shed, and maxVer is the highest version the stream
// assigned each key. The kill arm uses maxVer as its one-sided bound;
// the clean arm uses landed as the exact expected state.
func simulateTiered(seed uint64, n int, shed map[int]bool) (landed, maxVer []uint32) {
	st := newTieredStream(seed)
	landed = make([]uint32, tieredKeys)
	for i := range n {
		op := st.next()
		switch op.kind {
		case tieredOpSet:
			if !shed[i] {
				landed[op.key] = op.ver
			}
		case tieredOpDel:
			landed[op.key] = 0
		}
	}
	return landed, st.ver
}

// tieredRig is the runtime composite under test plus the in-process
// shadow the worker self-checks against. The shadow tracks what
// landed (shed SETs excluded), which the composite must serve exactly
// while the process lives; durability is the parent's business.
type tieredRig struct {
	seed   uint64
	db     *sqlo1b.Store
	tr     *sqlo1.Tiered
	st     *tieredStream
	landed []uint32
	ops    int
	sheds  int
	onShed func(op int)
}

func newTieredRuntime(dir string, seed uint64) (*tieredRig, error) {
	db, err := sqlo1b.CreateStore(filepath.Join(dir, tieredDataFile), tieredSegSize)
	if err != nil {
		return nil, err
	}
	db.SetMaxBytes(tieredMaxBytes)
	db.SetCheckpointPolicy(sqlo1b.CheckpointPolicy{Bytes: tieredCkptBytes})
	tr := sqlo1.NewTiered(db, sqlo1.TieredConfig{
		Budget:   sqlo1.Budget{Entries: tieredHotEntries, Arenas: tieredHotArenas},
		PromoteP: 0.5,
		Seed:     seed,
	})
	return &tieredRig{
		seed:   seed,
		db:     db,
		tr:     tr,
		st:     newTieredStream(seed),
		landed: make([]uint32, tieredKeys),
	}, nil
}

// step runs one stream op through the composite and self-checks it
// against the shadow, then ticks on the fixed cadence. ErrShed is the
// one tolerated failure: the version stays unlanded and the shed hook
// records the op index for the clean arm's exact replay.
func (r *tieredRig) step(ctx context.Context) error {
	op := r.st.next()
	key := tieredKey(op.key)
	switch op.kind {
	case tieredOpSet:
		val := tieredValue(r.seed, uint32(op.key), op.ver)
		err := r.tr.Set(ctx, key, val, sqlo1.TagString)
		switch {
		case errors.Is(err, sqlo1.ErrShed):
			r.sheds++
			if r.onShed != nil {
				r.onShed(r.ops)
			}
		case err != nil:
			return fmt.Errorf("op %d: Set(%s): %w", r.ops, key, err)
		default:
			r.landed[op.key] = op.ver
		}
	case tieredOpDel:
		gone, err := r.tr.Del(ctx, key)
		if err != nil {
			return fmt.Errorf("op %d: Del(%s): %w", r.ops, key, err)
		}
		if gone != (r.landed[op.key] != 0) {
			return fmt.Errorf("op %d: Del(%s) = %v, shadow holds version %d", r.ops, key, gone, r.landed[op.key])
		}
		r.landed[op.key] = 0
	case tieredOpGet:
		v, ok, err := r.tr.Get(ctx, key)
		if err != nil {
			return fmt.Errorf("op %d: Get(%s): %w", r.ops, key, err)
		}
		want := r.landed[op.key]
		if ok != (want != 0) {
			return fmt.Errorf("op %d: Get(%s) hit %v, shadow holds version %d", r.ops, key, ok, want)
		}
		if ok && !bytes.Equal(v, tieredValue(r.seed, uint32(op.key), want)) {
			return fmt.Errorf("op %d: Get(%s) bytes do not match version %d", r.ops, key, want)
		}
	}
	r.ops++
	if r.ops%tieredTickEvery == 0 {
		if err := r.tr.Tick(ctx); err != nil {
			return fmt.Errorf("op %d: Tick: %w", r.ops, err)
		}
	}
	return nil
}

// pressureProven reports whether all three maintenance loops have
// demonstrably fired: drain (the store's high-water mark advanced),
// eviction (the composite's victim counter moved), and compaction
// (the debt controller's counter moved). The worker refuses to open
// a kill window until this holds, so the knobs failing to reach any
// loop is a loud rig failure, never a silently weaker matrix.
func (r *tieredRig) pressureProven() bool {
	return r.db.Stats().HighWater > 0 &&
		r.tr.Stats().Evictions > 0 &&
		r.db.DebtStats().Compactions > 0
}

func (r *tieredRig) pressureReport() string {
	ss := r.db.Stats()
	ts := r.tr.Stats()
	ds := r.db.DebtStats()
	p := r.db.Pressure()
	return fmt.Sprintf(
		"ops=%d hw=%d keys=%d evictions=%d evictedBytes=%d coldHits=%d promotions=%d "+
			"compactions=%d relocatedBytes=%d garbage=%d candidates=%d overThreshold=%d "+
			"sheds=%d wal=%.2f extent=%.2f shed=%v",
		r.ops, ss.HighWater, ss.Keys, ts.Evictions, ts.EvictedBytes, ts.ColdHits, ts.Promotions,
		ds.Compactions, ds.RelocatedBytes, ds.GarbageBytes, ds.Candidates, ds.OverThreshold,
		r.sheds, p.Wal, p.Extent, p.Shed)
}

// readStoreGroup reads one group image at store geometry: group 0 is
// the short payload behind the extent header, the rest are whole
// 4 KiB groups (the fileGroups shape, reimplemented here because the
// parent verifies from outside the store).
func readStoreGroup(f *os.File, es uint32, ext uint64, grp uint16) ([]byte, error) {
	off := int64(ext)*int64(es) + int64(grp)*sqlo1b.GroupSize
	n := sqlo1b.GroupSize
	if grp == 0 {
		off += sqlo1b.ExtentHeaderSize
		n = sqlo1b.Group0Payload
	}
	b := make([]byte, n)
	if _, err := f.ReadAt(b, off); err != nil {
		return nil, fmt.Errorf("group %d/%d: %w", ext, grp, err)
	}
	return b, nil
}

// tieredGridFromSuper rebuilds the grid the way OpenStore does: from
// the committed allocmap snapshot (root group of full pointers, then
// bitmap pages, everything verified on the way) or fresh before the
// first checkpoint.
func tieredGridFromSuper(f *os.File, sb *sqlo1b.Superblock) (*sqlo1b.Grid, error) {
	if sb.AllocmapRoot == (sqlo1b.FullPtr{}) {
		return sqlo1b.NewGrid(sb.ExtentCount), nil
	}
	pos := sqlo1b.Pos(sb.AllocmapRoot.Pos)
	root, err := readStoreGroup(f, sb.ExtentSize, pos.Extent(), pos.Group())
	if err != nil {
		return nil, fmt.Errorf("allocmap root: %w", err)
	}
	if err := sb.AllocmapRoot.Verify(root); err != nil {
		return nil, fmt.Errorf("allocmap root: %w", err)
	}
	need := (sb.ExtentCount + 7) / 8
	pages := (need + sqlo1b.GroupSize - 1) / sqlo1b.GroupSize
	bitmap := make([]byte, 0, pages*sqlo1b.GroupSize)
	for i := range pages {
		pp := sqlo1b.FullPtr{
			Pos: binary.LittleEndian.Uint64(root[i*16:]),
			Sum: binary.LittleEndian.Uint64(root[i*16+8:]),
		}
		p := sqlo1b.Pos(pp.Pos)
		img, err := readStoreGroup(f, sb.ExtentSize, p.Extent(), p.Group())
		if err != nil {
			return nil, fmt.Errorf("allocmap page %d: %w", i, err)
		}
		if err := pp.Verify(img); err != nil {
			return nil, fmt.Errorf("allocmap page %d: %w", i, err)
		}
		bitmap = append(bitmap, img...)
	}
	return sqlo1b.LoadGrid(bitmap[:need], sb.ExtentCount)
}

// scrubTieredImage is the format-level pass over the killed image,
// before the store touches it: recovery picks a superblock and
// replays the tail, the grid restores from the committed snapshot
// plus the quarantine set, and the scrubber sweeps. Only findings on
// extents with a replayed seal checksum are failures: the allocmap
// bitmap does not distinguish active from sealed, so an extent that
// was an active tail at checkpoint time legitimately scrubs as
// unsealed-on-disk, exactly as in the B1 verifier.
func scrubTieredImage(dataPath, walPath string) error {
	f, err := os.Open(dataPath)
	if err != nil {
		return err
	}
	defer f.Close()
	rec, err := sqlo1b.Recover(f, walPath, tieredSegSize, nil)
	if err != nil {
		return fmt.Errorf("recover: %w", err)
	}
	defer rec.WAL.Close()
	sb := rec.Super
	if sb.ExtentSize != sqlo1b.DefaultExtentSize {
		return fmt.Errorf("superblock extent size %d, the store writes %d", sb.ExtentSize, sqlo1b.DefaultExtentSize)
	}
	grid, err := tieredGridFromSuper(f, sb)
	if err != nil {
		return err
	}
	if err := rec.RestoreGrid(grid); err != nil {
		return fmt.Errorf("grid restore: %w", err)
	}
	sums := map[uint64]uint64{}
	for _, s := range rec.Format.Seals {
		sums[s.Extent] = s.Sum
	}
	scr := &sqlo1b.Scrubber{File: f, ExtentSize: sb.ExtentSize, Grid: grid, Sums: sums}
	for _, fd := range scr.Sweep().Findings {
		if _, tracked := sums[fd.Extent]; tracked {
			return fmt.Errorf("sealed extent %d damaged after recovery: %v", fd.Extent, fd.Err)
		}
	}
	return nil
}

// scanTiered walks every live record and classifies it: the key must
// come from the stream's keyset, appear once, carry no expiry or
// generation (the stream sets neither), and the value must parse as
// f(key, version).
func scanTiered(ctx context.Context, db *sqlo1b.Store, seed uint64) (map[int]uint32, error) {
	seen := map[int]uint32{}
	var scanErr error
	_, err := db.Scan(ctx, nil, func(r sqlo1.Record) bool {
		idx, ok := parseTieredKey(r.Key)
		if !ok {
			scanErr = fmt.Errorf("scan found foreign key %q", r.Key)
			return false
		}
		if _, dup := seen[idx]; dup {
			scanErr = fmt.Errorf("scan delivered key %s twice", r.Key)
			return false
		}
		if r.Gen != 0 || r.ExpireMs != 0 {
			scanErr = fmt.Errorf("key %s carries gen %d expire %d, the stream sets neither", r.Key, r.Gen, r.ExpireMs)
			return false
		}
		ver, err := parseTieredValue(seed, uint32(idx), r.Value)
		if err != nil {
			scanErr = fmt.Errorf("key %s: %w", r.Key, err)
			return false
		}
		seen[idx] = ver
		return true
	})
	if err != nil {
		return nil, fmt.Errorf("scan: %w", err)
	}
	return seen, scanErr
}

// tieredRecovered summarizes what a verified image held, for the
// iteration log: a green matrix should show real state coming back,
// not vacuous passes over empty stores.
type tieredRecovered struct {
	Keys      int64
	HighWater int64
}

// verifyTieredRecovered holds a worker's image against the seed. The
// kill arm passes maxVer, the one-sided bound: any surviving version
// must be one the stream assigned, and missing records are legal
// because dirty hot-tier data is lost at this seam by design. The
// clean arm passes exact instead and demands the final state to the
// byte, which is the zero-loss control. minHW is the high-water mark
// the worker reported durable; recovery may only be at or past it,
// the exactly-once property from the seam contract. Both arms end
// with a second open of the same dir, because recovery must be
// repeatable, not merely survivable.
func verifyTieredRecovered(dataPath string, seed uint64, maxVer, exact []uint32, minHW int64) (tieredRecovered, error) {
	var out tieredRecovered
	if err := scrubTieredImage(dataPath, sqlo1.WALPath(dataPath)); err != nil {
		return out, fmt.Errorf("scrub pass: %w", err)
	}
	ctx := context.Background()

	db, err := sqlo1b.OpenStore(dataPath, tieredSegSize)
	if err != nil {
		return out, fmt.Errorf("reopen: %w", err)
	}
	st1 := db.Stats()
	out = tieredRecovered{Keys: st1.Keys, HighWater: st1.HighWater}
	seen, err := scanTiered(ctx, db, seed)
	if cerr := db.Close(); err == nil && cerr != nil {
		err = fmt.Errorf("close after scan: %w", cerr)
	}
	if err != nil {
		return out, err
	}
	if st1.HighWater < minHW {
		return out, fmt.Errorf("recovered high-water %d below the worker's durable %d", st1.HighWater, minHW)
	}
	if st1.Keys < 0 || st1.Keys > tieredKeys {
		return out, fmt.Errorf("recovered store holds %d keys over a %d-key stream", st1.Keys, tieredKeys)
	}
	if int64(len(seen)) != st1.Keys {
		return out, fmt.Errorf("scan delivered %d records, stats count %d", len(seen), st1.Keys)
	}
	if st1.DiskBytes <= 0 || st1.DiskBytes%int64(sqlo1b.DefaultExtentSize) != 0 {
		return out, fmt.Errorf("disk bytes %d is not a whole positive extent count", st1.DiskBytes)
	}
	if exact != nil {
		for idx, ver := range seen {
			if exact[idx] != ver {
				return out, fmt.Errorf("key %d recovered at version %d, clean shutdown owed %d", idx, ver, exact[idx])
			}
		}
		for idx, ver := range exact {
			if ver != 0 {
				if _, ok := seen[idx]; !ok {
					return out, fmt.Errorf("key %d version %d was acked and flushed but is gone", idx, ver)
				}
			}
		}
	} else {
		for idx, ver := range seen {
			if ver > maxVer[idx] {
				return out, fmt.Errorf("key %d recovered at version %d, the stream never went past %d", idx, ver, maxVer[idx])
			}
		}
	}

	// Recovery must be repeatable: a second open of the same dir has
	// to land on the same state, not merely succeed.
	db2, err := sqlo1b.OpenStore(dataPath, tieredSegSize)
	if err != nil {
		return out, fmt.Errorf("second reopen: %w", err)
	}
	st2 := db2.Stats()
	seen2, err := scanTiered(ctx, db2, seed)
	if cerr := db2.Close(); err == nil && cerr != nil {
		err = fmt.Errorf("close after second scan: %w", cerr)
	}
	if err != nil {
		return out, fmt.Errorf("second open: %w", err)
	}
	if st2.Keys != st1.Keys || st2.HighWater != st1.HighWater {
		return out, fmt.Errorf("second open drifted: keys %d -> %d, high-water %d -> %d",
			st1.Keys, st2.Keys, st1.HighWater, st2.HighWater)
	}
	if len(seen2) != len(seen) {
		return out, fmt.Errorf("second open scanned %d records, first %d", len(seen2), len(seen))
	}
	for idx, ver := range seen {
		if seen2[idx] != ver {
			return out, fmt.Errorf("second open holds key %d at version %d, first at %d", idx, seen2[idx], ver)
		}
	}
	return out, nil
}
