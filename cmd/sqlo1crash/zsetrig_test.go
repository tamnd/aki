package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"sort"
	"strconv"
	"strings"

	"github.com/cespare/xxhash/v2"
	"github.com/tamnd/aki/engine/sqlo1"
	"github.com/tamnd/aki/engine/sqlo1b"
)

// The T4 exit-gate crash rig (spec 2064/sqlo1, milestone T4): the
// zset ladder over Tiered over sqlo1b, SIGKILLed mid-stream,
// recovered, and held against the dual-plane oracle. The keyset spans
// the representation rungs at once (inline roots, segmented zsets a
// few fences and runs wide, one paged zset past both page boundaries,
// the member fence's 128-segment wall and the score fence's 100-run
// flat cap), and the steady state is score-move heavy on purpose:
// half the stream is ZADD with a fresh score, which on a live member
// is the dual-plane move (delete from the old run, insert into the
// new, member entry rewritten) whose torn tails Z-I4 exists for. A
// ZRANGESTORE cadence keeps bulk-build commits inside the kill
// windows, alternating a real window build with an empty window whose
// result deletes the destination.
//
// What the suite claims, Z-I2 and Z-I4 plus the zset half of rule W3:
// any recovered image must be a self-consistent zset state. ZVerify
// must pass on every key, which holds the score runs against the
// sorted member side and both against ZCARD; the ZSCAN walk, ZCARD,
// and per-member ZSCORE point reads must agree exactly; every walked
// member must regenerate byte for byte from (seed, key, index) and
// carry a score the stream actually assigned it; a member the bounded
// stream never removed must still be there (population is flushed
// before the kill window opens); and recovery must be repeatable.
// Losing a suffix of undrained writes is legal; a half-moved member,
// a member on one plane only, or a score neither old nor new never
// is. The clean-shutdown control arm demands the stream's exact final
// state back, member by member and score by score, destination
// included.

const (
	zsetDataFile = "z.aki"
	// zsetKeys spans the rungs: 4 inline, 2 segmented, 1 paged, plus
	// one ZRANGESTORE destination outside the banded universe.
	zsetKeys = 8
	// Band widths, members per key. Inline stays under both inline
	// caps (128 members, 2 KiB, with 8 score bytes riding every
	// member), segmented crosses the byte cap into a handful of
	// member segments and score runs, paged crosses both page
	// boundaries at once (~180 member segments past the 128-segment
	// fence wall, ~180 score runs past the 100-run flat cap, at
	// these member sizes; TestZSetPagedMembers and
	// TestZFencePagedLadder in the engine pin the paged machinery
	// itself).
	zsetInlineMembers = 40
	zsetSegMembers    = 500
	zsetPagedMembers  = 9000
	// Member-size bands, the set rig's shape.
	zsetMemInline = 24
	zsetMemSeg    = 40
	zsetMemPaged  = 64
	// Hot-tier budget, deliberately roomy: no sheds, no evictions.
	zsetHotEntries = 8192
	zsetHotArenas  = 64 << 20
	// Cadences: Flush is the durability ratchet, Tick keeps the
	// background loops honest, the STORE cadence keeps bulk-build
	// commits inside the kill windows.
	zsetTickEvery     = 64
	zsetFlushEvery    = 512
	zsetStoreEvery    = 192
	zsetProgressEvery = 256
	zsetBoundSlack    = 1 << 16
	// zsetStoreWindow is the real ZRANGESTORE arm: the first 400
	// ranks of the first segmented key.
	zsetStoreWindow = 400
	// zsetStoreSrc is that source key's index.
	zsetStoreSrc = 4
)

func zsetKeyName(idx int) []byte {
	return fmt.Appendf(nil, "zk%d", idx)
}

func zsetDestName() []byte {
	return []byte("zd0")
}

// zsetBandMembers is the member universe width of key idx.
func zsetBandMembers(idx int) int {
	switch {
	case idx < 4:
		return zsetInlineMembers
	case idx < 6:
		return zsetSegMembers
	default:
		return zsetPagedMembers
	}
}

func zsetMemberLen(idx int) int {
	switch {
	case idx < 4:
		return zsetMemInline
	case idx < 6:
		return zsetMemSeg
	default:
		return zsetMemPaged
	}
}

// zsetMember is the self-describing member: key and index up front,
// seeded filler in the middle, an xxhash64 of everything before it at
// the tail, a deterministic function of (seed, key, index) so the
// parent classifies any recovered member with no journal.
func zsetMember(seed uint64, key, idx uint32) []byte {
	n := zsetMemberLen(int(key))
	rng := rand.New(rand.NewPCG(seed^0x7a736574, uint64(key)<<32|uint64(idx)))
	b := make([]byte, 0, n)
	b = fmt.Appendf(b, "z%d-m%05d-", key, idx)
	for len(b) < n-8 {
		b = append(b, 'a'+byte(rng.UintN(26)))
	}
	var tail [8]byte
	binary.LittleEndian.PutUint64(tail[:], xxhash.Sum64(b))
	return append(b, tail[:]...)
}

// parseZsetMember verifies a recovered member against the key it sits
// under and returns the index it embeds. The regeneration compare at
// the end is the authority.
func parseZsetMember(seed uint64, key uint32, m []byte) (int, error) {
	s := string(m)
	if !strings.HasPrefix(s, fmt.Sprintf("z%d-m", key)) {
		return 0, fmt.Errorf("member %q does not carry key %d's prefix", m, key)
	}
	rest := s[strings.Index(s, "-m")+2:]
	dash := strings.IndexByte(rest, '-')
	if dash < 0 {
		return 0, fmt.Errorf("member %q has no index terminator", m)
	}
	idx, err := strconv.Atoi(rest[:dash])
	if err != nil || idx < 0 || idx >= zsetBandMembers(int(key)) {
		return 0, fmt.Errorf("member %q index is outside key %d's band", m, key)
	}
	if !bytes.Equal(m, zsetMember(seed, key, uint32(idx))) {
		return 0, fmt.Errorf("member does not regenerate from (key %d, index %d)", key, idx)
	}
	return idx, nil
}

// The op stream. A fixed population prefix adds every member of every
// banded key once at a generator-drawn score, which forces the paged
// key onto both page rungs before any kill window opens; after that,
// 50 percent ZADD with a fresh score (the dual-plane move on a live
// member), 20 percent ZINCRBY, 15 percent ZREM, 15 percent ZSCORE
// probes, and every zsetStoreEvery ops a ZRANGESTORE lands on the
// destination, alternating the real [0, 400) window of the first
// segmented key (a real bulk build across both planes) with a window
// past any reachable rank, whose empty result deletes the
// destination. STOREs ride the op counter, not the generator, so the
// parent replays the stream bit-exactly.
//
// Scores live on the quarter grid (n/4 with |n| bounded), so every
// sum in the stream is exact in float64 and the shadow compares
// scores with plain equality; nothing in the stream can produce -0 or
// NaN.
const (
	zOpAdd = iota
	zOpIncr
	zOpRem
	zOpScore
)

type zOp struct {
	kind   int
	key    int
	member int
	score  float64
}

type zStream struct {
	rng *rand.Rand
	pop int
}

func zsetPopOps() int {
	n := 0
	for k := range 7 {
		n += zsetBandMembers(k)
	}
	return n
}

func newZStream(seed uint64) *zStream {
	return &zStream{rng: rand.New(rand.NewPCG(seed, 0x54347a736574))}
}

func (s *zStream) freshScore() float64 {
	return float64(s.rng.IntN(100_000)) / 4
}

func (s *zStream) delta() float64 {
	return float64(s.rng.IntN(201)-100) / 4
}

func (s *zStream) next() zOp {
	if s.pop < zsetPopOps() {
		i := s.pop
		s.pop++
		for k := range 7 {
			if i < zsetBandMembers(k) {
				return zOp{kind: zOpAdd, key: k, member: i, score: s.freshScore()}
			}
			i -= zsetBandMembers(k)
		}
	}
	k := s.rng.IntN(7)
	m := s.rng.IntN(zsetBandMembers(k))
	switch p := s.rng.IntN(100); {
	case p < 50:
		return zOp{kind: zOpAdd, key: k, member: m, score: s.freshScore()}
	case p < 70:
		return zOp{kind: zOpIncr, key: k, member: m, score: s.delta()}
	case p < 85:
		return zOp{kind: zOpRem, key: k, member: m}
	default:
		return zOp{kind: zOpScore, key: k, member: m}
	}
}

// zsetShadow is the state the stream implies: presence and current
// score per banded member, plus the destination's last stored window
// (nil while deleted or never built). k4mem caches the source key's
// member bytes for the window sort's tiebreak.
type zdestEnt struct {
	member int
	score  float64
}

type zsetShadow struct {
	seed    uint64
	present [][]bool
	score   [][]float64
	dest    []zdestEnt
	k4mem   [][]byte
}

func newZsetShadow(seed uint64) *zsetShadow {
	sh := &zsetShadow{seed: seed, present: make([][]bool, 7), score: make([][]float64, 7)}
	for k := range sh.present {
		sh.present[k] = make([]bool, zsetBandMembers(k))
		sh.score[k] = make([]float64, zsetBandMembers(k))
	}
	sh.k4mem = make([][]byte, zsetBandMembers(zsetStoreSrc))
	for i := range sh.k4mem {
		sh.k4mem[i] = zsetMember(seed, zsetStoreSrc, uint32(i))
	}
	return sh
}

// applyStore folds the op-counter STORE cadence into the shadow: even
// stores snapshot the source's first zsetStoreWindow ranks in
// (score, member) order, odd stores are the empty window that deletes
// dest.
func (sh *zsetShadow) applyStore(nth int) {
	if nth%2 == 1 {
		sh.dest = nil
		return
	}
	var live []zdestEnt
	for i, p := range sh.present[zsetStoreSrc] {
		if p {
			live = append(live, zdestEnt{member: i, score: sh.score[zsetStoreSrc][i]})
		}
	}
	sort.Slice(live, func(a, b int) bool {
		if live[a].score != live[b].score {
			return live[a].score < live[b].score
		}
		return bytes.Compare(sh.k4mem[live[a].member], sh.k4mem[live[b].member]) < 0
	})
	if len(live) > zsetStoreWindow {
		live = live[:zsetStoreWindow]
	}
	sh.dest = live
}

// simulateZ replays the stream for n ops: the final shadow, legal
// marking every score each member ever held in [0, n) (population
// included), and everRemoved marking members some ZREM touched. The
// kill arm's must-be-present set is population minus everRemoved, and
// a recovered score outside legal is a torn move by definition; the
// clean arm uses the shadow as the exact state owed.
func simulateZ(seed uint64, n int) (sh *zsetShadow, legal []map[uint64]map[float64]bool, everRemoved [][]bool) {
	st := newZStream(seed)
	sh = newZsetShadow(seed)
	legal = make([]map[uint64]map[float64]bool, 7)
	everRemoved = make([][]bool, 7)
	for k := range 7 {
		legal[k] = map[uint64]map[float64]bool{}
		everRemoved[k] = make([]bool, zsetBandMembers(k))
	}
	mark := func(k, m int, sc float64) {
		set := legal[k][uint64(m)]
		if set == nil {
			set = map[float64]bool{}
			legal[k][uint64(m)] = set
		}
		set[sc] = true
	}
	for i := 1; i <= n; i++ {
		op := st.next()
		switch op.kind {
		case zOpAdd:
			sh.present[op.key][op.member] = true
			sh.score[op.key][op.member] = op.score
			mark(op.key, op.member, op.score)
		case zOpIncr:
			base := 0.0
			if sh.present[op.key][op.member] {
				base = sh.score[op.key][op.member]
			}
			sh.present[op.key][op.member] = true
			sh.score[op.key][op.member] = base + op.score
			mark(op.key, op.member, base+op.score)
		case zOpRem:
			if sh.present[op.key][op.member] {
				everRemoved[op.key][op.member] = true
			}
			sh.present[op.key][op.member] = false
		}
		if i > zsetPopOps() && i%zsetStoreEvery == 0 {
			sh.applyStore(i / zsetStoreEvery)
		}
	}
	return sh, legal, everRemoved
}

// zsetCrashRig is the composite under test plus the in-process shadow.
type zsetCrashRig struct {
	seed   uint64
	db     *sqlo1b.Store
	tr     *sqlo1.Tiered
	z      *sqlo1.ZSet
	st     *zStream
	shadow *zsetShadow
	ops    int
}

func newZsetCrashRig(dir string, seed uint64) (*zsetCrashRig, error) {
	db, err := sqlo1b.CreateStore(dir+"/"+zsetDataFile, tieredSegSize)
	if err != nil {
		return nil, err
	}
	db.SetCheckpointPolicy(sqlo1b.CheckpointPolicy{Bytes: tieredCkptBytes})
	tr := sqlo1.NewTiered(db, sqlo1.TieredConfig{
		Budget: sqlo1.Budget{Entries: zsetHotEntries, Arenas: zsetHotArenas},
		Seed:   seed,
	})
	z, err := sqlo1.NewZSet(tr, sqlo1.HashConfig{})
	if err != nil {
		db.Close()
		return nil, err
	}
	return &zsetCrashRig{seed: seed, db: db, tr: tr, z: z, st: newZStream(seed), shadow: newZsetShadow(seed)}, nil
}

// step runs one stream op through the composite and self-checks it
// against the shadow live. Any engine error is fatal: budgets are
// sized so nothing sheds, so a refusal here is a rig bug, not load.
func (r *zsetCrashRig) step(ctx context.Context) error {
	op := r.st.next()
	key := zsetKeyName(op.key)
	member := zsetMember(r.seed, uint32(op.key), uint32(op.member))
	present := r.shadow.present[op.key][op.member]
	oldScore := r.shadow.score[op.key][op.member]
	switch op.kind {
	case zOpAdd:
		added, changed, out, outOK, err := r.z.ZAdd(ctx, key, member, op.score, sqlo1.ZAddFlags{})
		if err != nil {
			return fmt.Errorf("op %d: ZAdd(%s, m%05d): %w", r.ops, key, op.member, err)
		}
		if added == present || !outOK || out != op.score {
			return fmt.Errorf("op %d: ZAdd(%s, m%05d) = (%v, %v, %v, %v), shadow disagrees", r.ops, key, op.member, added, changed, out, outOK)
		}
		if wantChanged := present && op.score != oldScore; changed != wantChanged {
			return fmt.Errorf("op %d: ZAdd(%s, m%05d) changed=%v, shadow says %v", r.ops, key, op.member, changed, wantChanged)
		}
		r.shadow.present[op.key][op.member] = true
		r.shadow.score[op.key][op.member] = op.score
	case zOpIncr:
		want := op.score
		if present {
			want = oldScore + op.score
		}
		out, err := r.z.ZIncrBy(ctx, key, op.score, member)
		if err != nil {
			return fmt.Errorf("op %d: ZIncrBy(%s, m%05d): %w", r.ops, key, op.member, err)
		}
		if out != want {
			return fmt.Errorf("op %d: ZIncrBy(%s, m%05d) = %v, shadow holds %v", r.ops, key, op.member, out, want)
		}
		r.shadow.present[op.key][op.member] = true
		r.shadow.score[op.key][op.member] = want
	case zOpRem:
		gone, err := r.z.ZRem(ctx, key, member)
		if err != nil {
			return fmt.Errorf("op %d: ZRem(%s, m%05d): %w", r.ops, key, op.member, err)
		}
		if gone != present {
			return fmt.Errorf("op %d: ZRem(%s, m%05d) = %v, shadow disagrees", r.ops, key, op.member, gone)
		}
		r.shadow.present[op.key][op.member] = false
	case zOpScore:
		sc, ok, err := r.z.ZScore(ctx, key, member)
		if err != nil {
			return fmt.Errorf("op %d: ZScore(%s, m%05d): %w", r.ops, key, op.member, err)
		}
		if ok != present || (ok && sc != oldScore) {
			return fmt.Errorf("op %d: ZScore(%s, m%05d) = (%v, %v), shadow holds (%v, %v)", r.ops, key, op.member, sc, ok, oldScore, present)
		}
	}
	r.ops++
	if r.ops > zsetPopOps() && r.ops%zsetStoreEvery == 0 {
		nth := r.ops / zsetStoreEvery
		var lo, hi int64 = 0, zsetStoreWindow
		if nth%2 == 1 {
			lo, hi = 1<<40, 1<<40+8
		}
		n, err := r.z.ZRangeStore(ctx, zsetDestName(), zsetKeyName(zsetStoreSrc), lo, hi)
		if err != nil {
			return fmt.Errorf("op %d: store %d: %w", r.ops, nth, err)
		}
		r.shadow.applyStore(nth)
		if n != int64(len(r.shadow.dest)) {
			return fmt.Errorf("op %d: store %d returned %d, shadow holds %d", r.ops, nth, n, len(r.shadow.dest))
		}
	}
	if r.ops%zsetTickEvery == 0 {
		if err := r.tr.Tick(ctx); err != nil {
			return fmt.Errorf("op %d: Tick: %w", r.ops, err)
		}
	}
	return nil
}

// selfCheckCounts holds ZCARD against the live shadow on every key,
// the in-process half of the count oracle.
func (r *zsetCrashRig) selfCheckCounts(ctx context.Context) error {
	for k := range 7 {
		want := int64(0)
		for _, p := range r.shadow.present[k] {
			if p {
				want++
			}
		}
		n, err := r.z.ZCard(ctx, zsetKeyName(k))
		if err != nil {
			return fmt.Errorf("ZCard(%s): %w", zsetKeyName(k), err)
		}
		if n != want {
			return fmt.Errorf("ZCard(%s) = %d, shadow holds %d live members", zsetKeyName(k), n, want)
		}
	}
	return nil
}

// zsetRecovered summarizes a verified image for the iteration log.
type zsetRecovered struct {
	Members   int
	HighWater int64
}

// verifyZsetRecovered holds a killed or cleanly closed image against
// the seed. The kill arm passes legal plus everRemoved (a member
// outside everRemoved must be present, population is flushed, and any
// present member's score must be one the stream actually assigned it)
// with exact nil; the clean arm passes the exact shadow and demands
// the final state to the score, destination included. Both arms run
// ZVerify on every key first, the Z-I4 dual-plane oracle over the
// recovered image, then hold the ZSCAN walk, ZCARD, and per-member
// point reads to agreement, then reopen a second time and demand the
// same state back.
func verifyZsetRecovered(dataPath string, seed uint64, legal []map[uint64]map[float64]bool, everRemoved [][]bool, exact *zsetShadow, minHW int64) (zsetRecovered, error) {
	var out zsetRecovered
	if err := scrubTieredImage(dataPath, sqlo1.WALPath(dataPath)); err != nil {
		return out, fmt.Errorf("scrub pass: %w", err)
	}
	ctx := context.Background()

	open := func() (*sqlo1b.Store, *sqlo1.ZSet, error) {
		db, err := sqlo1b.OpenStore(dataPath, tieredSegSize)
		if err != nil {
			return nil, nil, fmt.Errorf("reopen: %w", err)
		}
		tr := sqlo1.NewTiered(db, sqlo1.TieredConfig{
			Budget: sqlo1.Budget{Entries: zsetHotEntries, Arenas: zsetHotArenas},
			Seed:   seed + 1,
		})
		z, err := sqlo1.NewZSet(tr, sqlo1.HashConfig{})
		if err != nil {
			db.Close()
			return nil, nil, err
		}
		return db, z, nil
	}

	// walkKey walks one key under ZVerify and the count oracle and
	// returns member id to score; parse classifies a member without
	// engine calls, so the aliasing emit contract holds.
	walkKey := func(z *sqlo1.ZSet, key []byte, parse func(m []byte) (uint64, error)) (map[uint64]float64, error) {
		if err := z.ZVerify(ctx, key); err != nil {
			return nil, err
		}
		walked := map[uint64]float64{}
		var walkErr error
		cursor := uint64(0)
		for {
			next, err := z.ZScan(ctx, key, cursor, 512, func(m []byte, sc float64) {
				if walkErr != nil {
					return
				}
				id, err := parse(m)
				if err != nil {
					walkErr = fmt.Errorf("%s: %w", key, err)
					return
				}
				if _, dup := walked[id]; dup {
					walkErr = fmt.Errorf("%s: walk delivered member %d twice", key, id)
					return
				}
				walked[id] = sc
			})
			if err != nil {
				return nil, fmt.Errorf("%s: ZScan: %w", key, err)
			}
			if walkErr != nil {
				return nil, walkErr
			}
			if next == 0 {
				break
			}
			cursor = next
		}
		card, err := z.ZCard(ctx, key)
		if err != nil {
			return nil, fmt.Errorf("%s: ZCard: %w", key, err)
		}
		if int(card) != len(walked) {
			return nil, fmt.Errorf("%s: ZCARD %d but %d members reachable (Z-I2 drift)", key, card, len(walked))
		}
		return walked, nil
	}

	// checkKey is walkKey plus the per-member point reads over the
	// key's whole band and the per-member legality or exactness arm.
	checkKey := func(z *sqlo1.ZSet, k int) (map[uint64]float64, error) {
		key := zsetKeyName(k)
		walked, err := walkKey(z, key, func(m []byte) (uint64, error) {
			idx, err := parseZsetMember(seed, uint32(k), m)
			return uint64(idx), err
		})
		if err != nil {
			return nil, err
		}
		for i := range zsetBandMembers(k) {
			sc, ok, err := z.ZScore(ctx, key, zsetMember(seed, uint32(k), uint32(i)))
			if err != nil {
				return nil, fmt.Errorf("%s: ZScore m%05d: %w", key, i, err)
			}
			wsc, hit := walked[uint64(i)]
			if ok != hit || (ok && sc != wsc) {
				return nil, fmt.Errorf("%s member m%05d: point read (%v, %v), walk (%v, %v)", key, i, sc, ok, wsc, hit)
			}
		}
		if _, _, err := z.Encoding(ctx, key); err != nil {
			return nil, fmt.Errorf("%s: Encoding: %w", key, err)
		}
		if exact == nil {
			for i, removed := range everRemoved[k] {
				sc, hit := walked[uint64(i)]
				if !removed && !hit {
					return nil, fmt.Errorf("%s member m%05d was flushed at population, never removed, and is gone", key, i)
				}
				if hit && !legal[k][uint64(i)][sc] {
					return nil, fmt.Errorf("%s member m%05d recovered at score %v, which the stream never assigned (torn move)", key, i, sc)
				}
			}
		} else {
			for i, want := range exact.present[k] {
				sc, hit := walked[uint64(i)]
				if want != hit || (hit && sc != exact.score[k][i]) {
					return nil, fmt.Errorf("%s member m%05d recovered (%v, %v), clean shutdown owed (%v, %v)", key, i, sc, hit, exact.score[k][i], want)
				}
			}
		}
		return walked, nil
	}

	// checkDest verifies the ZRANGESTORE destination: every member
	// must parse under the source key with a score the source stream
	// assigned it; the kill arm stops there since the dest's legal
	// states churn with the cadence, the clean arm demands the last
	// stored window exactly.
	checkDest := func(z *sqlo1.ZSet) (map[uint64]float64, error) {
		key := zsetDestName()
		walked, err := walkKey(z, key, func(m []byte) (uint64, error) {
			idx, err := parseZsetMember(seed, zsetStoreSrc, m)
			return uint64(idx), err
		})
		if err != nil {
			return nil, err
		}
		for id, sc := range walked {
			psc, ok, err := z.ZScore(ctx, key, zsetMember(seed, zsetStoreSrc, uint32(id)))
			if err != nil || !ok || psc != sc {
				return nil, fmt.Errorf("%s: walked member m%05d fails the point read: (%v, %v, %v)", key, id, psc, ok, err)
			}
			if exact == nil && !legal[zsetStoreSrc][id][sc] {
				return nil, fmt.Errorf("%s member m%05d carries score %v the source stream never assigned", key, id, sc)
			}
		}
		if exact != nil {
			if len(walked) != len(exact.dest) {
				return nil, fmt.Errorf("%s holds %d members, clean shutdown owed %d", key, len(walked), len(exact.dest))
			}
			for _, e := range exact.dest {
				sc, hit := walked[uint64(e.member)]
				if !hit || sc != e.score {
					return nil, fmt.Errorf("%s member m%05d recovered (%v, %v), last store owed %v", key, e.member, sc, hit, e.score)
				}
			}
		}
		return walked, nil
	}

	db, z, err := open()
	if err != nil {
		return out, err
	}
	st1 := db.Stats()
	out.HighWater = st1.HighWater
	first := make([]map[uint64]float64, zsetKeys)
	for k := range 7 {
		walked, err := checkKey(z, k)
		if err != nil {
			db.Close()
			return out, err
		}
		first[k] = walked
		out.Members += len(walked)
	}
	destWalked, err := checkDest(z)
	if err != nil {
		db.Close()
		return out, err
	}
	first[7] = destWalked
	if err := db.Close(); err != nil {
		return out, fmt.Errorf("close after verify: %w", err)
	}
	if st1.HighWater < minHW {
		return out, fmt.Errorf("recovered high-water %d below the worker's durable %d", st1.HighWater, minHW)
	}

	// Recovery must be repeatable: a second open of the same dir has
	// to land on the same zset states, scores included, not merely
	// succeed.
	db2, z2, err := open()
	if err != nil {
		return out, fmt.Errorf("second reopen: %w", err)
	}
	defer db2.Close()
	if hw2 := db2.Stats().HighWater; hw2 != st1.HighWater {
		return out, fmt.Errorf("second open drifted: high-water %d -> %d", st1.HighWater, hw2)
	}
	same := func(name string, a, b map[uint64]float64) error {
		if len(a) != len(b) {
			return fmt.Errorf("second open walked %d members on %s, first %d", len(b), name, len(a))
		}
		for id, sc := range a {
			sc2, hit := b[id]
			if !hit || sc2 != sc {
				return fmt.Errorf("second open drifted on %s member m%05d: (%v, %v) vs %v", name, id, sc2, hit, sc)
			}
		}
		return nil
	}
	for k := range 7 {
		walked, err := checkKey(z2, k)
		if err != nil {
			return out, fmt.Errorf("second open: %w", err)
		}
		if err := same(string(zsetKeyName(k)), first[k], walked); err != nil {
			return out, err
		}
	}
	destWalked2, err := checkDest(z2)
	if err != nil {
		return out, fmt.Errorf("second open: %w", err)
	}
	return out, same("zd0", first[7], destWalked2)
}
