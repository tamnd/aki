package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"strconv"
	"strings"

	"github.com/cespare/xxhash/v2"
	"github.com/tamnd/aki/engine/sqlo1"
	"github.com/tamnd/aki/engine/sqlo1b"
)

// The T3 exit-gate crash rig (spec 2064/sqlo1, milestone T3): the set
// ladder over Tiered over sqlo1b, SIGKILLed mid-stream, recovered, and
// held against the SCARD exactness oracle. The keyset spans the three
// representation rungs at once (inline roots, segmented sets a few
// fences wide, paged sets past the 128-segment boundary), and the
// stream folds in a STORE cadence so kill windows also cut the bulk
// build's multi-record commits (segments, fence pages, the root PUT).
//
// What the suite claims, SE-I2 plus the set half of rule W3: any
// recovered image must be a self-consistent set state. SCARD, the
// walk's begin(count), the walked member set, and per-member point
// reads must agree exactly on every key, every walked member must
// regenerate byte for byte from (seed, key, index), a member the
// bounded stream never removed must still be there (population is
// flushed before the kill window opens), and recovery must be
// repeatable. Losing a suffix of undrained writes is legal;
// disagreeing with itself never is. The STORE destination adds the
// atomicity claim: the bulk build's root PUT is the commit point, so
// the destination is always absent or a whole self-consistent set,
// never a partial build. Members carry no version channel the way
// hash values do, so the kill arm's membership legality is one-sided
// where the hash's was version-bounded; the clean-shutdown control
// arm closes that gap by demanding the stream's exact final state
// back, destination included. SPOP stays out of the stream on
// purpose: the engine picks the victim, so a killed parent cannot
// replay the choice; SPOP's crash story is the removal path it rides,
// which SREM covers.

const (
	setDataFile = "s.aki"
	// setKeys spans the rungs: 4 inline, 2 segmented, 2 paged, plus
	// one STORE destination that is not part of the banded universe.
	setKeys = 8
	// Band widths, members per key. Inline stays under both inline
	// caps (128 members, 2 KiB), segmented crosses the byte cap into
	// a handful of segments, paged crosses the 128-segment fence
	// boundary (~150 segments at these member sizes).
	setInlineMembers = 48
	setSegMembers    = 600
	setPagedMembers  = 9000
	// Member-size bands. A member is prefix, filler, xxhash tail;
	// the paged band's 64 B members are what push segment counts
	// past the page boundary with a keyset a local run can walk.
	setMemInline = 24
	setMemSeg    = 40
	setMemPaged  = 64
	// Hot-tier budget, deliberately roomy: no sheds, no evictions.
	setHotEntries = 8192
	setHotArenas  = 64 << 20
	// Cadences, the hash rig's shape: Flush is the durability
	// ratchet, Tick keeps the background loops honest, the STORE
	// cadence keeps bulk-build commits inside the kill windows.
	setTickEvery     = 64
	setFlushEvery    = 512
	setStoreEvery    = 192
	setProgressEvery = 256
	setBoundSlack    = 1 << 16
)

func setKeyName(idx int) []byte {
	return fmt.Appendf(nil, "sk%d", idx)
}

func setDestName() []byte {
	return []byte("sd0")
}

// setBandMembers is the member universe width of key idx.
func setBandMembers(idx int) int {
	switch {
	case idx < 4:
		return setInlineMembers
	case idx < 6:
		return setSegMembers
	default:
		return setPagedMembers
	}
}

func setMemberLen(idx int) int {
	switch {
	case idx < 4:
		return setMemInline
	case idx < 6:
		return setMemSeg
	default:
		return setMemPaged
	}
}

// setMember is the self-describing member: key and index up front,
// seeded filler in the middle, an xxhash64 of everything before it at
// the tail. The whole member is a deterministic function of
// (seed, key, index), so the parent classifies any recovered member
// with no journal.
func setMember(seed uint64, key, idx uint32) []byte {
	n := setMemberLen(int(key))
	rng := rand.New(rand.NewPCG(seed^0x73657476, uint64(key)<<32|uint64(idx)))
	b := make([]byte, 0, n)
	b = fmt.Appendf(b, "k%d-m%05d-", key, idx)
	for len(b) < n-8 {
		b = append(b, 'a'+byte(rng.UintN(26)))
	}
	var tail [8]byte
	binary.LittleEndian.PutUint64(tail[:], xxhash.Sum64(b))
	return append(b, tail[:]...)
}

// parseSetMember verifies a recovered member against the key it sits
// under and returns the index it embeds. The regeneration compare at
// the end is the authority.
func parseSetMember(seed uint64, key uint32, m []byte) (int, error) {
	s := string(m)
	if !strings.HasPrefix(s, fmt.Sprintf("k%d-m", key)) {
		return 0, fmt.Errorf("member %q does not carry key %d's prefix", m, key)
	}
	rest := s[strings.Index(s, "-m")+2:]
	dash := strings.IndexByte(rest, '-')
	if dash < 0 {
		return 0, fmt.Errorf("member %q has no index terminator", m)
	}
	idx, err := strconv.Atoi(rest[:dash])
	if err != nil || idx < 0 || idx >= setBandMembers(int(key)) {
		return 0, fmt.Errorf("member %q index is outside key %d's band", m, key)
	}
	if !bytes.Equal(m, setMember(seed, key, uint32(idx))) {
		return 0, fmt.Errorf("member does not regenerate from (key %d, index %d)", key, idx)
	}
	return idx, nil
}

// The op stream. A fixed population prefix adds every member of every
// banded key once, which forces the paged keys onto the rtype 5 rung
// before any kill window opens; after that, 55 percent SADD, 30
// percent SREM, 15 percent SISMEMBER over the whole universe, and
// every setStoreEvery ops a STORE lands on the destination key,
// alternating SUNIONSTORE over the segmented pair (a real bulk build)
// with SINTERSTORE of the same pair, whose result is empty by
// construction (the pair's member universes are disjoint) and so
// exercises the empty-result delete. STOREs ride the op counter, not
// the generator, so the parent replays the stream bit-exactly.
const (
	setOpAdd = iota
	setOpRem
	setOpIsMember
)

type setOp struct {
	kind   int
	key    int
	member int
}

type setStream struct {
	rng *rand.Rand
	pop int
}

func setPopOps() int {
	n := 0
	for k := range 7 {
		n += setBandMembers(k)
	}
	return n
}

func newSetStream(seed uint64) *setStream {
	return &setStream{rng: rand.New(rand.NewPCG(seed, 0x543373657473))}
}

func (s *setStream) next() setOp {
	if s.pop < setPopOps() {
		i := s.pop
		s.pop++
		for k := range 7 {
			if i < setBandMembers(k) {
				return setOp{kind: setOpAdd, key: k, member: i}
			}
			i -= setBandMembers(k)
		}
	}
	k := s.rng.IntN(7)
	m := s.rng.IntN(setBandMembers(k))
	switch p := s.rng.IntN(100); {
	case p < 55:
		return setOp{kind: setOpAdd, key: k, member: m}
	case p < 85:
		return setOp{kind: setOpRem, key: k, member: m}
	default:
		return setOp{kind: setOpIsMember, key: k, member: m}
	}
}

// setShadow is the presence state the stream implies: one bool band
// per banded key plus the destination's last stored result (nil while
// the destination is deleted or never built).
type setShadow struct {
	present [][]bool
	dest    [][]bool
}

func newSetShadow() *setShadow {
	sh := &setShadow{present: make([][]bool, 7)}
	for k := range sh.present {
		sh.present[k] = make([]bool, setBandMembers(k))
	}
	return sh
}

// applyStore folds the op-counter STORE cadence into the shadow: even
// stores are the union of the segmented pair, odd stores are their
// intersection, which is empty by construction and deletes dest.
func (sh *setShadow) applyStore(nth int) {
	if nth%2 == 1 {
		sh.dest = nil
		return
	}
	sh.dest = [][]bool{
		append([]bool(nil), sh.present[4]...),
		append([]bool(nil), sh.present[5]...),
	}
}

// simulateSet replays the stream for n ops with no store: the final
// shadow, plus everRemoved marking members some SREM in [0, n)
// touched. The kill arm's must-be-present set is population minus
// everRemoved; the clean arm uses the shadow as the exact state owed.
func simulateSet(seed uint64, n int) (sh *setShadow, everRemoved [][]bool) {
	st := newSetStream(seed)
	sh = newSetShadow()
	everRemoved = make([][]bool, 7)
	for k := range everRemoved {
		everRemoved[k] = make([]bool, setBandMembers(k))
	}
	for i := 1; i <= n; i++ {
		op := st.next()
		switch op.kind {
		case setOpAdd:
			sh.present[op.key][op.member] = true
		case setOpRem:
			sh.present[op.key][op.member] = false
			everRemoved[op.key][op.member] = true
		}
		if i > setPopOps() && i%setStoreEvery == 0 {
			sh.applyStore(i / setStoreEvery)
		}
	}
	return sh, everRemoved
}

// setCrashRig is the composite under test plus the in-process shadow.
type setCrashRig struct {
	seed   uint64
	db     *sqlo1b.Store
	tr     *sqlo1.Tiered
	se     *sqlo1.Set
	st     *setStream
	shadow *setShadow
	ops    int
}

func newSetCrashRig(dir string, seed uint64) (*setCrashRig, error) {
	db, err := sqlo1b.CreateStore(dir+"/"+setDataFile, tieredSegSize)
	if err != nil {
		return nil, err
	}
	db.SetCheckpointPolicy(sqlo1b.CheckpointPolicy{Bytes: tieredCkptBytes})
	tr := sqlo1.NewTiered(db, sqlo1.TieredConfig{
		Budget: sqlo1.Budget{Entries: setHotEntries, Arenas: setHotArenas},
		Seed:   seed,
	})
	se, err := sqlo1.NewSet(tr, sqlo1.HashConfig{})
	if err != nil {
		db.Close()
		return nil, err
	}
	return &setCrashRig{seed: seed, db: db, tr: tr, se: se, st: newSetStream(seed), shadow: newSetShadow()}, nil
}

// step runs one stream op through the composite and self-checks it
// against the shadow live. Any engine error is fatal: budgets are
// sized so nothing sheds, so a refusal here is a rig bug, not load.
func (r *setCrashRig) step(ctx context.Context) error {
	op := r.st.next()
	key := setKeyName(op.key)
	member := setMember(r.seed, uint32(op.key), uint32(op.member))
	switch op.kind {
	case setOpAdd:
		created, err := r.se.SAdd(ctx, key, member)
		if err != nil {
			return fmt.Errorf("op %d: SAdd(%s, m%05d): %w", r.ops, key, op.member, err)
		}
		if created == r.shadow.present[op.key][op.member] {
			return fmt.Errorf("op %d: SAdd(%s, m%05d) created=%v, shadow disagrees", r.ops, key, op.member, created)
		}
		r.shadow.present[op.key][op.member] = true
	case setOpRem:
		gone, err := r.se.SRem(ctx, key, member)
		if err != nil {
			return fmt.Errorf("op %d: SRem(%s, m%05d): %w", r.ops, key, op.member, err)
		}
		if gone != r.shadow.present[op.key][op.member] {
			return fmt.Errorf("op %d: SRem(%s, m%05d) = %v, shadow disagrees", r.ops, key, op.member, gone)
		}
		r.shadow.present[op.key][op.member] = false
	case setOpIsMember:
		ok, err := r.se.SIsMember(ctx, key, member)
		if err != nil {
			return fmt.Errorf("op %d: SIsMember(%s, m%05d): %w", r.ops, key, op.member, err)
		}
		if ok != r.shadow.present[op.key][op.member] {
			return fmt.Errorf("op %d: SIsMember(%s, m%05d) = %v, shadow disagrees", r.ops, key, op.member, ok)
		}
	}
	r.ops++
	if r.ops > setPopOps() && r.ops%setStoreEvery == 0 {
		nth := r.ops / setStoreEvery
		srcs := [][]byte{setKeyName(4), setKeyName(5)}
		var n int64
		var err error
		if nth%2 == 1 {
			n, err = r.se.SInterStore(ctx, setDestName(), srcs)
		} else {
			n, err = r.se.SUnionStore(ctx, setDestName(), srcs)
		}
		if err != nil {
			return fmt.Errorf("op %d: store %d: %w", r.ops, nth, err)
		}
		r.shadow.applyStore(nth)
		want := int64(0)
		if r.shadow.dest != nil {
			for _, band := range r.shadow.dest {
				for _, p := range band {
					if p {
						want++
					}
				}
			}
		}
		if n != want {
			return fmt.Errorf("op %d: store %d returned %d, shadow holds %d", r.ops, nth, n, want)
		}
	}
	if r.ops%setTickEvery == 0 {
		if err := r.tr.Tick(ctx); err != nil {
			return fmt.Errorf("op %d: Tick: %w", r.ops, err)
		}
	}
	return nil
}

// selfCheckCounts holds SCARD against the live shadow on every key,
// the in-process half of the count oracle.
func (r *setCrashRig) selfCheckCounts(ctx context.Context) error {
	for k := range 7 {
		want := int64(0)
		for _, p := range r.shadow.present[k] {
			if p {
				want++
			}
		}
		n, err := r.se.SCard(ctx, setKeyName(k))
		if err != nil {
			return fmt.Errorf("SCard(%s): %w", setKeyName(k), err)
		}
		if n != want {
			return fmt.Errorf("SCard(%s) = %d, shadow holds %d live members", setKeyName(k), n, want)
		}
	}
	return nil
}

// setRecovered summarizes a verified image for the iteration log.
type setRecovered struct {
	Members   int
	HighWater int64
}

// verifySetRecovered holds a killed or cleanly closed image against
// the seed. The kill arm passes everRemoved (members outside it must
// be present, population is flushed) with exact nil; the clean arm
// passes the exact shadow and demands the final state to the byte,
// destination included. Both arms enforce the count oracle on every
// key: begin(count), the walked member set, SCARD, and per-member
// point reads must all agree (SE-I2, rule W3), then a second open
// must land on the same state.
func verifySetRecovered(dataPath string, seed uint64, everRemoved [][]bool, exact *setShadow, minHW int64) (setRecovered, error) {
	var out setRecovered
	if err := scrubTieredImage(dataPath, sqlo1.WALPath(dataPath)); err != nil {
		return out, fmt.Errorf("scrub pass: %w", err)
	}
	ctx := context.Background()

	open := func() (*sqlo1b.Store, *sqlo1.Set, error) {
		db, err := sqlo1b.OpenStore(dataPath, tieredSegSize)
		if err != nil {
			return nil, nil, fmt.Errorf("reopen: %w", err)
		}
		tr := sqlo1.NewTiered(db, sqlo1.TieredConfig{
			Budget: sqlo1.Budget{Entries: setHotEntries, Arenas: setHotArenas},
			Seed:   seed + 1,
		})
		se, err := sqlo1.NewSet(tr, sqlo1.HashConfig{})
		if err != nil {
			db.Close()
			return nil, nil, err
		}
		return db, se, nil
	}

	// walkKey walks one key under the count oracle and returns the
	// reachable member indices; for the destination, indices are
	// (band<<32)|idx over the segmented pair.
	walkKey := func(se *sqlo1.Set, key []byte, parse func(m []byte) (uint64, error)) (map[uint64]bool, error) {
		walked := map[uint64]bool{}
		beginCount := -1
		var walkErr error
		err := se.SMembers(ctx, key, func(count int) { beginCount = count }, func(m []byte) {
			if walkErr != nil {
				return
			}
			id, err := parse(m)
			if err != nil {
				walkErr = fmt.Errorf("%s: %w", key, err)
				return
			}
			if walked[id] {
				walkErr = fmt.Errorf("%s: walk delivered member %d twice", key, id)
				return
			}
			walked[id] = true
		})
		if err != nil {
			return nil, fmt.Errorf("%s: SMembers: %w", key, err)
		}
		if walkErr != nil {
			return nil, walkErr
		}
		if beginCount != len(walked) {
			return nil, fmt.Errorf("%s: begin(%d) but %d members walked (W3 count drift)", key, beginCount, len(walked))
		}
		card, err := se.SCard(ctx, key)
		if err != nil {
			return nil, fmt.Errorf("%s: SCard: %w", key, err)
		}
		if int(card) != len(walked) {
			return nil, fmt.Errorf("%s: SCARD %d but %d members reachable (SE-I2 drift)", key, card, len(walked))
		}
		return walked, nil
	}

	// checkKey is walkKey plus the per-member point reads over the
	// key's whole band.
	checkKey := func(se *sqlo1.Set, k int) (map[uint64]bool, error) {
		key := setKeyName(k)
		walked, err := walkKey(se, key, func(m []byte) (uint64, error) {
			idx, err := parseSetMember(seed, uint32(k), m)
			return uint64(idx), err
		})
		if err != nil {
			return nil, err
		}
		for i := range setBandMembers(k) {
			ok, err := se.SIsMember(ctx, key, setMember(seed, uint32(k), uint32(i)))
			if err != nil {
				return nil, fmt.Errorf("%s: SIsMember m%05d: %w", key, i, err)
			}
			if ok != walked[uint64(i)] {
				return nil, fmt.Errorf("%s member m%05d: point read hit=%v, walk hit=%v", key, i, ok, walked[uint64(i)])
			}
		}
		if _, _, err := se.Encoding(ctx, key); err != nil {
			return nil, fmt.Errorf("%s: Encoding: %w", key, err)
		}
		return walked, nil
	}

	// checkDest verifies the STORE destination: every member must
	// parse under one of the segmented pair, and the count oracle
	// holds. Point reads run only over walked members; the dest's
	// legal states churn with the STORE cadence, so absence proves
	// nothing on the kill arm.
	checkDest := func(se *sqlo1.Set) (map[uint64]bool, error) {
		key := setDestName()
		walked, err := walkKey(se, key, func(m []byte) (uint64, error) {
			for band, k := range []uint32{4, 5} {
				if bytes.HasPrefix(m, fmt.Appendf(nil, "k%d-m", k)) {
					idx, err := parseSetMember(seed, k, m)
					return uint64(band)<<32 | uint64(idx), err
				}
			}
			return 0, fmt.Errorf("destination member %q is from neither source", m)
		})
		if err != nil {
			return nil, err
		}
		for id := range walked {
			k := uint32(4 + id>>32)
			ok, err := se.SIsMember(ctx, key, setMember(seed, k, uint32(id&0xffffffff)))
			if err != nil || !ok {
				return nil, fmt.Errorf("%s: walked member (k%d, m%05d) fails the point read: %v %v", key, k, id&0xffffffff, ok, err)
			}
		}
		return walked, nil
	}

	db, se, err := open()
	if err != nil {
		return out, err
	}
	st1 := db.Stats()
	out.HighWater = st1.HighWater
	first := make([]map[uint64]bool, setKeys)
	for k := range 7 {
		walked, err := checkKey(se, k)
		if err != nil {
			db.Close()
			return out, err
		}
		first[k] = walked
		out.Members += len(walked)
		if exact == nil {
			for i, removed := range everRemoved[k] {
				if !removed && !walked[uint64(i)] {
					db.Close()
					return out, fmt.Errorf("sk%d member m%05d was flushed at population, never removed, and is gone", k, i)
				}
			}
		} else {
			for i, want := range exact.present[k] {
				if want != walked[uint64(i)] {
					db.Close()
					return out, fmt.Errorf("sk%d member m%05d recovered=%v, clean shutdown owed %v", k, i, walked[uint64(i)], want)
				}
			}
		}
	}
	destWalked, err := checkDest(se)
	if err != nil {
		db.Close()
		return out, err
	}
	first[7] = destWalked
	if exact != nil {
		want := map[uint64]bool{}
		if exact.dest != nil {
			for band, members := range exact.dest {
				for i, p := range members {
					if p {
						want[uint64(band)<<32|uint64(i)] = true
					}
				}
			}
		}
		if len(destWalked) != len(want) {
			db.Close()
			return out, fmt.Errorf("sd0 holds %d members, clean shutdown owed %d", len(destWalked), len(want))
		}
		for id := range want {
			if !destWalked[id] {
				db.Close()
				return out, fmt.Errorf("sd0 lost member (k%d, m%05d) owed by the last store", 4+id>>32, id&0xffffffff)
			}
		}
	}
	if err := db.Close(); err != nil {
		return out, fmt.Errorf("close after verify: %w", err)
	}
	if st1.HighWater < minHW {
		return out, fmt.Errorf("recovered high-water %d below the worker's durable %d", st1.HighWater, minHW)
	}

	// Recovery must be repeatable: a second open of the same dir has
	// to land on the same set states, not merely succeed.
	db2, se2, err := open()
	if err != nil {
		return out, fmt.Errorf("second reopen: %w", err)
	}
	defer db2.Close()
	if hw2 := db2.Stats().HighWater; hw2 != st1.HighWater {
		return out, fmt.Errorf("second open drifted: high-water %d -> %d", st1.HighWater, hw2)
	}
	for k := range 7 {
		walked, err := checkKey(se2, k)
		if err != nil {
			return out, fmt.Errorf("second open: %w", err)
		}
		if len(walked) != len(first[k]) {
			return out, fmt.Errorf("second open walked %d members on sk%d, first %d", len(walked), k, len(first[k]))
		}
		for id := range first[k] {
			if !walked[id] {
				return out, fmt.Errorf("second open lost sk%d member m%05d", k, id)
			}
		}
	}
	destWalked2, err := checkDest(se2)
	if err != nil {
		return out, fmt.Errorf("second open: %w", err)
	}
	if len(destWalked2) != len(first[7]) {
		return out, fmt.Errorf("second open walked %d members on sd0, first %d", len(destWalked2), len(first[7]))
	}
	for id := range first[7] {
		if !destWalked2[id] {
			return out, fmt.Errorf("second open lost sd0 member (k%d, m%05d)", 4+id>>32, id&0xffffffff)
		}
	}
	return out, nil
}
