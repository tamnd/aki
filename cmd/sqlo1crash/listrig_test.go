package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"

	"github.com/cespare/xxhash/v2"
	"github.com/tamnd/aki/engine/sqlo1"
	"github.com/tamnd/aki/engine/sqlo1b"
)

// The T5 exit-gate crash rig (spec 2064/sqlo1, milestone T5): the list
// ladder over Tiered over sqlo1b, SIGKILLed mid-stream, recovered, and
// held against an exact-state oracle. The keyset spans the
// representation rungs at once (inline queues, a noded queue, a noded
// middle-op key, one paged queue past the 167-node flat fence wall),
// and the steady state mixes every T5 write family: edge pushes and
// pops, LSET rewrites, LINSERT splits, LREM with the merge
// counterweight, and an LMOVE plus LTRIM capped-feed cadence that
// keeps two-root frame groups and trim cuts inside the kill windows.
//
// What the suite claims, L-I2 and L-I6 plus the list half of the
// replay contract: a list's records ride the drain in per-key
// all-or-nothing groups (edge amendments and in-place pages in the
// root's batch, fresh records flushed ahead, dead records deleted
// after), so any recovered list must be EXACTLY a state the command
// stream produced at some op count at or past the durable population
// flush. The oracle is correspondingly strict: elements are
// self-describing (key, sequence number, seeded filler, xxhash tail),
// the parent replays the deterministic stream and collects the digest
// of every key's content at every post-population op count, and a
// recovered key whose walked digest is in nobody's history fails by
// name. On top of that every key passes Verify (the L-I6 fence-order
// scrub), LLEN must equal the walked length exactly, every position
// must answer the same element to LINDEX that the LRANGE walk
// delivered, and recovery must be repeatable across a second open.
// The clean-shutdown control arm demands the stream's exact final
// state back, element by element, destination included.

const (
	listDataFile = "l.aki"
	// listKeys spans the rungs: lk0..lk3 inline queues, lk4 a noded
	// queue (the feed source), lk5 the noded middle-op key, lk6 the
	// paged queue, plus the capped-feed destination ld0.
	listKeys    = 8
	listDestIdx = 7
	// Population fill, floor, and ceiling per band. The floors keep
	// every key alive and on its rung, the ceilings bound the walk;
	// the paged band's floor keeps lk6 past the 167-node flat cap
	// (10200 elements at 68 encoded bytes cut ~173 nodes of 59), and
	// the transition is one way regardless.
	listInlineFill, listInlineFloor, listInlineCeil = 40, 20, 60
	listNodedFill, listNodedFloor, listNodedCeil    = 500, 400, 600
	listPagedFill, listPagedFloor, listPagedCeil    = 10500, 10200, 10800
	// Element payload widths. The inline band stays under both inline
	// caps at its ceiling (60 x 28 encoded bytes), the noded band's 40
	// B elements match the lnode lab's queue shape, and the paged
	// band's 64 B elements cut nodes at 59 elements so the fence
	// crosses the flat wall during population.
	listElemInlineLen = 24
	listElemNodedLen  = 40
	listElemPagedLen  = 64
	// Hot-tier budget, deliberately roomy: no sheds, no evictions.
	listHotEntries = 8192
	listHotArenas  = 64 << 20
	// Cadences: Flush is the durability ratchet, Tick keeps the
	// background loops honest, the feed cadence keeps two-root moves
	// and trim cuts inside the kill windows.
	listTickEvery     = 64
	listFlushEvery    = 512
	listFeedEvery     = 192
	listProgressEvery = 256
	listBoundSlack    = 1 << 16
	// The capped feed: every listFeedEvery ops one LMOVE from lk4's
	// tail onto ld0's head, then LTRIM ld0 to its newest listFeedCap
	// elements, the PRED-SQLO1-T5-FEED shape under kills.
	listFeedCap = 100
	listFeedSrc = 4
)

func listKeyName(idx int) []byte {
	if idx == listDestIdx {
		return []byte("ld0")
	}
	return fmt.Appendf(nil, "lk%d", idx)
}

func listBandFill(idx int) int {
	switch {
	case idx < 4:
		return listInlineFill
	case idx < 6:
		return listNodedFill
	default:
		return listPagedFill
	}
}

func listBandFloor(idx int) int {
	switch {
	case idx < 4:
		return listInlineFloor
	case idx < 6:
		return listNodedFloor
	default:
		return listPagedFloor
	}
}

func listBandCeil(idx int) int {
	switch {
	case idx < 4:
		return listInlineCeil
	case idx < 6:
		return listNodedCeil
	default:
		return listPagedCeil
	}
}

func listElemLen(idx int) int {
	switch {
	case idx < 4:
		return listElemInlineLen
	case idx < 6:
		return listElemNodedLen
	default:
		return listElemPagedLen
	}
}

// listElem is the self-describing element: key and sequence number up
// front, seeded filler in the middle, an xxhash64 of everything before
// it at the tail, a deterministic function of (seed, key, seq) so the
// parent classifies any recovered element with no journal.
func listElem(seed uint64, key uint32, seq uint64) []byte {
	n := listElemLen(int(key))
	rng := rand.New(rand.NewPCG(seed^0x6c697374, uint64(key)<<32|seq))
	b := make([]byte, 0, n)
	b = fmt.Appendf(b, "l%d-s%08d-", key, seq)
	for len(b) < n-8 {
		b = append(b, 'a'+byte(rng.UintN(26)))
	}
	var tail [8]byte
	binary.LittleEndian.PutUint64(tail[:], xxhash.Sum64(b))
	return append(b, tail[:]...)
}

// parseListElem verifies a recovered element against the key universe
// it sits under and returns the sequence number it embeds. The
// regeneration compare at the end is the authority.
func parseListElem(seed uint64, key uint32, e []byte) (uint64, error) {
	s := string(e)
	prefix := fmt.Sprintf("l%d-s", key)
	if !strings.HasPrefix(s, prefix) {
		return 0, fmt.Errorf("element %q does not carry key %d's prefix", e, key)
	}
	rest := s[len(prefix):]
	dash := strings.IndexByte(rest, '-')
	if dash < 0 {
		return 0, fmt.Errorf("element %q has no seq terminator", e)
	}
	seq, err := strconv.ParseUint(rest[:dash], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("element %q has a malformed seq", e)
	}
	if !bytes.Equal(e, listElem(seed, key, seq)) {
		return 0, fmt.Errorf("element does not regenerate from (key %d, seq %d)", key, seq)
	}
	return seq, nil
}

// hashSeqs digests one key's content, the seq vector head to tail, for
// the exact-state oracle. buf is reusable scratch, returned grown.
func hashSeqs(buf []byte, seqs []uint64) ([]byte, uint64) {
	buf = buf[:0]
	for _, s := range seqs {
		buf = binary.LittleEndian.AppendUint64(buf, s)
	}
	return buf, xxhash.Sum64(buf)
}

// The op stream. A fixed population prefix right-pushes every band to
// its fill, one element per op, which carries lk6 across the flat
// fence wall onto pages before any kill window opens; after that the
// queue keys (lk0..lk4, lk6) run a floor-and-ceiling bounded mix of
// head pushes, tail pops, and LINDEX probes, and lk5 runs the
// middle-op mix: LSET rewrites, LINSERT at a drawn pivot, LREM of a
// drawn element alternating scan directions, plus edge traffic and
// probes. Every listFeedEvery ops the feed cadence moves lk4's tail
// element onto ld0's head and trims ld0 to listFeedCap. The cadence
// rides the op counter, not the generator, so the parent replays the
// stream bit-exactly; sequence numbers mint per key and never repeat,
// so every element value is unique and LINSERT pivots and LREM
// targets are unambiguous.
//
// The stream owns the shadow: next() decides an op from the generator
// and the shadow, captures the expected outcome, applies the op, and
// returns both, so the worker, the simulator, and the clean arm all
// share one definition of the stream's meaning.
const (
	lOpPush = iota
	lOpPop
	lOpSet
	lOpIns
	lOpRem
	lOpProbe
)

type lOp struct {
	kind    int
	key     int
	left    bool   // push or pop end; for lOpIns the before flag
	seq     uint64 // fresh element for push, set, insert
	pos     int64  // position for set and probe
	target  uint64 // expected element: popped, probed, pivot, or removed
	dir     int64  // LREM count argument, +1 or -1
	wantLen int64  // expected length reply for push and insert
}

func listPopOps() int {
	n := 0
	for k := range listKeys - 1 {
		n += listBandFill(k)
	}
	return n
}

type lStream struct {
	rng  *rand.Rand
	pop  int
	seqs [listKeys][]uint64 // content, head first; listDestIdx is ld0
	mint [listKeys]uint64   // next fresh seq per banded key
}

func newLStream(seed uint64) *lStream {
	return &lStream{rng: rand.New(rand.NewPCG(seed, 0x54356c697374))}
}

func (s *lStream) next() lOp {
	if s.pop < listPopOps() {
		i := s.pop
		s.pop++
		for k := range listKeys - 1 {
			if i < listBandFill(k) {
				seq := s.mint[k]
				s.mint[k]++
				s.seqs[k] = append(s.seqs[k], seq)
				return lOp{kind: lOpPush, key: k, left: false, seq: seq, wantLen: int64(len(s.seqs[k]))}
			}
			i -= listBandFill(k)
		}
	}
	k := s.rng.IntN(listKeys - 1)
	n := len(s.seqs[k])
	p := s.rng.IntN(100)
	if k == 5 {
		switch {
		case n >= listBandCeil(k):
			return s.opRem(k)
		case n <= listBandFloor(k) || p >= 65 && p < 80:
			return s.opPush(k, s.rng.IntN(2) == 0)
		case p < 25:
			return s.opSet(k)
		case p < 45:
			return s.opIns(k)
		case p < 65:
			return s.opRem(k)
		case p < 90:
			return s.opPop(k, s.rng.IntN(2) == 0)
		default:
			return s.opProbe(k)
		}
	}
	switch {
	case n <= listBandFloor(k):
		return s.opPush(k, true)
	case n >= listBandCeil(k):
		return s.opPop(k, false)
	case p < 40:
		return s.opPush(k, true)
	case p < 80:
		return s.opPop(k, false)
	default:
		return s.opProbe(k)
	}
}

func (s *lStream) opPush(k int, left bool) lOp {
	seq := s.mint[k]
	s.mint[k]++
	if left {
		s.seqs[k] = slices.Insert(s.seqs[k], 0, seq)
	} else {
		s.seqs[k] = append(s.seqs[k], seq)
	}
	return lOp{kind: lOpPush, key: k, left: left, seq: seq, wantLen: int64(len(s.seqs[k]))}
}

func (s *lStream) opPop(k int, left bool) lOp {
	i := len(s.seqs[k]) - 1
	if left {
		i = 0
	}
	target := s.seqs[k][i]
	s.seqs[k] = slices.Delete(s.seqs[k], i, i+1)
	return lOp{kind: lOpPop, key: k, left: left, target: target}
}

func (s *lStream) opSet(k int) lOp {
	pos := s.rng.IntN(len(s.seqs[k]))
	seq := s.mint[k]
	s.mint[k]++
	s.seqs[k][pos] = seq
	return lOp{kind: lOpSet, key: k, pos: int64(pos), seq: seq}
}

func (s *lStream) opIns(k int) lOp {
	pos := s.rng.IntN(len(s.seqs[k]))
	before := s.rng.IntN(2) == 0
	pivot := s.seqs[k][pos]
	seq := s.mint[k]
	s.mint[k]++
	at := pos + 1
	if before {
		at = pos
	}
	s.seqs[k] = slices.Insert(s.seqs[k], at, seq)
	return lOp{kind: lOpIns, key: k, left: before, seq: seq, target: pivot, wantLen: int64(len(s.seqs[k]))}
}

func (s *lStream) opRem(k int) lOp {
	pos := s.rng.IntN(len(s.seqs[k]))
	target := s.seqs[k][pos]
	dir := int64(1)
	if s.rng.IntN(2) == 1 {
		dir = -1
	}
	s.seqs[k] = slices.Delete(s.seqs[k], pos, pos+1)
	return lOp{kind: lOpRem, key: k, target: target, dir: dir}
}

func (s *lStream) opProbe(k int) lOp {
	pos := s.rng.IntN(len(s.seqs[k]))
	return lOp{kind: lOpProbe, key: k, pos: int64(pos), target: s.seqs[k][pos]}
}

// applyMove folds the feed cadence's LMOVE into the shadow and returns
// the moved seq; applyTrim folds the LTRIM and returns ld0's length
// after it. They are separate because the cadence is two commands and
// recovery may legally land between them, so the simulator digests the
// intermediate state too.
func (s *lStream) applyMove() uint64 {
	src := s.seqs[listFeedSrc]
	moved := src[len(src)-1]
	s.seqs[listFeedSrc] = src[:len(src)-1]
	s.seqs[listDestIdx] = slices.Insert(s.seqs[listDestIdx], 0, moved)
	return moved
}

func (s *lStream) applyTrim() int {
	if len(s.seqs[listDestIdx]) > listFeedCap {
		s.seqs[listDestIdx] = s.seqs[listDestIdx][:listFeedCap]
	}
	return len(s.seqs[listDestIdx])
}

// simulateL replays the stream for n ops and collects the exact-state
// oracle: for every key, the xxhash64 digest of its content at the
// population flush and after every later op that touched it, feed
// cadence states included, intermediate move-before-trim state too.
// The kill arm holds every recovered key's walked digest to
// membership; the clean arm uses the final shadow as the exact state
// owed.
func simulateL(seed uint64, n int) (*lStream, []map[uint64]bool) {
	st := newLStream(seed)
	legal := make([]map[uint64]bool, listKeys)
	for k := range legal {
		legal[k] = map[uint64]bool{}
	}
	var hb []byte
	var h uint64
	mark := func(k int) {
		hb, h = hashSeqs(hb, st.seqs[k])
		legal[k][h] = true
	}
	for i := 1; i <= n; i++ {
		op := st.next()
		if i == listPopOps() {
			for k := range listKeys {
				mark(k)
			}
		}
		if i > listPopOps() {
			if op.kind != lOpProbe {
				mark(op.key)
			}
			if i%listFeedEvery == 0 {
				st.applyMove()
				mark(listFeedSrc)
				mark(listDestIdx)
				st.applyTrim()
				mark(listDestIdx)
			}
		}
	}
	return st, legal
}

// listCrashRig is the composite under test plus the stream that owns
// the in-process shadow.
type listCrashRig struct {
	seed uint64
	db   *sqlo1b.Store
	tr   *sqlo1.Tiered
	l    *sqlo1.List
	st   *lStream
	ops  int
}

func newListCrashRig(dir string, seed uint64) (*listCrashRig, error) {
	db, err := sqlo1b.CreateStore(dir+"/"+listDataFile, tieredSegSize)
	if err != nil {
		return nil, err
	}
	db.SetCheckpointPolicy(sqlo1b.CheckpointPolicy{Bytes: tieredCkptBytes})
	tr := sqlo1.NewTiered(db, sqlo1.TieredConfig{
		Budget: sqlo1.Budget{Entries: listHotEntries, Arenas: listHotArenas},
		Seed:   seed,
	})
	l, err := sqlo1.NewList(tr, sqlo1.ListConfig{})
	if err != nil {
		db.Close()
		return nil, err
	}
	return &listCrashRig{seed: seed, db: db, tr: tr, l: l, st: newLStream(seed)}, nil
}

// step runs one stream op through the composite and self-checks every
// reply against the stream's expectation live. Any engine error is
// fatal: budgets are sized so nothing sheds, so a refusal here is a
// rig bug, not load.
func (r *listCrashRig) step(ctx context.Context) error {
	op := r.st.next()
	key := listKeyName(op.key)
	switch op.kind {
	case lOpPush:
		n, err := r.l.Push(ctx, key, op.left, false, listElem(r.seed, uint32(op.key), op.seq))
		if err != nil {
			return fmt.Errorf("op %d: Push(%s, s%d): %w", r.ops, key, op.seq, err)
		}
		if n != op.wantLen {
			return fmt.Errorf("op %d: Push(%s) = %d, shadow holds %d", r.ops, key, n, op.wantLen)
		}
	case lOpPop:
		vals, ok, err := r.l.Pop(ctx, key, op.left, 1)
		if err != nil || !ok || len(vals) != 1 {
			return fmt.Errorf("op %d: Pop(%s) = (%d vals, %v, %v)", r.ops, key, len(vals), ok, err)
		}
		if want := listElem(r.seed, uint32(op.key), op.target); !bytes.Equal(vals[0], want) {
			return fmt.Errorf("op %d: Pop(%s) returned %q, shadow holds s%d", r.ops, key, vals[0], op.target)
		}
	case lOpSet:
		if err := r.l.Set(ctx, key, op.pos, listElem(r.seed, uint32(op.key), op.seq)); err != nil {
			return fmt.Errorf("op %d: Set(%s, %d): %w", r.ops, key, op.pos, err)
		}
	case lOpIns:
		n, err := r.l.Insert(ctx, key, op.left, listElem(r.seed, uint32(op.key), op.target), listElem(r.seed, uint32(op.key), op.seq))
		if err != nil {
			return fmt.Errorf("op %d: Insert(%s, s%d): %w", r.ops, key, op.seq, err)
		}
		if n != op.wantLen {
			return fmt.Errorf("op %d: Insert(%s) = %d, shadow holds %d", r.ops, key, n, op.wantLen)
		}
	case lOpRem:
		n, err := r.l.Rem(ctx, key, op.dir, listElem(r.seed, uint32(op.key), op.target))
		if err != nil {
			return fmt.Errorf("op %d: Rem(%s, s%d): %w", r.ops, key, op.target, err)
		}
		if n != 1 {
			return fmt.Errorf("op %d: Rem(%s, s%d) removed %d, want 1", r.ops, key, op.target, n)
		}
	case lOpProbe:
		e, ok, err := r.l.Index(ctx, key, op.pos)
		if err != nil || !ok {
			return fmt.Errorf("op %d: Index(%s, %d) = (%v, %v)", r.ops, key, op.pos, ok, err)
		}
		if want := listElem(r.seed, uint32(op.key), op.target); !bytes.Equal(e, want) {
			return fmt.Errorf("op %d: Index(%s, %d) returned %q, shadow holds s%d", r.ops, key, op.pos, e, op.target)
		}
	}
	r.ops++
	if r.ops > listPopOps() && r.ops%listFeedEvery == 0 {
		moved := r.st.applyMove()
		e, ok, err := r.l.Move(ctx, listKeyName(listFeedSrc), listKeyName(listDestIdx), false, true)
		if err != nil || !ok {
			return fmt.Errorf("op %d: Move = (%v, %v)", r.ops, ok, err)
		}
		if want := listElem(r.seed, listFeedSrc, moved); !bytes.Equal(e, want) {
			return fmt.Errorf("op %d: Move returned %q, shadow holds s%d", r.ops, e, moved)
		}
		dstLen := r.st.applyTrim()
		if err := r.l.Trim(ctx, listKeyName(listDestIdx), 0, listFeedCap-1); err != nil {
			return fmt.Errorf("op %d: feed Trim: %w", r.ops, err)
		}
		n, err := r.l.Len(ctx, listKeyName(listDestIdx))
		if err != nil || n != int64(dstLen) {
			return fmt.Errorf("op %d: ld0 length %d after trim, shadow holds %d (%v)", r.ops, n, dstLen, err)
		}
	}
	if r.ops%listTickEvery == 0 {
		if err := r.tr.Tick(ctx); err != nil {
			return fmt.Errorf("op %d: Tick: %w", r.ops, err)
		}
	}
	return nil
}

// selfCheckLens holds LLEN against the live shadow on every key, the
// in-process half of the L-I2 oracle.
func (r *listCrashRig) selfCheckLens(ctx context.Context) error {
	for k := range listKeys {
		n, err := r.l.Len(ctx, listKeyName(k))
		if err != nil {
			return fmt.Errorf("Len(%s): %w", listKeyName(k), err)
		}
		if n != int64(len(r.st.seqs[k])) {
			return fmt.Errorf("Len(%s) = %d, shadow holds %d", listKeyName(k), n, len(r.st.seqs[k]))
		}
	}
	return nil
}

// listRecovered summarizes a verified image for the iteration log.
type listRecovered struct {
	Elems     int
	HighWater int64
}

// verifyListRecovered holds a killed or cleanly closed image against
// the seed. The kill arm passes the legal digest sets (every recovered
// key must sit at exactly a state its command stream produced at or
// past the population flush) with exact nil; the clean arm passes the
// final stream and demands its state element for element. Both arms
// run Verify on every key first, the L-I6 fence-order scrub over the
// recovered image, then hold the LRANGE walk, LLEN, and per-position
// LINDEX point reads to agreement, then reopen a second time and
// demand the same state back.
func verifyListRecovered(dataPath string, seed uint64, legal []map[uint64]bool, exact *lStream, minHW int64) (listRecovered, error) {
	var out listRecovered
	if err := scrubTieredImage(dataPath, sqlo1.WALPath(dataPath)); err != nil {
		return out, fmt.Errorf("scrub pass: %w", err)
	}
	ctx := context.Background()

	open := func() (*sqlo1b.Store, *sqlo1.List, error) {
		db, err := sqlo1b.OpenStore(dataPath, tieredSegSize)
		if err != nil {
			return nil, nil, fmt.Errorf("reopen: %w", err)
		}
		tr := sqlo1.NewTiered(db, sqlo1.TieredConfig{
			Budget: sqlo1.Budget{Entries: listHotEntries, Arenas: listHotArenas},
			Seed:   seed + 1,
		})
		l, err := sqlo1.NewList(tr, sqlo1.ListConfig{})
		if err != nil {
			db.Close()
			return nil, nil, err
		}
		return db, l, nil
	}

	// checkKey scrubs one key, walks it, and holds every agreement the
	// arms share. ld0's elements parse under the source key's universe,
	// the moved bytes' home.
	checkKey := func(l *sqlo1.List, k int) ([]uint64, error) {
		key := listKeyName(k)
		if err := l.Verify(ctx, key); err != nil {
			return nil, fmt.Errorf("%s: Verify: %w", key, err)
		}
		pk := uint32(k)
		if k == listDestIdx {
			pk = listFeedSrc
		}
		var walked []uint64
		var werr error
		err := l.Range(ctx, key, 0, -1, func(n int) { walked = make([]uint64, 0, n) }, func(e []byte) {
			if werr != nil {
				return
			}
			seq, err := parseListElem(seed, pk, e)
			if err != nil {
				werr = fmt.Errorf("%s: %w", key, err)
				return
			}
			walked = append(walked, seq)
		})
		if err != nil {
			return nil, fmt.Errorf("%s: Range: %w", key, err)
		}
		if werr != nil {
			return nil, werr
		}
		n, err := l.Len(ctx, key)
		if err != nil {
			return nil, fmt.Errorf("%s: Len: %w", key, err)
		}
		if int(n) != len(walked) {
			return nil, fmt.Errorf("%s: LLEN %d but %d elements walked (L-I2 drift)", key, n, len(walked))
		}
		for i, seq := range walked {
			e, ok, err := l.Index(ctx, key, int64(i))
			if err != nil || !ok {
				return nil, fmt.Errorf("%s: Index(%d) = (%v, %v)", key, i, ok, err)
			}
			if !bytes.Equal(e, listElem(seed, pk, seq)) {
				return nil, fmt.Errorf("%s position %d: point read %q, walk delivered s%d", key, i, e, seq)
			}
		}
		if len(walked) > 0 {
			e, ok, err := l.Index(ctx, key, -1)
			if err != nil || !ok || !bytes.Equal(e, listElem(seed, pk, walked[len(walked)-1])) {
				return nil, fmt.Errorf("%s: Index(-1) disagrees with the walk's tail (%v, %v)", key, ok, err)
			}
		}
		if k < listDestIdx {
			// The banded keys never leave their rungs: the inline band's
			// ceiling stays under both inline caps and the noded ladder
			// is one way. ld0 legitimately sits on either rung depending
			// on when the feed last ran.
			enc, ok, err := l.Encoding(ctx, key)
			want := "quicklist"
			if k < 4 {
				want = "listpack"
			}
			if err != nil || !ok || enc != want {
				return nil, fmt.Errorf("%s recovered on %q (%v, %v), want %q", key, enc, ok, err, want)
			}
		}
		if exact == nil {
			_, h := hashSeqs(nil, walked)
			if !legal[k][h] {
				return nil, fmt.Errorf("%s recovered at a %d-element state the stream never produced (digest %#x)", key, len(walked), h)
			}
		} else if !slices.Equal(walked, exact.seqs[k]) {
			return nil, fmt.Errorf("%s recovered %d elements, clean shutdown owed %d and the contents differ", key, len(walked), len(exact.seqs[k]))
		}
		return walked, nil
	}

	db, l, err := open()
	if err != nil {
		return out, err
	}
	st1 := db.Stats()
	out.HighWater = st1.HighWater
	first := make([][]uint64, listKeys)
	for k := range listKeys {
		walked, err := checkKey(l, k)
		if err != nil {
			db.Close()
			return out, err
		}
		first[k] = walked
		out.Elems += len(walked)
	}
	if err := db.Close(); err != nil {
		return out, fmt.Errorf("close after verify: %w", err)
	}
	if st1.HighWater < minHW {
		return out, fmt.Errorf("recovered high-water %d below the worker's durable %d", st1.HighWater, minHW)
	}

	// Recovery must be repeatable: a second open of the same dir has to
	// land on the same list states, element order included, not merely
	// succeed.
	db2, l2, err := open()
	if err != nil {
		return out, fmt.Errorf("second reopen: %w", err)
	}
	defer db2.Close()
	if hw2 := db2.Stats().HighWater; hw2 != st1.HighWater {
		return out, fmt.Errorf("second open drifted: high-water %d -> %d", st1.HighWater, hw2)
	}
	for k := range listKeys {
		walked, err := checkKey(l2, k)
		if err != nil {
			return out, fmt.Errorf("second open: %w", err)
		}
		if !slices.Equal(walked, first[k]) {
			return out, fmt.Errorf("second open drifted on %s: %d elements vs %d and the contents differ", listKeyName(k), len(walked), len(first[k]))
		}
	}
	return out, nil
}
