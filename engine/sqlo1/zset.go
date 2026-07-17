package sqlo1

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

// ZSet is the sorted-set layer over Tiered: the doc 09 model, whose
// member side is the doc 06 hash machinery with the value fixed at
// the 8-byte sortable score image. It rides the same Hash type with
// the encZMem codec dimension, so the representation ladder (inline
// root, member segments, fence paging), the mh partitioning, and the
// W1-W4 write rules are all the hash's, byte discipline included.
// The stored score bytes are the big-endian #950 sortable transform,
// so the exact bytes a member entry holds are the bytes the score
// runs sort and fence on, with no re-encoding at the seam. The score
// side lives in zrun.go; ZAdd, ZIncrBy, and ZRem drive both families
// under one root frame per command (Z-I1), and the inline tier keeps
// its entries in (score, member) order so the future range family
// reads it as a sorted run.
type ZSet struct {
	h *Hash

	// sbuf holds the encoded score image of the current point write.
	sbuf [zmemScoreLen]byte

	// Score-side scratch, zrun.go: the decoded fence of the root
	// under operation (paged mode: the loaded leaf's entries), the
	// tail image a root write lands, the run images an op builds, and
	// the run subkey.
	zfence []zFenceEnt
	ztail  []byte
	zrbuf  []byte
	zrbuf2 []byte
	zkbuf  [SubkeySize]byte

	// Paged score fence state, zfencepage.go: whether the root under
	// operation pages, its decoded root index, the loaded upper page's
	// entries, the loaded positions (-1 until loaded), and the page
	// scratch. zloadTail resets all of it per op.
	zpaged bool
	zridx  []zIdxEnt
	zupper []zIdxEnt
	zui    int
	zli    int
	zpbuf  []byte
	zkbuf2 [SubkeySize]byte

	// Bulk build scratch, zstore.go: the STORE-family builder's arenas
	// and fences, reused across commands.
	zbld zBuilder

	// Pop scratch, zpop.go: the collected window a pop removes and
	// then emits, reused across commands.
	zparena []byte
	zppairs []zbuildPair

	// Algebra scratch, zalgebra.go: the aux ladder pool and loaded
	// sources, the fold-order and rest views, the walk window (members,
	// fhs, aggregating scores) with its probe routing, the union
	// cursors, and the result pairs the callers sort and reply or
	// bulk-build from.
	zalg       []*Hash
	zasrcs     []zalgSrc
	zaorder    []*zalgSrc
	zarest     []*zalgSrc
	zwarena    []byte
	zwmem      [][]byte
	zwfh       []uint64
	zwsc       []float64
	zagrpSeg   []uint64
	zagrpStart []int
	zahit      []bool
	zacurs     []zalgCursor
	zaarena    []byte
	zapairs    []zbuildPair
}

// NewZSet builds the zset layer over t.
func NewZSet(t *Tiered, cfg HashConfig) (*ZSet, error) {
	h, err := newSegLadder(t, cfg)
	if err != nil {
		return nil, err
	}
	h.tag, h.subSeg, h.subInline = TagZset, zsetSubSeg, zsetSubInline
	h.enc = encZMem
	return &ZSet{h: h}, nil
}

// memSet writes member's score on the member side, reporting whether
// the member was created: the single-family half the score-side tests
// drive standalone. It keeps hset's insertion order and knows nothing
// of the score runs, so production writes go through ZAdd, which
// upholds Z-I1 across both families and the inline sort order.
func (z *ZSet) memSet(ctx context.Context, key, member []byte, score float64) (bool, error) {
	binary.BigEndian.PutUint64(z.sbuf[:], zScoreSortable(score))
	return z.h.hset(ctx, key, member, z.sbuf[:], 0)
}

// memScore is ZScore under its historical score-side-test name.
func (z *ZSet) memScore(ctx context.Context, key, member []byte) (float64, bool, error) {
	return z.ZScore(ctx, key, member)
}

// memDel removes member from the member side, reporting whether it
// existed: memSet's counterpart, score-side test scaffolding only.
func (z *ZSet) memDel(ctx context.Context, key, member []byte) (bool, error) {
	return z.h.HDel(ctx, key, member)
}

// ErrZSetNaN is the increment overflow rule of ZINCRBY and ZADD INCR:
// an increment whose result is NaN (inf plus -inf) leaves the zset
// untouched. The text is Redis's wire error without the ERR prefix.
var ErrZSetNaN = errors.New("resulting score is not a number (NaN)")

// ZAddFlags carries ZADD's option set. The command layer has already
// rejected the conflicting combinations (NX with XX, NX with GT or LT,
// GT with LT) and NaN input scores, so the type layer only applies.
type ZAddFlags struct {
	NX, XX, GT, LT, Incr bool
}

// ZAdd writes one (member, score) pair under the doc 09 dual-write
// discipline: the member entry and the score-run entry move in one
// command, and on the segmented rung the command lands exactly one
// full root frame, last, as the plane's commit point (Z-I4's replay
// contract). added reports a created member, changed a live member
// whose score moved (the two halves of the CH answer), out the final
// score with outOK false when NX, XX, GT, or LT vetoed the write,
// which is INCR's nil reply.
func (z *ZSet) ZAdd(ctx context.Context, key, member []byte, score float64, f ZAddFlags) (added, changed bool, out float64, outOK bool, err error) {
	h := z.h
	st, hi, expMs, err := h.stateOf(ctx, key)
	if err != nil {
		return false, false, 0, false, err
	}
	switch st {
	case hashAbsent:
		if f.XX {
			return false, false, 0, false, nil
		}
		if err := z.zaddCreate(ctx, key, member, score, expMs); err != nil {
			return false, false, 0, false, err
		}
		return true, false, score, true, nil
	case hashInlineState:
		return z.zaddInline(ctx, key, hi, member, score, f, expMs)
	}
	return z.zaddSeg(ctx, key, member, score, f, expMs)
}

// ZIncrBy adds incr to member's score, creating the member at incr,
// the ZINCRBY surface: ZAdd's INCR mode with no veto flags, so the
// returned score is always present.
func (z *ZSet) ZIncrBy(ctx context.Context, key []byte, incr float64, member []byte) (float64, error) {
	_, _, out, _, err := z.ZAdd(ctx, key, member, incr, ZAddFlags{Incr: true})
	return out, err
}

// zaddDecide applies the flag set against member's current score:
// the final score, whether the write proceeds (false is INCR's nil),
// and the NaN door. Absent members skip it; GT and LT never block a
// create.
func zaddDecide(sOld uint64, score float64, f ZAddFlags) (sNew uint64, write bool, err error) {
	if f.Incr {
		score = zScoreFromSortable(sOld) + score
		if math.IsNaN(score) {
			return 0, false, ErrZSetNaN
		}
	}
	sNew = zScoreSortable(score)
	if (f.GT && sNew <= sOld) || (f.LT && sNew >= sOld) {
		return 0, false, nil
	}
	return sNew, true, nil
}

// zaddCreate builds the fresh key: a one-entry inline root, or the
// segmented rung immediately when the member alone cannot fit inline,
// the hset shape with the zset's sorted-region invariant trivially
// held.
func (z *ZSet) zaddCreate(ctx context.Context, key, member []byte, score float64, expMs int64) error {
	h := z.h
	binary.BigEndian.PutUint64(z.sbuf[:], zScoreSortable(score))
	h.rootBuf = appendHashInlineHdr(h.rootBuf[:0], h.subInline, 1, 0, false)
	h.rootBuf = appendHashEntry(h.rootBuf, member, z.sbuf[:], 0, h.enc)
	if len(h.rootBuf) > hashInlineMax {
		return z.zupgrade(ctx, key, expMs)
	}
	if err := h.t.Set(ctx, key, h.rootBuf, h.tag|TagRoot); err != nil {
		return err
	}
	return h.restamp(ctx, key, expMs)
}

// zaddInline is ZAdd on the inline rung: one root record rebuilt
// around the write with the region kept in (score, member) order,
// atomic per batch with no plane discipline to uphold. The write that
// crosses a threshold upgrades both families.
func (z *ZSet) zaddInline(ctx context.Context, key []byte, hi hashInline, member []byte, score float64, f ZAddFlags, expMs int64) (bool, bool, float64, bool, error) {
	h := z.h
	var sOld uint64
	oldOK := false
	it := hashEntryIter{p: hi.entries, enc: h.enc}
	for {
		m, v, _, ok, err := it.next()
		if err != nil {
			return false, false, 0, false, err
		}
		if !ok {
			break
		}
		if bytes.Equal(m, member) {
			sOld = binary.BigEndian.Uint64(v)
			oldOK = true
			break
		}
	}
	if !oldOK && f.XX {
		return false, false, 0, false, nil
	}
	if oldOK && f.NX {
		return false, false, 0, false, nil
	}
	sNew := zScoreSortable(score)
	if oldOK {
		var write bool
		var err error
		sNew, write, err = zaddDecide(sOld, score, f)
		if err != nil || !write {
			return false, false, 0, false, err
		}
		if sNew == sOld {
			return false, false, zScoreFromSortable(sNew), true, nil
		}
	}
	binary.BigEndian.PutUint64(z.sbuf[:], sNew)

	// One ordered rebuild: entries below the new (score, member) rank
	// copy first, the old entry's span drops wherever it sits, and the
	// new entry lands at its rank.
	h.rootBuf = grow(h.rootBuf, hashInlineHdrLen)
	it = hashEntryIter{p: hi.entries, enc: h.enc}
	placed := false
	for {
		before := it.p
		m, v, _, ok, err := it.next()
		if err != nil {
			return false, false, 0, false, err
		}
		if !ok {
			break
		}
		if !placed {
			c := bytes.Compare(v, z.sbuf[:])
			if c > 0 || (c == 0 && bytes.Compare(m, member) > 0) {
				h.rootBuf = appendHashEntry(h.rootBuf, member, z.sbuf[:], 0, h.enc)
				placed = true
			}
		}
		if bytes.Equal(m, member) {
			continue
		}
		h.rootBuf = append(h.rootBuf, before[:len(before)-len(it.p)]...)
	}
	if !placed {
		h.rootBuf = appendHashEntry(h.rootBuf, member, z.sbuf[:], 0, h.enc)
	}
	count := hi.count
	if !oldOK {
		count++
	}
	out := zScoreFromSortable(sNew)
	if count > hashInlineMaxCount || len(h.rootBuf) > hashInlineMax {
		if err := z.zupgrade(ctx, key, expMs); err != nil {
			return false, false, 0, false, err
		}
		return !oldOK, oldOK, out, true, nil
	}
	putHashInlineHdr(h.rootBuf, h.subInline, count, 0, false)
	if err := h.t.Set(ctx, key, h.rootBuf, h.tag|TagRoot); err != nil {
		return false, false, 0, false, err
	}
	return !oldOK, oldOK, out, true, h.restamp(ctx, key, expMs)
}

// zupgrade moves an inline zset to segments, both families in one
// command: the member family through the shared hash upgrade, then
// score runs cut at zRunMax straight from the region, which the
// inline discipline keeps in (score, member) order, and exactly one
// full root frame at the end. The caller has finished the region in
// h.rootBuf past the header slot, the write that crossed a threshold
// included.
func (z *ZSet) zupgrade(ctx context.Context, key []byte, expMs int64) error {
	h := z.h
	h.deferRoot = true
	h.t.ht.pinRoot(key)
	defer func() { h.deferRoot, h.rootPend = false, false; h.t.ht.unpinRoot() }()
	region := h.rootBuf[hashInlineHdrLen:]
	if _, err := h.upgrade(ctx, key, region, true, expMs); err != nil {
		return err
	}
	r := &h.segRoot
	z.zfence = z.zfence[:0]
	z.zpaged, z.zridx = false, z.zridx[:0]
	z.zui, z.zli = -1, -1
	z.zrbuf = append(z.zrbuf[:0], make([]byte, zRunHdrLen)...)
	runN := 0
	runLo := uint64(0)
	closeRun := func() error {
		if r.nextSegid > hashFenceSegidMax {
			return fmt.Errorf("sqlo1: zset segid space of rooth %#x is spent", r.rooth)
		}
		putZRunHdr(z.zrbuf, runN)
		if err := z.writeRun(ctx, r.nextSegid, z.zrbuf); err != nil {
			return err
		}
		lo := runLo
		if len(z.zfence) == 0 {
			lo = 0 // the sentinel separator, below every legal score
		}
		z.zfence = append(z.zfence, zFenceEnt{lo: lo, segid: r.nextSegid, count: uint32(runN)})
		r.nextSegid++
		z.zrbuf = append(z.zrbuf[:0], make([]byte, zRunHdrLen)...)
		runN = 0
		return nil
	}
	it := hashEntryIter{p: region, enc: h.enc}
	for {
		m, v, _, ok, err := it.next()
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		s := binary.BigEndian.Uint64(v)
		if runN > 0 && len(z.zrbuf)+zRunEntHdrLen+len(m) > zRunMax {
			if err := closeRun(); err != nil {
				return err
			}
		}
		if runN == 0 {
			runLo = s
		}
		z.zrbuf = appendZRunEnt(z.zrbuf, s, m)
		runN++
	}
	if err := closeRun(); err != nil {
		return err
	}
	// The runs land and flush before the root that references them,
	// the splitSeg order the member segments already took inside
	// upgrade.
	if err := h.t.Flush(ctx); err != nil {
		return err
	}
	return z.zflushRoot(ctx, key, expMs)
}

// zaddSeg is ZAdd on the segmented rung, the dual write proper: both
// side effects run under the deferred root, so however many segment,
// run, split, or page records the two families touch, the command
// lands one full root frame, after all of them.
func (z *ZSet) zaddSeg(ctx context.Context, key, member []byte, score float64, f ZAddFlags, expMs int64) (bool, bool, float64, bool, error) {
	h := z.h
	if err := z.zloadTail(); err != nil {
		return false, false, 0, false, err
	}
	fh := hashFH(member)
	i, err := h.fenceIdx(ctx, fh)
	if err != nil {
		return false, false, 0, false, err
	}
	seg, err := h.readSeg(ctx, h.segRoot.fence[i].segid)
	if err != nil {
		return false, false, 0, false, err
	}
	v, _, oldOK, err := hashSegGet(seg, fh, member)
	if err != nil {
		return false, false, 0, false, err
	}
	var sOld uint64
	if oldOK {
		sOld = binary.BigEndian.Uint64(v)
	}
	if !oldOK && f.XX {
		return false, false, 0, false, nil
	}
	if oldOK && f.NX {
		return false, false, 0, false, nil
	}
	sNew := zScoreSortable(score)
	if oldOK {
		var write bool
		sNew, write, err = zaddDecide(sOld, score, f)
		if err != nil || !write {
			return false, false, 0, false, err
		}
		if sNew == sOld {
			return false, false, zScoreFromSortable(sNew), true, nil
		}
	}
	// The capacity probe runs before either family writes: a spent
	// two-level fence must reject the command whole, never tear the
	// dual write (Z-I1).
	if err := z.zaddRoom(ctx, sNew, member); err != nil {
		return false, false, 0, false, err
	}

	h.deferRoot = true
	h.t.ht.pinRoot(key)
	defer func() { h.deferRoot, h.rootPend = false, false; h.t.ht.unpinRoot() }()
	binary.BigEndian.PutUint64(z.sbuf[:], sNew)
	if _, err := h.hsetSeg(ctx, key, member, z.sbuf[:], expMs, 0); err != nil {
		return false, false, 0, false, err
	}
	if oldOK {
		ok, err := z.zrunDelSeg(ctx, key, sOld, member)
		if err != nil {
			return false, false, 0, false, err
		}
		if !ok {
			return false, false, 0, false, fmt.Errorf("sqlo1: zset member %q of rooth %#x is missing from its score run", member, h.segRoot.rooth)
		}
	}
	ok, err := z.zrunAddSeg(ctx, key, sNew, member)
	if err != nil {
		return false, false, 0, false, err
	}
	if !ok {
		return false, false, 0, false, fmt.Errorf("sqlo1: zset score run of rooth %#x already holds %q", h.segRoot.rooth, member)
	}
	if err := z.zflushRoot(ctx, key, expMs); err != nil {
		return false, false, 0, false, err
	}
	return !oldOK, oldOK, zScoreFromSortable(sNew), true, nil
}

// zflushRoot ends a dual command on the segmented rung: the score
// section re-encodes into the tail and the root lands as one full
// frame, always written even when the image matches the last one,
// because the root frame is the plane's replay commit point and every
// dual command must own one.
func (z *ZSet) zflushRoot(ctx context.Context, key []byte, expMs int64) error {
	h := z.h
	h.deferRoot, h.rootPend = false, false
	z.zencodeTail()
	if err := h.writeSegRoot(ctx, key, false); err != nil {
		return err
	}
	return h.restamp(ctx, key, expMs)
}

// ZRem removes member, reporting whether it was there. The removal
// that empties the zset deletes the key (Redis's empty-key rule); on
// the segmented rung that is the plane retire, which the genbump
// covers at replay, so only the two-sided removal needs the dual
// discipline.
func (z *ZSet) ZRem(ctx context.Context, key, member []byte) (bool, error) {
	h := z.h
	st, _, expMs, err := h.stateOf(ctx, key)
	if err != nil {
		return false, err
	}
	switch st {
	case hashAbsent:
		return false, nil
	case hashInlineState:
		// A single ordered record: dropping an entry keeps the order,
		// and HDel already owns the rebuild and the empty-key death.
		return h.HDel(ctx, key, member)
	}
	if h.segRoot.count == 1 {
		// The last member: hdelSeg retires the whole plane, runs
		// included, behind a genbump, and answers false untouched when
		// member is not the one.
		return h.hdelSeg(ctx, key, member, expMs)
	}
	if err := z.zloadTail(); err != nil {
		return false, err
	}
	fh := hashFH(member)
	i, err := h.fenceIdx(ctx, fh)
	if err != nil {
		return false, err
	}
	seg, err := h.readSeg(ctx, h.segRoot.fence[i].segid)
	if err != nil {
		return false, err
	}
	v, _, ok, err := hashSegGet(seg, fh, member)
	if err != nil || !ok {
		return false, err
	}
	sOld := binary.BigEndian.Uint64(v)

	h.deferRoot = true
	h.t.ht.pinRoot(key)
	defer func() { h.deferRoot, h.rootPend = false, false; h.t.ht.unpinRoot() }()
	removed, err := h.hdelSeg(ctx, key, member, expMs)
	if err != nil {
		return false, err
	}
	if !removed {
		return false, fmt.Errorf("sqlo1: zset member %q of rooth %#x vanished mid-command", member, h.segRoot.rooth)
	}
	ok, err = z.zrunDelSeg(ctx, key, sOld, member)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, fmt.Errorf("sqlo1: zset member %q of rooth %#x is missing from its score run", member, h.segRoot.rooth)
	}
	return true, z.zflushRoot(ctx, key, expMs)
}

// Encoding is the OBJECT ENCODING answer for zset keys: listpack for
// the inline tier, skiplist past it, doc 09's parity rule.
func (z *ZSet) Encoding(ctx context.Context, key []byte) (string, bool, error) {
	st, _, _, err := z.h.stateOf(ctx, key)
	if err != nil {
		return "", false, err
	}
	switch st {
	case hashAbsent:
		return "", false, nil
	case hashSegState:
		return "skiplist", true, nil
	}
	return "listpack", true, nil
}

// ZCard answers the member count. The member-side root count is the
// Z-I2 authority the score-run counts must sum to.
func (z *ZSet) ZCard(ctx context.Context, key []byte) (int64, error) {
	return z.h.HLen(ctx, key)
}

// zmemStats is the member-side occupancy telemetry memStats answers:
// segment count, member total, summed encoded payload bytes, and the
// smallest and largest per-segment member counts. The doc 09 band
// (80-150 members per segment at leaderboard-shaped keys) is what the
// occupancy test holds the ladder to.
type zmemStats struct {
	segs    int
	members int
	bytes   int
	minSeg  int
	maxSeg  int
}

// memStats walks every member segment of a segmented zset and sums
// the occupancy picture. Inline roots answer the zero value with the
// member count; a telemetry walk, not a serving path, so it reads
// segments one at a time.
func (z *ZSet) memStats(ctx context.Context, key []byte) (zmemStats, error) {
	h := z.h
	st, hi, _, err := h.stateOf(ctx, key)
	if err != nil || st == hashAbsent {
		return zmemStats{}, err
	}
	if st == hashInlineState {
		return zmemStats{members: hi.count, bytes: len(hi.entries)}, nil
	}
	r := &h.segRoot
	s := zmemStats{members: int(r.count)}
	pages := 1
	if r.paged {
		pages = len(r.pidx)
	}
	for p := range pages {
		if err := h.loadPage(ctx, p); err != nil {
			return zmemStats{}, err
		}
		for _, e := range r.fence {
			seg, err := h.readSeg(ctx, e.segid)
			if err != nil {
				return zmemStats{}, err
			}
			if s.segs == 0 || seg.n < s.minSeg {
				s.minSeg = seg.n
			}
			s.maxSeg = max(s.maxSeg, seg.n)
			s.segs++
			s.bytes += len(seg.entries)
		}
	}
	return s, nil
}
