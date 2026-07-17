package sqlo1

// The stream type layer, doc 10 sections 1 and 3: XADD, XLEN, and the
// range walks over the run codec (streamrun.go) and the ID-keyed root
// fence (streamroot.go). XADD is the type's hot path and X-I1 is its
// law: appends amend the tail run in place until the lab thresholds cut
// a fresh one, and a sealed run is never rewritten. Trims, tombstones,
// groups, and the pending machinery are later slices.

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

// XADD's ID grammar, parsed by the command layer into one of three
// modes. A bare ms with no seq part is ms-0, Redis's rule, not the
// auto-seq form.
const (
	xidExplicit = iota // ms-seq
	xidAutoSeq         // ms-*: ms given, seq auto
	xidAuto            // *: both auto
)

// The XADD validation errors, carrying Redis's wire texts behind
// storeErr's ERR prefix.
var (
	errXaddZeroID    = errors.New("The ID specified in XADD must be greater than 0-0")
	errXaddSmallID   = errors.New("The ID specified in XADD is equal or smaller than the target stream top item")
	errXaddExhausted = errors.New("The stream has exhausted the last possible ID, unable to add more items")
)

// streamRangeBatchRuns is the run prefetch width of a range walk,
// listRangeBatchNodes's reasoning at the run size: a full round is 16
// runs of up to ~4 KiB, bounding the IO round at about 64 KiB while
// amortizing the cold index path across the batch.
const streamRangeBatchRuns = 16

// StreamConfig parameterizes the stream layer's plane minting.
type StreamConfig struct {
	// Shard namespaces the rooth mint, doc 03 section 6.3.
	Shard uint16
	// LeaseN is the mint lease size. Default defaultLeaseN.
	LeaseN uint64
}

// Stream is the stream type layer over the shard runtime. Not safe for
// concurrent use, like the other type layers: the caller serializes
// (R1).
type Stream struct {
	t    *Tiered
	mint Minter
	cfg  StreamConfig

	// The current mint lease: counters [leaseNext, leaseEnd) are ours.
	leaseNext uint64
	leaseEnd  uint64

	// root is the decoded root of the key the current op holds; its
	// fence lives in the fence scratch, copied out on decode so it
	// survives the run reads the op does next.
	root  streamRoot
	fence []streamFenceEnt

	// kbuf holds the subkey of the run being read or written; shared
	// because the seam doors copy key bytes before returning.
	kbuf [SubkeySize]byte

	// rootBuf stages a rebuilt root payload and runBuf a rebuilt or
	// amended run; both safe to fill from spans aliasing a read because
	// Tiered copies on Set and nothing else touches them between.
	rootBuf []byte
	runBuf  []byte

	// ents, fvPool, and fvOffs carry one tail run's decoded entries
	// across the walk for the re-encode path: the walker reuses its
	// pair scratch, so each entry's fv headers copy into the flat pool
	// and re-slice after the walk (the name and value bytes themselves
	// alias the read, which stays valid until the write). names rebuilds
	// the encoder's first-seen table, which canonical form guarantees
	// is derivable from the entries.
	ents   []streamEntry
	fvPool [][]byte
	fvOffs []int
	names  [][]byte

	// mgKeyBuf, mgKeys, mgVals, mgRoots, and mgExps carry one range
	// walk's prefetch round, the list Range shape.
	mgKeyBuf []byte
	mgKeys   [][]byte
	mgVals   [][]byte
	mgRoots  []bool
	mgExps   []int64
}

// NewStream builds the stream layer over t. The store must carry the
// Minter capability: streams are planed from their first entry.
func NewStream(t *Tiered, cfg StreamConfig) (*Stream, error) {
	mint, ok := t.st.(Minter)
	if !ok {
		return nil, fmt.Errorf("sqlo1: store %T lacks the Minter capability the stream layer needs", t.st)
	}
	if cfg.LeaseN == 0 {
		cfg.LeaseN = defaultLeaseN
	}
	return &Stream{t: t, mint: mint, cfg: cfg}, nil
}

// nextRooth mints one rooth, taking a fresh durable lease when the
// current one is spent.
func (x *Stream) nextRooth(ctx context.Context) (uint64, error) {
	if x.leaseNext == x.leaseEnd {
		start, err := x.mint.MintLease(ctx, x.cfg.LeaseN)
		if err != nil {
			return 0, err
		}
		end, err := LeaseEnd(start, x.cfg.LeaseN)
		if err != nil {
			return 0, err
		}
		x.leaseNext, x.leaseEnd = start, end
	}
	c := x.leaseNext
	x.leaseNext++
	return MintRooth(x.cfg.Shard, c)
}

// restamp mirrors Str.restamp: puts a key's expiry back after a write
// that may have gone through a fresh hot header.
func (x *Stream) restamp(ctx context.Context, key []byte, expMs int64) error {
	if expMs == 0 {
		return nil
	}
	_, err := x.t.ExpireAt(ctx, key, expMs)
	return err
}

// stateOf reads key and classifies it: exists false is a missing key.
// The decoded root lands in x.root, fence copied out of the read, and
// stays valid across the run reads the op does next.
func (x *Stream) stateOf(ctx context.Context, key []byte) (exists bool, expMs int64, err error) {
	v, root, expMs, ok, err := x.t.LookupEntry(ctx, key)
	if err != nil || !ok {
		return false, 0, err
	}
	if !root {
		return false, 0, ErrWrongType
	}
	tag, _, err := sniffRoot(v)
	if err != nil {
		return false, 0, err
	}
	if tag != TagStream {
		return false, 0, ErrWrongType
	}
	x.root, err = decodeStreamRoot(v, x.fence[:0])
	if err != nil {
		return false, 0, err
	}
	x.fence = x.root.fence
	return true, expMs, nil
}

// readRun reads the raw run payload at segid under the current root's
// plane. The bytes alias the read and die on the next Tiered call.
func (x *Stream) readRun(ctx context.Context, segid uint64) ([]byte, error) {
	putHashSegKey(x.kbuf[:], x.root.rooth, segid)
	v, ok, err := x.t.Get(ctx, x.kbuf[:])
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("sqlo1: stream run %d of rooth %#x is missing", segid, x.root.rooth)
	}
	return v, nil
}

// writeRun writes a run image under the current root's plane.
func (x *Stream) writeRun(ctx context.Context, segid uint64, payload []byte) error {
	putHashSegKey(x.kbuf[:], x.root.rooth, segid)
	return x.t.SetGen(ctx, x.kbuf[:], payload, TagStream, x.root.rootgen)
}

// writeRoot encodes x.root and lands it under key. Always a full image,
// writeNodeRoot's reasoning: the reconcilers do not know this sub, so a
// W2 delta claim would be a silent no-op.
func (x *Stream) writeRoot(ctx context.Context, key []byte) error {
	x.root.fence = x.fence
	x.rootBuf = appendStreamRoot(x.rootBuf[:0], &x.root)
	return x.t.Set(ctx, key, x.rootBuf, TagStream|TagRoot)
}

// resolveXaddID turns the parsed ID grammar into the entry ID against
// the stream's last generated ID (the zero ID on a fresh stream), or
// the Redis validation error.
func resolveXaddID(mode int, req streamID, nowMs int64, last streamID) (streamID, error) {
	switch mode {
	case xidAuto:
		id := streamID{ms: uint64(nowMs)}
		if last.less(id) {
			return id, nil
		}
		// The clock is at or behind the last ID: bump off it, ms up
		// when seq saturates, Redis's streamNextID.
		if last.seq < math.MaxUint64 {
			return streamID{ms: last.ms, seq: last.seq + 1}, nil
		}
		if last.ms == math.MaxUint64 {
			return streamID{}, errXaddExhausted
		}
		return streamID{ms: last.ms + 1}, nil
	case xidAutoSeq:
		switch {
		case req.ms < last.ms:
			return streamID{}, errXaddSmallID
		case req.ms > last.ms:
			return streamID{ms: req.ms}, nil
		}
		// Same millisecond: the next seq, which on a fresh stream also
		// yields Redis's 0-1 for 0-* (a saturated seq answers the
		// too-small error, Redis 8.8's observed reply, not the
		// exhausted text).
		if last.seq == math.MaxUint64 {
			return streamID{}, errXaddSmallID
		}
		return streamID{ms: req.ms, seq: last.seq + 1}, nil
	}
	if req == (streamID{}) {
		return streamID{}, errXaddZeroID
	}
	if !last.less(req) {
		return streamID{}, errXaddSmallID
	}
	return req, nil
}

// Add is XADD: resolve the ID against the last generated one, append
// the entry, and report the ID. ok is false only for NOMKSTREAM on a
// missing key, XADD's null reply. fv is the flat name/value pair list
// in argument order.
func (x *Stream) Add(ctx context.Context, key []byte, mode int, req streamID, nowMs int64, noMk bool, fv [][]byte) (streamID, bool, error) {
	exists, expMs, err := x.stateOf(ctx, key)
	if err != nil {
		return streamID{}, false, err
	}
	if !exists {
		if noMk {
			return streamID{}, false, nil
		}
		id, err := resolveXaddID(mode, req, nowMs, streamID{})
		if err != nil {
			return streamID{}, false, err
		}
		return id, true, x.create(ctx, key, id, fv)
	}
	id, err := resolveXaddID(mode, req, nowMs, x.root.last)
	if err != nil {
		return streamID{}, false, err
	}
	return id, true, x.append(ctx, key, id, fv, expMs)
}

// create handles the first XADD of a key: mint the plane, cut run 0,
// and land the root after the plane flushes, the fresh-plane rule every
// upgrade path follows (every crash prefix reads a missing key over a
// plane nothing references yet).
func (x *Stream) create(ctx context.Context, key []byte, id streamID, fv [][]byte) error {
	rooth, err := x.nextRooth(ctx)
	if err != nil {
		return err
	}
	x.root = streamRoot{rootgen: 1, rooth: rooth, count: 1, added: 1, last: id, nextSegid: 1}
	x.ents = append(x.ents[:0], streamEntry{id: id, fv: fv})
	x.runBuf = appendStreamRun(x.runBuf[:0], x.ents)
	if err := x.writeRun(ctx, 0, x.runBuf); err != nil {
		return err
	}
	x.fence = append(x.fence[:0], streamFenceEnt{base: id, segid: 0, count: 1})
	if err := x.t.Flush(ctx); err != nil {
		return err
	}
	return x.writeRoot(ctx, key)
}

// appendCut starts a fresh tail run holding just the new entry. The
// refusal happens before any write, so it is side-effect free.
func (x *Stream) appendCut(ctx context.Context, key []byte, id streamID, fv [][]byte, expMs int64) error {
	if len(x.fence) >= streamFenceMaxRuns {
		return errStreamFenceFull
	}
	r := &x.root
	x.ents = append(x.ents[:0], streamEntry{id: id, fv: fv})
	x.runBuf = appendStreamRun(x.runBuf[:0], x.ents)
	segid := r.nextSegid
	if err := x.writeRun(ctx, segid, x.runBuf); err != nil {
		return err
	}
	x.fence = append(x.fence, streamFenceEnt{base: id, segid: segid, count: 1})
	r.nextSegid++
	r.count++
	r.added++
	r.last = id
	if err := x.writeRoot(ctx, key); err != nil {
		return err
	}
	return x.restamp(ctx, key, expMs)
}

// append lands one entry on a live stream: amend the tail run in place
// while the lab thresholds allow, cut a fresh run when they do not.
// The amendment has two shapes. When the run's name table already
// covers the entry (or its names legally inline) and no tomb bitmap
// rides the tail, the new bytes append to the existing image with only
// the header count patched, and canonical form makes that byte-equal to
// a from-scratch re-encode, which the tests assert. A new table-bound
// name or a tomb bitmap re-encodes the run whole instead.
func (x *Stream) append(ctx context.Context, key []byte, id streamID, fv [][]byte, expMs int64) error {
	r := &x.root
	if len(x.fence) == 0 {
		// A fully trimmed stream: the next entry starts a fresh fence.
		return x.appendCut(ctx, key, id, fv, expMs)
	}
	te := &x.fence[len(x.fence)-1]
	v, err := x.readRun(ctx, te.segid)
	if err != nil {
		return err
	}

	// One walk collects everything the decision needs: the entries
	// (copied out of the walker's pair scratch into the flat pool; the
	// underlying bytes alias v, which stays live because nothing reads
	// again before the write) and the rebuilt first-seen name table.
	x.ents, x.fvPool, x.fvOffs, x.names = x.ents[:0], x.fvPool[:0], x.fvOffs[:0], x.names[:0]
	info, err := walkStreamRun(v, func(i int, e streamEntry) error {
		x.fvOffs = append(x.fvOffs, len(x.fvPool))
		x.fvPool = append(x.fvPool, e.fv...)
		x.ents = append(x.ents, streamEntry{id: e.id, dead: e.dead})
		for f := 0; f < len(e.fv); f += 2 {
			name := e.fv[f]
			if len(name) > streamNameMaxLen || len(x.names) == streamNameTableMax {
				continue
			}
			if streamNameRef(x.names, name) < 0 {
				x.names = append(x.names, name)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	x.fvOffs = append(x.fvOffs, len(x.fvPool))
	for i := range x.ents {
		x.ents[i].fv = x.fvPool[x.fvOffs[i]:x.fvOffs[i+1]]
	}

	// Size the new entry under the current table and spot the names
	// that would grow it. The projection is exact for both amendment
	// shapes: a table-bound name adds its table bytes and refs where
	// the fast path would inline.
	var dms, dseq uint64
	dms = id.ms - info.last.ms
	if dms == 0 {
		dseq = id.seq - info.last.seq
	} else {
		dseq = id.seq
	}
	entBytes := streamUvarintLen(dms) + streamUvarintLen(dseq) + streamUvarintLen(uint64(len(fv)/2))
	tableAdd := 0
	for f := 0; f < len(fv); f += 2 {
		name, val := fv[f], fv[f+1]
		nameSeen := streamNameRef(x.names, name) >= 0
		switch {
		case nameSeen:
			entBytes++
		case len(name) <= streamNameMaxLen && len(x.names) < streamNameTableMax:
			// First-seen growth: the name joins the table (u8 length
			// plus bytes) and the field pays one ref byte. Growing the
			// scratch table here keeps a second occurrence in the same
			// entry from double-counting.
			tableAdd += 1 + len(name)
			entBytes++
			x.names = append(x.names, name)
		default:
			entBytes += 1 + streamUvarintLen(uint64(len(name))) + len(name)
		}
		entBytes += streamUvarintLen(uint64(len(val))) + len(val)
	}
	bitmapGrow := 0
	if info.tombs {
		bitmapGrow = (info.n+1+7)/8 - (info.n+7)/8
	}

	if info.n+1 > streamRunMaxEntries || len(v)+entBytes+tableAdd+bitmapGrow > streamRunMax {
		return x.appendCut(ctx, key, id, fv, expMs)
	}

	if tableAdd == 0 && !info.tombs {
		// Fast amendment: the stored image plus the new entry's bytes
		// is already the canonical encoding of the grown run.
		x.runBuf = append(x.runBuf[:0], v...)
		binary.LittleEndian.PutUint16(x.runBuf[16:], uint16(info.n+1))
		x.runBuf = appendStreamRunEntry(x.runBuf, x.names, info.last, id, fv)
	} else {
		x.ents = append(x.ents, streamEntry{id: id, fv: fv})
		x.runBuf = appendStreamRun(x.runBuf[:0], x.ents)
	}
	if err := x.writeRun(ctx, te.segid, x.runBuf); err != nil {
		return err
	}
	te.count++
	r.count++
	r.added++
	r.last = id
	if err := x.writeRoot(ctx, key); err != nil {
		return err
	}
	return x.restamp(ctx, key, expMs)
}

// Len is XLEN: the root count, zero for a missing key.
func (x *Stream) Len(ctx context.Context, key []byte) (int64, error) {
	exists, _, err := x.stateOf(ctx, key)
	if err != nil || !exists {
		return 0, err
	}
	return int64(x.root.count), nil
}

// fenceSeek finds the interval of fence indexes whose runs can hold IDs
// in [start, end]: lo is the last run whose base is at or below start
// (clamped to 0), hi the last whose base is at or below end. ok is
// false when no run can overlap.
func (x *Stream) fenceSeek(start, end streamID) (lo, hi int, ok bool) {
	if len(x.fence) == 0 || end.less(x.fence[0].base) || x.root.last.less(start) {
		return 0, 0, false
	}
	hi = len(x.fence) - 1
	for i := 1; i < len(x.fence); i++ {
		if end.less(x.fence[i].base) {
			hi = i - 1
			break
		}
	}
	for i := hi; i >= 0; i-- {
		if !start.less(x.fence[i].base) {
			return i, hi, true
		}
	}
	return 0, hi, true
}

// errStreamWalkDone is the early-exit sentinel of a bounded run walk.
var errStreamWalkDone = errors.New("sqlo1: stream walk done")

// countRunIn decodes the run at segid and counts live entries inside
// [start, end].
func (x *Stream) countRunIn(ctx context.Context, segid uint64, start, end streamID) (int64, error) {
	v, err := x.readRun(ctx, segid)
	if err != nil {
		return 0, err
	}
	n := int64(0)
	_, err = walkStreamRun(v, func(i int, e streamEntry) error {
		if end.less(e.id) {
			return errStreamWalkDone
		}
		if !e.dead && !e.id.less(start) {
			n++
		}
		return nil
	})
	if err != nil && !errors.Is(err, errStreamWalkDone) {
		return 0, err
	}
	return n, nil
}

// Range streams the live entries with IDs in [start, end], XRANGE's
// inclusive grammar with the command layer resolving the exclusive
// forms; rev walks the same window backward. count caps the emits, -1
// meaning no cap. begin runs exactly once, before any emit, with the
// exact number of entries that will follow, so the RESP writer puts the
// array header down and streams the rest; emitted spans alias the
// current IO round and die at the next emit or Tiered call. A missing
// key is begin(0).
func (x *Stream) Range(ctx context.Context, key []byte, start, end streamID, count int64, rev bool, begin func(n int), emit func(id streamID, fv [][]byte)) error {
	exists, _, err := x.stateOf(ctx, key)
	if err != nil {
		return err
	}
	if !exists || count == 0 || end.less(start) {
		begin(0)
		return nil
	}
	lo, hi, ok := x.fenceSeek(start, end)
	if !ok {
		begin(0)
		return nil
	}

	// The exact count first: interior runs answer from their fence
	// counts, and only the boundary runs decode. The boundary reads
	// warm the hot tier for the emit pass behind them.
	total := int64(0)
	if lo == hi {
		total, err = x.countRunIn(ctx, x.fence[lo].segid, start, end)
	} else {
		for i := lo + 1; i < hi; i++ {
			total += int64(x.fence[i].count)
		}
		var c int64
		if c, err = x.countRunIn(ctx, x.fence[lo].segid, start, end); err == nil {
			total += c
			c, err = x.countRunIn(ctx, x.fence[hi].segid, start, end)
			total += c
		}
	}
	if err != nil {
		return err
	}
	if count > 0 && total > count {
		total = count
	}
	begin(int(total))
	if total == 0 {
		return nil
	}
	if rev {
		return x.emitRev(ctx, lo, hi, start, end, total, emit)
	}
	return x.emitFwd(ctx, lo, hi, start, end, total, emit)
}

// prefetchRuns reads runs [base, base+w) of the fence in one IO round;
// the views land in mgVals and die at the next Tiered call.
func (x *Stream) prefetchRuns(ctx context.Context, base, w int) error {
	x.mgKeyBuf = grow(x.mgKeyBuf, w*SubkeySize)
	x.mgKeys = x.mgKeys[:0]
	for j := range w {
		k := x.mgKeyBuf[j*SubkeySize : (j+1)*SubkeySize]
		putHashSegKey(k, x.root.rooth, x.fence[base+j].segid)
		x.mgKeys = append(x.mgKeys, k)
	}
	var err error
	x.mgVals, x.mgRoots, x.mgExps, err = x.t.LookupBatch(ctx, x.mgKeys, x.mgVals, x.mgRoots, x.mgExps)
	return err
}

// emitFwd walks runs lo..hi forward in prefetched rounds, emitting live
// entries inside the bounds until total are out.
func (x *Stream) emitFwd(ctx context.Context, lo, hi int, start, end streamID, total int64, emit func(id streamID, fv [][]byte)) error {
	remaining := total
	for base := lo; base <= hi && remaining > 0; base += streamRangeBatchRuns {
		w := min(streamRangeBatchRuns, hi+1-base)
		if err := x.prefetchRuns(ctx, base, w); err != nil {
			return err
		}
		for j := 0; j < w && remaining > 0; j++ {
			if x.mgVals[j] == nil {
				return fmt.Errorf("sqlo1: stream run %d of rooth %#x is missing", x.fence[base+j].segid, x.root.rooth)
			}
			_, err := walkStreamRun(x.mgVals[j], func(i int, e streamEntry) error {
				if remaining == 0 || end.less(e.id) {
					return errStreamWalkDone
				}
				if !e.dead && !e.id.less(start) {
					emit(e.id, e.fv)
					remaining--
				}
				return nil
			})
			if err != nil && !errors.Is(err, errStreamWalkDone) {
				return err
			}
		}
	}
	return nil
}

// emitRev walks runs hi..lo backward in prefetched rounds. Entries
// inside a run buffer through the fv pool (bytes alias the round's
// read, which outlives the buffering) and replay in reverse before the
// next run decodes.
func (x *Stream) emitRev(ctx context.Context, lo, hi int, start, end streamID, total int64, emit func(id streamID, fv [][]byte)) error {
	remaining := total
	for top := hi; top >= lo && remaining > 0; top -= streamRangeBatchRuns {
		base := max(lo, top-streamRangeBatchRuns+1)
		w := top - base + 1
		if err := x.prefetchRuns(ctx, base, w); err != nil {
			return err
		}
		for j := w - 1; j >= 0 && remaining > 0; j-- {
			if x.mgVals[j] == nil {
				return fmt.Errorf("sqlo1: stream run %d of rooth %#x is missing", x.fence[base+j].segid, x.root.rooth)
			}
			x.ents, x.fvPool, x.fvOffs = x.ents[:0], x.fvPool[:0], x.fvOffs[:0]
			_, err := walkStreamRun(x.mgVals[j], func(i int, e streamEntry) error {
				if end.less(e.id) {
					return errStreamWalkDone
				}
				if !e.dead && !e.id.less(start) {
					x.fvOffs = append(x.fvOffs, len(x.fvPool))
					x.fvPool = append(x.fvPool, e.fv...)
					x.ents = append(x.ents, streamEntry{id: e.id})
				}
				return nil
			})
			if err != nil && !errors.Is(err, errStreamWalkDone) {
				return err
			}
			x.fvOffs = append(x.fvOffs, len(x.fvPool))
			for i := len(x.ents) - 1; i >= 0 && remaining > 0; i-- {
				emit(x.ents[i].id, x.fvPool[x.fvOffs[i]:x.fvOffs[i+1]])
				remaining--
			}
		}
	}
	return nil
}
