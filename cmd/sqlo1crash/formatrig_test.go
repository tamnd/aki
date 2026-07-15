package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"slices"
	"strings"

	"github.com/tamnd/aki/engine/sqlo1"
	"github.com/tamnd/aki/engine/sqlo1b"
)

// The B1 format crash matrix rig (spec 2064/sqlo1 doc 03, milestone
// B1 exit gate). One op-driving rig serves both injection arms: the
// torn arm runs it in-process over a FaultFile and cuts power with a
// seeded sector mask, the kill arm runs it in a worker process that
// the parent SIGKILLs mid-stream. Both arms end in verifyIteration,
// which recovers the durable image and holds it against the oracle:
// the file always opens, no acked frame is lost past the trim
// barrier, acked seals replay with their checksums, the grid restores
// from the superblock's allocmap root, and every checksummed sealed
// extent survives a scrub.
//
// Data frames carry no store at B1, so a checkpoint's Drain is a
// data-file sync and frames at or below the frozen target leave the
// verifiable set when the checkpoint commits; store-level exactly
// once is the A2 and B2 harness's job. Freed extents also leave the
// verifiable set for good, because a released extent's content is
// legitimately overwritten by reuse; the free and quarantine paths
// still run under crash, which is what F4 needs exercised.

const (
	// rigExtentSize is 4 groups, small enough that one iteration
	// churns through many extent lifecycles.
	rigExtentSize = 4 * sqlo1b.GroupSize
	rigGroups     = 4
	// rigSegSize keeps a torn-arm iteration inside one WAL segment,
	// which the tail mangler leans on; the kill arm's worker may grow
	// past it and multi-segment recovery is the transport's own
	// tested ground.
	rigSegSize = 256 << 10
)

// errRigCrash marks a planned checkpoint-boundary crash.
var errRigCrash = errors.New("sqlo1crash: planned crash point")

// rigPayload is the self-describing data frame payload: counter and
// seed up front, then bytes derived from both, so the verifier can
// regenerate any delivered frame from the payload alone.
func rigPayload(seed, ctr uint64) []byte {
	rng := rand.New(rand.NewPCG(seed, ctr+0x9E3779B97F4A7C15))
	b := make([]byte, 16+rng.IntN(240))
	binary.LittleEndian.PutUint64(b[0:], ctr)
	binary.LittleEndian.PutUint64(b[8:], seed)
	for i := 16; i < len(b); i++ {
		b[i] = byte(rng.UintN(256))
	}
	return b
}

type rigSeal struct{ walSeq, ext, sum uint64 }

// rigOracle is what the crashed store owed its callers: everything
// acked at a sync barrier, the highest superblock known committed,
// and the extents whose content stopped being verifiable.
type rigOracle struct {
	ackedData  []uint64
	ackedSeals []rigSeal
	committed  uint64
	freed      map[uint64]bool
	// lastAcked is the WAL seq the last sync barrier covered; frames
	// past it are fair game for the tail mangler.
	lastAcked uint64
}

type rigStream struct {
	ext     uint64
	groups  uint16
	payload uint32
}

type formatRig struct {
	seed uint64
	rng  *rand.Rand
	file sqlo1b.FileIO
	base *os.File
	wal  *sqlo1.WAL
	grid *sqlo1b.Grid
	sb   *sqlo1b.Superblock
	o    *rigOracle

	ctr      uint64
	sealSeq  uint64
	extents  uint64
	streams  map[uint16]*rigStream
	sealed   []uint64
	pendData []uint64
	pendSeal []rigSeal

	// Kill-arm hooks, called only after the state they report is
	// durable; nil in the torn arm.
	onAck    func(data []uint64, seals []rigSeal)
	onCommit func(seq uint64)
	onFree   func(ext uint64)
}

// newFormatRig formats a fresh file (both superblock slots, synced)
// and opens the sidecar under its db_id.
func newFormatRig(seed uint64, file sqlo1b.FileIO, base *os.File, walPath string) (*formatRig, error) {
	sb, err := sqlo1b.NewSuperblock()
	if err != nil {
		return nil, err
	}
	sb.ExtentSize = rigExtentSize
	sb.IOUnit = sqlo1b.GroupSize
	if err := sqlo1b.InitSuperblocks(file, sb); err != nil {
		return nil, err
	}
	if err := base.Truncate(rigExtentSize); err != nil {
		return nil, err
	}
	w, err := sqlo1.OpenWAL(walPath, sb.WALDBID(), rigSegSize)
	if err != nil {
		return nil, err
	}
	return &formatRig{
		seed:    seed,
		rng:     rand.New(rand.NewPCG(seed, 0xDA3E39CB94B95BDB)),
		file:    file,
		base:    base,
		wal:     w,
		grid:    sqlo1b.NewGrid(1),
		sb:      sb,
		o:       &rigOracle{committed: 1, freed: map[uint64]bool{}},
		extents: 1,
		streams: map[uint16]*rigStream{},
	}, nil
}

// step runs one weighted-random op; every boundary between two steps
// is a legal crash point.
func (r *formatRig) step() error {
	switch p := r.rng.IntN(100); {
	case p < 35:
		return r.opPut()
	case p < 45:
		return r.wal.Flush()
	case p < 60:
		return r.opBarrier()
	case p < 80:
		return r.opGroup(uint16(r.rng.IntN(2)))
	case p < 90:
		return r.opSeal(uint16(r.rng.IntN(2)))
	case p < 95:
		return r.opFree()
	default:
		return r.opCheckpoint(0)
	}
}

func (r *formatRig) opPut() error {
	payload := rigPayload(r.seed, r.ctr)
	r.ctr++
	seq, err := r.wal.Append(uint16(r.rng.IntN(2)), sqlo1.WALOpPut, 0, payload)
	if err != nil {
		return err
	}
	r.pendData = append(r.pendData, seq)
	return nil
}

// opBarrier is the ack barrier: flush, fsync, and only then does the
// oracle count the pending frames as owed.
func (r *formatRig) opBarrier() error {
	if err := r.wal.Flush(); err != nil {
		return err
	}
	if err := r.wal.Sync(); err != nil {
		return err
	}
	r.o.ackedData = append(r.o.ackedData, r.pendData...)
	r.o.ackedSeals = append(r.o.ackedSeals, r.pendSeal...)
	r.o.lastAcked = r.wal.LastSeq()
	if r.onAck != nil {
		r.onAck(r.pendData, r.pendSeal)
	}
	r.pendData, r.pendSeal = nil, nil
	return nil
}

func (r *formatRig) allocate(kind uint8, shard uint16) (uint64, error) {
	ext, err := r.grid.Allocate(kind, shard)
	if err != nil {
		r.grid.Grow(8)
		if ext, err = r.grid.Allocate(kind, shard); err != nil {
			return 0, err
		}
	}
	if ext+1 > r.extents {
		if err := r.base.Truncate(int64(ext+1) * rigExtentSize); err != nil {
			return 0, err
		}
		r.extents = ext + 1
	}
	return ext, nil
}

func (r *formatRig) opGroup(shard uint16) error {
	st := r.streams[shard]
	if st != nil && st.groups == rigGroups {
		if err := r.sealStream(shard); err != nil {
			return err
		}
		st = nil
	}
	if st == nil {
		ext, err := r.allocate(sqlo1b.KindVlog, shard)
		if err != nil {
			return err
		}
		st = &rigStream{ext: ext}
		r.streams[shard] = st
	}
	gcap := sqlo1b.GroupSize
	off := int64(st.ext)*rigExtentSize + int64(st.groups)*sqlo1b.GroupSize
	if st.groups == 0 {
		gcap = sqlo1b.Group0Payload
		off = int64(st.ext)*rigExtentSize + sqlo1b.ExtentHeaderSize
	}
	gb := sqlo1b.NewGroupBuilder(gcap)
	for range 1 + r.rng.IntN(4) {
		rec := make([]byte, 60+r.rng.IntN(640))
		if !gb.Fits(len(rec)) {
			break
		}
		for i := range rec {
			rec[i] = byte(r.rng.UintN(256))
		}
		if _, err := gb.Append(rec); err != nil {
			return err
		}
		st.payload += uint32(len(rec))
	}
	if _, err := r.file.WriteAt(gb.Close(), off); err != nil {
		return err
	}
	st.groups++
	return nil
}

func (r *formatRig) opSeal(shard uint16) error {
	if st := r.streams[shard]; st == nil || st.groups == 0 {
		return r.opGroup(shard)
	}
	return r.sealStream(shard)
}

// sealStream writes the sealed header, syncs the file, and only then
// emits the SEAL frame: an extent must be durable before anything
// references it (F2), which is also why every replayed seal is safe
// to scrub even when its frame never got a sync barrier.
func (r *formatRig) sealStream(shard uint16) error {
	st := r.streams[shard]
	r.sealSeq++
	h := sqlo1b.ExtentHeader{
		Kind:       sqlo1b.KindVlog,
		EFlags:     sqlo1b.EFlagSealed,
		Shard:      shard,
		SealSeq:    r.sealSeq,
		PayloadLen: st.payload,
		GroupCount: st.groups,
	}
	if _, err := r.file.WriteAt(h.Encode(), int64(st.ext)*rigExtentSize); err != nil {
		return err
	}
	if err := r.file.Sync(); err != nil {
		return err
	}
	sum, err := sqlo1b.ExtentChecksum(r.file, rigExtentSize, st.ext)
	if err != nil {
		return err
	}
	if _, err := r.grid.Seal(sqlo1b.KindVlog, shard); err != nil {
		return err
	}
	seq, err := r.wal.Append(shard, sqlo1b.FrameSeal, 0,
		sqlo1b.SealOp{Extent: st.ext, Sum: sum, Kind: sqlo1b.KindVlog}.Encode())
	if err != nil {
		return err
	}
	if err := r.wal.Flush(); err != nil {
		return err
	}
	r.pendSeal = append(r.pendSeal, rigSeal{walSeq: seq, ext: st.ext, sum: sum})
	r.sealed = append(r.sealed, st.ext)
	delete(r.streams, shard)
	return nil
}

func (r *formatRig) opFree() error {
	if len(r.sealed) == 0 {
		return r.opPut()
	}
	i := r.rng.IntN(len(r.sealed))
	ext := r.sealed[i]
	if err := r.grid.Free(ext, r.sb.Seq+1); err != nil {
		return err
	}
	r.sealed = slices.Delete(r.sealed, i, i+1)
	r.o.freed[ext] = true
	if r.onFree != nil {
		r.onFree(ext)
	}
	return nil
}

// opCheckpoint runs the six-step protocol, failing at failStep when
// nonzero; a planned failure comes back as errRigCrash. Step 5 is the
// commit point, so the oracle advances whenever Run hands back the
// successor even on a later-boundary crash.
func (r *formatRig) opCheckpoint(failStep int) error {
	ck := &sqlo1b.Checkpointer{WAL: r.wal, File: r.file, Grid: r.grid}
	if failStep > 0 {
		ck.SetCrashPoint(func(step int) error {
			if step == failStep {
				return errRigCrash
			}
			return nil
		})
	}
	next, err := ck.Run(r.sb, r)
	if next != nil {
		r.sb = next
		r.o.committed = next.Seq
		if r.onCommit != nil {
			r.onCommit(next.Seq)
		}
		// Frames at or below the frozen target are the snapshot's
		// now: seals verify through the allocmap root, data frames
		// leave the B1-verifiable set (no store to drain them into).
		var seals []rigSeal
		var covered []rigSeal
		for _, s := range r.pendSeal {
			if s.walSeq <= next.WALTrimSeq {
				covered = append(covered, s)
			} else {
				seals = append(seals, s)
			}
		}
		r.o.ackedSeals = append(r.o.ackedSeals, covered...)
		if r.onAck != nil && len(covered) > 0 {
			r.onAck(nil, covered)
		}
		r.pendSeal = seals
		var data []uint64
		for _, q := range r.pendData {
			if q > next.WALTrimSeq {
				data = append(data, q)
			}
		}
		r.pendData = data
	}
	if err != nil {
		if errors.Is(err, errRigCrash) {
			return errRigCrash
		}
		return err
	}
	return nil
}

// Drain, FlushIndex, and Snapshot make the rig its own
// CheckpointSource. Drain is a data-file sync (B1 has no record
// store); Snapshot writes the allocmap into a fresh sealed extent and
// returns its verifiable root, which is the arrow recovery walks.
func (r *formatRig) Drain(uint64) error { return r.file.Sync() }

func (r *formatRig) FlushIndex(uint64) error { return nil }

func (r *formatRig) Snapshot(uint64) (sqlo1b.Roots, error) {
	ext, err := r.allocate(sqlo1b.KindAllocmap, 0)
	if err != nil {
		return sqlo1b.Roots{}, err
	}
	if _, err := r.grid.Seal(sqlo1b.KindAllocmap, 0); err != nil {
		return sqlo1b.Roots{}, err
	}
	am := r.grid.Allocmap()
	h := sqlo1b.ExtentHeader{
		Kind:       sqlo1b.KindAllocmap,
		EFlags:     sqlo1b.EFlagSealed,
		PayloadLen: uint32(len(am)),
	}
	off := int64(ext) * rigExtentSize
	if _, err := r.file.WriteAt(h.Encode(), off); err != nil {
		return sqlo1b.Roots{}, err
	}
	if _, err := r.file.WriteAt(am, off+sqlo1b.ExtentHeaderSize); err != nil {
		return sqlo1b.Roots{}, err
	}
	if err := r.file.Sync(); err != nil {
		return sqlo1b.Roots{}, err
	}
	pos, err := sqlo1b.NewBlobPos(ext, 0)
	if err != nil {
		return sqlo1b.Roots{}, err
	}
	return sqlo1b.Roots{Allocmap: sqlo1b.MakeFullPtr(pos, am)}, nil
}

// mangleWALTail injects a torn write into the sidecar's unacked tail:
// pick a frame past the last sync barrier, then either zero from a
// random offset inside it to the segment's end (its tail sectors and
// everything after never landed) or flip one byte in it (a torn
// sector inside the flushed-but-unsynced region). Frames at or below
// ackedSeq are durable and never touched.
func mangleWALTail(path string, ackedSeq uint64, rng *rand.Rand) error {
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	type frame struct{ start, end, segEnd int64 }
	var cand []frame
	for segOff := int64(0); segOff < int64(len(b)); segOff += rigSegSize {
		segEnd := min(segOff+rigSegSize, int64(len(b)))
		for off := segOff; off+28 <= segEnd; {
			flen := int64(binary.LittleEndian.Uint32(b[off:]))
			if flen < 28 || off+flen > segEnd {
				break
			}
			if seq := binary.LittleEndian.Uint64(b[off+8:]); seq > ackedSeq {
				cand = append(cand, frame{off, off + flen, segEnd})
			}
			off += flen
		}
	}
	if len(cand) == 0 {
		return nil
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	fr := cand[rng.IntN(len(cand))]
	if rng.IntN(2) == 0 {
		cut := fr.start + 1 + rng.Int64N(fr.end-fr.start)
		_, err = f.WriteAt(make([]byte, fr.segEnd-cut), cut)
		return err
	}
	off := fr.start + rng.Int64N(fr.end-fr.start)
	b[off] ^= 0x40
	_, err = f.WriteAt(b[off:off+1], off)
	return err
}

// collectSink keeps every replayed data frame for the verifier.
type collectSink struct{ frames map[uint64][]byte }

func (s *collectSink) ApplyData(fr sqlo1.WALFrame) error {
	s.frames[fr.Seq] = slices.Clone(fr.Payload)
	return nil
}

// verifyIteration recovers the durable image and holds it against
// the oracle; any return is a matrix failure.
func verifyIteration(data *os.File, walPath string, seed uint64, o *rigOracle) error {
	sink := &collectSink{frames: map[uint64][]byte{}}
	rec, err := sqlo1b.Recover(data, walPath, rigSegSize, sink)
	if err != nil {
		return fmt.Errorf("recover: %w", err)
	}
	defer rec.WAL.Close()
	sb := rec.Super
	if sb.Seq < o.committed {
		return fmt.Errorf("survivor superblock seq %d but seq %d was committed", sb.Seq, o.committed)
	}
	// F10: the rig's geometry rides the superblock, not the code.
	if sb.ExtentSize != rigExtentSize {
		return fmt.Errorf("superblock extent size %d, the rig wrote %d", sb.ExtentSize, rigExtentSize)
	}
	for seq, payload := range sink.frames {
		if len(payload) < 16 {
			return fmt.Errorf("frame %d payload is %d bytes, below the self-describing minimum", seq, len(payload))
		}
		ctr := binary.LittleEndian.Uint64(payload)
		if want := rigPayload(seed, ctr); !bytes.Equal(payload, want) {
			return fmt.Errorf("frame %d payload does not regenerate from counter %d", seq, ctr)
		}
	}
	for _, seq := range o.ackedData {
		if seq > sb.WALTrimSeq {
			if _, ok := sink.frames[seq]; !ok {
				return fmt.Errorf("acked data frame %d lost, trim %d tip %d", seq, sb.WALTrimSeq, rec.Tip)
			}
		}
	}
	replayed := map[uint64]uint64{}
	for _, s := range rec.Format.Seals {
		replayed[s.Extent] = s.Sum
	}
	for _, s := range o.ackedSeals {
		if o.freed[s.ext] || s.walSeq <= sb.WALTrimSeq {
			continue
		}
		sum, ok := replayed[s.ext]
		if !ok {
			return fmt.Errorf("acked seal of extent %d at wal seq %d lost, trim %d", s.ext, s.walSeq, sb.WALTrimSeq)
		}
		if sum != s.sum {
			return fmt.Errorf("extent %d replayed checksum %#x, acked %#x", s.ext, sum, s.sum)
		}
	}
	g, err := gridFromSuper(data, sb)
	if err != nil {
		return err
	}
	if err := rec.RestoreGrid(g); err != nil {
		return fmt.Errorf("grid restore: %w", err)
	}
	sums := map[uint64]uint64{}
	for ext, sum := range replayed {
		if !o.freed[ext] {
			sums[ext] = sum
		}
	}
	for _, s := range o.ackedSeals {
		if o.freed[s.ext] {
			continue
		}
		sums[s.ext] = s.sum
		if s.ext >= g.ExtentCount() || g.State(s.ext) != sqlo1b.StateSealed {
			return fmt.Errorf("acked sealed extent %d not sealed in the restored grid", s.ext)
		}
	}
	scr := &sqlo1b.Scrubber{File: data, ExtentSize: rigExtentSize, Grid: g, Sums: sums}
	for _, fd := range scr.Sweep().Findings {
		if _, tracked := sums[fd.Extent]; tracked {
			return fmt.Errorf("sealed extent %d damaged after recovery: %v", fd.Extent, fd.Err)
		}
	}
	return nil
}

// gridFromSuper rebuilds the grid the way a real open would: from the
// superblock's allocmap root when one is committed (header, payload,
// and the FullPtr checksum all verify on the way), or fresh from the
// file size before the first checkpoint.
func gridFromSuper(data *os.File, sb *sqlo1b.Superblock) (*sqlo1b.Grid, error) {
	if sb.AllocmapRoot == (sqlo1b.FullPtr{}) {
		st, err := data.Stat()
		if err != nil {
			return nil, err
		}
		return sqlo1b.NewGrid(max(uint64((st.Size()+rigExtentSize-1)/rigExtentSize), 1)), nil
	}
	ext := sqlo1b.Pos(sb.AllocmapRoot.Pos).Extent()
	hb := make([]byte, sqlo1b.ExtentHeaderSize)
	if _, err := data.ReadAt(hb, int64(ext)*rigExtentSize); err != nil {
		return nil, fmt.Errorf("allocmap root extent %d: %w", ext, err)
	}
	h, err := sqlo1b.DecodeExtentHeader(hb)
	if err != nil {
		return nil, fmt.Errorf("allocmap root extent %d: %w", ext, err)
	}
	if h.Kind != sqlo1b.KindAllocmap || !h.Sealed() {
		return nil, fmt.Errorf("allocmap root extent %d is kind %d sealed %v", ext, h.Kind, h.Sealed())
	}
	am := make([]byte, h.PayloadLen)
	if _, err := data.ReadAt(am, int64(ext)*rigExtentSize+sqlo1b.ExtentHeaderSize); err != nil {
		return nil, fmt.Errorf("allocmap payload: %w", err)
	}
	if err := sb.AllocmapRoot.Verify(am); err != nil {
		return nil, fmt.Errorf("allocmap root: %w", err)
	}
	return sqlo1b.LoadGrid(am, uint64(len(am))*8)
}

// parseAckLog rebuilds the oracle from a killed worker's log. Lines
// land in single appends, so only a missing trailing newline marks a
// torn final line to drop.
func parseAckLog(path string) (*rigOracle, error) {
	o := &rigOracle{freed: map[uint64]bool{}}
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return o, nil
		}
		return nil, err
	}
	s := string(b)
	torn := !strings.HasSuffix(s, "\n")
	lines := strings.Split(strings.TrimSuffix(s, "\n"), "\n")
	for i, ln := range lines {
		if ln == "" {
			continue
		}
		var a, c, d uint64
		switch {
		case strings.HasPrefix(ln, "D "):
			_, err = fmt.Sscanf(ln, "D %d", &a)
			o.ackedData = append(o.ackedData, a)
		case strings.HasPrefix(ln, "S "):
			_, err = fmt.Sscanf(ln, "S %d %d %d", &a, &c, &d)
			o.ackedSeals = append(o.ackedSeals, rigSeal{walSeq: a, ext: c, sum: d})
		case strings.HasPrefix(ln, "C "):
			_, err = fmt.Sscanf(ln, "C %d", &a)
			o.committed = max(o.committed, a)
		case strings.HasPrefix(ln, "F "):
			_, err = fmt.Sscanf(ln, "F %d", &a)
			o.freed[a] = true
		default:
			err = fmt.Errorf("unknown record")
		}
		if err != nil {
			if torn && i == len(lines)-1 {
				break
			}
			return nil, fmt.Errorf("ack log line %d %q: %w", i+1, ln, err)
		}
	}
	return o, nil
}
