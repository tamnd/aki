package sqlo1b

import (
	"bytes"
	"cmp"
	"context"
	"fmt"
	"io"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/tamnd/aki/engine/sqlo1"

	"encoding/binary"
	"math/bits"
)

// Store wires the format core into the sqlo1.Store seam: one shard,
// synchronous, a mutex around everything. ApplyBatch drains a batch
// into vlog groups and index inserts under WAL cover, Get and
// BatchGet read through the dirty chunks or the cold IndexReader,
// Scan walks buckets in order. Checkpoint runs the doc 03 section 13
// protocol; the runtime owns the cadence, this type only executes.
//
// Deviations from the doc, all v0 and all revisited by later slices:
// deletes remove the index entry without writing a vlog tombstone
// (the WAL DEL frame plus the checkpoint-consistent index carry the
// same information in a synchronous store, and B3 compaction probes
// the index for liveness anyway); every expiring record sits in the
// near expiry class until the WATT slice refines classes; the
// directory root and the allocmap root each fit one group, which
// caps v0 at 65536 buckets and about 8 TiB of extents.

// StoreFile is the data-file shape the store runs on: FileIO plus
// Truncate for whole-extent growth. os.File satisfies it; the crash
// harness wraps a FaultFile and delegates Truncate to the base.
type StoreFile interface {
	io.ReaderAt
	io.WriterAt
	Sync() error
	Truncate(size int64) error
}

const (
	// storeInitialExtents sizes a fresh file: extent 0 for the
	// superblocks plus room to start every stream without growing.
	storeInitialExtents = 8
	// storeGrowExtents is the whole-extent growth unit when the grid
	// runs out of free extents.
	storeGrowExtents = 8
)

// Grid stream discriminators. The store is single-shard, so the
// grid's shard argument is free to split the vlog kind into a record
// stream and a blob stream with independent active extents. The
// split is RAM-only: extent headers keep shard 0 and streams are
// rebuilt from scratch on open.
const (
	recStream  uint16 = 0
	blobStream uint16 = 1
)

// streamCursor tracks one append stream's active extent: where the
// next group goes and what the seal will need.
type streamCursor struct {
	kind    uint8
	stream  uint16
	eflags  uint8
	ext     uint64
	next    uint16 // next unwritten group
	groups  uint16 // groups carrying payload
	payload uint32 // payload bytes laid down
	first   uint64 // first WAL seq the extent covers
	active  bool
}

// Store is the Track B backend. Not safe for concurrent use of a
// single method call, but every method locks, so callers can share
// it.
type Store struct {
	mu sync.Mutex

	f       StoreFile
	wal     *sqlo1.WAL
	sb      *Superblock
	grid    *Grid
	dir     *Directory
	rd      *IndexReader
	closeFn func() error

	// Linear hash state, committed as hash_epoch at checkpoint.
	level   uint8
	split   uint64
	entries uint64
	garbage uint64
	hw      int64

	// garbageExt is the per-extent side of the garbage counter, the
	// compaction debt feed (doc 03 section 9). It is advisory runtime
	// state: reopen starts it empty and supersessions rebuild it, so
	// correctness never depends on it.
	garbageExt map[uint64]uint64

	// Write amplification telemetry, runtime-only like garbageExt.
	// logicalBytes counts encoded record bytes the store accepted
	// (batch puts, replayed puts, gen records); dataBytes counts
	// physical vlog group and blob run writes including the open
	// group's tear-safe rewrites; indexBytes counts chunk, directory,
	// and allocmap group images; relocatedBytes and compactions come
	// from the compactor.
	logicalBytes   uint64
	dataBytes      uint64
	indexBytes     uint64
	relocatedBytes uint64
	compactions    uint64

	// Pressure gauge parameters (pressure.go): ckptPolicy.Bytes is
	// the WAL rung's denominator, and maxBytes caps the file for the
	// free-extent rung, 0 meaning unbounded, which reads that rung's
	// signals as zero.
	ckptPolicy CheckpointPolicy
	maxBytes   int64

	// dirty holds every bucket chain touched since the last durable
	// checkpoint; RAM is authoritative for these. pending maps the
	// in-flight batch's vlog positions to encoded records, covering
	// the open group builder that is not yet readable from the file.
	dirty   map[uint64][]*Chunk
	pending map[Pos][]byte

	vlog streamCursor
	blob streamCursor
	idx  streamCursor
	dirp streamCursor
	am   streamCursor

	// blobSlab mirrors the active blob extent so PlaceBlob can lay
	// out runs before the bytes hit the file.
	blobSlab []byte

	// Open group builder, only non-nil inside a batch.
	gb    *GroupBuilder
	gbExt uint64
	gbGrp uint16

	// pendingDir carries the FlushIndex root to Snapshot.
	pendingDir FullPtr

	// nowMS is the expiry clock, injectable in tests.
	nowMS func() int64

	// ckptCrash threads a step failpoint into the Checkpointer.
	ckptCrash func(step int) error

	// broken poisons the store after a half-applied batch: the WAL
	// has the truth, RAM does not, and only a reopen replays it.
	broken error
}

var (
	_ sqlo1.Store      = (*Store)(nil)
	_ CheckpointSource = (*Store)(nil)
)

func wallMS() int64 { return time.Now().UnixMilli() }

// CreateStore makes a fresh single-file store at path with its WAL
// sidecar next to it.
func CreateStore(path string, walSegSize int64) (*Store, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return nil, err
	}
	s, err := CreateStoreOn(f, sqlo1.WALPath(path), walSegSize)
	if err != nil {
		f.Close()
		return nil, err
	}
	s.closeFn = f.Close
	return s, nil
}

// CreateStoreOn initializes a store on a caller-owned file, which the
// crash harness uses to slip a FaultFile underneath. The caller keeps
// ownership of f; Close only closes the WAL.
func CreateStoreOn(f StoreFile, walPath string, walSegSize int64) (*Store, error) {
	sb, err := NewSuperblock()
	if err != nil {
		return nil, err
	}
	sb.ExtentCount = storeInitialExtents
	if err := f.Truncate(int64(sb.ExtentCount) * int64(sb.ExtentSize)); err != nil {
		return nil, err
	}
	if err := InitSuperblocks(f, sb); err != nil {
		return nil, err
	}
	w, err := sqlo1.OpenWAL(walPath, sb.WALDBID(), walSegSize)
	if err != nil {
		return nil, err
	}
	base, err := NewChunk(0, 0)
	if err != nil {
		w.Close()
		return nil, err
	}
	s := &Store{
		f:          f,
		wal:        w,
		sb:         sb,
		grid:       NewGrid(sb.ExtentCount),
		dir:        NewDirectory(FullPtr{}),
		dirty:      map[uint64][]*Chunk{0: {base}},
		pending:    map[Pos][]byte{},
		nowMS:      wallMS,
		ckptPolicy: DefaultCheckpointPolicy(),
	}
	s.initCursors()
	s.rd = &IndexReader{Dir: s.dir, Groups: fileGroups{s.f, s.sb.ExtentSize}, Blob: s.readBlobPos}
	return s, nil
}

// OpenStore opens an existing store, running recovery: pick the
// surviving superblock, load the grid and directory it references,
// then replay the WAL tail through the normal write path.
func OpenStore(path string, walSegSize int64) (*Store, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	s, err := OpenStoreOn(f, sqlo1.WALPath(path), walSegSize)
	if err != nil {
		f.Close()
		return nil, err
	}
	s.closeFn = f.Close
	return s, nil
}

// OpenStoreOn is OpenStore on a caller-owned file.
func OpenStoreOn(f StoreFile, walPath string, walSegSize int64) (*Store, error) {
	var tail replayTail
	rec, err := Recover(f, walPath, walSegSize, &tail)
	if err != nil {
		return nil, err
	}
	s, err := restoreStore(f, rec)
	if err != nil {
		rec.WAL.Close()
		return nil, err
	}
	// Re-apply the buffered tail through the normal write path with
	// WAL emission suppressed: those frames are already durable, only
	// the RAM index and fresh vlog copies need rebuilding. Records
	// re-drained this way abandon their pre-crash extents; the old
	// copies are garbage until compaction.
	for _, op := range tail.ops {
		switch {
		case op.mark:
			s.hw = op.markSeq
		case op.del:
			err = s.applyDel(op.key)
		case op.genbump:
			err = s.applyGenbump(op.key, op.newgen)
		default:
			err = s.applyPut(op.rec)
		}
		if err != nil {
			rec.WAL.Close()
			return nil, fmt.Errorf("sqlo1b: replay re-apply: %w", err)
		}
	}
	if err := s.finishApply(); err != nil {
		rec.WAL.Close()
		return nil, err
	}
	return s, nil
}

// restoreStore rebuilds the RAM state the surviving superblock
// describes, before any tail re-apply.
func restoreStore(f StoreFile, rec *Recovery) (*Store, error) {
	sb := rec.Super
	grid, err := loadStoreGrid(f, sb)
	if err != nil {
		return nil, err
	}
	if err := rec.RestoreGrid(grid); err != nil {
		return nil, err
	}
	split, level := UnpackHashEpoch(sb.HashEpoch)
	s := &Store{
		f:          f,
		wal:        rec.WAL,
		sb:         sb,
		grid:       grid,
		level:      level,
		split:      split,
		entries:    sb.RecordCount,
		garbage:    sb.GarbageBytes,
		hw:         sb.HighWater,
		dirty:      map[uint64][]*Chunk{},
		pending:    map[Pos][]byte{},
		nowMS:      wallMS,
		ckptPolicy: DefaultCheckpointPolicy(),
	}
	s.initCursors()
	if sb.DirRoot == (FullPtr{}) {
		// Never checkpointed: the superblock is the creation image and
		// the replayed tail rebuilds the whole index from bucket 0.
		base, err := NewChunk(0, 0)
		if err != nil {
			return nil, err
		}
		s.dir = NewDirectory(FullPtr{})
		s.dirty[0] = []*Chunk{base}
	} else {
		d, err := loadStoreDirectory(f, sb)
		if err != nil {
			return nil, err
		}
		s.dir = d
	}
	// Replayed seals can reference extents past the committed count;
	// RestoreGrid grew the grid for them, so follow with the file and
	// the RAM count. Shrinking is safe too: nothing the survivor
	// references lives past its extent_count.
	if g := grid.ExtentCount(); g > sb.ExtentCount {
		sb.ExtentCount = g
	}
	if err := f.Truncate(int64(sb.ExtentCount) * int64(sb.ExtentSize)); err != nil {
		return nil, err
	}
	s.rd = &IndexReader{Dir: s.dir, Groups: fileGroups{s.f, s.sb.ExtentSize}, Blob: s.readBlobPos}
	return s, nil
}

func (s *Store) initCursors() {
	s.vlog = streamCursor{kind: KindVlog, stream: recStream}
	s.blob = streamCursor{kind: KindVlog, stream: blobStream, eflags: EFlagBlob}
	s.idx = streamCursor{kind: KindIndex}
	s.dirp = streamCursor{kind: KindDirectory}
	s.am = streamCursor{kind: KindAllocmap}
}

// tailOp is one buffered data frame from the recovery replay.
type tailOp struct {
	del     bool
	mark    bool
	genbump bool
	markSeq int64
	newgen  uint32
	key     []byte
	rec     *Record
}

// replayTail buffers decoded data frames during Recover. Buffering
// (instead of applying inline) lets the grid finish loading before
// re-application allocates extents.
type replayTail struct {
	ops []tailOp
}

func (t *replayTail) ApplyData(fr sqlo1.WALFrame) error {
	switch fr.Op {
	case sqlo1.WALOpPut:
		rec, err := DecodePutPayload(fr.Payload)
		if err != nil {
			return err
		}
		seq, isMark, err := MarkSeq(rec)
		if err != nil {
			return err
		}
		if isMark {
			t.ops = append(t.ops, tailOp{mark: true, markSeq: seq})
			return nil
		}
		t.ops = append(t.ops, tailOp{rec: cloneRecord(rec)})
	case sqlo1.WALOpDel:
		key, err := DecodeDelPayload(fr.Payload)
		if err != nil {
			return err
		}
		t.ops = append(t.ops, tailOp{del: true, key: bytes.Clone(key)})
	case sqlo1.WALOpGenbump:
		key, newgen, err := DecodeGenbumpPayload(fr.Payload)
		if err != nil {
			return err
		}
		t.ops = append(t.ops, tailOp{genbump: true, key: bytes.Clone(key), newgen: newgen})
	default:
		return fmt.Errorf("sqlo1b: replayed data op %d, this store emits only PUT, DEL, and GENBUMP", fr.Op)
	}
	return nil
}

func cloneRecord(r *Record) *Record {
	c := *r
	c.Key = bytes.Clone(r.Key)
	c.Value = bytes.Clone(r.Value)
	return &c
}

// loadStoreGrid rebuilds the grid from the committed allocmap
// snapshot, or fresh when no checkpoint ever committed one.
func loadStoreGrid(f StoreFile, sb *Superblock) (*Grid, error) {
	if sb.AllocmapRoot == (FullPtr{}) {
		return NewGrid(sb.ExtentCount), nil
	}
	bitmap, err := readSnapshot(f, sb, sb.AllocmapRoot, (sb.ExtentCount+7)/8)
	if err != nil {
		return nil, fmt.Errorf("sqlo1b: allocmap snapshot: %w", err)
	}
	return LoadGrid(bitmap, sb.ExtentCount)
}

// loadStoreDirectory rebuilds the resident directory from the
// committed root, verifying every page against its pointer.
func loadStoreDirectory(f StoreFile, sb *Superblock) (*Directory, error) {
	split, level := UnpackHashEpoch(sb.HashEpoch)
	dirLen := NumBuckets(level, split)
	groups := fileGroups{f, sb.ExtentSize}
	pos := Pos(sb.DirRoot.Pos)
	raw, err := groups.ReadGroup(pos.Extent(), pos.Group())
	if err != nil {
		return nil, err
	}
	if err := sb.DirRoot.Verify(raw); err != nil {
		return nil, fmt.Errorf("sqlo1b: directory root: %w", err)
	}
	n := DirPages(dirLen) * dirEntrySize
	return LoadDirectory(raw[:n], dirLen, func(pp FullPtr) ([]byte, error) {
		p := Pos(pp.Pos)
		return groups.ReadGroup(p.Extent(), p.Group())
	})
}

// readSnapshot reads a paged snapshot laid out by Snapshot: a root
// group of page pointers, then dataLen bytes across whole groups.
func readSnapshot(f StoreFile, sb *Superblock, root FullPtr, dataLen uint64) ([]byte, error) {
	groups := fileGroups{f, sb.ExtentSize}
	pos := Pos(root.Pos)
	raw, err := groups.ReadGroup(pos.Extent(), pos.Group())
	if err != nil {
		return nil, err
	}
	if err := root.Verify(raw); err != nil {
		return nil, err
	}
	pages := (dataLen + GroupSize - 1) / GroupSize
	if pages*dirEntrySize > GroupSize {
		return nil, fmt.Errorf("sqlo1b: snapshot of %d pages does not fit one root group", pages)
	}
	out := make([]byte, 0, dataLen)
	for i := range pages {
		pp := getPtr(raw[i*dirEntrySize:])
		pg, err := groups.ReadGroup(Pos(pp.Pos).Extent(), Pos(pp.Pos).Group())
		if err != nil {
			return nil, err
		}
		if err := pp.Verify(pg); err != nil {
			return nil, fmt.Errorf("sqlo1b: snapshot page %d: %w", i, err)
		}
		take := min(dataLen-uint64(len(out)), GroupSize)
		out = append(out, pg[:take]...)
	}
	return out, nil
}

// Close flushes and closes the WAL, and the data file when the store
// opened it. Unflushed seal frames may be lost, which is safe: any
// extent whose seal frame vanishes is either covered by a committed
// allocmap or referenced by nothing durable.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.wal.Flush()
	if cerr := s.wal.Close(); err == nil {
		err = cerr
	}
	if s.closeFn != nil {
		if cerr := s.closeFn(); err == nil {
			err = cerr
		}
	}
	return err
}

// Get returns the record for key, or sqlo1.ErrNotFound for a miss or
// an expired record (mirroring the Track A gate).
func (s *Store) Get(ctx context.Context, key []byte) (sqlo1.Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.broken != nil {
		return sqlo1.Record{}, s.broken
	}
	if err := ctx.Err(); err != nil {
		return sqlo1.Record{}, err
	}
	rec, err := s.lookup(key)
	if err != nil {
		return sqlo1.Record{}, err
	}
	if rec == nil || rec.RType == RecMeta || s.expiredRec(rec) {
		return sqlo1.Record{}, sqlo1.ErrNotFound
	}
	return seamOut(rec), nil
}

// BatchGet is Get in a loop under one lock; a zero Record (nil Key)
// marks a miss.
func (s *Store) BatchGet(ctx context.Context, keys [][]byte) ([]sqlo1.Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.broken != nil {
		return nil, s.broken
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	out := make([]sqlo1.Record, len(keys))
	for i, k := range keys {
		rec, err := s.lookup(k)
		if err != nil {
			return nil, err
		}
		if rec == nil || rec.RType == RecMeta || s.expiredRec(rec) {
			continue
		}
		out[i] = seamOut(rec)
	}
	return out, nil
}

// lookup resolves key through the dirty chains first, then the cold
// index. A nil record is a miss.
func (s *Store) lookup(key []byte) (*Record, error) {
	rec, _, err := s.lookupPos(key)
	return rec, err
}

// lookupPos is lookup keeping the position the index entry names,
// which is what compaction's liveness probe compares against.
func (s *Store) lookupPos(key []byte) (*Record, Pos, error) {
	h := KeyHash(key)
	bucket := BucketOf(PlacementBits(h), s.level, s.split)
	if chain, ok := s.dirty[bucket]; ok {
		_, _, rec, pos, found, err := s.findInChain(chain, Fingerprint(h), key)
		if err != nil || !found {
			return nil, 0, err
		}
		return rec, pos, nil
	}
	rec, pos, err := s.rd.Lookup(key, PackHashEpoch(s.split, s.level))
	return rec, pos, err
}

func (s *Store) expiredRec(rec *Record) bool {
	return rec.HasExpiry() && int64(rec.ExpireMS) <= s.nowMS()
}

func seamOut(rec *Record) sqlo1.Record {
	out := sqlo1.Record{Key: rec.Key, Value: rec.Value, Gen: rec.Rootgen}
	if rec.HasExpiry() {
		out.ExpireMs = int64(rec.ExpireMS)
	}
	return out
}

// seamRecord maps a flat seam record onto the vlog envelope. Only
// segment records carry a rootgen (doc 03 section 6), so a nonzero
// Gen must arrive on a 16-byte subkey minted by the per-type layer;
// the envelope validation at encode enforces it.
func seamRecord(r *sqlo1.Record) *Record {
	rec := &Record{Key: r.Key, Value: r.Value}
	if r.Gen > 0 {
		rec.RType = RecSeg
		rec.RFlags |= RFlagRootgen
		rec.Rootgen = r.Gen
	} else {
		rec.RType = RecString
	}
	if r.ExpireMs > 0 {
		rec.RFlags |= RFlagExpiry
		rec.ExpireMS = uint64(r.ExpireMs)
	}
	return rec
}

// plannedFrame is one WAL frame of a batch, encoded before anything
// is emitted so a bad op rejects the whole batch cleanly. pos is the
// vlog position the placement pass assigned to a put.
type plannedFrame struct {
	op  uint8
	pay []byte
	del bool
	key []byte
	rec *Record
	pos Pos
}

// placementRooth groups a put for vlog placement: records living under
// a subkey sort on their rooth so one collection's records land next
// to each other in the extent (scan locality, doc 04 section 7), and
// everything else sorts on 0, packing plain records by size class
// alone.
func placementRooth(rec *Record) uint64 {
	if (rec.RType == RecSeg || rec.RType == RecFence) && len(rec.Key) == SubkeySize {
		return binary.LittleEndian.Uint64(rec.Key)
	}
	return 0
}

// placementClass is the power-of-two size class of the encoded record;
// packing neighbors of one class caps a group's tail waste near one
// class step instead of one max-size record.
func placementClass(rec *Record) int {
	return bits.Len(uint(rec.EncodedLen()))
}

// ApplyBatch drains one batch: encode and validate every op, emit the
// data frames plus the high-water mark frame, sync the WAL (the
// durability point), then apply to vlog groups and index chunks. A
// batch at or below the high-water mark is the exactly-once no-op the
// seam requires. No op memory is retained: payloads and group buffers
// are copies, chunks hold positions.
//
// Placement and apply are separate passes. Puts place into vlog groups
// in packing order, sorted by collection then size class (doc 04
// section 7); index updates then apply in batch order, so op semantics
// never depend on where a record landed. The open group carries across
// batches and only closes when full, which is what keeps a run of
// small drain cycles from padding out one group each.
//
// WAL framing follows doc 06 rule W2 degenerately at this layer: every
// seam record is either a segment post-image (Gen above zero) or an
// inline-mode root (a plain record is its own root), and W2 frames
// both in full. Root-frame elision and the W3 replay reconciliation
// only apply to RecRoot images, which the per-type layer introduces;
// none cross the seam yet.
func (s *Store) ApplyBatch(ctx context.Context, b *sqlo1.DrainBatch) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.broken != nil {
		return s.broken
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if b.Seq <= s.hw {
		return nil
	}
	frames := make([]plannedFrame, 0, len(b.Ops)+1)
	for i := range b.Ops {
		op := &b.Ops[i]
		if op.Del {
			pay, err := EncodeDelPayload(op.Rec.Key)
			if err != nil {
				return fmt.Errorf("sqlo1b: batch %d op %d: %w", b.Seq, i, err)
			}
			frames = append(frames, plannedFrame{op: sqlo1.WALOpDel, pay: pay, del: true, key: op.Rec.Key})
			continue
		}
		rec := seamRecord(&op.Rec)
		pay, err := EncodePutPayload(rec)
		if err != nil {
			return fmt.Errorf("sqlo1b: batch %d op %d: %w", b.Seq, i, err)
		}
		frames = append(frames, plannedFrame{op: sqlo1.WALOpPut, pay: pay, rec: rec})
	}
	mark, err := EncodeMarkPayload(b.Seq)
	if err != nil {
		return err
	}
	frames = append(frames, plannedFrame{op: sqlo1.WALOpPut, pay: mark})
	for _, fr := range frames {
		if _, err := s.wal.Append(0, fr.op, 0, fr.pay); err != nil {
			return err
		}
	}
	if err := s.wal.Flush(); err != nil {
		return err
	}
	if err := s.wal.Sync(); err != nil {
		return err
	}
	// Durability point. A failure past here leaves RAM half-applied
	// with no way back inside this process, so it poisons the store;
	// reopening replays the WAL into a clean state.
	puts := make([]int, 0, len(frames))
	for i := range frames {
		if frames[i].rec != nil {
			puts = append(puts, i)
		}
	}
	slices.SortFunc(puts, func(a, b int) int {
		ra, rb := frames[a].rec, frames[b].rec
		if c := cmp.Compare(placementRooth(ra), placementRooth(rb)); c != 0 {
			return c
		}
		if c := cmp.Compare(placementClass(ra), placementClass(rb)); c != 0 {
			return c
		}
		return cmp.Compare(a, b)
	})
	for _, i := range puts {
		fr := &frames[i]
		enc, err := fr.rec.Encode()
		if err != nil {
			s.broken = err
			return err
		}
		s.logicalBytes += uint64(len(enc))
		if fr.pos, err = s.appendVlog(enc); err != nil {
			s.broken = err
			return err
		}
	}
	for i := range frames {
		fr := &frames[i]
		var err error
		switch {
		case fr.del:
			err = s.applyDel(fr.key)
		case fr.rec != nil:
			err = s.indexPut(fr.rec, fr.pos)
		}
		if err != nil {
			s.broken = err
			return err
		}
	}
	if err := s.finishApply(); err != nil {
		s.broken = err
		return err
	}
	s.hw = b.Seq
	return nil
}

// applyPut writes one record to the vlog and upserts its index entry;
// the recovery replay path uses it op-at-a-time. ApplyBatch runs the
// two halves as separate passes so placement order and apply order can
// differ.
func (s *Store) applyPut(rec *Record) error {
	enc, err := rec.Encode()
	if err != nil {
		return err
	}
	s.logicalBytes += uint64(len(enc))
	pos, err := s.appendVlog(enc)
	if err != nil {
		return err
	}
	return s.indexPut(rec, pos)
}

// indexPut upserts the index entry for a record already placed at pos.
func (s *Store) indexPut(rec *Record, pos Pos) error {
	h := KeyHash(rec.Key)
	bucket := BucketOf(PlacementBits(h), s.level, s.split)
	chain, err := s.mutableChain(bucket)
	if err != nil {
		return err
	}
	fp := Fingerprint(h)
	ci, ei, old, oldPos, found, err := s.findInChain(chain, fp, rec.Key)
	if err != nil {
		return err
	}
	if found {
		meta, err := entryMetaFor(rec, h, chain[ci].WindowBase())
		if err != nil {
			return err
		}
		if err := chain[ci].SetEntry(ei, meta, uint64(pos)); err != nil {
			return err
		}
		s.noteGarbage(oldPos, old)
		return nil
	}
	return s.insertNew(bucket, chain, fp, rec, h, pos)
}

// insertNew appends a fresh entry to bucket's chain and runs any due
// splits. The caller has already ruled out an existing entry.
func (s *Store) insertNew(bucket uint64, chain []*Chunk, fp uint16, rec *Record, h uint64, pos Pos) error {
	last := chain[len(chain)-1]
	if last.Count() == ChunkCap {
		// The full final chunk cannot take a chain pointer, so shift
		// its last entry into the fresh overflow chunk before linking
		// (the write-time shape from the packing slice): non-final
		// chunks stay at or below ChunkChainCap and SetChain at flush
		// can never fail.
		nc, err := NewChunk(bucket, chain[0].WindowBase())
		if err != nil {
			return err
		}
		mf, mm, mv := last.EntryAt(ChunkCap - 1)
		if err := last.RemoveEntry(ChunkCap - 1); err != nil {
			return err
		}
		if err := nc.InsertEntry(mf, mm, mv); err != nil {
			return err
		}
		chain = append(chain, nc)
		s.dirty[bucket] = chain
		last = nc
	}
	meta, err := entryMetaFor(rec, h, last.WindowBase())
	if err != nil {
		return err
	}
	if err := last.InsertEntry(fp, meta, uint64(pos)); err != nil {
		return err
	}
	s.entries++
	for ShouldSplit(s.entries, NumBuckets(s.level, s.split)) {
		if err := s.splitOne(); err != nil {
			return err
		}
	}
	return nil
}

// applyGenbump upserts a generation record, keeping generations
// monotonic: a bump at or below the recorded one is a no-op, which
// makes WAL replay idempotent without a high-water mark.
func (s *Store) applyGenbump(key []byte, newgen uint32) error {
	h := KeyHash(key)
	bucket := BucketOf(PlacementBits(h), s.level, s.split)
	chain, err := s.mutableChain(bucket)
	if err != nil {
		return err
	}
	fp := Fingerprint(h)
	ci, ei, old, oldPos, found, err := s.findInChain(chain, fp, key)
	if err != nil {
		return err
	}
	if found {
		oldgen, err := genOf(old)
		if err != nil {
			return err
		}
		if newgen <= oldgen {
			return nil
		}
	}
	rec := genRecord(key, newgen)
	enc, err := rec.Encode()
	if err != nil {
		return err
	}
	s.logicalBytes += uint64(len(enc))
	pos, err := s.appendVlog(enc)
	if err != nil {
		return err
	}
	if found {
		meta, err := entryMetaFor(rec, h, chain[ci].WindowBase())
		if err != nil {
			return err
		}
		if err := chain[ci].SetEntry(ei, meta, uint64(pos)); err != nil {
			return err
		}
		s.noteGarbage(oldPos, old)
		return nil
	}
	return s.insertNew(bucket, chain, fp, rec, h, pos)
}

// applyDel removes key's index entry. Deleting an absent key is a
// no-op at the seam. No vlog tombstone in v0: the WAL DEL frame and
// the checkpointed index carry the fact.
func (s *Store) applyDel(key []byte) error {
	h := KeyHash(key)
	bucket := BucketOf(PlacementBits(h), s.level, s.split)
	chain, err := s.mutableChain(bucket)
	if err != nil {
		return err
	}
	ci, ei, old, oldPos, found, err := s.findInChain(chain, Fingerprint(h), key)
	if err != nil {
		return err
	}
	if !found {
		return nil
	}
	if err := chain[ci].RemoveEntry(ei); err != nil {
		return err
	}
	s.entries--
	s.noteGarbage(oldPos, old)
	return nil
}

// finishApply write-throughs the open vlog group and drops the pending
// map: every position is now resolvable from the file. The group stays
// open, so the next batch keeps filling it and rewrites a fuller image
// in place; only a full group closes for good (appendVlog does it).
func (s *Store) finishApply() error {
	if err := s.flushBatchGroup(); err != nil {
		return err
	}
	clear(s.pending)
	return nil
}

// GenBump durably bumps rooth's generation: the O(1) side of
// deleting or type-overwriting a collection (doc 03 section 6.3).
// Segments minted under an older generation stay bytes on disk but
// fail RootLive, and compaction drops them at B3. The frame needs no
// high-water mark because the apply is monotonic and replaying it is
// a no-op.
func (s *Store) GenBump(ctx context.Context, rooth uint64, newgen uint32) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.broken != nil {
		return s.broken
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	key := GenKey(rooth)
	pay, err := EncodeGenbumpPayload(key, newgen)
	if err != nil {
		return err
	}
	if _, err := s.wal.Append(0, sqlo1.WALOpGenbump, 0, pay); err != nil {
		return err
	}
	if err := s.wal.Flush(); err != nil {
		return err
	}
	if err := s.wal.Sync(); err != nil {
		return err
	}
	// Durability point, same discipline as ApplyBatch: a failure past
	// here poisons the store and reopening replays into a clean state.
	if err := s.applyGenbump(key, newgen); err != nil {
		s.broken = err
		return err
	}
	if err := s.finishApply(); err != nil {
		s.broken = err
		return err
	}
	return nil
}

// RootLive is the liveness probe compaction runs on segment records
// (doc 04 section 10): a segment is dead only when a durable bump
// went past its rootgen. A rooth with no generation record was never
// bumped, so creation costs nothing.
func (s *Store) RootLive(rooth uint64, rootgen uint32) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.broken != nil {
		return false, s.broken
	}
	return s.rootLive(rooth, rootgen)
}

// rootLive is RootLive's unlocked core, shared with compaction.
func (s *Store) rootLive(rooth uint64, rootgen uint32) (bool, error) {
	rec, err := s.lookup(GenKey(rooth))
	if err != nil {
		return false, err
	}
	if rec == nil {
		return true, nil
	}
	g, err := genOf(rec)
	if err != nil {
		return false, err
	}
	return rootgen >= g, nil
}

// entryMetaFor packs the chunk entry meta for a record landing in a
// chain with the given window base. Every expiring record sits in
// the near class for now; the WATT slice assigns real classes.
func entryMetaFor(rec *Record, h uint64, base uint8) (uint16, error) {
	cls := uint8(ExpClassNone)
	if rec.HasExpiry() {
		cls = ExpClassNear
	}
	m, err := MakeEntryMeta(rec.RType, cls, rec.RType == RecRoot)
	if err != nil {
		return 0, err
	}
	w, err := SplitWindow(h, base)
	if err != nil {
		return 0, err
	}
	return MetaWithWindow(m, w)
}

// findInChain resolves fingerprint candidates in chain order and
// returns the entry whose record key matches, along with the vlog
// position its vptr names.
func (s *Store) findInChain(chain []*Chunk, fp uint16, key []byte) (ci, ei int, rec *Record, pos Pos, found bool, err error) {
	for i, c := range chain {
		var herr error
		c.Probe(fp, func(j int, meta uint16, vptr uint64) bool {
			r, e := s.resolveAt(Pos(vptr))
			if e != nil {
				herr = e
				return false
			}
			if bytes.Equal(r.Key, key) {
				ci, ei, rec, pos, found = i, j, r, Pos(vptr), true
				return false
			}
			return true
		})
		if herr != nil {
			return 0, 0, nil, 0, false, herr
		}
		if found {
			return ci, ei, rec, pos, true, nil
		}
	}
	return 0, 0, nil, 0, false, nil
}

// noteGarbage credits a superseded or deleted record's bytes to the
// extent holding its dead copy. The total feeds the superblock; the
// per-extent side is what the compaction debt controller selects by.
func (s *Store) noteGarbage(pos Pos, rec *Record) {
	n := uint64(rec.EncodedLen())
	s.garbage += n
	if s.garbageExt == nil {
		s.garbageExt = map[uint64]uint64{}
	}
	s.garbageExt[pos.Extent()] += n
}

// ExtentGarbage reports one extent's advisory dead-byte estimate.
func (s *Store) ExtentGarbage(ext uint64) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.garbageExt[ext]
}

// resolveAt reads the record behind a vptr, checking the in-flight
// batch's not-yet-written group first.
func (s *Store) resolveAt(pos Pos) (*Record, error) {
	if enc, ok := s.pending[pos]; ok {
		return DecodeRecord(enc)
	}
	if pos.IsBlob() {
		return ReadBlob(s.f, s.sb.ExtentSize, pos)
	}
	img, err := fileGroups{s.f, s.sb.ExtentSize}.ReadGroup(pos.Extent(), pos.Group())
	if err != nil {
		return nil, err
	}
	view, err := ParseGroup(img)
	if err != nil {
		return nil, err
	}
	raw, err := view.Record(pos.Slot())
	if err != nil {
		return nil, err
	}
	return DecodeRecord(raw)
}

func (s *Store) readBlobPos(pos Pos) (*Record, error) {
	return ReadBlob(s.f, s.sb.ExtentSize, pos)
}

// mutableChain returns the bucket's chain from the dirty map, pulling
// and caching the cold image on first touch.
func (s *Store) mutableChain(bucket uint64) ([]*Chunk, error) {
	if chain, ok := s.dirty[bucket]; ok {
		return chain, nil
	}
	chain, err := s.coldChain(bucket)
	if err != nil {
		return nil, err
	}
	s.dirty[bucket] = chain
	return chain, nil
}

// coldChain reads a bucket's chunk chain off the durable index and
// returns unlinked clones the store can mutate; flush relinks them at
// their new positions.
func (s *Store) coldChain(bucket uint64) ([]*Chunk, error) {
	ptr, err := s.dir.Get(bucket)
	if err != nil {
		return nil, err
	}
	if ptr == (FullPtr{}) {
		return nil, fmt.Errorf("sqlo1b: bucket %d has no durable image and is not dirty", bucket)
	}
	img, err := s.chunkImageAt(Pos(ptr.Pos))
	if err != nil {
		return nil, err
	}
	if err := ptr.Verify(img); err != nil {
		return nil, fmt.Errorf("sqlo1b: bucket %d base chunk: %w", bucket, err)
	}
	c, err := ParseChunk(img, bucket)
	if err != nil {
		return nil, err
	}
	var chain []*Chunk
	for {
		chain = append(chain, c)
		if !c.Chained() {
			break
		}
		pos, check, err := c.ChainPtr()
		if err != nil {
			return nil, err
		}
		img, err := s.chunkImageAt(pos)
		if err != nil {
			return nil, err
		}
		if got := ChunkCheck32(img); got != check {
			return nil, fmt.Errorf("sqlo1b: bucket %d chain chunk at %s: check32 %#x, want %#x", bucket, pos, got, check)
		}
		c, err = ParseChunk(img, bucket)
		if err != nil {
			return nil, err
		}
	}
	for _, cc := range chain {
		// Only chained chunks carry a pointer in slot 41; on a full
		// final chunk that slot is live entry 41 and must survive.
		if cc.Chained() {
			cc.ClearChain()
		}
	}
	return chain, nil
}

// chunkImageAt clones the 512-byte chunk image at pos out of its
// index group.
func (s *Store) chunkImageAt(pos Pos) ([]byte, error) {
	if pos.Slot() >= chunksPerGroup {
		return nil, fmt.Errorf("sqlo1b: chunk position %s has no slot %d", pos, pos.Slot())
	}
	img, err := fileGroups{s.f, s.sb.ExtentSize}.ReadGroup(pos.Extent(), pos.Group())
	if err != nil {
		return nil, err
	}
	if len(img) != GroupSize {
		return nil, fmt.Errorf("sqlo1b: chunk group at %s is %d bytes", pos, len(img))
	}
	off := int(pos.Slot()) * ChunkSize
	return bytes.Clone(img[off : off+ChunkSize]), nil
}

// splitOne splits the bucket at the split pointer and advances it.
func (s *Store) splitOne() error {
	bucket := s.split
	chain, err := s.mutableChain(bucket)
	if err != nil {
		return err
	}
	refresh := func(vptrs []uint64) ([]uint64, error) {
		hs := make([]uint64, len(vptrs))
		for i, vp := range vptrs {
			r, err := s.resolveAt(Pos(vp))
			if err != nil {
				return nil, err
			}
			hs[i] = KeyHash(r.Key)
		}
		return hs, nil
	}
	left, right, err := SplitBucket(chain, bucket, s.level, refresh)
	if err != nil {
		return err
	}
	newBucket := bucket + uint64(1)<<s.level
	if got := s.dir.Append(FullPtr{}); got != newBucket {
		return fmt.Errorf("sqlo1b: directory appended bucket %d, split made %d", got, newBucket)
	}
	s.dirty[bucket] = left
	s.dirty[newBucket] = right
	s.level, s.split = AdvanceSplit(s.level, s.split)
	return nil
}

// Scan walks buckets in order, resolving every entry and skipping
// expired records. The cursor is the next bucket number, so a resumed
// scan may re-deliver records from the bucket it stopped in (SCAN
// semantics allow it). Linear hashing only ever moves keys to
// higher-numbered buckets, so keys present for the whole scan are
// delivered even across splits.
func (s *Store) Scan(ctx context.Context, cur sqlo1.Cursor, fn func(sqlo1.Record) bool) (sqlo1.Cursor, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.broken != nil {
		return nil, s.broken
	}
	var start uint64
	if len(cur) > 0 {
		if len(cur) != 8 {
			return nil, fmt.Errorf("sqlo1b: scan cursor of %d bytes", len(cur))
		}
		start = binary.LittleEndian.Uint64(cur)
	}
	now := s.nowMS()
	for bkt := start; bkt < NumBuckets(s.level, s.split); bkt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		chain, ok := s.dirty[bkt]
		if !ok {
			var err error
			if chain, err = s.coldChain(bkt); err != nil {
				return nil, err
			}
		}
		for _, c := range chain {
			for i := range c.Count() {
				_, _, vptr := c.EntryAt(i)
				rec, err := s.resolveAt(Pos(vptr))
				if err != nil {
					return nil, err
				}
				if rec.RType == RecMeta || (rec.HasExpiry() && int64(rec.ExpireMS) <= now) {
					continue
				}
				if !fn(seamOut(rec)) {
					out := make(sqlo1.Cursor, 8)
					binary.LittleEndian.PutUint64(out, bkt)
					return out, nil
				}
			}
		}
	}
	return nil, nil
}

// Stats reports the store's own accounting. Keys counts index
// entries, so expired-but-unreaped records and generation records are
// included until a delete or compaction removes them. Get, BatchGet,
// and Scan hide both; the filter lives at the seam rather than in
// lookup because RootLive resolves generation records through lookup.
func (s *Store) Stats() sqlo1.StoreStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return sqlo1.StoreStats{
		Keys:      int64(s.entries),
		DiskBytes: int64(s.sb.ExtentCount) * int64(s.sb.ExtentSize),
		HighWater: s.hw,
	}
}

// Checkpoint runs one doc 03 section 13 checkpoint. On success the
// dirty map empties and reads go cold; on failure RAM stays
// authoritative and the next attempt rewrites everything, leaking at
// worst the abandoned extents until compaction. Cadence is the
// runtime's job (CheckpointPolicy); nothing here self-schedules.
func (s *Store) Checkpoint() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.broken != nil {
		return s.broken
	}
	s.sb.HashEpoch = PackHashEpoch(s.split, s.level)
	ck := &Checkpointer{WAL: s.wal, File: s.f, Grid: s.grid}
	ck.SetCrashPoint(s.ckptCrash)
	next, err := ck.Run(s.sb, s)
	if err != nil {
		return err
	}
	s.sb = next
	clear(s.dirty)
	return nil
}

// Drain is checkpoint step 2. Batches already wrote their groups
// (write-through), so making them durable is one data-file sync.
func (s *Store) Drain(t uint64) error {
	return s.f.Sync()
}

// FlushIndex is checkpoint step 3: pack every dirty chain into index
// groups, link backward so each check32 covers final bytes, point the
// directory at the new base chunks, then write the directory pages
// and root. The whole directory rewrites every checkpoint in v0.
func (s *Store) FlushIndex(t uint64) error {
	if len(s.dirty) == 0 {
		s.pendingDir = s.sb.DirRoot
		return nil
	}
	buckets := make([]uint64, 0, len(s.dirty))
	for b := range s.dirty {
		buckets = append(buckets, b)
	}
	slices.Sort(buckets)
	w := &chunkWriter{s: s, bufs: map[Pos]*chunkGroupBuf{}}
	for _, bkt := range buckets {
		chain := compactChain(s.dirty[bkt])
		s.dirty[bkt] = chain
		poss := make([]Pos, len(chain))
		var err error
		for i := range chain {
			if poss[i], err = w.nextSlot(); err != nil {
				return err
			}
		}
		for i := len(chain) - 1; i > 0; i-- {
			if err := chain[i-1].SetChain(poss[i], ChunkCheck32(chain[i].Bytes())); err != nil {
				return fmt.Errorf("sqlo1b: bucket %d: %w", bkt, err)
			}
		}
		for i, c := range chain {
			if err := w.write(poss[i], c.Bytes()); err != nil {
				return err
			}
		}
		if err := s.dir.Set(bkt, MakeFullPtr(poss[0], chain[0].Bytes())); err != nil {
			return err
		}
		if err := w.settle(); err != nil {
			return err
		}
	}
	if err := w.finish(); err != nil {
		return err
	}

	pages := s.dir.Pages()
	if uint64(len(pages))*dirEntrySize > GroupSize {
		return fmt.Errorf("sqlo1b: directory of %d pages does not fit one root group, the v0 bound", len(pages))
	}
	ptrs := make([]FullPtr, len(pages))
	for i, pg := range pages {
		ext, grp, err := s.nextPageGroup(&s.dirp)
		if err != nil {
			return err
		}
		if err := s.writeGroupImage(ext, grp, pg); err != nil {
			return err
		}
		p, err := NewPos(ext, grp, 0)
		if err != nil {
			return err
		}
		ptrs[i] = MakeFullPtr(p, pg)
	}
	rootImg := make([]byte, GroupSize)
	copy(rootImg, EncodeDirRoot(ptrs))
	ext, grp, err := s.nextPageGroup(&s.dirp)
	if err != nil {
		return err
	}
	if err := s.writeGroupImage(ext, grp, rootImg); err != nil {
		return err
	}
	rp, err := NewPos(ext, grp, 0)
	if err != nil {
		return err
	}
	s.pendingDir = MakeFullPtr(rp, rootImg)
	return nil
}

// compactChain drops empty overflow chunks; the base stays even when
// empty, because the directory always points at something.
func compactChain(chain []*Chunk) []*Chunk {
	out := chain[:1]
	for _, c := range chain[1:] {
		if c.Count() > 0 {
			out = append(out, c)
		}
	}
	return out
}

// Snapshot is checkpoint step 4: page the allocmap into its stream
// and return the roots for the superblock commit. The bitmap must not
// change while its pages write, so stream activation and any extent
// roll happen before it is taken; the page writes themselves touch no
// grid state.
func (s *Store) Snapshot(t uint64) (Roots, error) {
	var bitmap []byte
	for {
		if !s.am.active {
			if err := s.allocStream(&s.am); err != nil {
				return Roots{}, err
			}
			continue
		}
		bitmap = s.grid.Allocmap()
		pages := (uint64(len(bitmap)) + GroupSize - 1) / GroupSize
		if pages*dirEntrySize > GroupSize || pages+1 > uint64(s.groupsPerExtent())-1 {
			return Roots{}, fmt.Errorf("sqlo1b: allocmap of %d pages exceeds the v0 bound", pages)
		}
		if uint64(s.groupsPerExtent()-s.am.next) < pages+1 {
			if err := s.rollStream(&s.am, nil); err != nil {
				return Roots{}, err
			}
			continue
		}
		break
	}
	ptrs := make([]FullPtr, 0, (len(bitmap)+GroupSize-1)/GroupSize)
	for off := 0; off < len(bitmap); off += GroupSize {
		img := make([]byte, GroupSize)
		copy(img, bitmap[off:min(off+GroupSize, len(bitmap))])
		ext, grp, err := s.nextPageGroup(&s.am)
		if err != nil {
			return Roots{}, err
		}
		if err := s.writeGroupImage(ext, grp, img); err != nil {
			return Roots{}, err
		}
		p, err := NewPos(ext, grp, 0)
		if err != nil {
			return Roots{}, err
		}
		ptrs = append(ptrs, MakeFullPtr(p, img))
	}
	rootImg := make([]byte, GroupSize)
	copy(rootImg, EncodeDirRoot(ptrs))
	ext, grp, err := s.nextPageGroup(&s.am)
	if err != nil {
		return Roots{}, err
	}
	if err := s.writeGroupImage(ext, grp, rootImg); err != nil {
		return Roots{}, err
	}
	rp, err := NewPos(ext, grp, 0)
	if err != nil {
		return Roots{}, err
	}
	return Roots{
		Dir:          s.pendingDir,
		Allocmap:     MakeFullPtr(rp, rootImg),
		RecordCount:  s.entries,
		GarbageBytes: s.garbage,
		HighWater:    s.hw,
	}, nil
}

// chunkWriter packs dirty chunks eight to a group in the index
// stream. Positions are handed out before bytes exist (chain linking
// needs them), so group images assemble in RAM and flush when their
// eighth slot lands; extent seals are deferred to settle, by which
// point every group of the sealed extent is on disk.
type chunkWriter struct {
	s     *Store
	bufs  map[Pos]*chunkGroupBuf // keyed by the group's slot-0 Pos
	seals []streamCursor
	cur   Pos
	slot  int
	open  bool
}

type chunkGroupBuf struct {
	img    []byte
	filled int
}

func (w *chunkWriter) nextSlot() (Pos, error) {
	if !w.open || w.slot == chunksPerGroup {
		ext, grp, err := w.s.nextChunkGroup(&w.seals)
		if err != nil {
			return 0, err
		}
		gp, err := NewPos(ext, grp, 0)
		if err != nil {
			return 0, err
		}
		w.bufs[gp] = &chunkGroupBuf{img: make([]byte, GroupSize)}
		w.cur, w.slot, w.open = gp, 0, true
	}
	p, err := NewPos(w.cur.Extent(), w.cur.Group(), uint16(w.slot))
	if err != nil {
		return 0, err
	}
	w.slot++
	return p, nil
}

func (w *chunkWriter) write(p Pos, img []byte) error {
	gp, err := NewPos(p.Extent(), p.Group(), 0)
	if err != nil {
		return err
	}
	buf := w.bufs[gp]
	if buf == nil {
		return fmt.Errorf("sqlo1b: chunk write into flushed group %s", p)
	}
	copy(buf.img[int(p.Slot())*ChunkSize:], img)
	buf.filled++
	if buf.filled == chunksPerGroup {
		delete(w.bufs, gp)
		return w.s.writeGroupImage(p.Extent(), p.Group(), buf.img)
	}
	return nil
}

// settle finalizes deferred seals, flushing any of the sealed
// extent's groups still assembling first.
func (w *chunkWriter) settle() error {
	if len(w.seals) == 0 {
		return nil
	}
	for _, c := range w.seals {
		for gp, buf := range w.bufs {
			if gp.Extent() != c.ext {
				continue
			}
			delete(w.bufs, gp)
			if err := w.s.writeGroupImage(gp.Extent(), gp.Group(), buf.img); err != nil {
				return err
			}
		}
		if err := w.s.finalizeSeal(c); err != nil {
			return err
		}
	}
	w.seals = w.seals[:0]
	return nil
}

// finish flushes every partial group (zero-padded tails are
// unreferenced) and settles remaining seals.
func (w *chunkWriter) finish() error {
	for gp, buf := range w.bufs {
		if err := w.s.writeGroupImage(gp.Extent(), gp.Group(), buf.img); err != nil {
			return err
		}
	}
	clear(w.bufs)
	return w.settle()
}

// appendVlog routes one encoded record to the batch's vlog groups or
// the blob stream and returns its position.
func (s *Store) appendVlog(enc []byte) (Pos, error) {
	if len(enc) > BlobThreshold {
		return s.appendBlobRec(enc)
	}
	if s.gb == nil || !s.gb.Fits(len(enc)) {
		if err := s.closeBatchGroup(); err != nil {
			return 0, err
		}
		if err := s.openBatchGroup(); err != nil {
			return 0, err
		}
	}
	slot, err := s.gb.Append(enc)
	if err != nil {
		return 0, err
	}
	pos, err := NewPos(s.gbExt, s.gbGrp, slot)
	if err != nil {
		return 0, err
	}
	s.pending[pos] = enc
	return pos, nil
}

// openBatchGroup starts the next vlog group. The open group carries
// across batches, write-through at each batch end; a closed group is
// on disk for good.
func (s *Store) openBatchGroup() error {
	if !s.vlog.active {
		if err := s.allocStream(&s.vlog); err != nil {
			return err
		}
	} else if s.vlog.next >= s.groupsPerExtent() {
		if err := s.rollStream(&s.vlog, nil); err != nil {
			return err
		}
	}
	capacity := GroupSize
	if s.vlog.next == 0 {
		capacity = Group0Payload
	}
	s.gb = NewGroupBuilder(capacity)
	s.gbExt, s.gbGrp = s.vlog.ext, s.vlog.next
	return nil
}

// closeBatchGroup pads and writes the open vlog group and ends it: the
// stream accounting advances and the next put starts a fresh group.
func (s *Store) closeBatchGroup() error {
	if s.gb == nil {
		return nil
	}
	img := s.gb.Close()
	if err := s.writeVlogGroup(img); err != nil {
		return err
	}
	s.vlog.next++
	s.vlog.groups++
	s.vlog.payload += uint32(len(img))
	s.gb = nil
	return nil
}

// flushBatchGroup writes the open group's current image in place
// without ending it. Rewriting settled records is tear-safe (their
// bytes are identical at identical offsets), and anything newer that a
// torn write could garble is WAL-covered and re-drains on replay.
func (s *Store) flushBatchGroup() error {
	if s.gb == nil {
		return nil
	}
	return s.writeVlogGroup(s.gb.Image())
}

func (s *Store) writeVlogGroup(img []byte) error {
	off := int64(s.gbExt)*int64(s.sb.ExtentSize) + int64(s.gbGrp)*GroupSize
	if s.gbGrp == 0 {
		off += ExtentHeaderSize
	}
	if _, err := s.f.WriteAt(img, off); err != nil {
		return fmt.Errorf("sqlo1b: vlog group %d/%d: %w", s.gbExt, s.gbGrp, err)
	}
	s.dataBytes += uint64(len(img))
	return nil
}

// appendBlobRec places one byte-addressed record in the blob stream,
// writing through immediately so ReadBlob works before the batch
// ends. One extent is the v0 size ceiling per record.
func (s *Store) appendBlobRec(enc []byte) (Pos, error) {
	gpe := int(s.groupsPerExtent())
	if BlobRunGroups(0, len(enc)) > gpe {
		return 0, fmt.Errorf("sqlo1b: %d-byte record does not fit one blob extent, the v0 bound", len(enc))
	}
	if !s.blob.active {
		if err := s.allocStream(&s.blob); err != nil {
			return 0, err
		}
		s.blobSlab = make([]byte, s.sb.ExtentSize)
	}
	start := s.blob.next
	if int(start)+BlobRunGroups(start, len(enc)) > gpe {
		if err := s.rollStream(&s.blob, nil); err != nil {
			return 0, err
		}
		s.blobSlab = make([]byte, s.sb.ExtentSize)
		start = 0
	}
	next, err := PlaceBlob(s.blobSlab, start, enc)
	if err != nil {
		return 0, err
	}
	lo := blobOffset(start)
	hi := min(int(next)*GroupSize, len(s.blobSlab))
	off := int64(s.blob.ext)*int64(s.sb.ExtentSize) + int64(lo)
	if _, err := s.f.WriteAt(s.blobSlab[lo:hi], off); err != nil {
		return 0, fmt.Errorf("sqlo1b: blob run at %d/%d: %w", s.blob.ext, start, err)
	}
	s.dataBytes += uint64(hi - lo)
	s.blob.groups += next - start
	s.blob.payload += uint32(len(enc))
	s.blob.next = next
	return NewBlobPos(s.blob.ext, start)
}

func (s *Store) groupsPerExtent() uint16 {
	return uint16(s.sb.ExtentSize / GroupSize)
}

// allocStream activates a cursor on a fresh extent, growing the file
// by whole extents when the grid is full, and stamps the open header.
func (s *Store) allocStream(c *streamCursor) error {
	ext, err := s.grid.Allocate(c.kind, c.stream)
	if err != nil {
		s.grid.Grow(storeGrowExtents)
		s.sb.ExtentCount += storeGrowExtents
		if terr := s.f.Truncate(int64(s.sb.ExtentCount) * int64(s.sb.ExtentSize)); terr != nil {
			return fmt.Errorf("sqlo1b: grow to %d extents: %w", s.sb.ExtentCount, terr)
		}
		if ext, err = s.grid.Allocate(c.kind, c.stream); err != nil {
			return err
		}
	}
	c.ext, c.groups, c.payload, c.active = ext, 0, 0, true
	c.next = 0
	if c.kind != KindVlog {
		// Chunk, directory, and allocmap groups are whole 4 KiB
		// images, so group 0's 4032-byte payload goes unused.
		c.next = 1
	}
	c.first = s.wal.LastSeq() + 1
	hdr := &ExtentHeader{Kind: c.kind, EFlags: c.eflags, FirstWALSeq: c.first}
	if _, err := s.f.WriteAt(hdr.Encode(), int64(ext)*int64(s.sb.ExtentSize)); err != nil {
		return fmt.Errorf("sqlo1b: extent %d header: %w", ext, err)
	}
	return nil
}

// rollStream seals the cursor's active extent and opens a fresh one.
// With deferred nil the seal finalizes now, which requires every
// payload group already on disk; otherwise finalization queues for
// the caller, which still owes the extent group writes.
func (s *Store) rollStream(c *streamCursor, deferred *[]streamCursor) error {
	old := *c
	if _, err := s.grid.Seal(c.kind, c.stream); err != nil {
		return err
	}
	if deferred != nil {
		*deferred = append(*deferred, old)
	} else if err := s.finalizeSeal(old); err != nil {
		return err
	}
	return s.allocStream(c)
}

// finalizeSeal writes the sealed header, checksums the full extent
// off the file, and appends the SEAL frame whose seq the header
// already names. Single-writer, so predicting LastSeq+1 is exact.
func (s *Store) finalizeSeal(c streamCursor) error {
	sealSeq := s.wal.LastSeq() + 1
	hdr := &ExtentHeader{
		Kind:        c.kind,
		EFlags:      c.eflags | EFlagSealed,
		SealSeq:     sealSeq,
		PayloadLen:  c.payload,
		GroupCount:  c.groups,
		FirstWALSeq: c.first,
	}
	if _, err := s.f.WriteAt(hdr.Encode(), int64(c.ext)*int64(s.sb.ExtentSize)); err != nil {
		return fmt.Errorf("sqlo1b: seal extent %d: %w", c.ext, err)
	}
	sum, err := ExtentChecksum(s.f, s.sb.ExtentSize, c.ext)
	if err != nil {
		return err
	}
	seq, err := s.wal.Append(0, FrameSeal, 0, SealOp{Extent: c.ext, Sum: sum, Kind: c.kind}.Encode())
	if err != nil {
		return err
	}
	if seq != sealSeq {
		return fmt.Errorf("sqlo1b: seal frame for extent %d landed at seq %d, header says %d", c.ext, seq, sealSeq)
	}
	return nil
}

// nextChunkGroup hands FlushIndex the next whole group in the index
// stream, deferring any roll's seal because the extent's bytes are
// still assembling.
func (s *Store) nextChunkGroup(deferred *[]streamCursor) (uint64, uint16, error) {
	c := &s.idx
	if !c.active {
		if err := s.allocStream(c); err != nil {
			return 0, 0, err
		}
	} else if c.next >= s.groupsPerExtent() {
		if err := s.rollStream(c, deferred); err != nil {
			return 0, 0, err
		}
	}
	g := c.next
	c.next++
	c.groups++
	c.payload += GroupSize
	return c.ext, g, nil
}

// nextPageGroup is the write-through variant for the directory and
// allocmap streams: callers write each group before asking for the
// next, so rolls seal inline.
func (s *Store) nextPageGroup(c *streamCursor) (uint64, uint16, error) {
	if !c.active {
		if err := s.allocStream(c); err != nil {
			return 0, 0, err
		}
	} else if c.next >= s.groupsPerExtent() {
		if err := s.rollStream(c, nil); err != nil {
			return 0, 0, err
		}
	}
	g := c.next
	c.next++
	c.groups++
	c.payload += GroupSize
	return c.ext, g, nil
}

func (s *Store) writeGroupImage(ext uint64, grp uint16, img []byte) error {
	if len(img) != GroupSize {
		return fmt.Errorf("sqlo1b: group image of %d bytes", len(img))
	}
	off := int64(ext)*int64(s.sb.ExtentSize) + int64(grp)*GroupSize
	if _, err := s.f.WriteAt(img, off); err != nil {
		return fmt.Errorf("sqlo1b: group %d/%d: %w", ext, grp, err)
	}
	s.indexBytes += uint64(len(img))
	return nil
}

// fileGroups serves group images straight off the data file. Group 0
// is the short payload behind the extent header; the rest are whole
// 4 KiB groups.
type fileGroups struct {
	r  io.ReaderAt
	es uint32
}

func (g fileGroups) ReadGroup(ext uint64, grp uint16) ([]byte, error) {
	off := int64(ext)*int64(g.es) + int64(grp)*GroupSize
	n := GroupSize
	if grp == 0 {
		off += ExtentHeaderSize
		n = Group0Payload
	}
	b := make([]byte, n)
	if _, err := g.r.ReadAt(b, off); err != nil {
		return nil, fmt.Errorf("sqlo1b: group %d/%d: %w", ext, grp, err)
	}
	return b, nil
}
