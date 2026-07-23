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
// stream, a blob stream, and a compaction output stream with
// independent active extents. The split is RAM-only: extent headers
// keep shard 0 and streams are rebuilt from scratch on open; the
// compact stream's extents alone carry EFlagCompressed, which IS
// durable in their headers and drives read dispatch.
const (
	recStream     uint16 = 0
	blobStream    uint16 = 1
	compactStream uint16 = 2
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
	io      *IOBridge
	closeFn func() error

	// ioBackend names the live IO backend ("ioring" or "iopool"),
	// settled once by startIO and surfaced through Stats because a
	// gate run must know which backend ran (doc 13).
	ioBackend string

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

	vlog  streamCursor
	blob  streamCursor
	cvlog streamCursor
	idx   streamCursor
	dirp  streamCursor
	am    streamCursor

	// blobSlab mirrors the active blob extent so PlaceBlob can lay
	// out runs before the bytes hit the file.
	blobSlab []byte

	// Open group builder, only non-nil inside a batch.
	gb    *GroupBuilder
	gbExt uint64
	gbGrp uint16

	// Open compaction output group builder (frame format, cgroup.go),
	// only non-nil while relocations are in flight between a
	// compaction and the next checkpoint's Drain, which force-closes
	// it because frame images are not tear-safe under rewrite.
	cgb    *CGroupBuilder
	cgbExt uint64
	cgbGrp uint16

	// extFlags is the lazy extent-eflags cache behind read dispatch:
	// whether a position's extent holds raw slotted groups or
	// compressed frames. Filled from the 64-byte header on first
	// touch and overwritten by allocStream when an extent is reused,
	// so a freed-and-reallocated extent cannot serve stale flags.
	extFlags map[uint64]uint8

	// fc memoizes decoded frame payloads across point reads, shared
	// with the IndexReader; allocStream drops an extent's entries on
	// reuse, next to the extFlags refresh.
	fc *FrameCache

	// schemeGroups counts compressed-frame groups written per scheme,
	// advisory runtime telemetry like garbageExt; the selection slice
	// grows this into the doc 04 selection histogram.
	schemeGroups [NumSchemes]uint64

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
	s.fc = NewFrameCache()
	s.startIO()
	s.rd = &IndexReader{Dir: s.dir, Groups: backendGroups{s.io, PrioFG}, Blob: s.readBlobPos, Compressed: s.extCompressed, Frames: s.fc}
	return s, nil
}

// startIO stands up the IO backend under the store, ring first with
// the pool as the silent fallback (doc 04 section 12): a ring that
// cannot set up or fails its self-test means iopool, never an error
// the caller has to route, and Stats records which backend is live
// because a gate run must know which one ran (doc 13). Runs after the
// superblock is settled because both backends need the extent size,
// and before the IndexReader because live group reads ride it.
func (s *Store) startIO() {
	comp := make(chan IOResult, storeIOComp)
	if r := s.tryRing(comp); r != nil {
		s.io = NewIOBridge(r, comp)
		s.ioBackend = "ioring"
		return
	}
	// Fresh mailbox: an abandoned ring may still post a stale
	// completion to the old one.
	comp = make(chan IOResult, storeIOComp)
	s.io = NewIOBridge(NewIOPool(s.f, s.sb.ExtentSize, storeIOWorkers, comp), comp)
	s.ioBackend = "iopool"
}

// tryRing stands the ring up over the data file and self-tests it
// before the store trusts it: one group read through the ring,
// compared byte for byte against a plain pread of the same range. Any
// refusal (not Linux, kernel too old, seccomp denying the syscalls,
// registration refused) or a wrong answer returns nil and the caller
// runs the pool. The self-test rides the production ring instance
// with the bridge router not yet started, so the completion is
// consumed here and the ring hands over clean.
func (s *Store) tryRing(comp chan IOResult) *IORing {
	if ForceIOPool {
		return nil
	}
	osf, ok := s.f.(*os.File)
	if !ok {
		// A wrapped file (the crash harness FaultFile) has no fd the
		// ring could submit against.
		return nil
	}
	r, err := NewIORing(osf, s.sb.ExtentSize, storeRingDepth, 0, comp)
	if err != nil {
		return nil
	}
	want := make([]byte, GroupSize)
	if _, err := osf.ReadAt(want, 0); err != nil {
		r.Close()
		return nil
	}
	got := make([]byte, GroupSize)
	const selfTestTag = ^uint64(0)
	r.Submit([]IOReq{{Op: OpRead, Prio: PrioFG, Ext: 0, Off: 0, Buf: got, Tag: selfTestTag}})
	select {
	case res := <-comp:
		if res.Err != nil || res.Tag != selfTestTag || !bytes.Equal(want, got) {
			r.Close()
			return nil
		}
	case <-time.After(ringSelfTestTimeout):
		// Deliberately not closed: teardown joins the reaper, which
		// may be stuck on the same completion that never came. A
		// one-time leak on a pathological box beats a hung startup.
		return nil
	}
	return r
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
	ops := truncateTail(tail.ops)
	patches, dropped, err := s.reconcileTail(ops)
	if err != nil {
		rec.WAL.Close()
		return nil, fmt.Errorf("sqlo1b: replay reconciliation: %w", err)
	}
	// Re-apply the buffered tail through the normal write path with
	// WAL emission suppressed: those frames are already durable, only
	// the RAM index and fresh vlog copies need rebuilding. Records
	// re-drained this way abandon their pre-crash extents; the old
	// copies are garbage until compaction. Ops reconciliation rolled
	// back (a structural window that lost its root frame) are skipped;
	// the plane recovers to its last rooted batch instead.
	ops = append(ops, patches...)
	for i, op := range ops {
		if dropped[i] {
			continue
		}
		switch {
		case op.mark:
			s.hw = op.markSeq
		case op.del:
			err = s.applyDel(op.key)
		case op.genbump:
			err = s.applyGenbump(op.key, op.newgen)
		case op.lease:
			err = s.applyLease(op.leaseMark)
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
	s.fc = NewFrameCache()
	s.startIO()
	s.rd = &IndexReader{Dir: s.dir, Groups: backendGroups{s.io, PrioFG}, Blob: s.readBlobPos, Compressed: s.extCompressed, Frames: s.fc}
	return s, nil
}

func (s *Store) initCursors() {
	s.vlog = streamCursor{kind: KindVlog, stream: recStream}
	s.blob = streamCursor{kind: KindVlog, stream: blobStream, eflags: EFlagBlob}
	s.cvlog = streamCursor{kind: KindVlog, stream: compactStream, eflags: EFlagCompressed}
	s.idx = streamCursor{kind: KindIndex}
	s.dirp = streamCursor{kind: KindDirectory}
	s.am = streamCursor{kind: KindAllocmap}
}

// tailOp is one buffered data frame from the recovery replay.
type tailOp struct {
	del       bool
	mark      bool
	genbump   bool
	lease     bool
	markSeq   int64
	newgen    uint32
	leaseMark uint64
	key       []byte
	rec       *Record
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
		mark, isLease, err := LeaseMark(rec)
		if err != nil {
			return err
		}
		if isLease {
			t.ops = append(t.ops, tailOp{lease: true, leaseMark: mark})
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

// truncateTail drops PUT and DEL frames past the last high-water mark.
// ApplyBatch syncs a batch's data frames and its trailing mark as one
// durability point, so data frames after the last mark belong to a
// batch whose ApplyBatch never returned: nothing above the store ever
// learned of them, and a torn multi-frame batch applied as a prefix
// can be worse than lost, because a structural batch's early frames
// (a trimmed segment, say) are only correct together with its later
// ones (the split-off segment and the root that maps it). Dropping
// the whole unacknowledged suffix lands recovery on the last
// acknowledged batch boundary. GENBUMP and mint-lease frames stay:
// GenBump and MintLease sync them standalone at command time, their
// applies are monotonic, and they reference nothing in the dropped
// puts, so a trailing genbump is a durable acknowledged delete and a
// trailing lease merely advances the mint counter.
func truncateTail(ops []tailOp) []tailOp {
	last := -1
	for i := range ops {
		if ops[i].mark {
			last = i
		}
	}
	out := ops[:0]
	for i := range ops {
		if i <= last || ops[i].genbump || ops[i].lease {
			out = append(out, ops[i])
		}
	}
	return out
}

// planeSegOp is one tail segment frame in a plane's unrooted window:
// its position in the tail, the batch it rode in, and the count and
// min_expire drift its post-image carries over its pre-image.
type planeSegOp struct {
	idx   int
	batch int
	segid uint64
	del   bool
	dn    int64
	min   int64
}

// tailPageImg is one tail image of a fence page: where it sits in the
// tail, the batch it rode in, and the payload it wrote there, nil for
// a delete. The walk collects these per page subkey so a paged root's
// fenced set can pick the image that was current at its root frame,
// and so a plane rollback can drop the page frames of the rolled-back
// batches alongside their segments.
type tailPageImg struct {
	idx   int
	batch int
	val   []byte
}

// reconcileTail is rule W3's replay side (doc 06 section 5): walk the
// truncated tail before anything applies and settle, per plane, the
// window of segment frames past the last durable root image. A window
// confined to segids the root's fence references is count-only (the
// root frame was W2-elided or lost to the batch split W1 allows), and
// the walk diffs each segment post-image against its pre-replay image
// and emits a patched root that appends after the tail, wins by
// order, lands in the vlog like any replayed put, and rides the next
// checkpoint forward. A window carrying a segment delete or an
// unfenced segid is a structural change whose root frame the crash
// took: no count repair can make the stale fence route reads to the
// relaid segments, so the plane rolls back instead, dropping its
// window ops from the batch with the first structural evidence
// onward, which recovers the plane to its last rooted batch. The walk
// runs against the committed state the checkpoint left behind, raw
// lookups with no expiry gating, so the result is a pure function of
// the data file and the tail and repeat recoveries are idempotent.
//
// Planes whose root claims RollbackRef instead settle by the other
// discipline: the root frame is the command's commit point, so every
// plane frame past the last root frame drops whole, puts and deletes
// alike, segments, auxiliary records, and fence pages. Deletes drop
// because the drain queue's deferred-root refile floats the root to
// the queue tail, so a co-command record delete can drain a batch
// ahead of its commanding root; applying such a delete would dangle
// a reference the last durable root still holds, while dropping it
// leaves at worst an orphan record the plane retire cleans, the same
// story as an un-rooted put.
func (s *Store) reconcileTail(ops []tailOp) ([]tailOp, map[int]bool, error) {
	segs := map[string]int{}          // subkey bytes -> latest known entry count
	wins := map[uint64][]planeSegOp{} // rooth -> window past its last root frame
	aux := map[uint64][]int{}         // rooth -> put frames of other subkey kinds
	rootRecs := map[string]*Record{}  // user key -> latest tail root image, nil = deleted
	rootKeys := map[uint64][]byte{}   // rooth -> user key, from tail rootkey records
	// Fence pages are neutral in the walk: never evidence, never
	// counted, always applied. The walk only remembers their tail
	// images so a paged root's fenced set can resolve each page as of
	// the root frame it patches (a later tail page image may belong to
	// a split whose own root frame the crash took, and reading it
	// would misclassify that split's window). A nil val is a tail
	// delete of the page.
	fpages := map[string][]tailPageImg{} // fence subkey bytes -> tail images, walk order
	rootAt := map[uint64]int{}           // rooth -> op index of its last full root frame
	isSeg := func(k []byte) bool {
		return len(k) == SubkeySize && k[8] == sqlo1.SubkindSeg
	}
	isFence := func(k []byte) bool {
		return len(k) == SubkeySize && k[8] == sqlo1.SubkindFence
	}
	// The remaining subkey kinds are type-namespaced auxiliaries
	// (popcount caches under strings, score runs under zsets); which
	// plane owns one resolves at settle through the durable root, so
	// the walk only remembers where their put frames sit.
	isAux := func(k []byte) bool {
		return len(k) == SubkeySize && k[8] != 0 &&
			k[8] != sqlo1.SubkindSeg && k[8] != sqlo1.SubkindFence
	}
	segPre := func(key []byte) (int, error) {
		if n, ok := segs[string(key)]; ok {
			return n, nil
		}
		rec, err := s.lookup(key)
		if err != nil || rec == nil {
			return 0, err
		}
		n, _, ok := sqlo1.SegCounts(rec.Value)
		if !ok {
			return 0, nil
		}
		return n, nil
	}
	segid := func(k []byte) uint64 {
		var seg [8]byte
		copy(seg[:7], k[9:])
		return binary.LittleEndian.Uint64(seg[:])
	}
	batch := 0
	for i := range ops {
		op := &ops[i]
		switch {
		case op.lease:
		case op.mark:
			batch++
		case op.genbump:
			// The plane retired; whatever its count was is moot, and
			// its rootkey record may soon point at a recreated key.
			delete(wins, binary.LittleEndian.Uint64(op.key))
			delete(aux, binary.LittleEndian.Uint64(op.key))
		case op.del:
			if isSeg(op.key) {
				rooth := binary.LittleEndian.Uint64(op.key)
				wins[rooth] = append(wins[rooth], planeSegOp{
					idx: i, batch: batch, segid: segid(op.key), del: true,
				})
				segs[string(op.key)] = 0
			} else if isFence(op.key) {
				fpages[string(op.key)] = append(fpages[string(op.key)], tailPageImg{idx: i, batch: batch})
			} else if isAux(op.key) {
				// An auxiliary delete is a plane record dying, not a
				// user key: neutral to reconcile settle, dropped past
				// the last root frame by rollback settle like any
				// other plane frame.
				rooth := binary.LittleEndian.Uint64(op.key)
				aux[rooth] = append(aux[rooth], i)
			} else {
				rootRecs[string(op.key)] = nil
			}
		default:
			rec := op.rec
			if rooth, ukey, ok := RootkeyRef(rec); ok {
				rootKeys[rooth] = ukey
				continue
			}
			switch {
			case rec.RType == RecFence && isFence(rec.Key):
				fpages[string(rec.Key)] = append(fpages[string(rec.Key)], tailPageImg{idx: i, batch: batch, val: rec.Value})
			case rec.RType == RecFence && isAux(rec.Key):
				// A fence page on a type-namespaced kind (the zset's
				// score fence pages): a plane record like any other
				// auxiliary, so rollback settle drops it past the last
				// root frame. Only kind-3 pages carry a reconcilable
				// root's fenced set and need the image tracking above.
				rooth := binary.LittleEndian.Uint64(rec.Key)
				aux[rooth] = append(aux[rooth], i)
			case rec.RType == RecSeg && isSeg(rec.Key):
				post, postMin, ok := sqlo1.SegCounts(rec.Value)
				if !ok {
					break // no countable header, contributes nothing
				}
				pre, err := segPre(rec.Key)
				if err != nil {
					return nil, nil, err
				}
				rooth := binary.LittleEndian.Uint64(rec.Key)
				wins[rooth] = append(wins[rooth], planeSegOp{
					idx: i, batch: batch, segid: segid(rec.Key),
					dn: int64(post - pre), min: postMin,
				})
				segs[string(rec.Key)] = post
			case rec.RType == RecSeg && isAux(rec.Key):
				rooth := binary.LittleEndian.Uint64(rec.Key)
				aux[rooth] = append(aux[rooth], i)
			case rec.RType == RecRoot:
				rootRecs[string(rec.Key)] = rec
				if rooth, ok := sqlo1.ReconcileRef(rec.Value); ok {
					// A full root frame is exact as of this point;
					// only segments past it can drift the plane.
					delete(wins, rooth)
					rootAt[rooth] = i
				} else if rooth, ok := sqlo1.RollbackRef(rec.Value); ok {
					// A rollback plane's commit point: everything the
					// frame describes landed at or before it, so the
					// window past it starts empty here.
					delete(wins, rooth)
					delete(aux, rooth)
					rootAt[rooth] = i
				}
			}
		}
	}
	rooths := make([]uint64, 0, len(wins)+len(aux))
	for rooth := range wins {
		rooths = append(rooths, rooth)
	}
	for rooth := range aux {
		if _, dup := wins[rooth]; !dup {
			rooths = append(rooths, rooth)
		}
	}
	// A rollback plane's tail can carry nothing but fence-page frames
	// past its root, so those rooths settle too; for reconcilable
	// planes a page-only tail stays what it always was, neutral, and
	// the settle loop skips them right after the rollback branch.
	pageOnly := map[uint64]bool{}
	for k := range fpages {
		rooth := binary.LittleEndian.Uint64([]byte(k))
		if _, dup := wins[rooth]; dup {
			continue
		}
		if _, dup := aux[rooth]; dup {
			continue
		}
		if !pageOnly[rooth] {
			pageOnly[rooth] = true
			rooths = append(rooths, rooth)
		}
	}
	slices.Sort(rooths)
	var patches []tailOp
	dropped := map[int]bool{}
	for _, rooth := range rooths {
		win := wins[rooth]
		ukey := rootKeys[rooth]
		if ukey == nil {
			rec, err := s.lookup(RootkeyKey(rooth))
			if err != nil {
				return nil, nil, err
			}
			if rec == nil {
				// No durable mapping: the plane's first root frame is
				// past the crash, so nothing references these
				// segments and they sit harmlessly until compaction.
				continue
			}
			ukey = bytes.Clone(rec.Value)
		}
		var root *Record
		if r, seen := rootRecs[string(ukey)]; seen {
			root = r // nil here means the tail deleted the key
		} else {
			r, err := s.lookup(ukey)
			if err != nil {
				return nil, nil, err
			}
			root = r
		}
		if root == nil || root.RType != RecRoot {
			continue
		}
		if rb, ok := sqlo1.RollbackRef(root.Value); ok {
			if rb != rooth {
				continue // stale mapping, the key was recreated
			}
			// Roll the plane back to its last root frame: the window
			// maps were cleared at every root frame, so whatever they
			// still hold sits past the last one and drops whole, puts
			// and deletes alike, per the rule above.
			for _, o := range win {
				dropped[o.idx] = true
			}
			for _, idx := range aux[rooth] {
				dropped[idx] = true
			}
			lastRoot := -1
			if at, ok := rootAt[rooth]; ok {
				lastRoot = at
			}
			for k, imgs := range fpages {
				if binary.LittleEndian.Uint64([]byte(k)) != rooth {
					continue
				}
				for _, img := range imgs {
					if img.idx > lastRoot {
						dropped[img.idx] = true
					}
				}
			}
			continue
		}
		if pageOnly[rooth] {
			continue // fence pages alone never reconcile a plane
		}
		if rr, ok := sqlo1.ReconcileRef(root.Value); !ok || rr != rooth {
			continue // stale mapping, the key was recreated
		}
		fenced := map[uint64]bool{}
		pageids, paged, perr := sqlo1.ReconcilePages(root.Value)
		if perr != nil {
			return nil, nil, fmt.Errorf("root %q under rooth %x does not decode: %w", ukey, rooth, perr)
		}
		if paged {
			// The fenced set is the union of segids across the root's
			// pages, each resolved to the image current at the root
			// frame: the latest tail image at or before rootAt when the
			// root came from the tail, else the committed record. Later
			// tail page images belong to writes past the root frame and
			// must not classify this window.
			limit, inTail := rootAt[rooth]
			for _, pid := range pageids {
				pkey := sqlo1.Subkey{Rooth: rooth, Kind: sqlo1.SubkindFence, Segid: pid}.Encode()
				var pval []byte
				found := false
				if inTail {
					for _, img := range fpages[string(pkey)] {
						if img.idx > limit {
							break
						}
						pval, found = img.val, img.val != nil
					}
				}
				if !found {
					rec, err := s.lookup(pkey)
					if err != nil {
						return nil, nil, err
					}
					if rec == nil {
						return nil, nil, fmt.Errorf("root %q under rooth %x references fence page %d, which neither the tail nor the data file holds", ukey, rooth, pid)
					}
					pval = rec.Value
				}
				ids, err := sqlo1.FencePageSegids(pval)
				if err != nil {
					return nil, nil, fmt.Errorf("root %q under rooth %x, fence page %d: %w", ukey, rooth, pid, err)
				}
				for _, id := range ids {
					fenced[id] = true
				}
			}
		} else {
			fence, ok := sqlo1.ReconcileFence(root.Value)
			if !ok {
				return nil, nil, fmt.Errorf("root %q under rooth %x does not decode", ukey, rooth)
			}
			for _, id := range fence {
				fenced[id] = true
			}
		}
		// The kept prefix ends at the batch with the first structural
		// evidence; everything from that batch on rolls back.
		keep := len(win)
		for j, o := range win {
			if o.del || !fenced[o.segid] {
				keep = j
				for keep > 0 && win[keep-1].batch == o.batch {
					keep--
				}
				break
			}
		}
		for _, o := range win[keep:] {
			dropped[o.idx] = true
		}
		if keep < len(win) {
			// A rolled-back plane drops its fence-page frames from the
			// rollback batch onward too: a page image from an un-rooted
			// split must not overwrite the committed page the surviving
			// root still references. Frames at or before the last root
			// frame stay, the root that landed depends on them.
			cutBatch := win[keep].batch
			lastRoot := -1
			if at, ok := rootAt[rooth]; ok {
				lastRoot = at
			}
			for k, imgs := range fpages {
				if binary.LittleEndian.Uint64([]byte(k)) != rooth {
					continue
				}
				for _, img := range imgs {
					if img.idx > lastRoot && img.batch >= cutBatch {
						dropped[img.idx] = true
					}
				}
			}
		}
		var dn, minExp int64
		for _, o := range win[:keep] {
			dn += o.dn
			if o.min != 0 && (minExp == 0 || o.min < minExp) {
				minExp = o.min
			}
		}
		if dn == 0 && minExp == 0 {
			continue
		}
		patched, err := sqlo1.ReconcileRoot(root.Value, dn, minExp)
		if err != nil {
			return nil, nil, fmt.Errorf("root %q under rooth %x: %w", ukey, rooth, err)
		}
		pr := *root
		pr.Key = bytes.Clone(root.Key)
		pr.Value = patched
		patches = append(patches, tailOp{rec: &pr})
	}
	return patches, dropped, nil
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
	// The bridge stops before the file closes underneath its workers.
	if s.io != nil {
		s.io.Close()
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
	out := sqlo1.Record{Key: rec.Key, Value: rec.Value, Gen: rec.Rootgen, Root: rec.RType == RecRoot}
	if rec.HasExpiry() {
		out.ExpireMs = int64(rec.ExpireMS)
	}
	return out
}

// seamRecord maps a flat seam record onto the vlog envelope. Only
// segment and fence records carry a rootgen (doc 03 section 6), so a
// nonzero Gen must arrive on a 16-byte subkey minted by the per-type
// layer, and a root's own generation lives in its payload, never in
// the envelope; either way the validation at encode enforces it, so a
// Root op with a nonzero Gen rejects the batch loudly instead of
// dropping the generation. A Fence op maps to rtype 5 and needs a
// nonzero Gen too; ApplyBatch pre-checks both Fence combinations so
// the reject is loud rather than an encode failure downstream.
func seamRecord(r *sqlo1.Record) *Record {
	rec := &Record{Key: r.Key, Value: r.Value}
	switch {
	case r.Root:
		rec.RType = RecRoot
		if r.Gen > 0 {
			rec.RFlags |= RFlagRootgen
			rec.Rootgen = r.Gen
		}
	case r.Fence:
		rec.RType = RecFence
		rec.RFlags |= RFlagRootgen
		rec.Rootgen = r.Gen
	case r.Gen > 0:
		rec.RType = RecSeg
		rec.RFlags |= RFlagRootgen
		rec.Rootgen = r.Gen
	default:
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
	op   uint8
	pay  []byte
	del  bool
	key  []byte
	rec  *Record
	bump *sqlo1.Bump
	pos  Pos
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
// WAL framing follows doc 06 rule W2: segment post-images and
// inline-mode roots frame in full, and a reconcilable root arriving
// with the seam's Delta flag (a count-only image) skips its WAL frame
// entirely, because the W3 replay reconciliation rebuilds it from the
// batch's segment frames. The elision is gated twice: the type layer
// must claim Delta, and ReconcileRef must recognize the payload, so a
// future type that claims the flag before teaching the reconciler its
// layout still frames in full. An elided root skips only the WAL; it
// places into the vlog and indexes like every other put, so
// checkpoints carry it and only the crash window needs the rebuild.
// Structural (non-Delta) reconcilable roots additionally frame a
// rootkey record ahead of themselves, the durable rooth-to-user-key
// mapping replay resolves patched roots through.
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
		if op.Rec.Delta && !op.Rec.Root {
			return fmt.Errorf("sqlo1b: batch %d op %d: delta flag on non-root record %x", b.Seq, i, op.Rec.Key)
		}
		if op.Rec.Fence && op.Rec.Root {
			return fmt.Errorf("sqlo1b: batch %d op %d: fence flag on root record %x", b.Seq, i, op.Rec.Key)
		}
		if op.Rec.Fence && op.Rec.Gen == 0 {
			return fmt.Errorf("sqlo1b: batch %d op %d: fence record %x without a generation", b.Seq, i, op.Rec.Key)
		}
		rec := seamRecord(&op.Rec)
		if rec.RType == RecRoot {
			if rooth, ok := sqlo1.ReconcileRef(rec.Value); ok {
				if op.Rec.Delta {
					// W2 elision: no WAL frame, replay rebuilds this
					// image from the segment frames it rode in with.
					frames = append(frames, plannedFrame{rec: rec})
					continue
				}
				rk := rootkeyRecord(rooth, rec.Key)
				rkPay, err := EncodePutPayload(rk)
				if err != nil {
					return fmt.Errorf("sqlo1b: batch %d op %d rootkey: %w", b.Seq, i, err)
				}
				frames = append(frames, plannedFrame{op: sqlo1.WALOpPut, pay: rkPay, rec: rk})
			} else if rooth, ok := sqlo1.RollbackRef(rec.Value); ok {
				// A rollback plane's root frame is its replay commit
				// point: never elided, Delta flag or not, because no
				// reconciliation can rebuild its fence counts from
				// segment frames. The mapping frames ahead of every
				// one so replay can resolve the plane it settles.
				rk := rootkeyRecord(rooth, rec.Key)
				rkPay, err := EncodePutPayload(rk)
				if err != nil {
					return fmt.Errorf("sqlo1b: batch %d op %d rootkey: %w", b.Seq, i, err)
				}
				frames = append(frames, plannedFrame{op: sqlo1.WALOpPut, pay: rkPay, rec: rk})
			}
		}
		pay, err := EncodePutPayload(rec)
		if err != nil {
			return fmt.Errorf("sqlo1b: batch %d op %d: %w", b.Seq, i, err)
		}
		frames = append(frames, plannedFrame{op: sqlo1.WALOpPut, pay: pay, rec: rec})
	}
	for i := range b.Bumps {
		bp := &b.Bumps[i]
		pay, err := EncodeGenbumpPayload(sqlo1.GenKey(bp.Rooth), bp.NewGen)
		if err != nil {
			return fmt.Errorf("sqlo1b: batch %d bump %d: %w", b.Seq, i, err)
		}
		frames = append(frames, plannedFrame{op: sqlo1.WALOpGenbump, pay: pay, bump: bp})
	}
	mark, err := EncodeMarkPayload(b.Seq)
	if err != nil {
		return err
	}
	frames = append(frames, plannedFrame{op: sqlo1.WALOpPut, pay: mark})
	for _, fr := range frames {
		if fr.pay == nil {
			continue // elided delta root, W2
		}
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
		case fr.bump != nil:
			err = s.applyGenbump(sqlo1.GenKey(fr.bump.Rooth), fr.bump.NewGen)
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

// applyLease upserts the mint-lease record, keeping the mark
// monotonic: a mark at or below the recorded one is a no-op, which
// makes WAL replay idempotent without a high-water mark.
func (s *Store) applyLease(mark uint64) error {
	h := KeyHash(leaseKey)
	bucket := BucketOf(PlacementBits(h), s.level, s.split)
	chain, err := s.mutableChain(bucket)
	if err != nil {
		return err
	}
	fp := Fingerprint(h)
	ci, ei, old, oldPos, found, err := s.findInChain(chain, fp, leaseKey)
	if err != nil {
		return err
	}
	if found {
		oldMark, err := leaseOf(old)
		if err != nil {
			return err
		}
		if mark <= oldMark {
			return nil
		}
	}
	rec := leaseRecord(mark)
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
	if err := s.flushCompactGroup(); err != nil {
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

var _ sqlo1.Minter = (*Store)(nil)

// MintLease durably reserves the next n rooth counters and returns
// the first (the seam Minter contract): the new mark is WAL-framed
// and synced before the range is handed out, so a restart can never
// re-issue a counter whose rooth may own durable records. The frame
// is a plain PUT of the lease record and, like GENBUMP, needs no
// high-water mark: the apply is monotonic, replaying it is a no-op,
// and replay truncation keeps it even when it trails the last marked
// batch. Counters a crash strands in a lease are abandoned; the mint
// is a bijection, so holes waste only address space.
func (s *Store) MintLease(ctx context.Context, n uint64) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.broken != nil {
		return 0, s.broken
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	start, err := s.currentLease()
	if err != nil {
		return 0, err
	}
	mark, err := sqlo1.LeaseEnd(start, n)
	if err != nil {
		return 0, err
	}
	pay, err := EncodeLeasePayload(mark)
	if err != nil {
		return 0, err
	}
	if _, err := s.wal.Append(0, sqlo1.WALOpPut, 0, pay); err != nil {
		return 0, err
	}
	if err := s.wal.Flush(); err != nil {
		return 0, err
	}
	if err := s.wal.Sync(); err != nil {
		return 0, err
	}
	// Durability point, same discipline as ApplyBatch: a failure past
	// here poisons the store and reopening replays into a clean state.
	if err := s.applyLease(mark); err != nil {
		s.broken = err
		return 0, err
	}
	if err := s.finishApply(); err != nil {
		s.broken = err
		return 0, err
	}
	return start, nil
}

// currentLease reads the recorded lease mark, zero when nothing was
// ever leased.
func (s *Store) currentLease() (uint64, error) {
	rec, err := s.lookup(leaseKey)
	if err != nil {
		return 0, err
	}
	if rec == nil {
		return 0, nil
	}
	return leaseOf(rec)
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
	img, err := backendGroups{s.io, PrioFG}.ReadGroup(pos.Extent(), pos.Group())
	if err != nil {
		return nil, err
	}
	comp, err := s.extCompressed(pos.Extent())
	if err != nil {
		return nil, err
	}
	var raw []byte
	if comp {
		view, err := s.fc.View(pos.Extent(), pos.Group(), img)
		if err != nil {
			return nil, err
		}
		raw, err = view.Record(pos.Slot())
		if err != nil {
			return nil, err
		}
	} else {
		view, err := ParseGroup(img)
		if err != nil {
			return nil, err
		}
		raw, err = view.Record(pos.Slot())
		if err != nil {
			return nil, err
		}
	}
	return DecodeRecord(raw)
}

func (s *Store) readBlobPos(pos Pos) (*Record, error) {
	return ReadBlob(s.f, s.sb.ExtentSize, pos)
}

// extCompressed reports whether an extent holds compressed frame
// groups, from the eflags cache or one 64-byte header read on first
// touch. allocStream overwrites the cache entry whenever an extent
// activates, so reuse of a freed extent cannot serve the old
// stream's flags. Zero steady-state IO: every referenced extent is
// touched once per open.
func (s *Store) extCompressed(ext uint64) (bool, error) {
	if fl, ok := s.extFlags[ext]; ok {
		return fl&EFlagCompressed != 0, nil
	}
	hb := make([]byte, ExtentHeaderSize)
	if _, err := s.f.ReadAt(hb, int64(ext)*int64(s.sb.ExtentSize)); err != nil {
		return false, fmt.Errorf("sqlo1b: extent %d header: %w", ext, err)
	}
	hdr, err := DecodeExtentHeader(hb)
	if err != nil {
		return false, err
	}
	s.noteExtFlags(ext, hdr.EFlags)
	return hdr.EFlags&EFlagCompressed != 0, nil
}

func (s *Store) noteExtFlags(ext uint64, eflags uint8) {
	if s.extFlags == nil {
		s.extFlags = map[uint64]uint8{}
	}
	s.extFlags[ext] = eflags
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
	img, err := backendGroups{s.io, PrioFG}.ReadGroup(pos.Extent(), pos.Group())
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
	st := sqlo1.StoreStats{
		Keys:      int64(s.entries),
		DiskBytes: int64(s.sb.ExtentCount) * int64(s.sb.ExtentSize),
		HighWater: s.hw,
		IOBackend: s.ioBackend,
	}
	for scheme, n := range s.schemeGroups {
		if n == 0 {
			continue
		}
		if st.SchemeGroups == nil {
			st.SchemeGroups = make([]int64, NumSchemes)
		}
		st.SchemeGroups[scheme] = int64(n)
	}
	fs := s.fc.Stats()
	st.FrameDecodes = int64(fs.Decodes)
	st.FrameDecodeBytes = int64(fs.DecodeBytes)
	st.FrameHits = int64(fs.Hits)
	return st
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
// (write-through), so making them durable is one data-file sync. The
// open compaction output group force-closes first: its frame image is
// not tear-safe under rewrite, and after this checkpoint commits the
// index durably references its positions, so it must never be
// rewritten again (the raw vlog group may stay open because settled
// records rewrite identically at identical offsets).
func (s *Store) Drain(t uint64) error {
	if err := s.closeCompactGroup(); err != nil {
		return err
	}
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

// appendCompact routes one relocated record to the compaction output
// stream's frame groups, the appendVlog mirror for gen-C extents.
// Byte-addressed records stay on the raw blob stream: blob runs are
// already one record per run and gain nothing from framing.
func (s *Store) appendCompact(enc []byte) (Pos, error) {
	if len(enc) > BlobThreshold {
		return s.appendBlobRec(enc)
	}
	var slot uint16
	var err error
	if s.cgb != nil && !s.cgb.Fits(len(enc)) {
		// Past the raw projection, try packing: the group keeps
		// accepting records while a compressed image still fits it.
		if ps, ok, perr := s.cgb.AppendPacked(enc); perr != nil {
			return 0, perr
		} else if ok {
			slot = ps
			pos, err := NewPos(s.cgbExt, s.cgbGrp, slot)
			if err != nil {
				return 0, err
			}
			s.pending[pos] = enc
			return pos, nil
		}
	}
	if s.cgb == nil || !s.cgb.Fits(len(enc)) {
		if err := s.closeCompactGroup(); err != nil {
			return 0, err
		}
		if err := s.openCompactGroup(); err != nil {
			return 0, err
		}
	}
	slot, err = s.cgb.Append(enc)
	if err != nil {
		return 0, err
	}
	pos, err := NewPos(s.cgbExt, s.cgbGrp, slot)
	if err != nil {
		return 0, err
	}
	s.pending[pos] = enc
	return pos, nil
}

// openCompactGroup starts the next compaction output group. Like the
// batch group it carries across compactions, write-through at each
// finishApply; unlike it, Drain force-closes it at checkpoint (see
// Drain).
func (s *Store) openCompactGroup() error {
	if !s.cvlog.active {
		if err := s.allocStream(&s.cvlog); err != nil {
			return err
		}
	} else if s.cvlog.next >= s.groupsPerExtent() {
		if err := s.rollStream(&s.cvlog, nil); err != nil {
			return err
		}
	}
	capacity := GroupSize
	if s.cvlog.next == 0 {
		capacity = Group0Payload
	}
	s.cgb = NewCGroupBuilder(capacity)
	s.cgbExt, s.cgbGrp = s.cvlog.ext, s.cvlog.next
	return nil
}

// closeCompactGroup writes the open frame group's final image and
// ends it, advancing the stream accounting and the per-scheme
// telemetry. Nil builder is a no-op so Drain can call it blindly.
func (s *Store) closeCompactGroup() error {
	if s.cgb == nil {
		return nil
	}
	img := s.cgb.Seal()
	if err := s.writeCompactGroup(img); err != nil {
		return err
	}
	s.cvlog.next++
	s.cvlog.groups++
	s.cvlog.payload += uint32(len(img))
	s.schemeGroups[s.cgb.Scheme()]++
	s.cgb = nil
	return nil
}

// flushCompactGroup writes the open frame group's current image in
// place without ending it, making its pending positions readable off
// the file. The rewrite is not tear-safe, which is fine only here:
// until the next checkpoint's Drain closes the group, no durable
// index references its positions, so a torn image is unreferenced
// garbage after recovery.
func (s *Store) flushCompactGroup() error {
	if s.cgb == nil {
		return nil
	}
	return s.writeCompactGroup(s.cgb.Image())
}

func (s *Store) writeCompactGroup(img []byte) error {
	off := int64(s.cgbExt)*int64(s.sb.ExtentSize) + int64(s.cgbGrp)*GroupSize
	if s.cgbGrp == 0 {
		off += ExtentHeaderSize
	}
	if _, err := s.f.WriteAt(img, off); err != nil {
		return fmt.Errorf("sqlo1b: compact group %d/%d: %w", s.cgbExt, s.cgbGrp, err)
	}
	// A packed open group flushes non-raw images that later rewrites
	// replace, so the frame cache must not keep serving the old one.
	s.fc.DropGroup(s.cgbExt, s.cgbGrp)
	s.dataBytes += uint64(len(img))
	return nil
}

// SchemeGroups reports how many compressed frame groups this store
// wrote per scheme since open, advisory runtime telemetry.
func (s *Store) SchemeGroups() [NumSchemes]uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.schemeGroups
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
	// Refresh the read-dispatch caches: a freed extent reused by a
	// different stream must not keep serving the old stream's flags,
	// and the frame cache must not keep serving its decoded groups.
	s.noteExtFlags(ext, c.eflags)
	s.fc.DropExtent(ext)
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
// 4 KiB groups. Only recovery-time readers use it now; once the store
// is up, group reads go through backendGroups so the backend seam
// sees them (R-I4).
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
