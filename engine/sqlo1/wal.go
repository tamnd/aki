package sqlo1

import (
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"slices"
	"time"
)

// The aki WAL sidecar, doc 03 section 12: framed physical-logical ops
// in a ring of large segments, group-committed on an fsync window, and
// replayed from the trim barrier until the first torn frame. This is
// the transport layer only and it is backend-independent by design:
// op payload encodings (PUT record images and friends) belong to the
// milestones that produce them, and the checkpoint protocol that moves
// the trim barrier is doc 03 section 13's slice.
//
// Torn-tail semantics: recovery trusts a frame when its fcrc verifies
// and its seq continues the chain, and the first failure ends replay.
// Later readable frames are ignored by design, because group commit
// means nothing after the first tear was ever acknowledged. The seq
// chain check is also what makes segment recycling safe: a recycled
// segment's stale remnant frames verify individually but cannot
// continue the chain, so the scan never wanders into a previous life.

const (
	// walSegmentSize is the production ring segment; tests shrink the
	// field to exercise the ring without writing gigabytes.
	walSegmentSize = 64 << 20
	// walFsyncWindow and walBatchCap are the group-commit policy: the
	// owner fdatasyncs on the earlier of the two. The timer belongs to
	// the server loop; the WAL provides Append, Flush, and Sync.
	walFsyncWindow = 2 * time.Millisecond
	walBatchCap    = 256
	// walHdrSize is the fixed frame header: flen, fcrc, seq, db_id_lo,
	// shard, op, oflags.
	walHdrSize = 28
)

// Frame ops, doc 03 section 12.2.
const (
	walOpPut     uint8 = 1
	walOpDel     uint8 = 2
	walOpPexpire uint8 = 3
	walOpGenbump uint8 = 4
	walOpSeal    uint8 = 5
	walOpCkpt    uint8 = 6
	walOpTrim    uint8 = 7
)

var (
	errWalForeign  = errors.New("sqlo1: wal frame from another database")
	errWalTooLarge = errors.New("sqlo1: wal frame larger than a segment")
	walCastagnoli  = crc32.MakeTable(crc32.Castagnoli)
)

// walFrame is one replayed frame; Payload aliases the replay buffer
// and is only valid until the callback returns.
type walFrame struct {
	Seq     uint64
	Shard   uint16
	Op      uint8
	Oflags  uint8
	Payload []byte
}

// walSeg is one ring slot's directory entry; firstSeq 0 means free.
type walSeg struct {
	off      int64
	firstSeq uint64
	lastSeq  uint64
	end      int64 // valid bytes within the segment
}

type wal struct {
	f       *os.File
	dbID    uint64
	segSize int64
	segs    []walSeg
	cur     int
	nextSeq uint64
	trimSeq uint64
	// sinceTrim gauges checkpoint lag for the backpressure ladder:
	// frame bytes appended past the trim barrier. SetTrim resets it
	// and Replay rebuilds it, so the gauge survives a reopen.
	sinceTrim int64
	// buf accumulates appended frames until Flush, which is the group
	// commit barrier: one command's frames go in one batch, so they
	// never straddle it.
	buf     []byte
	pending int
	scratch []byte
}

// openWAL opens or creates the sidecar and scans it: every segment's
// frame chain is walked to rebuild the directory, the highest live seq
// picks up the sequence, and writing resumes after the last trusted
// frame. A torn tail needs no repair; it simply is the end.
func openWAL(path string, dbID uint64, segSize int64) (*wal, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	w := &wal{f: f, dbID: dbID, segSize: segSize, nextSeq: 1}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	// A crash can leave a partial tail segment (truncate is metadata);
	// its frames are still scanned and the file is rounded back to
	// whole segments before writing resumes.
	for off := int64(0); off < st.Size(); off += segSize {
		seg := walSeg{off: off}
		if err := w.scanSegment(&seg, 0, nil); err != nil {
			f.Close()
			return nil, err
		}
		w.segs = append(w.segs, seg)
	}
	if len(w.segs) == 0 {
		if err := w.addSegment(); err != nil {
			f.Close()
			return nil, err
		}
		return w, nil
	}
	if err := f.Truncate(int64(len(w.segs)) * segSize); err != nil {
		f.Close()
		return nil, err
	}
	// The tail segment is the live one with the highest firstSeq; the
	// chain must reach it through every live segment in seq order, and
	// anything that does not connect is a previous life to recycle.
	order := w.liveOrder()
	last := uint64(0)
	w.cur = 0
	for _, i := range order {
		if last != 0 && w.segs[i].firstSeq != last+1 {
			w.segs[i] = walSeg{off: w.segs[i].off}
			continue
		}
		last = w.segs[i].lastSeq
		w.cur = i
	}
	if last != 0 {
		w.nextSeq = last + 1
	}
	return w, nil
}

func (w *wal) Close() error { return w.f.Close() }

// liveOrder returns indexes of live segments sorted by firstSeq.
func (w *wal) liveOrder() []int {
	var order []int
	for i := range w.segs {
		if w.segs[i].firstSeq != 0 {
			order = append(order, i)
		}
	}
	slices.SortFunc(order, func(a, b int) int {
		return cmp.Compare(w.segs[a].firstSeq, w.segs[b].firstSeq)
	})
	return order
}

// scanSegment walks seg's frame chain from its start: a frame is
// trusted when its bounds make sense, its fcrc verifies, its db_id
// matches, and its seq continues the chain. after > 0 additionally
// requires the first frame to continue from it (cross-segment chain
// during replay). Each trusted frame with seq > from goes to fn when
// fn is set. The walk ends at the first failure, which for the tail
// segment is exactly the torn-tail rule.
func (w *wal) scanSegment(seg *walSeg, after uint64, fn func(walFrame) error) error {
	seg.firstSeq, seg.lastSeq, seg.end = 0, 0, 0
	off := seg.off
	segEnd := seg.off + w.segSize
	var hdr [walHdrSize]byte
	prev := after
	for off+walHdrSize <= segEnd {
		if _, err := w.f.ReadAt(hdr[:4], off); err != nil {
			break
		}
		flen := int64(binary.LittleEndian.Uint32(hdr[:4]))
		if flen < walHdrSize || off+flen > segEnd {
			break
		}
		if cap(w.scratch) < int(flen) {
			w.scratch = make([]byte, flen)
		}
		b := w.scratch[:flen]
		if _, err := w.f.ReadAt(b, off); err != nil {
			break
		}
		if binary.LittleEndian.Uint32(b[4:8]) != crc32.Checksum(b[8:], walCastagnoli) {
			break
		}
		seq := binary.LittleEndian.Uint64(b[8:16])
		if prev != 0 && seq != prev+1 {
			break
		}
		if binary.LittleEndian.Uint64(b[16:24]) != w.dbID {
			// A verified frame from another database is a sidecar
			// mixup, never a tear; refusing loudly beats silently
			// treating someone else's WAL as recyclable space.
			return errWalForeign
		}
		if fn != nil && seq > w.trimSeq {
			fr := walFrame{
				Seq:     seq,
				Shard:   binary.LittleEndian.Uint16(b[24:26]),
				Op:      b[26],
				Oflags:  b[27],
				Payload: b[walHdrSize:flen],
			}
			if err := fn(fr); err != nil {
				return err
			}
		}
		if seg.firstSeq == 0 {
			seg.firstSeq = seq
		}
		seg.lastSeq = seq
		prev = seq
		off += flen
		seg.end = off - seg.off
	}
	return nil
}

// Append files one frame into the pending batch and returns its seq.
// Nothing is on disk until Flush; nothing is durable until Sync.
func (w *wal) Append(shard uint16, op, oflags uint8, payload []byte) (uint64, error) {
	flen := walHdrSize + len(payload)
	if int64(flen) > w.segSize {
		return 0, errWalTooLarge
	}
	seq := w.nextSeq
	w.nextSeq++
	n := len(w.buf)
	w.buf = append(w.buf, make([]byte, walHdrSize)...)
	w.buf = append(w.buf, payload...)
	b := w.buf[n : n+flen]
	binary.LittleEndian.PutUint32(b[0:4], uint32(flen))
	binary.LittleEndian.PutUint64(b[8:16], seq)
	binary.LittleEndian.PutUint64(b[16:24], w.dbID)
	binary.LittleEndian.PutUint16(b[24:26], shard)
	b[26] = op
	b[27] = oflags
	binary.LittleEndian.PutUint32(b[4:8], crc32.Checksum(b[8:], walCastagnoli))
	w.pending++
	w.sinceTrim += int64(flen)
	return seq, nil
}

// Flush writes the pending batch contiguously after the last frame,
// advancing the ring first when the batch does not fit the current
// segment (the whole batch moves, so one command's frames stay
// contiguous and never straddle a segment or fsync barrier).
func (w *wal) Flush() error {
	if w.pending == 0 {
		return nil
	}
	if int64(len(w.buf)) > w.segSize {
		return errWalTooLarge
	}
	seg := &w.segs[w.cur]
	if seg.off+seg.end+int64(len(w.buf)) > seg.off+w.segSize {
		if err := w.advance(); err != nil {
			return err
		}
		seg = &w.segs[w.cur]
	}
	if _, err := w.f.WriteAt(w.buf, seg.off+seg.end); err != nil {
		return err
	}
	first := binary.LittleEndian.Uint64(w.buf[8:16])
	if seg.firstSeq == 0 {
		seg.firstSeq = first
	}
	seg.lastSeq = first + uint64(w.pending) - 1
	seg.end += int64(len(w.buf))
	w.buf = w.buf[:0]
	w.pending = 0
	return nil
}

// Sync makes everything flushed durable; the fsync-window timer that
// decides when to call it lives in the server loop.
func (w *wal) Sync() error { return w.f.Sync() }

// LastSeq reports the seq of the last appended frame, zero when the
// WAL has never carried one; checkpoint freezes its target here.
func (w *wal) LastSeq() uint64 { return w.nextSeq - 1 }

// SetTrim advances the trim barrier: frames at or below seq are
// subsumed by checkpointed state, and segments wholly below it become
// recyclable the next time the ring advances. The lag gauge resets
// here; a frame appended before the trim but sequenced after it (the
// CKPT frame) goes uncounted, which under-reports by a few bytes and
// can only delay the next checkpoint, never force a spurious one.
func (w *wal) SetTrim(seq uint64) {
	w.trimSeq = seq
	w.sinceTrim = 0
}

// SinceTrim reports frame bytes appended past the trim barrier, the
// WAL rung's checkpoint-lag signal. After a reopen it covers the
// replayed tail, so a crash-looping process that never checkpoints
// still feels the pressure.
func (w *wal) SinceTrim() int64 { return w.sinceTrim }

// advance seals the current segment (a zero flen sentinel ends its
// chain) and moves writing to a recycled segment when one is wholly
// under the trim barrier, or to a fresh one appended to the file.
func (w *wal) advance() error {
	seg := &w.segs[w.cur]
	if seg.end+4 <= w.segSize {
		var z [4]byte
		if _, err := w.f.WriteAt(z[:], seg.off+seg.end); err != nil {
			return err
		}
	}
	for i := range w.segs {
		if i == w.cur {
			continue
		}
		s := &w.segs[i]
		if s.firstSeq == 0 || s.lastSeq <= w.trimSeq {
			*s = walSeg{off: s.off}
			w.cur = i
			return nil
		}
	}
	return w.addSegment()
}

func (w *wal) addSegment() error {
	off := int64(len(w.segs)) * w.segSize
	if err := w.f.Truncate(off + w.segSize); err != nil {
		return err
	}
	w.segs = append(w.segs, walSeg{off: off})
	w.cur = len(w.segs) - 1
	return nil
}

// Replay walks every trusted frame with seq above the trim barrier in
// seq order and hands it to fn; the frame's payload aliases an
// internal buffer and is only valid inside the call. Replay ends at
// the first torn frame, and cost is O(live WAL), never O(data), which
// is principle P6.
func (w *wal) Replay(fn func(walFrame) error) error {
	w.sinceTrim = 0
	counted := func(fr walFrame) error {
		w.sinceTrim += int64(walHdrSize + len(fr.Payload))
		if fn == nil {
			return nil
		}
		return fn(fr)
	}
	order := w.liveOrder()
	after := uint64(0)
	for _, i := range order {
		if w.segs[i].lastSeq != 0 && w.segs[i].lastSeq <= w.trimSeq && after == 0 {
			// Wholly subsumed segments still anchor the seq chain.
			after = w.segs[i].lastSeq
			continue
		}
		seg := w.segs[i]
		if err := w.scanSegment(&seg, after, counted); err != nil {
			return err
		}
		if seg.lastSeq == 0 || (after != 0 && seg.firstSeq != after+1) {
			break
		}
		after = seg.lastSeq
	}
	return nil
}

// walPath derives the sidecar name from the data file path.
func walPath(data string) string { return fmt.Sprintf("%s.aki-wal", data) }
