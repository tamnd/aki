package obs1

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Flusher is the doc 04 section 4 write-path drain: one goroutine per
// node that swaps dirty group buffers into WAL objects and PUTs them
// with a bounded pipeline. Owners append encoded frames under a short
// lock; the swap is the only other place that lock is taken, so the
// hot path never waits on a PUT. Everything here lives in RAM, there
// is no spool file and no fsync anywhere (W-I4).
//
// Trigger constants are the #960 flush-cadence lab verdicts.
const (
	// DefaultFlushSize is the buffered-bytes trigger.
	DefaultFlushSize = 8 << 20
	// DefaultFlushAge is the oldest-dirty-byte trigger.
	DefaultFlushAge = 50 * time.Millisecond
	// DefaultBarrierFloor is the minimum spacing between a barrier
	// flush and the previous swap, so strict ackers cannot turn the
	// flusher into a per-command PUT storm.
	DefaultBarrierFloor = 5 * time.Millisecond
	// ThriftFlushAge is the doc 04 section 4.4 low-traffic profile,
	// trading ack latency for a 5x request-cost cut.
	ThriftFlushAge = 250 * time.Millisecond

	// walPipelineDepth is how many WAL PUTs ride concurrently. The
	// swap-and-continue rule keeps appends possible while all four
	// are out; commit records stay WAL-seq ordered downstream.
	walPipelineDepth = 4

	// walRetryBase and walRetryCap bound the jittered exponential
	// backoff on transient PUT failures, the doc 10 taxonomy row.
	walRetryBase = 20 * time.Millisecond
	walRetryCap  = time.Second
)

// ErrFlusherClosed rejects appends after Close started.
var ErrFlusherClosed = errors.New("obs1: flusher closed")

// FlushSink receives every flushed WAL object exactly once, strictly
// in WAL-seq order, from a single goroutine. The commit-record slice
// implements this to chain the object; tests record the calls. A sink
// error is fatal to the flusher.
type FlushSink interface {
	WALFlushed(walSeq uint64, size int64, index []WALIndexEntry) error
}

// FlusherConfig configures one node's flusher. Zero values take the
// defaults above; CapBytes defaults to four times the flush size.
type FlusherConfig struct {
	Store  Store
	Sink   FlushSink
	Prefix string
	Node   uint64

	FlushSize    int
	FlushAge     time.Duration
	BarrierFloor time.Duration
	CapBytes     int

	// StartSeq is the first WAL object seq this flusher writes,
	// 1 if zero. Restart hand-off sets it past the last object the
	// previous incarnation left under this node's prefix.
	StartSeq uint64
}

// FlusherStats is a counter snapshot for the doc 10 taxonomy.
type FlusherStats struct {
	Flushes        uint64
	BarrierFlushes uint64
	PutRetries     uint64
	BytesFlushed   uint64
}

// groupBuf is one group's open buffer: raw already-encoded frames plus
// the bookkeeping AppendWALRaw wants. lastEver survives swaps so seq
// monotonicity holds across objects, not just within one.
type groupBuf struct {
	frames   []byte
	nframes  uint32
	firstSeq uint64
	lastSeq  uint64
	epoch    uint32
	lastEver uint64
	haveEver bool
}

type putResult struct {
	walSeq     uint64
	size       int64
	index      []WALIndexEntry
	frameBytes int
	err        error
}

// Flusher drains group buffers into WAL objects. New starts it; Close
// flushes what is buffered, drains the pipeline, delivers everything
// to the sink, and returns the first fatal error if there was one.
type Flusher struct {
	cfg    FlusherConfig
	ctx    context.Context
	cancel context.CancelFunc

	mu           sync.Mutex
	groups       map[uint16]*groupBuf
	dirtyBytes   int
	pendingBytes int
	firstAppend  time.Time
	lastSwap     time.Time
	barrier      bool
	flushAge     time.Duration
	nextSeq      uint64
	inflight     int
	stopping     bool
	failed       bool
	failErr      error
	stats        FlusherStats

	wakeC     chan struct{}
	putDoneC  chan putResult
	deliverC  chan putResult
	doneC     chan struct{}
	closeOnce sync.Once

	// lag is the cap flag the shard gate reads before running a write
	// handler (backpressure, doc 04 section 6): buffered plus in-flight
	// bytes sit over CapBytes. The cap is a parking threshold, not an
	// admission bound. Append always accepts, because by emission time
	// the write already mutated RAM and holding its encoded frames aside
	// while later emissions pass would break the per-group seq order;
	// the gate stops the next write handler before it runs instead, so
	// the overshoot past the cap is bounded by the handlers already past
	// the gate when the flag rose. flushes counts successful WAL PUTs,
	// the flushlag progress signal for the stall window.
	lag     atomic.Bool
	flushes atomic.Uint64
}

// NewFlusher starts a flusher. The sink runs on its own goroutine and
// must not call back into the flusher's append side while handling a
// delivery it wants to block on.
func NewFlusher(cfg FlusherConfig) (*Flusher, error) {
	if cfg.Store == nil {
		return nil, fmt.Errorf("obs1: flusher needs a store")
	}
	if cfg.Sink == nil {
		return nil, fmt.Errorf("obs1: flusher needs a sink")
	}
	if cfg.FlushSize <= 0 {
		cfg.FlushSize = DefaultFlushSize
	}
	if cfg.FlushAge <= 0 {
		cfg.FlushAge = DefaultFlushAge
	}
	if cfg.BarrierFloor <= 0 {
		cfg.BarrierFloor = DefaultBarrierFloor
	}
	if cfg.CapBytes <= 0 {
		cfg.CapBytes = 4 * cfg.FlushSize
	}
	if cfg.StartSeq == 0 {
		cfg.StartSeq = 1
	}
	ctx, cancel := context.WithCancel(context.Background())
	fl := &Flusher{
		cfg:      cfg,
		ctx:      ctx,
		cancel:   cancel,
		groups:   make(map[uint16]*groupBuf),
		lastSwap: time.Now(),
		flushAge: cfg.FlushAge,
		nextSeq:  cfg.StartSeq,
		wakeC:    make(chan struct{}, 1),
		putDoneC: make(chan putResult, walPipelineDepth),
		deliverC: make(chan putResult, 16),
		doneC:    make(chan struct{}),
	}
	go fl.deliverLoop()
	go fl.run()
	return fl, nil
}

// AppendOp encodes f into group's buffer. Epoch must match the open
// buffer's epoch; the lease layer drains before an epoch bump, so a
// mismatch on a non-empty buffer is an invariant violation, not a
// race to tolerate.
func (fl *Flusher) AppendOp(group uint16, epoch uint32, f WALFrame) error {
	flen := walFrameFixed + len(f.Key) + len(f.Payload)
	fl.mu.Lock()
	defer fl.mu.Unlock()
	g, err := fl.admitLocked(group, epoch, f.Seq)
	if err != nil {
		return err
	}
	fb, err := appendWALFrame(g.frames, f)
	if err != nil {
		return err
	}
	g.frames = fb
	fl.noteAppendedLocked(g, epoch, f.Seq, flen)
	return nil
}

// AppendStrSet is the owner hot path: the #956 encoder straight into
// the group buffer, no WALFrame value and no payload copy in between.
func (fl *Flusher) AppendStrSet(group uint16, epoch uint32, slot uint16, seq uint64, key, value []byte, expiryMS uint64, ladder uint8) error {
	flen := walFrameFixed + len(key) + len(value) + 9
	fl.mu.Lock()
	defer fl.mu.Unlock()
	g, err := fl.admitLocked(group, epoch, seq)
	if err != nil {
		return err
	}
	fb, err := AppendStrSetFrame(g.frames, slot, seq, key, value, expiryMS, ladder)
	if err != nil {
		return err
	}
	g.frames = fb
	fl.noteAppendedLocked(g, epoch, seq, flen)
	return nil
}

// AppendRun encodes a whole frame run into group's buffer atomically:
// every frame or none, appended under one hold of the lock. The swap
// takes the same lock, so a run can
// never split across WAL objects and a txn-bracketed run stays contiguous
// in the group's section of one object, the doc 04 section 2 contiguity
// rule. Seqs must be strictly increasing across the run, the caller's job
// (the write log draws them consecutively).
func (fl *Flusher) AppendRun(group uint16, epoch uint32, frames []WALFrame) error {
	if len(frames) == 0 {
		return fmt.Errorf("obs1: an op run needs at least one frame")
	}
	total := 0
	for i := range frames {
		total += walFrameFixed + len(frames[i].Key) + len(frames[i].Payload)
		if i > 0 && frames[i].Seq <= frames[i-1].Seq {
			return fmt.Errorf("obs1: run seq %d after %d, must be strictly increasing", frames[i].Seq, frames[i-1].Seq)
		}
	}
	fl.mu.Lock()
	defer fl.mu.Unlock()
	g, err := fl.admitLocked(group, epoch, frames[0].Seq)
	if err != nil {
		return err
	}
	mark := len(g.frames)
	fb := g.frames
	for i := range frames {
		if fb, err = appendWALFrame(fb, frames[i]); err != nil {
			// All or nothing: drop what the run wrote so a half-encoded
			// run never reaches a WAL object.
			g.frames = g.frames[:mark]
			return err
		}
	}
	g.frames = fb
	if g.nframes == 0 {
		g.epoch = epoch
		g.firstSeq = frames[0].Seq
	}
	g.nframes += uint32(len(frames))
	last := frames[len(frames)-1].Seq
	g.lastSeq = last
	g.lastEver = last
	g.haveEver = true
	if fl.dirtyBytes == 0 {
		fl.firstAppend = time.Now()
	}
	fl.dirtyBytes += total
	fl.recalcLagLocked()
	fl.wake()
	return nil
}

func (fl *Flusher) admitLocked(group uint16, epoch uint32, seq uint64) (*groupBuf, error) {
	if fl.failed {
		return nil, fl.failErr
	}
	if fl.stopping {
		return nil, ErrFlusherClosed
	}
	g := fl.groups[group]
	if g == nil {
		g = &groupBuf{}
		fl.groups[group] = g
	}
	if g.nframes > 0 && g.epoch != epoch {
		return nil, fmt.Errorf("obs1: group %d append at epoch %d into an open epoch %d buffer, the lease must drain before the bump", group, epoch, g.epoch)
	}
	if g.haveEver && seq <= g.lastEver {
		return nil, fmt.Errorf("obs1: group %d seq %d after %d, must be strictly increasing", group, seq, g.lastEver)
	}
	return g, nil
}

func (fl *Flusher) noteAppendedLocked(g *groupBuf, epoch uint32, seq uint64, flen int) {
	if g.nframes == 0 {
		g.epoch = epoch
		g.firstSeq = seq
	}
	g.nframes++
	g.lastSeq = seq
	g.lastEver = seq
	g.haveEver = true
	if fl.dirtyBytes == 0 {
		fl.firstAppend = time.Now()
	}
	fl.dirtyBytes += flen
	fl.recalcLagLocked()
	fl.wake()
}

// recalcLagLocked refreshes the cap flag wherever the byte accounting
// moves: appends raise dirtyBytes, a swap shifts dirty to pending with
// the sum unchanged, and a PUT completion drops pendingBytes. A failed
// flusher clears the flag so a gated write runs its handler and takes
// the fatal-stall reply from the append instead of parking on a lag
// that can never drain.
func (fl *Flusher) recalcLagLocked() {
	fl.lag.Store(!fl.failed && fl.dirtyBytes+fl.pendingBytes > fl.cfg.CapBytes)
}

// Lagged reports the cap flag: buffered plus in-flight bytes over
// CapBytes (see the field comment for why this parks the next write at
// the shard gate rather than refusing the append).
func (fl *Flusher) Lagged() bool { return fl.lag.Load() }

// FlushCount counts successfully completed WAL PUTs, the flushlag
// progress signal: a parked write is making progress exactly when this
// advances.
func (fl *Flusher) FlushCount() uint64 { return fl.flushes.Load() }

// lastEmitted snapshots every group's highest emitted frame seq under
// the buffer lock, the marks a commit barrier must cover before it can
// claim that everything appended ahead of this call is durable. The
// lock is the same one every append takes, so the snapshot sits at a
// definite point in the emission order. Groups that never buffered a
// frame have nothing to cover and do not appear; lastEver survives
// swaps, so a group whose frames are all in flight or committed still
// reports its mark.
func (fl *Flusher) lastEmitted() (groups []uint16, seqs []uint64) {
	fl.mu.Lock()
	defer fl.mu.Unlock()
	for gid, g := range fl.groups {
		if g.haveEver {
			groups = append(groups, gid)
			seqs = append(seqs, g.lastEver)
		}
	}
	return groups, seqs
}

// Barrier asks for the current buffer to go out now, subject to the
// floor since the last swap. One-shot: cleared by the swap it causes.
// With nothing buffered it is a no-op, there is nothing the caller
// could be waiting on.
func (fl *Flusher) Barrier() {
	fl.mu.Lock()
	if !fl.failed && !fl.stopping && fl.dirtyBytes > 0 {
		fl.barrier = true
	}
	fl.mu.Unlock()
	fl.wake()
}

// SetFlushAge retunes the age trigger live, the doc 04 section 4.4
// thrift-profile knob.
func (fl *Flusher) SetFlushAge(d time.Duration) {
	if d <= 0 {
		return
	}
	fl.mu.Lock()
	fl.flushAge = d
	fl.mu.Unlock()
	fl.wake()
}

// Err reports the first fatal error, nil while healthy.
func (fl *Flusher) Err() error {
	fl.mu.Lock()
	defer fl.mu.Unlock()
	if fl.failed {
		return fl.failErr
	}
	return nil
}

// Stats snapshots the counters.
func (fl *Flusher) Stats() FlusherStats {
	fl.mu.Lock()
	defer fl.mu.Unlock()
	return fl.stats
}

// Close flushes what is buffered, waits for the pipeline and delivery
// to drain, and returns the first fatal error. Idempotent.
func (fl *Flusher) Close() error {
	fl.closeOnce.Do(func() {
		fl.mu.Lock()
		fl.stopping = true
		fl.mu.Unlock()
		fl.wake()
	})
	<-fl.doneC
	return fl.Err()
}

func (fl *Flusher) wake() {
	select {
	case fl.wakeC <- struct{}{}:
	default:
	}
}

func (fl *Flusher) fail(err error) {
	fl.mu.Lock()
	fl.failLocked(err)
	fl.mu.Unlock()
	fl.wake()
}

func (fl *Flusher) failLocked(err error) {
	if fl.failed {
		return
	}
	fl.failed = true
	fl.failErr = err
	fl.recalcLagLocked()
	fl.cancel()
}

func (fl *Flusher) walKey(seq uint64) string {
	return walObjectKey(fl.cfg.Prefix, fl.cfg.Node, seq)
}

// run is the flusher goroutine: swap when a trigger fires and a
// pipeline slot is free, then wait on the next deadline or event.
func (fl *Flusher) run() {
	defer close(fl.deliverC)
	timer := time.NewTimer(time.Hour)
	timer.Stop()
	completed := make(map[uint64]putResult)
	nextDeliver := fl.cfg.StartSeq
	for {
		fl.mu.Lock()
		now := time.Now()
		if !fl.failed {
			for fl.inflight < walPipelineDepth && fl.swapDueLocked(now) {
				fl.startFlushLocked(now)
			}
		}
		done := fl.inflight == 0 && (fl.failed || (fl.stopping && fl.dirtyBytes == 0))
		wait, hasWait := time.Duration(0), false
		if !done && !fl.failed && !fl.stopping {
			wait, hasWait = fl.nextDeadlineLocked(now)
		}
		fl.mu.Unlock()
		if done {
			return
		}
		if hasWait {
			timer.Reset(wait)
		} else {
			timer.Stop()
		}
		select {
		case <-fl.wakeC:
		case <-timer.C:
		case r := <-fl.putDoneC:
			nextDeliver = fl.finishPut(r, completed, nextDeliver)
		}
	}
}

func (fl *Flusher) swapDueLocked(now time.Time) bool {
	if fl.dirtyBytes == 0 {
		return false
	}
	if fl.stopping {
		return true
	}
	if fl.dirtyBytes >= fl.cfg.FlushSize {
		return true
	}
	if now.Sub(fl.firstAppend) >= fl.flushAge {
		return true
	}
	if fl.barrier && now.Sub(fl.lastSwap) >= fl.cfg.BarrierFloor {
		return true
	}
	return false
}

func (fl *Flusher) nextDeadlineLocked(now time.Time) (time.Duration, bool) {
	if fl.dirtyBytes == 0 {
		return 0, false
	}
	d := fl.firstAppend.Add(fl.flushAge).Sub(now)
	if fl.barrier {
		if b := fl.lastSwap.Add(fl.cfg.BarrierFloor).Sub(now); b < d {
			d = b
		}
	}
	if d <= 0 {
		// Due but gated on the pipeline; a putDone wakes us.
		return 0, false
	}
	return d, true
}

// startFlushLocked swaps every dirty group out and hands the object to
// a PUT goroutine. Swap-and-continue: owners keep appending into fresh
// buffers while up to four objects ride.
func (fl *Flusher) startFlushLocked(now time.Time) {
	sections := make([]RawSection, 0, len(fl.groups))
	for gid, g := range fl.groups {
		if g.nframes == 0 {
			continue
		}
		sections = append(sections, RawSection{
			Group: gid, Epoch: g.epoch, Frames: g.frames,
			NFrames: g.nframes, FirstSeq: g.firstSeq, LastSeq: g.lastSeq,
		})
		g.frames = nil
		g.nframes = 0
	}
	sort.Slice(sections, func(i, j int) bool { return sections[i].Group < sections[j].Group })
	walSeq := fl.nextSeq
	fl.nextSeq++
	frameBytes := fl.dirtyBytes
	fl.dirtyBytes = 0
	fl.pendingBytes += frameBytes
	if fl.barrier {
		fl.barrier = false
		fl.stats.BarrierFlushes++
	}
	fl.stats.Flushes++
	fl.lastSwap = now
	fl.inflight++
	go fl.flushObject(walSeq, frameBytes, sections)
}

func (fl *Flusher) flushObject(walSeq uint64, frameBytes int, sections []RawSection) {
	obj, index, err := AppendWALRaw(nil, fl.cfg.Node, sections)
	if err == nil {
		tag := WriteTag{Writer: fmt.Sprintf("%016x", fl.cfg.Node), Batch: seq16(walSeq)}
		err = fl.putWAL(fl.walKey(walSeq), tag, obj)
	}
	fl.putDoneC <- putResult{walSeq: walSeq, size: int64(len(obj)), index: index, frameBytes: frameBytes, err: err}
}

// putWAL is the append.go recheck shape on a key nothing else may
// write: node id owns the wal/<node16>/ namespace, so RecheckOther is
// fencing failure, not contention. Transient errors retry forever
// under jittered exponential backoff; only Close-after-failure or a
// fatal recheck stops it, because dropping a swapped buffer is losing
// acknowledged writes.
func (fl *Flusher) putWAL(key string, tag WriteTag, body []byte) error {
	backoff := walRetryBase
	for {
		_, err := fl.cfg.Store.PutIfAbsent(fl.ctx, key, body, tag)
		if err == nil {
			return nil
		}
		if fl.ctx.Err() != nil {
			return err
		}
		if errors.Is(err, ErrPrecondition) || errors.Is(err, ErrConflict) || errors.Is(err, ErrAmbiguous) {
			out, _, _, rerr := fl.cfg.Store.Recheck(fl.ctx, key, tag)
			switch {
			case rerr != nil:
				if fl.ctx.Err() != nil {
					return rerr
				}
				// Transient recheck failure, back off and take the
				// whole round again.
			case out == RecheckOurs:
				return nil
			case out == RecheckOther:
				return fmt.Errorf("obs1: WAL key %s is held by another writer, node fencing is broken", key)
			}
			// RecheckAbsent: the PUT never landed, same bytes go again.
		}
		fl.mu.Lock()
		fl.stats.PutRetries++
		fl.mu.Unlock()
		sleep := backoff/2 + rand.N(backoff/2+1)
		select {
		case <-fl.ctx.Done():
			return fl.ctx.Err()
		case <-time.After(sleep):
		}
		if backoff *= 2; backoff > walRetryCap {
			backoff = walRetryCap
		}
	}
}

// finishPut books a PUT completion and releases the maximal in-order
// prefix to the delivery goroutine. pendingBytes drops here, at PUT
// done, the #960 accounting rule: cap admission counts bytes until
// they are safely off-box, not until they are swapped.
func (fl *Flusher) finishPut(r putResult, completed map[uint64]putResult, nextDeliver uint64) uint64 {
	fl.mu.Lock()
	fl.inflight--
	fl.pendingBytes -= r.frameBytes
	if r.err != nil {
		fl.failLocked(fmt.Errorf("obs1: WAL %d flush failed: %w", r.walSeq, r.err))
	}
	failed := fl.failed
	if !failed {
		fl.stats.BytesFlushed += uint64(r.size)
		completed[r.walSeq] = r
		fl.flushes.Add(1)
	}
	fl.recalcLagLocked()
	fl.mu.Unlock()
	if failed {
		return nextDeliver
	}
	for {
		d, ok := completed[nextDeliver]
		if !ok {
			return nextDeliver
		}
		delete(completed, nextDeliver)
		fl.deliverC <- d
		nextDeliver++
	}
}

// deliverLoop hands flushed objects to the sink one at a time, in WAL
// seq order. A sink error is fatal; later deliveries are skipped so
// the run loop can drain and exit.
func (fl *Flusher) deliverLoop() {
	defer close(fl.doneC)
	for d := range fl.deliverC {
		if fl.Err() != nil {
			continue
		}
		if err := fl.cfg.Sink.WALFlushed(d.walSeq, d.size, d.index); err != nil {
			fl.fail(fmt.Errorf("obs1: flush sink rejected WAL %d: %w", d.walSeq, err))
		}
	}
}
