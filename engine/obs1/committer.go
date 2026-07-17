package obs1

import (
	"context"
	"fmt"
	"slices"
	"sync"
)

// ChainWriter is the slice of ChainAppender the committer appends
// through, an interface so tests can gate or fail the chain on purpose.
type ChainWriter interface {
	Append(ctx context.Context, records []ChainRecord) (ChainPos, error)
}

var _ ChainWriter = (*ChainAppender)(nil)

// ErrCommitterClosed rejects deliveries after Close started.
var ErrCommitterClosed = fmt.Errorf("obs1: committer closed")

// CommitterStats is a counter snapshot. Batches at or below Records is
// the doc 04 section 4 coalescing rule working: when PUTs complete
// close together their commit records share one chain append.
type CommitterStats struct {
	Batches uint64
	Records uint64
}

// CommitterConfig configures one node's committer.
type CommitterConfig struct {
	Chain ChainWriter
	Node  uint64

	// OnCommitted, when set, hears every WAL seq whose commit record
	// landed, in WAL-seq order, with the chain position that carries
	// it. This is the fold-accounting seam; doc 06 plugs in here.
	OnCommitted func(walSeq uint64, pos ChainPos)

	// OnAppended, when set, hears the distinct groups of every batch whose
	// append landed, once per batch. This is the lease-renewal seam (doc 02
	// section 3.5): a successful append of the node's own is exactly what
	// extends its believed deadlines, so the LeaseGate plugs in here.
	OnAppended func(groups []uint16)
}

// Committer is the FlushSink that puts flushed WAL objects onto the
// chain, O1b slice 3. Deliveries queue on a bounded channel; a single
// goroutine drains whatever has queued into one chain batch, so a slow
// append coalesces the records behind it instead of multiplying chain
// traffic, and appends stay at or below PUT count (doc 04 section 4,
// the #960 head-of-line note).
//
// The committer does not advance watermarks itself: commit records
// fold back through the appender's applier during Append, the lease
// fold computes the fencing verdict, and Watermarks.ApplyVerdict moves
// only the live sections. A fenced commit lands on the chain but wakes
// nobody, which is the honest outcome.
//
// Close only after the flusher feeding this sink has closed; the
// flusher's Close drains every delivery first, so the queue is quiet
// by the time the channel closes.
type Committer struct {
	cfg CommitterConfig

	mu      sync.Mutex
	failed  bool
	failErr error
	closing bool
	stats   CommitterStats

	queueC    chan CommitRecord
	doneC     chan struct{}
	closeOnce sync.Once
}

// NewCommitter starts a committer over the chain writer.
func NewCommitter(cfg CommitterConfig) (*Committer, error) {
	if cfg.Chain == nil {
		return nil, fmt.Errorf("obs1: committer needs a chain writer")
	}
	if cfg.Node == 0 {
		return nil, fmt.Errorf("obs1: committer needs a nonzero node id")
	}
	c := &Committer{
		cfg:    cfg,
		queueC: make(chan CommitRecord, 16),
		doneC:  make(chan struct{}),
	}
	go c.run()
	return c, nil
}

// WALFlushed queues one flushed object's commit record. The flusher
// calls this in strict WAL-seq order from one goroutine, so the queue
// order is the chain order. A full queue blocks, which backs pressure
// up through the flusher's delivery channel into the WAL cap, the
// block-not-drop chain end to end.
func (c *Committer) WALFlushed(walSeq uint64, size int64, index []WALIndexEntry) error {
	c.mu.Lock()
	if c.failed {
		err := c.failErr
		c.mu.Unlock()
		return err
	}
	if c.closing {
		c.mu.Unlock()
		return ErrCommitterClosed
	}
	c.mu.Unlock()
	rec := CommitRecord{
		WALNode:  c.cfg.Node,
		WALSeq:   walSeq,
		WALSize:  uint64(size),
		Sections: make([]CommitSection, len(index)),
	}
	for i, e := range index {
		rec.Sections[i] = e.CommitSection()
	}
	c.queueC <- rec
	return nil
}

// Err reports the first fatal error, nil while healthy.
func (c *Committer) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failed {
		return c.failErr
	}
	return nil
}

// Stats snapshots the counters.
func (c *Committer) Stats() CommitterStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stats
}

// Close drains the queue onto the chain and returns the first fatal
// error. Idempotent. The feeding flusher must have closed first.
func (c *Committer) Close() error {
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.closing = true
		c.mu.Unlock()
		close(c.queueC)
	})
	<-c.doneC
	return c.Err()
}

func (c *Committer) fail(err error) {
	c.mu.Lock()
	if !c.failed {
		c.failed = true
		c.failErr = err
	}
	c.mu.Unlock()
}

// run drains deliveries into chain batches. After a failure it keeps
// draining so a blocked WALFlushed sender always gets through to see
// the error.
func (c *Committer) run() {
	defer close(c.doneC)
	for rec := range c.queueC {
		batch := []ChainRecord{rec}
		firstSeq := rec.WALSeq
	drain:
		for len(batch) < cap(c.queueC) {
			select {
			case more, ok := <-c.queueC:
				if !ok {
					break drain
				}
				batch = append(batch, more)
			default:
				break drain
			}
		}
		if c.Err() != nil {
			continue
		}
		pos, err := c.cfg.Chain.Append(context.Background(), batch)
		if err != nil {
			c.fail(fmt.Errorf("obs1: commit append for WAL %d failed: %w", firstSeq, err))
			continue
		}
		c.mu.Lock()
		c.stats.Batches++
		c.stats.Records += uint64(len(batch))
		c.mu.Unlock()
		if c.cfg.OnAppended != nil {
			// Distinct groups across the batch's sections; batches are a
			// handful of records, so the linear dedup beats a map.
			var groups []uint16
			for _, r := range batch {
				for _, s := range r.(CommitRecord).Sections {
					if !slices.Contains(groups, s.Group) {
						groups = append(groups, s.Group)
					}
				}
			}
			c.cfg.OnAppended(groups)
		}
		if c.cfg.OnCommitted != nil {
			for _, r := range batch {
				c.cfg.OnCommitted(r.(CommitRecord).WALSeq, pos)
			}
		}
	}
}

// Watermarks tracks the per-group committed frame seq, the thing a
// strict ack or a WAIT barrier parks on. It advances from fencing
// verdicts, not from append returns: assign ApplyVerdict to the lease
// fold's OnCommit (or call it from a wider hook) and only sections the
// fold judged live move the mark, so a fenced writer's commit wakes no
// waiter.
type Watermarks struct {
	mu      sync.Mutex
	seq     map[uint16]uint64
	changed map[uint16]chan struct{}
	notify  map[uint16][]wmNotify
}

// wmNotify is one registered callback: fn runs once the group's
// watermark reaches seq.
type wmNotify struct {
	seq uint64
	fn  func()
}

// NewWatermarks starts every group at zero.
func NewWatermarks() *Watermarks {
	return &Watermarks{
		seq:     make(map[uint16]uint64),
		changed: make(map[uint16]chan struct{}),
		notify:  make(map[uint16][]wmNotify),
	}
}

// ApplyVerdict advances the live sections' groups to their LastSeq and
// wakes their waiters. Matches the LeaseFold.OnCommit signature.
// Callbacks whose seq the advance covered run here on the fold's
// goroutine, outside the lock, in registration order per group; a
// strict ack's callback is a lock-free queue push (Conn.CompleteBlocked),
// so the fold pays a bounded, non-blocking step per waiter.
func (w *Watermarks) ApplyVerdict(v CommitVerdict) error {
	var due []func()
	w.mu.Lock()
	for i, s := range v.Commit.Sections {
		if !v.Live[i] || s.LastSeq <= w.seq[s.Group] {
			continue
		}
		w.seq[s.Group] = s.LastSeq
		if ch := w.changed[s.Group]; ch != nil {
			close(ch)
			delete(w.changed, s.Group)
		}
		if list := w.notify[s.Group]; len(list) != 0 {
			kept := list[:0]
			for _, n := range list {
				if n.seq <= s.LastSeq {
					due = append(due, n.fn)
				} else {
					kept = append(kept, n)
				}
			}
			for j := len(kept); j < len(list); j++ {
				list[j] = wmNotify{}
			}
			if len(kept) == 0 {
				delete(w.notify, s.Group)
			} else {
				w.notify[s.Group] = kept
			}
		}
	}
	w.mu.Unlock()
	for _, fn := range due {
		fn()
	}
	return nil
}

// Notify registers fn to run once the group's watermark reaches seq,
// the callback face of Wait for callers that must not block (a strict
// ack parked on an owner reply slot). Already-covered marks fire fn
// before Notify returns, on the caller's goroutine; otherwise fn runs
// from the ApplyVerdict that covers it. Registrations survive until
// covered; a mark under a fenced epoch never commits live, so its
// callback never fires, the same silence Wait shows as a stall.
func (w *Watermarks) Notify(group uint16, seq uint64, fn func()) {
	w.mu.Lock()
	if w.seq[group] >= seq {
		w.mu.Unlock()
		fn()
		return
	}
	w.notify[group] = append(w.notify[group], wmNotify{seq: seq, fn: fn})
	w.mu.Unlock()
}

// Committed reports a group's committed watermark, zero if nothing
// committed yet.
func (w *Watermarks) Committed(group uint16) uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.seq[group]
}

// Wait parks until the group's watermark reaches seq or ctx ends.
func (w *Watermarks) Wait(ctx context.Context, group uint16, seq uint64) error {
	for {
		w.mu.Lock()
		if w.seq[group] >= seq {
			w.mu.Unlock()
			return nil
		}
		ch := w.changed[group]
		if ch == nil {
			ch = make(chan struct{})
			w.changed[group] = ch
		}
		w.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ch:
		}
	}
}
