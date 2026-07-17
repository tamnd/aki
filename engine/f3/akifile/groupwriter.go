package akifile

import "sync"

// The group-commit writer: the one goroutine that owns a single-writer File's
// AppendGroup so S shard owners can re-home their record logs onto the one shared
// .aki without racing the append cursor (spec 2064/f3/07 section 2, the design
// note at 2064/f3/milestones/M8-group-commit-writer.md).
//
// A File is single-writer by construction (file.go): AppendGroup advances cursor
// and globalSeq with plain field writes, so two callers racing it corrupt both.
// Today RecordLogWriter.Flush calls AppendGroup directly, which is correct for a
// single-shard store but not for the real runtime, where every shard owns its own
// record log and would call AppendGroup on the one shared File at once. The group
// writer is the multiplexer that removes that race: it is the only caller of
// AppendGroup, and every shard reaches the file through it.
//
// The shape is doc 07 section 2's, restated against the symbols here. Each shard
// stages framed record rows into its own RecordLogWriter buffer (owner-local, no
// atomics), seals the batch, and hands it to this writer over the shard's own
// bounded ring. The writer parks when every ring is empty, wakes on a submit,
// drains every ring in one pass into a single group, calls AppendGroup once for
// the one fsync that covers the group, then resolves each batch's per-record
// addresses from the returned segment offsets and hands them back through the
// batch's completion. global_seq stays a plain field the writer owns (single
// owner, no atomic); shard_seq stays owner-local because a shard is the only
// producer on its own ring and the writer drains a ring in FIFO order.
//
// This is a second instance of the off-owner pattern the shard already runs. The
// per-shard ring mirrors the ioworker's SPSC hand-off (a bounded channel, single
// producer, single consumer, lazily started, joined by Stop), and the doorbell
// wakeup is the waker's park-on-empty rule realized with a capacity-one signal
// channel. A shard whose ring is full learns it through a false Submit and applies
// backpressure to its own inbound rather than blocking a foreground reply, which
// is the single-writer-saturation signal doc 07's falsifier keys on: if one writer
// saturates below the gate bar, split to two writers over the one file, never split
// the file.

// sealed is one shard's framed record batch on its way to the group writer: the
// payload (the RecordLogWriter's whole pending buffer for the batch), the per-record
// frame table that turns a segment offset into per-record addresses, the shard and
// its next shard_seq, and the completion the writer calls after the group's fsync.
// The producer owns payload and frames until done fires; the writer only reads them.
type sealed struct {
	shard    uint16
	shardSeq uint64
	payload  []byte
	frames   []RecordFrame
	done     func(addrs []uint64, err error)
}

// GroupWriter multiplexes every shard's record appends onto one single-writer File.
// rings is one bounded SPSC channel per shard, the owner-to-writer hand-off, sized
// so a submit the ring admits never blocks. signal is the capacity-one doorbell a
// producer rings after a push so a parked writer wakes; a full doorbell coalesces
// many pushes into one pending wake, which is fine because the writer drains every
// ring each pass. The goroutine starts lazily on the first submit and is joined by
// Stop, so a File no shard ever logs to keeps this at zero cost, not even a parked
// goroutine.
type GroupWriter struct {
	f      *File
	rings  []chan sealed
	signal chan struct{}
	quit   chan struct{}
	done   chan struct{}
	begin  sync.Once
	up     bool // goroutine started; producer-set under begin, read at Stop after producers quiesce
}

// NewGroupWriter builds a group writer over f with one ring per shard, each buffered
// to ring entries. ring is the per-shard in-flight bound: a shard with ring batches
// already queued gets a false Submit and defers, so ring is the admission control on
// how far one shard can run ahead of the writer. The goroutine is not started here;
// the first Submit starts it.
func NewGroupWriter(f *File, shards, ring int) *GroupWriter {
	if shards < 1 {
		shards = 1
	}
	if ring < 1 {
		ring = 1
	}
	rings := make([]chan sealed, shards)
	for i := range rings {
		rings[i] = make(chan sealed, ring)
	}
	return &GroupWriter{
		f:      f,
		rings:  rings,
		signal: make(chan struct{}, 1),
		quit:   make(chan struct{}),
		done:   make(chan struct{}),
	}
}

// Submit hands one sealed batch to the writer for shard, or returns false when the
// shard's ring is full (the writer is behind and the caller must apply backpressure
// rather than block). payload is the framed record run and frames the per-record
// table; the writer resolves each record's absolute address as the segment offset
// plus SegHeaderLen plus the frame offset and hands the addresses to done after the
// group's fsync, or a nil slice and the error if the group did not land durably. The
// caller must not reuse payload or frames until done fires. done may be nil for a
// batch whose addresses no one publishes. Submit is called on the shard owner only,
// the single producer on this shard's ring, so the push and the shard_seq that
// stamped the batch share the owner's program order.
func (w *GroupWriter) Submit(shard uint16, shardSeq uint64, payload []byte, frames []RecordFrame, done func([]uint64, error)) bool {
	w.begin.Do(func() {
		w.up = true
		go w.run()
	})
	b := sealed{shard: shard, shardSeq: shardSeq, payload: payload, frames: frames, done: done}
	select {
	case w.rings[shard] <- b:
		// Ring the doorbell after the push, so a writer that parked seeing empty
		// rings wakes and re-drains. The capacity-one send never blocks; a token
		// already pending means an earlier push will already wake the writer.
		select {
		case w.signal <- struct{}{}:
		default:
		}
		return true
	default:
		return false
	}
}

// run is the writer loop: drain every ring in one pass, commit the group, and park
// on the doorbell when a full pass drained nothing. It is the only goroutine that
// calls AppendGroup, so the file's single-writer contract holds by construction. On
// quit it drains once more so a batch submitted just before Stop still lands, then
// exits.
func (w *GroupWriter) run() {
	var group []sealed
	for {
		group = w.drainAll(group[:0])
		if len(group) > 0 {
			w.commit(group)
			continue
		}
		select {
		case <-w.signal:
		case <-w.quit:
			group = w.drainAll(group[:0])
			if len(group) > 0 {
				w.commit(group)
			}
			close(w.done)
			return
		}
	}
}

// drainAll collects every batch currently queued across all rings into group in one
// pass, non-blocking, so one drain forms one group and one AppendGroup covers it.
// It appends onto group so the caller's slice keeps its capacity across passes.
func (w *GroupWriter) drainAll(group []sealed) []sealed {
	for i := range w.rings {
	drain:
		for {
			select {
			case b := <-w.rings[i]:
				group = append(group, b)
			default:
				break drain
			}
		}
	}
	return group
}

// commit lays the whole group down as one AppendGroup and hands each batch its
// resolved addresses through its completion. It reports an address only when the
// entire group landed, because AppendGroup issues its fsync once after the last
// segment, so a mid-group write error leaves nothing in the group durably synced:
// on any error every completion gets the error and no address escapes, which is the
// publish-after-durable edge (doc 07 section 8, step 6 before step 7). On success
// each batch maps its frames onto its segment offset in group order.
func (w *GroupWriter) commit(group []sealed) {
	pend := make([]Pending, len(group))
	for i, b := range group {
		pend[i] = Pending{Shard: b.shard, Kind: KindLog, ShardSeq: b.shardSeq, Payload: b.payload}
	}
	offs, err := w.f.AppendGroup(pend)
	if err != nil {
		for _, b := range group {
			if b.done != nil {
				b.done(nil, err)
			}
		}
		return
	}
	for i, b := range group {
		if b.done == nil {
			continue
		}
		base := offs[i] + SegHeaderLen
		addrs := make([]uint64, len(b.frames))
		for j, fr := range b.frames {
			addrs[j] = base + fr.FrameOff
		}
		b.done(addrs, nil)
	}
}

// Stop shuts the writer down after every producer has quiesced (the runtime joins
// the shard owners first), so no Submit can race the drain. It signals quit, the run
// loop drains what is already queued one last time, and Stop waits for the exit. A
// writer no shard ever submitted to never started the goroutine, and Stop is then a
// plain return.
func (w *GroupWriter) Stop() {
	if !w.up {
		return
	}
	close(w.quit)
	<-w.done
}
