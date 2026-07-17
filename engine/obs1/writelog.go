package obs1

import (
	"errors"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"
)

// WriteLog is the composed durability pipeline one node runs (doc 04
// sections 1 and 3.1), O1b slice 4: the flusher draining group WAL
// buffers into bucket objects, the committer putting commit records on
// the chain, and the watermarks the verdicts advance. It implements the
// shard runtime's WriteLog seam, so a write handler's emission call is
// the relaxed ack point: when StrSet returns nil the frame is in the
// group's buffer and the reply the handler writes next is covered by
// the flush pipeline behind it.
//
// Sequencing contract: a group belongs to exactly one shard, so exactly
// one owner goroutine emits for it and the per-group seq counter has a
// single writer. SetGroup supplies the lease epoch and resume seq for a
// group before its owner serves writes (construction, boot replay, or a
// grant the owner itself observed); the fencing fold judges every
// commit against the same epochs, so a frame emitted under a stale
// epoch lands on the chain but folds dead and wakes nobody.
type WriteLog struct {
	fl    *Flusher
	cm    *Committer
	marks *Watermarks

	mapKey func(key []byte) (slot uint16, group uint16)
	groups []wlGroup

	// The doc 04 section 10 error taxonomy, atomics because INFO reads
	// them off-owner. They only move on error paths, so the hot gate
	// stays free of shared writes.
	encodeErrs atomic.Uint64
	stallErrs  atomic.Uint64
	fatalErrs  atomic.Uint64
	epochErrs  atomic.Uint64
}

// wlGroup is one group's emission state, single-writer under the
// group-to-shard mapping and padded so neighboring owners never share a
// cache line.
type wlGroup struct {
	next  uint64
	epoch uint32
	_     [52]byte
}

// The client replies the emission errors map to. errFlushStalled is the
// f3 stall text (doc 04 section 6); until slice 7 raises the flushlag
// park a buffer at capacity fails the write with it instead of parking,
// disclosed in the slice notes. errWALEncode is the doc 04 section 10
// bug row: the owner never acks what it could not frame.
var (
	errFlushStalled = errors.New("ERR store: flush stalled")
	errWALEncode    = errors.New("ERR internal: wal encode")
	errWALEpoch     = errors.New("ERR internal: wal epoch")
)

// WriteLogConfig configures one node's pipeline.
type WriteLogConfig struct {
	// Store, Prefix, and Node are the flusher's PUT target and identity.
	Store  Store
	Prefix string
	Node   uint64

	// Chain is the commit target, normally this node's ChainAppender.
	Chain ChainWriter

	// Fold is the lease fold the chain appender folds through. NewWriteLog
	// claims its OnCommit hook to drive the watermarks; a fold that
	// already has one is a wiring conflict and errors.
	Fold *LeaseFold

	// Groups is G, the slot-group count (shard.DefaultSlotGroups on a
	// default runtime); MapKey maps a key to its hash slot and group, the
	// same route the dispatcher uses (shard.HashSlot, shard.GroupOfSlot).
	Groups int
	MapKey func(key []byte) (slot uint16, group uint16)

	// Flusher knobs, zero for the doc 04 defaults.
	FlushSize    int
	FlushAge     time.Duration
	BarrierFloor time.Duration
	CapBytes     int

	// OnCommitted, when set, hears every committed WAL seq in order, the
	// doc 06 fold-accounting seam passed through to the committer.
	OnCommitted func(walSeq uint64, pos ChainPos)
}

// NewWriteLog builds and starts the pipeline: committer over the chain,
// flusher over the store with the committer as its sink, watermarks
// driven by the fold's verdicts.
func NewWriteLog(cfg WriteLogConfig) (*WriteLog, error) {
	if cfg.Fold == nil {
		return nil, fmt.Errorf("obs1: write log needs the lease fold")
	}
	if cfg.Fold.OnCommit != nil {
		return nil, fmt.Errorf("obs1: the lease fold's OnCommit hook is already claimed")
	}
	if cfg.Groups <= 0 || cfg.Groups > 1<<16 {
		return nil, fmt.Errorf("obs1: write log group count %d out of range", cfg.Groups)
	}
	if cfg.MapKey == nil {
		return nil, fmt.Errorf("obs1: write log needs a key mapper")
	}
	marks := NewWatermarks()
	cfg.Fold.OnCommit = marks.ApplyVerdict
	cm, err := NewCommitter(CommitterConfig{
		Chain:       cfg.Chain,
		Node:        cfg.Node,
		OnCommitted: cfg.OnCommitted,
	})
	if err != nil {
		cfg.Fold.OnCommit = nil
		return nil, err
	}
	fl, err := NewFlusher(FlusherConfig{
		Store:        cfg.Store,
		Sink:         cm,
		Prefix:       cfg.Prefix,
		Node:         cfg.Node,
		FlushSize:    cfg.FlushSize,
		FlushAge:     cfg.FlushAge,
		BarrierFloor: cfg.BarrierFloor,
		CapBytes:     cfg.CapBytes,
	})
	if err != nil {
		_ = cm.Close()
		cfg.Fold.OnCommit = nil
		return nil, err
	}
	return &WriteLog{
		fl:     fl,
		cm:     cm,
		marks:  marks,
		mapKey: cfg.MapKey,
		groups: make([]wlGroup, cfg.Groups),
	}, nil
}

// SetGroup supplies a group's lease epoch and next frame seq: 1 for a
// fresh epoch, the replayed high seq plus one after boot (O1c). Call it
// before the group's owner serves writes; the field has the owner as
// its only other toucher, so publication rides whatever started the
// owner (Runtime.Start, a grant the owner processed).
func (l *WriteLog) SetGroup(group uint16, epoch uint32, next uint64) {
	g := &l.groups[group]
	g.epoch = epoch
	g.next = next
}

// StrSet implements the shard seam: the resulting value of a string
// write as one strset frame on the owner's group buffer. Nil return is
// the relaxed ack point.
func (l *WriteLog) StrSet(key, value []byte, expireAtMs int64, counter bool) error {
	slot, group := l.mapKey(key)
	g := &l.groups[group]
	if g.epoch == 0 {
		return l.epochMissing(group)
	}
	var ladder uint8
	if counter {
		ladder = LadderCounter
	}
	err := l.fl.AppendStrSet(group, g.epoch, slot, g.next, key, value, uint64(expireAtMs), ladder)
	if err != nil {
		return l.classify(err)
	}
	g.next++
	return nil
}

// KeyDel implements the shard seam: one keydel frame for a removal of
// any type.
func (l *WriteLog) KeyDel(key []byte) error {
	slot, group := l.mapKey(key)
	g := &l.groups[group]
	if g.epoch == 0 {
		return l.epochMissing(group)
	}
	err := l.fl.AppendOp(group, g.epoch, WALFrame{
		Kind: OpKeyDel,
		Slot: slot,
		Seq:  g.next,
		Key:  key,
	})
	if err != nil {
		return l.classify(err)
	}
	g.next++
	return nil
}

// epochMissing is the write-before-grant bug row: dispatch only routes
// commands for groups this node owns, so an emission against a group
// with no epoch supplied means the wiring lied. Panics in a dev build,
// fails just the command in release, and never touches the flusher.
func (l *WriteLog) epochMissing(group uint16) error {
	l.epochErrs.Add(1)
	if devBuild {
		panic(fmt.Sprintf("obs1: wal emission for group %d before SetGroup", group))
	}
	return errWALEpoch
}

// classify maps a flusher append error to its client reply per the doc
// 04 section 10 taxonomy: the cap is the stall row (parking arrives
// with slice 7), a closed or failed flusher is fatal and reads as a
// stall to the client, and anything else is the encode bug row, which
// panics in a dev build (-tags obs1dev) and fails just the command in
// release.
func (l *WriteLog) classify(err error) error {
	switch {
	case errors.Is(err, ErrWALFull):
		l.stallErrs.Add(1)
		return errFlushStalled
	case errors.Is(err, ErrFlusherClosed), l.fl.Err() != nil:
		l.fatalErrs.Add(1)
		return errFlushStalled
	default:
		l.encodeErrs.Add(1)
		if devBuild {
			panic("obs1: wal encode failed: " + err.Error())
		}
		return errWALEncode
	}
}

// Barrier demands a flush now (floor-gated); strict acks and WAITAOF
// (slices 5 and 6) raise it, and tests use it to drain deterministically.
func (l *WriteLog) Barrier() { l.fl.Barrier() }

// SetFlushAge retunes the age trigger live, the thrift profile knob
// passed through to the flusher.
func (l *WriteLog) SetFlushAge(d time.Duration) { l.fl.SetFlushAge(d) }

// Marks is the committed-watermark surface strict acks and WAIT
// barriers park on.
func (l *WriteLog) Marks() *Watermarks { return l.marks }

// Err reports the pipeline's first fatal error, flusher first.
func (l *WriteLog) Err() error {
	if err := l.fl.Err(); err != nil {
		return err
	}
	return l.cm.Err()
}

// Close drains the pipeline in dependency order: the flusher flushes
// and delivers everything, then the committer drains its queue onto
// the chain. The first fatal error wins.
func (l *WriteLog) Close() error {
	flErr := l.fl.Close()
	cmErr := l.cm.Close()
	if flErr != nil {
		return flErr
	}
	return cmErr
}

// appendInfoRow writes one name:value INFO line.
func appendInfoRow(b []byte, name string, v uint64) []byte {
	b = append(b, name...)
	b = append(b, ':')
	b = strconv.AppendUint(b, v, 10)
	return append(b, '\r', '\n')
}

// AppendInfo renders the "# Durability" INFO section, the doc 04
// section 10 counter taxonomy: flushes, PUT retries, barrier flushes,
// flushed bytes, chain batches and records, and the emission error
// rows. Registered on the runtime through SetWALInfo; the park rows it
// sits beside are per-shard and stay in the f3 section.
func (l *WriteLog) AppendInfo(b []byte) []byte {
	fs := l.fl.Stats()
	cs := l.cm.Stats()
	b = append(b, "\r\n# Durability\r\n"...)
	b = appendInfoRow(b, "wal_flushes", fs.Flushes)
	b = appendInfoRow(b, "wal_barrier_flushes", fs.BarrierFlushes)
	b = appendInfoRow(b, "wal_put_retries", fs.PutRetries)
	b = appendInfoRow(b, "wal_flushed_bytes", fs.BytesFlushed)
	b = appendInfoRow(b, "chain_commit_batches", cs.Batches)
	b = appendInfoRow(b, "chain_commit_records", cs.Records)
	b = appendInfoRow(b, "wal_encode_errors", l.encodeErrs.Load())
	b = appendInfoRow(b, "wal_stall_errors", l.stallErrs.Load())
	b = appendInfoRow(b, "wal_fatal_errors", l.fatalErrs.Load())
	b = appendInfoRow(b, "wal_epoch_errors", l.epochErrs.Load())
	return b
}
