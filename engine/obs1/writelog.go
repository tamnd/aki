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
// write as one strset frame on the owner's group buffer. A nil error is
// the relaxed ack point, and the returned mark names the frame for a
// strict ack to park on.
func (l *WriteLog) StrSet(key, value []byte, expireAtMs int64, counter bool) (uint16, uint64, error) {
	slot, group := l.mapKey(key)
	g := &l.groups[group]
	if g.epoch == 0 {
		return 0, 0, l.epochMissing(group)
	}
	var ladder uint8
	if counter {
		ladder = LadderCounter
	}
	err := l.fl.AppendStrSet(group, g.epoch, slot, g.next, key, value, uint64(expireAtMs), ladder)
	if err != nil {
		return 0, 0, l.classify(err)
	}
	seq := g.next
	g.next++
	return group, seq, nil
}

// KeyDel implements the shard seam: one keydel frame for a removal of
// any type.
func (l *WriteLog) KeyDel(key []byte) (uint16, uint64, error) {
	slot, group := l.mapKey(key)
	g := &l.groups[group]
	if g.epoch == 0 {
		return 0, 0, l.epochMissing(group)
	}
	err := l.fl.AppendOp(group, g.epoch, WALFrame{
		Kind: OpKeyDel,
		Slot: slot,
		Seq:  g.next,
		Key:  key,
	})
	if err != nil {
		return 0, 0, l.classify(err)
	}
	seq := g.next
	g.next++
	return group, seq, nil
}

// keyedOp pairs an op with the key and slot it applies to, for a run
// whose frames span two keys in one group (SMOVE's co-located form).
type keyedOp struct {
	slot uint16
	key  []byte
	op   Op
}

// emitOps encodes ops as key's next frames and appends them: one AppendOp
// for a single frame, one AppendRun otherwise, so a multi-effect command's
// frames never split across WAL objects. Within one object replay applies
// frames in seq order and a tail cut removes the object whole, so a
// single-command run needs no txn markers; doc 04 section 2 mandates them
// for MULTI/EXEC bodies only. The returned mark is the last frame's, which
// covers the run under the per-group monotone watermark. The group's seq
// counter advances only after the append succeeds, so a refused emission
// leaves no gap.
func (l *WriteLog) emitOps(key []byte, ops ...Op) (uint16, uint64, error) {
	slot, group := l.mapKey(key)
	if len(ops) == 1 {
		return l.emitOne(group, slot, key, ops[0])
	}
	items := make([]keyedOp, len(ops))
	for i, op := range ops {
		items[i] = keyedOp{slot: slot, key: key, op: op}
	}
	return l.emitKeyed(group, items)
}

// emitOne appends a single already-routed frame, the no-run fast path.
func (l *WriteLog) emitOne(group, slot uint16, key []byte, op Op) (uint16, uint64, error) {
	g := &l.groups[group]
	if g.epoch == 0 {
		return 0, 0, l.epochMissing(group)
	}
	f, err := EncodeOp(slot, g.next, key, op)
	if err != nil {
		return 0, 0, l.classify(err)
	}
	if err := l.fl.AppendOp(group, g.epoch, f); err != nil {
		return 0, 0, l.classify(err)
	}
	seq := g.next
	g.next++
	return group, seq, nil
}

// emitKeyed appends items as one run on group, each frame carrying its
// own key and slot: the emitOps contract with the single-key restriction
// lifted, which is what lets a co-located SMOVE's two-key effect stay
// one atomic run.
func (l *WriteLog) emitKeyed(group uint16, items []keyedOp) (uint16, uint64, error) {
	if len(items) == 1 {
		return l.emitOne(group, items[0].slot, items[0].key, items[0].op)
	}
	g := &l.groups[group]
	if g.epoch == 0 {
		return 0, 0, l.epochMissing(group)
	}
	frames := make([]WALFrame, len(items))
	for i, it := range items {
		f, err := EncodeOp(it.slot, g.next+uint64(i), it.key, it.op)
		if err != nil {
			return 0, 0, l.classify(err)
		}
		frames[i] = f
	}
	if err := l.fl.AppendRun(group, g.epoch, frames); err != nil {
		return 0, 0, l.classify(err)
	}
	seq := g.next + uint64(len(items)-1)
	g.next += uint64(len(items))
	return group, seq, nil
}

// HashSet implements the shard seam: the written pairs as one colldelta
// hset, with a collnew ahead of it when the write created the hash and an
// hexpire behind it when a TTL-preserving verb kept a deadline the hset
// replay rule would clear. The collnew carries no hint bytes yet; doc 08
// owns that vocabulary and the encoding-hint slice fills them in.
func (l *WriteLog) HashSet(key []byte, created bool, fieldsValues [][]byte, keepAtMs int64) (uint16, uint64, error) {
	if len(fieldsValues) == 0 || len(fieldsValues)%2 != 0 {
		return 0, 0, l.classify(fmt.Errorf("obs1: a hash set emission needs field-value pairs, got %d items", len(fieldsValues)))
	}
	pairs := make([]FieldValue, len(fieldsValues)/2)
	for i := range pairs {
		pairs[i] = FieldValue{Field: fieldsValues[2*i], Value: fieldsValues[2*i+1]}
	}
	ops := make([]Op, 0, 3)
	if created {
		ops = append(ops, CollNew{Type: CollHash})
	}
	ops = append(ops, CollDelta{Sub: HSet{Pairs: pairs}})
	if keepAtMs != 0 {
		ops = append(ops, CollDelta{Sub: HExpire{AtMs: uint64(keepAtMs), Fields: fieldsValues[:1]}})
	}
	return l.emitOps(key, ops...)
}

// HashDel implements the shard seam: the removed fields as one colldelta
// hdel, with a colldrop behind it when the removal emptied the hash.
func (l *WriteLog) HashDel(key []byte, fields [][]byte, dropped bool) (uint16, uint64, error) {
	if dropped {
		return l.emitOps(key, CollDelta{Sub: HDel{Fields: fields}}, CollDrop{})
	}
	return l.emitOps(key, CollDelta{Sub: HDel{Fields: fields}})
}

// HashExpire implements the shard seam: one colldelta hexpire carrying the
// absolute deadline the named fields now ride, 0 clearing it.
func (l *WriteLog) HashExpire(key []byte, atMs int64, fields [][]byte) (uint16, uint64, error) {
	return l.emitOps(key, CollDelta{Sub: HExpire{AtMs: uint64(atMs), Fields: fields}})
}

// SetAdd implements the shard seam: the newly joined members as one
// colldelta sadd, with a collnew ahead of it when the write created the
// set. The collnew carries no hint bytes yet; doc 08 owns that
// vocabulary and the encoding-hint slice fills them in.
func (l *WriteLog) SetAdd(key []byte, created bool, members [][]byte) (uint16, uint64, error) {
	if created {
		return l.emitOps(key, CollNew{Type: CollSet}, CollDelta{Sub: SAdd{Members: members}})
	}
	return l.emitOps(key, CollDelta{Sub: SAdd{Members: members}})
}

// SetRem implements the shard seam: the removed members as one colldelta
// srem, with a colldrop behind it when the removal emptied the set.
// SPOP lands here as the srem it is (doc 04 section 2, no spop
// sub-kind), the drawn members recorded post-decision.
func (l *WriteLog) SetRem(key []byte, members [][]byte, dropped bool) (uint16, uint64, error) {
	if dropped {
		return l.emitOps(key, CollDelta{Sub: SRem{Members: members}}, CollDrop{})
	}
	return l.emitOps(key, CollDelta{Sub: SRem{Members: members}})
}

// SetStore implements the shard seam: a STORE form's wholesale
// replacement of its destination, one atomic run. A keydel leads when
// the string store held the key; a non-empty result follows as collnew
// plus the members as one sadd, the collnew replaying as reset-to-empty
// over a set the destination already held; an empty result instead
// trails a colldrop when there was a set to drop. The caller gates the
// no-effect case, so an empty op list here is the encode bug row.
func (l *WriteLog) SetStore(key []byte, delString, hadSet bool, members [][]byte) (uint16, uint64, error) {
	ops := make([]Op, 0, 3)
	if delString {
		ops = append(ops, KeyDel{})
	}
	if len(members) > 0 {
		ops = append(ops, CollNew{Type: CollSet}, CollDelta{Sub: SAdd{Members: members}})
	} else if hadSet {
		ops = append(ops, CollDrop{})
	}
	if len(ops) == 0 {
		return 0, 0, l.classify(fmt.Errorf("obs1: a set store emission with nothing to frame"))
	}
	return l.emitOps(key, ops...)
}

// SetMove implements the shard seam: SMOVE's two-key effect, a collnew
// plus sadd side on dst and an srem plus colldrop side on src. When the
// keys map to one group both sides ride one atomic run and srcSeq is 0,
// the dst mark covering the whole run. When the groups differ the
// destination run goes first, then the source run, each atomic alone:
// a tail cut between them replays the member into both sets rather
// than into neither, the disclosed cross-group non-atomicity. On a
// source-side error the destination frames are already buffered, so
// dstSeq rides the error for the caller to mark.
func (l *WriteLog) SetMove(src, dst, member []byte, srcDropped, dstCreated bool) (dstGroup uint16, dstSeq uint64, srcGroup uint16, srcSeq uint64, err error) {
	sslot, sgroup := l.mapKey(src)
	dslot, dgroup := l.mapKey(dst)
	one := [][]byte{member}
	if sgroup == dgroup {
		items := make([]keyedOp, 0, 4)
		if dstCreated {
			items = append(items, keyedOp{slot: dslot, key: dst, op: CollNew{Type: CollSet}})
		}
		items = append(items, keyedOp{slot: dslot, key: dst, op: CollDelta{Sub: SAdd{Members: one}}})
		items = append(items, keyedOp{slot: sslot, key: src, op: CollDelta{Sub: SRem{Members: one}}})
		if srcDropped {
			items = append(items, keyedOp{slot: sslot, key: src, op: CollDrop{}})
		}
		g, seq, err := l.emitKeyed(dgroup, items)
		return g, seq, 0, 0, err
	}
	dg, ds, err := l.SetAdd(dst, dstCreated, one)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	sg, ss, err := l.SetRem(src, one, srcDropped)
	if err != nil {
		return dg, ds, 0, 0, err
	}
	return dg, ds, sg, ss, nil
}

// NotifyCommitted implements the shard seam: fn runs once the group's
// committed watermark covers seq (Watermarks.Notify, inline when it
// already does). Registering also raises barrier demand, the doc 04
// section 3.2 rule that a pending strict ack lowers the effective flush
// age to the barrier floor; the barrier is floor-gated and a no-op when
// nothing is buffered, so a mark whose covering flush already left costs
// nothing extra.
func (l *WriteLog) NotifyCommitted(group uint16, seq uint64, fn func()) {
	l.marks.Notify(group, seq, fn)
	l.fl.Barrier()
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
