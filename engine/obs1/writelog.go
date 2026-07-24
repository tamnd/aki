package obs1

import (
	"bytes"
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

	// onKeyDel, when set, hears every successful keydel emission on the
	// owner goroutine, right after the frame's seq is drawn: the folder's
	// tombstone feed (SetKeyDelFeed). The key is borrowed; the callee
	// copies what it keeps.
	onKeyDel func(key []byte)

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
// f3 stall text (doc 04 section 6), the reply a flushlag-parked write
// takes when the stall window closes and the reply a fatal pipeline
// error maps to. errWALEncode is the doc 04 section 10 bug row: the
// owner never acks what it could not frame.
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

	// StartSeq is the first WAL object seq the flusher writes,
	// Recovery.NextWALSeq on a same-node restart; zero means 1, the
	// fresh-node shape. Reusing an occupied seq is unsafe: the PUT hits
	// our own tag on the previous incarnation's object and the recheck
	// silently adopts the old content (#1074's recorded hazard).
	StartSeq uint64

	// OnCommitted, when set, hears every committed WAL seq in order, the
	// doc 06 fold-accounting seam passed through to the committer.
	OnCommitted func(walSeq uint64, pos ChainPos)

	// OnVerdict, when set, hears every commit verdict before the watermarks
	// apply it: the manifest publisher's coverage feed
	// (ManifestPublisher.OnVerdict), which must observe the covering
	// position before ApplyVerdict can release a fold's publish gate.
	OnVerdict func(CommitVerdict) error

	// Gate, when set, is the node's lease gate: the committer's append hook
	// renews the groups each landed batch carried, so a serving gate's
	// deadlines extend exactly when the doc 02 section 3.5 progress rule
	// says they may. Nil on a single-node pipeline.
	Gate *LeaseGate
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
	if onVerdict := cfg.OnVerdict; onVerdict != nil {
		cfg.Fold.OnCommit = func(v CommitVerdict) error {
			if err := onVerdict(v); err != nil {
				return err
			}
			return marks.ApplyVerdict(v)
		}
	} else {
		cfg.Fold.OnCommit = marks.ApplyVerdict
	}
	ccfg := CommitterConfig{
		Chain:       cfg.Chain,
		Node:        cfg.Node,
		OnCommitted: cfg.OnCommitted,
	}
	if cfg.Gate != nil {
		ccfg.OnAppended = cfg.Gate.OnAppended
	}
	cm, err := NewCommitter(ccfg)
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
		StartSeq:     cfg.StartSeq,
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
	if l.onKeyDel != nil {
		// Fed after the seq draw so a mark the folder takes now covers
		// this delete (Folder.Delete's contract, doc 06 section 1.3).
		l.onKeyDel(key)
	}
	return group, seq, nil
}

// Expire implements the shard seam: one expire frame carrying the
// absolute deadline the key now rides, 0 for persist, any type.
func (l *WriteLog) Expire(key []byte, atMs int64) (uint16, uint64, error) {
	return l.emitOps(key, Expire{ExpiryMS: uint64(atMs)})
}

// SetKeyDelFeed registers fn to hear every successful keydel emission,
// normally Folder.Delete. Fixed after construction and before any owner
// serves writes, the SetGroup publication rule.
func (l *WriteLog) SetKeyDelFeed(fn func(key []byte)) { l.onKeyDel = fn }

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

// zaddEntries pairs the seam's parallel score and member slices into the
// op's entry list; the split exists because the shard package cannot
// import this one, so the seam speaks universe types.
func zaddEntries(scores []float64, members [][]byte) ([]ScoreMember, error) {
	if len(scores) == 0 || len(scores) != len(members) {
		return nil, fmt.Errorf("obs1: a zset emission needs parallel scores and members, got %d and %d", len(scores), len(members))
	}
	entries := make([]ScoreMember, len(members))
	for i := range entries {
		entries[i] = ScoreMember{Score: scores[i], Member: members[i]}
	}
	return entries, nil
}

// ZSetAdd implements the shard seam: the applied scored upserts as one
// colldelta zadd, with a collnew ahead of it when the write created the
// sorted set. scores parallels members; each pair is post-decision, the
// score the member now holds. The collnew carries no hint bytes yet; doc
// 08 owns that vocabulary and the encoding-hint slice fills them in.
func (l *WriteLog) ZSetAdd(key []byte, created bool, scores []float64, members [][]byte) (uint16, uint64, error) {
	entries, err := zaddEntries(scores, members)
	if err != nil {
		return 0, 0, l.classify(err)
	}
	if created {
		return l.emitOps(key, CollNew{Type: CollZSet}, CollDelta{Sub: ZAdd{Entries: entries}})
	}
	return l.emitOps(key, CollDelta{Sub: ZAdd{Entries: entries}})
}

// ZSetRem implements the shard seam: the removed members as one colldelta
// zrem, with a colldrop behind it when the removal emptied the sorted
// set. The pop family and the ZREMRANGEBY* verbs land here as the zrems
// they are (doc 04 section 2, no pop sub-kind), members post-decision.
func (l *WriteLog) ZSetRem(key []byte, members [][]byte, dropped bool) (uint16, uint64, error) {
	if dropped {
		return l.emitOps(key, CollDelta{Sub: ZRem{Members: members}}, CollDrop{})
	}
	return l.emitOps(key, CollDelta{Sub: ZRem{Members: members}})
}

// ZSetStore implements the shard seam: a STORE form's wholesale
// replacement of its destination, one atomic run, the sorted-set twin of
// SetStore. A keydel leads when the string store held the key; a
// non-empty result follows as collnew plus the pairs as one zadd, the
// collnew replaying as reset-to-empty over a sorted set the destination
// already held; an empty result instead trails a colldrop when there was
// a sorted set to drop. The caller gates the no-effect case, so an empty
// op list here is the encode bug row.
func (l *WriteLog) ZSetStore(key []byte, delString, hadZSet bool, scores []float64, members [][]byte) (uint16, uint64, error) {
	ops := make([]Op, 0, 3)
	if delString {
		ops = append(ops, KeyDel{})
	}
	if len(members) > 0 {
		entries, err := zaddEntries(scores, members)
		if err != nil {
			return 0, 0, l.classify(err)
		}
		ops = append(ops, CollNew{Type: CollZSet}, CollDelta{Sub: ZAdd{Entries: entries}})
	} else if hadZSet {
		ops = append(ops, CollDrop{})
	}
	if len(ops) == 0 {
		return 0, 0, l.classify(fmt.Errorf("obs1: a zset store emission with nothing to frame"))
	}
	return l.emitOps(key, ops...)
}

// pushDelta picks the sided push sub-op for one value list.
func pushDelta(front bool, values [][]byte) Op {
	if front {
		return CollDelta{Sub: LPush{Values: values}}
	}
	return CollDelta{Sub: RPush{Values: values}}
}

// popDelta picks the sided pop sub-op for a count of removals.
func popDelta(front bool, count uint32) Op {
	if front {
		return CollDelta{Sub: LPop{Count: count}}
	}
	return CollDelta{Sub: RPop{Count: count}}
}

// ListPush implements the shard seam: the pushed values as one sided
// push, with a collnew ahead of it when the write created the list. The
// collnew carries no hint bytes yet; doc 08 owns that vocabulary and the
// encoding-hint slice fills them in.
func (l *WriteLog) ListPush(key []byte, created, front bool, values [][]byte) (uint16, uint64, error) {
	if created {
		return l.emitOps(key, CollNew{Type: CollList}, pushDelta(front, values))
	}
	return l.emitOps(key, pushDelta(front, values))
}

// ListPop implements the shard seam: count removals from one end as a
// sided pop, with a colldrop behind it when the pop emptied the list.
// The frame carries only the count because replay knows the elements it
// is removing; the values the client saw never hit the WAL.
func (l *WriteLog) ListPop(key []byte, front bool, count int, dropped bool) (uint16, uint64, error) {
	if dropped {
		return l.emitOps(key, popDelta(front, uint32(count)), CollDrop{})
	}
	return l.emitOps(key, popDelta(front, uint32(count)))
}

// ListSet implements the shard seam: one lset carrying the resolved
// non-negative index and the value it now holds.
func (l *WriteLog) ListSet(key []byte, index int64, value []byte) (uint16, uint64, error) {
	return l.emitOps(key, CollDelta{Sub: LSet{Index: index, Value: value}})
}

// ListTrim implements the shard seam: LTRIM as the end cuts it is, an
// lpop run for the head drop and an rpop for the tail drop (doc 04
// section 2, no trim sub-kind). A trim that clears the list, the
// clamp-fail form, frames one bare colldrop instead; a kept range always
// keeps at least one element, so pops and the drop never mix. The caller
// gates the no-effect trim, so an empty op list here is the encode bug
// row.
func (l *WriteLog) ListTrim(key []byte, dropHead, dropTail int, dropped bool) (uint16, uint64, error) {
	if dropped {
		return l.emitOps(key, CollDrop{})
	}
	ops := make([]Op, 0, 2)
	if dropHead > 0 {
		ops = append(ops, popDelta(true, uint32(dropHead)))
	}
	if dropTail > 0 {
		ops = append(ops, popDelta(false, uint32(dropTail)))
	}
	if len(ops) == 0 {
		return 0, 0, l.classify(fmt.Errorf("obs1: a list trim emission with nothing to frame"))
	}
	return l.emitOps(key, ops...)
}

// ListRem implements the shard seam: the removed positions as one lrem,
// indices strictly ascending in the pre-removal list, with a colldrop
// behind it when the removal emptied the list.
func (l *WriteLog) ListRem(key []byte, indices []uint32, dropped bool) (uint16, uint64, error) {
	if dropped {
		return l.emitOps(key, CollDelta{Sub: LRem{Indices: indices}}, CollDrop{})
	}
	return l.emitOps(key, CollDelta{Sub: LRem{Indices: indices}})
}

// ListInsert implements the shard seam: one lins carrying the resolved
// position the value holds in the resulting list; the pivot search
// happened in the handler and never hits the WAL.
func (l *WriteLog) ListInsert(key []byte, index int64, value []byte) (uint16, uint64, error) {
	return l.emitOps(key, CollDelta{Sub: LIns{Index: index, Value: value}})
}

// ListMove implements the shard seam: LMOVE's two-key effect, a push
// side on dst and a pop side on src, each sided by its own flag. The
// same-key rotation is one run of the source pop then the destination
// push, pop first because both frames rewrite one list and replay must
// see them in command order. Distinct keys in one group ride one atomic
// run destination first and srcSeq is 0, the dst mark covering the whole
// run. When the groups differ the destination run goes first, then the
// source run, each atomic alone: a tail cut between them replays the
// value into both lists rather than into neither, the disclosed
// cross-group non-atomicity. On a source-side error the destination
// frames are already buffered, so dstSeq rides the error for the caller
// to mark.
func (l *WriteLog) ListMove(src, dst []byte, srcFront, dstFront bool, value []byte, srcDropped, dstCreated bool) (dstGroup uint16, dstSeq uint64, srcGroup uint16, srcSeq uint64, err error) {
	one := [][]byte{value}
	if bytes.Equal(src, dst) {
		g, seq, err := l.emitOps(src, popDelta(srcFront, 1), pushDelta(dstFront, one))
		return g, seq, 0, 0, err
	}
	sslot, sgroup := l.mapKey(src)
	dslot, dgroup := l.mapKey(dst)
	if sgroup == dgroup {
		items := make([]keyedOp, 0, 4)
		if dstCreated {
			items = append(items, keyedOp{slot: dslot, key: dst, op: CollNew{Type: CollList}})
		}
		items = append(items, keyedOp{slot: dslot, key: dst, op: pushDelta(dstFront, one)})
		items = append(items, keyedOp{slot: sslot, key: src, op: popDelta(srcFront, 1)})
		if srcDropped {
			items = append(items, keyedOp{slot: sslot, key: src, op: CollDrop{}})
		}
		g, seq, err := l.emitKeyed(dgroup, items)
		return g, seq, 0, 0, err
	}
	dg, ds, err := l.ListPush(dst, dstCreated, dstFront, one)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	sg, ss, err := l.ListPop(src, srcFront, 1, srcDropped)
	if err != nil {
		return dg, ds, 0, 0, err
	}
	return dg, ds, sg, ss, nil
}

// StreamAdd implements the shard seam: one appended entry at the id the
// owner assigned, as an xadd carrying the flat field-value pairs, with a
// collnew ahead of it when the write created the stream and an xtrim of
// trimmed entries behind it when XADD's trim clause removed any, all one
// atomic run since they are one command's effect.
func (l *WriteLog) StreamAdd(key []byte, created bool, idMs, idSeq uint64, fieldsValues [][]byte, trimmed uint64) (uint16, uint64, error) {
	if len(fieldsValues) == 0 || len(fieldsValues)%2 != 0 {
		return 0, 0, l.classify(fmt.Errorf("obs1: a stream add emission needs field-value pairs, got %d items", len(fieldsValues)))
	}
	pairs := make([]FieldValue, len(fieldsValues)/2)
	for i := range pairs {
		pairs[i] = FieldValue{Field: fieldsValues[2*i], Value: fieldsValues[2*i+1]}
	}
	ops := make([]Op, 0, 3)
	if created {
		ops = append(ops, CollNew{Type: CollStream})
	}
	ops = append(ops, CollDelta{Sub: XAdd{IDMs: idMs, IDSeq: idSeq, Pairs: pairs}})
	if trimmed > 0 {
		ops = append(ops, CollDelta{Sub: XTrim{Count: trimmed}})
	}
	return l.emitOps(key, ops...)
}

// StreamTrim implements the shard seam: XTRIM's removals as one xtrim of
// the count of dropped entries. Both trim bands only ever remove a
// prefix of the oldest live entries in id order, so the count is the
// whole post-decision effect. A trim never drops the stream, so there is
// no colldrop arm.
func (l *WriteLog) StreamTrim(key []byte, removed uint64) (uint16, uint64, error) {
	return l.emitOps(key, CollDelta{Sub: XTrim{Count: removed}})
}

// StreamDel implements the shard seam: XDEL's tombstones as one xdel of
// the ids that actually removed, ms paralleling seqs in argument order.
// An emptied stream persists (lastID never moves back), so there is no
// colldrop arm here either.
func (l *WriteLog) StreamDel(key []byte, ms, seqs []uint64) (uint16, uint64, error) {
	return l.emitOps(key, CollDelta{Sub: XDel{IDMs: ms, IDSeq: seqs}})
}

// StreamSetID implements the shard seam: XSETID's resulting state as one
// xsetid carrying all three values the stream now holds, last id,
// entries-added, and max-deleted id, the optional-argument merge already
// done by the owner. The command requires the key to exist, so a collnew
// never leads.
func (l *WriteLog) StreamSetID(key []byte, lastMs, lastSeq, entriesAdded, maxDelMs, maxDelSeq uint64) (uint16, uint64, error) {
	return l.emitOps(key, CollDelta{Sub: XSetID{
		LastMs:       lastMs,
		LastSeq:      lastSeq,
		EntriesAdded: entriesAdded,
		MaxDelMs:     maxDelMs,
		MaxDelSeq:    maxDelSeq,
	}})
}

// StreamGroupNew implements the shard seam: XGROUP CREATE's resulting
// cursor and lag basis, with a collnew ahead of it when MKSTREAM created
// the stream, one atomic run since they are one command's effect.
func (l *WriteLog) StreamGroupNew(key []byte, createdStream bool, group []byte, lastMs, lastSeq, entriesRead uint64, readValid bool) (uint16, uint64, error) {
	sub := GNew{Group: group, LastMs: lastMs, LastSeq: lastSeq, EntriesRead: entriesRead, ReadValid: readValid}
	if createdStream {
		return l.emitOps(key, CollNew{Type: CollStream}, GroupDelta{Sub: sub})
	}
	return l.emitOps(key, GroupDelta{Sub: sub})
}

// StreamGroupSetID implements the shard seam: the resulting cursor and
// lag basis after XGROUP SETID's optional-argument merge.
func (l *WriteLog) StreamGroupSetID(key, group []byte, lastMs, lastSeq, entriesRead uint64, readValid bool) (uint16, uint64, error) {
	return l.emitOps(key, GroupDelta{Sub: GSetID{Group: group, LastMs: lastMs, LastSeq: lastSeq, EntriesRead: entriesRead, ReadValid: readValid}})
}

// StreamGroupDrop implements the shard seam: XGROUP DESTROY of a group
// that existed; the group's consumers and PEL leave with it.
func (l *WriteLog) StreamGroupDrop(key, group []byte) (uint16, uint64, error) {
	return l.emitOps(key, GroupDelta{Sub: GDrop{Group: group}})
}

// StreamConsumerNew implements the shard seam: XGROUP CREATECONSUMER
// when it created one, seen at seenMs with no activity yet.
func (l *WriteLog) StreamConsumerNew(key, group, consumer []byte, seenMs int64) (uint16, uint64, error) {
	return l.emitOps(key, GroupDelta{Sub: GConsumerNew{Group: group, Consumer: consumer, SeenMs: seenMs}})
}

// StreamConsumerDel implements the shard seam: XGROUP DELCONSUMER of a
// consumer that existed; replay drains its pending entries by owner, so
// no id list rides the frame.
func (l *WriteLog) StreamConsumerDel(key, group, consumer []byte) (uint16, uint64, error) {
	return l.emitOps(key, GroupDelta{Sub: GConsumerDel{Group: group, Consumer: consumer}})
}

// StreamAck implements the shard seam: the ids that actually left the
// group's PEL, XACK's acknowledgments and the claim-path removals of
// deleted entries alike.
func (l *WriteLog) StreamAck(key, group []byte, ms, seqs []uint64) (uint16, uint64, error) {
	return l.emitOps(key, GroupDelta{Sub: GAck{Group: group, IDMs: ms, IDSeq: seqs}})
}

// StreamDeliver implements the shard seam: an XREADGROUP new-message
// delivery, cursor advanced to the last id listed, PEL entries inserted
// at timeMs unless the read was NOACK.
func (l *WriteLog) StreamDeliver(key, group, consumer []byte, noAck bool, timeMs int64, ms, seqs []uint64) (uint16, uint64, error) {
	return l.emitOps(key, GroupDelta{Sub: GDeliver{Group: group, Consumer: consumer, NoAck: noAck, TimeMs: timeMs, IDMs: ms, IDSeq: seqs}})
}

// StreamClaim implements the shard seam: resulting PEL entry state per
// id as XCLAIM, XAUTOCLAIM, or XNACK left it, owner and delivery time
// and delivery count all post-decision. Unowned is the XNACK shape. The
// claim paths also drop PEL entries whose log entries are gone; those
// ids ride a gack behind the gclaim in the same atomic run since they
// are one command's effect. Either half may be empty, not both.
func (l *WriteLog) StreamClaim(key, group, consumer []byte, unowned bool, ms, seqs []uint64, times []int64, counts []uint16, dropMs, dropSeqs []uint64) (uint16, uint64, error) {
	if len(ms) == 0 && len(dropMs) == 0 {
		return 0, 0, l.classify(fmt.Errorf("obs1: a stream claim emission needs claimed or dropped ids"))
	}
	ops := make([]Op, 0, 2)
	if len(ms) > 0 {
		ops = append(ops, GroupDelta{Sub: GClaim{Group: group, Consumer: consumer, Unowned: unowned, IDMs: ms, IDSeq: seqs, TimeMs: times, Counts: counts}})
	}
	if len(dropMs) > 0 {
		ops = append(ops, GroupDelta{Sub: GAck{Group: group, IDMs: dropMs, IDSeq: dropSeqs}})
	}
	return l.emitOps(key, ops...)
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

// NotifyAllCommitted implements the shard seam: fn runs once chain
// commits cover every frame any group had emitted when the call was
// made, the doc 04 section 3.3 chain commit barrier WAITAOF parks on.
// The snapshot is taken under the flusher's buffer lock, the same lock
// every emission takes, so it sits at a definite point in the emission
// order; a log that has emitted nothing, or whose whole snapshot is
// already covered, fires fn before the call returns on the caller's
// goroutine. Registering raises barrier demand like NotifyCommitted,
// and a fenced group's frames never fold live, so fn then never fires,
// the same silence a strict ack shows.
func (l *WriteLog) NotifyAllCommitted(fn func()) {
	groups, seqs := l.fl.lastEmitted()
	if len(groups) == 0 {
		fn()
		return
	}
	if len(groups) == 1 {
		l.NotifyCommitted(groups[0], seqs[0], fn)
		return
	}
	// The strict.go countdown shape: each mark's callback fires exactly
	// once, the last covering commit runs fn.
	left := new(atomic.Int32)
	left.Store(int32(len(groups)))
	for i := range groups {
		l.marks.Notify(groups[i], seqs[i], func() {
			if left.Add(-1) == 0 {
				fn()
			}
		})
	}
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
// 04 section 10 taxonomy: a closed or failed flusher is fatal and reads
// as a stall to the client, and anything else is the encode bug row,
// which panics in a dev build (-tags obs1dev) and fails just the
// command in release. The cap no longer produces an append error at
// all: the shard gate parks the write on the flushlag reason before its
// handler runs (FlushLagged), so the stallErrs counter behind the
// wal_stall_errors row only moves if a future path reintroduces a
// cap-side refusal; the row stays rendered for schema stability.
func (l *WriteLog) classify(err error) error {
	switch {
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

// FlushLagged mirrors the flusher's cap flag for the shard gate: the
// WAL buffer plus in-flight PUT bytes sit over the cap, so the next
// write handler parks on the flushlag reason instead of running
// (doc 04 section 6). One atomic load.
func (l *WriteLog) FlushLagged() bool { return l.fl.Lagged() }

// FlushCount counts successful WAL PUTs, the flushlag progress signal
// the stall window checks: a parked write is making progress exactly
// when this advances.
func (l *WriteLog) FlushCount() uint64 { return l.fl.FlushCount() }

// SetFlushAge retunes the age trigger live, the thrift profile knob
// passed through to the flusher.
func (l *WriteLog) SetFlushAge(d time.Duration) { l.fl.SetFlushAge(d) }

// Marks is the committed-watermark surface strict acks and WAIT
// barriers park on.
func (l *WriteLog) Marks() *Watermarks { return l.marks }

// GroupMark returns the group's lease epoch and the last WAL seq emitted
// on it, zero when nothing has been emitted since SetGroup. This is the
// fold-eligibility snapshot (doc 06 section 1.2): emission and cold
// staging share the group owner's goroutine, so a record staged now
// reflects every mutation through the returned seq, and a segment built
// from the stage publishes once the committed watermark covers it. Call it
// only from the owning shard's goroutine; wlGroup state is single-writer
// and unsynchronized.
func (l *WriteLog) GroupMark(group uint16) (epoch uint32, last uint64) {
	g := &l.groups[group]
	if g.next == 0 {
		return g.epoch, 0
	}
	return g.epoch, g.next - 1
}

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
