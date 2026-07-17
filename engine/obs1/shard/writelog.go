package shard

import (
	"errors"
	"sync/atomic"
)

// The durability seam (spec 2064/obs1 doc 04 sections 1 and 3.1). A write
// handler applies to RAM through the store as before, then emits the op's
// post-decision effect frame through the Ctx log, then writes its reply.
// With a log wired, the reply IS the relaxed ack: the frame sits in the
// group's WAL buffer when the client hears OK, and the flusher makes it
// durable behind the ack. With no log wired the runtime serves volatile,
// f3 parity, and the emission calls cost one nil check.
//
// The interface lives here because the command packages import shard and
// nothing else obs1; the composed implementation (flusher, committer,
// watermarks) is obs1.WriteLog one package up, wired in through
// Runtime.SetWriteLog before Start.

// WriteLog is what a write handler sees of the durability pipeline. Every
// method is owner-goroutine safe under the group-to-shard mapping: a group
// belongs to exactly one shard, so per-group state inside the
// implementation has a single writer, and cross-group admission is the
// implementation's own lock.
//
// An emission returns a nil error when the frame is buffered (the relaxed
// ack point) along with the frame's mark, the group and seq it took, which
// is what a strict ack later parks on. A non-nil error's text is the
// client reply, already in wire form ("ERR ..."): the handler fails the
// command with it and acks nothing, the doc 04 section 10 rule that the
// owner never acks what it could not frame. The mark is scalars rather
// than a shared struct so the implementation satisfies the seam without
// importing this package.
type WriteLog interface {
	// StrSet records the resulting value of a string write: SET verbatim,
	// the INCR family and APPEND and SETRANGE as the value the store now
	// holds (frames carry post-decision effects, doc 04 section 2).
	// expireAtMs is the absolute deadline riding on the key after the
	// write, 0 none; counter marks the INCR family for the doc 08 counter
	// encoding.
	StrSet(key, value []byte, expireAtMs int64, counter bool) (group uint16, seq uint64, err error)

	// KeyDel records a key removal of any type.
	KeyDel(key []byte) (group uint16, seq uint64, err error)

	// HashSet records applied hash-field writes as one colldelta hset of
	// the written pairs, preceded by a collnew when the write created the
	// hash. fieldsValues alternates field then value and carries
	// post-decision effects: HSETNX emits only a set that happened, the
	// hash INCR verbs emit the resulting rendering. keepAtMs, non-zero,
	// appends an hexpire restoring the first field's deadline, the
	// TTL-preserving verbs' counter to the hset replay rule that an
	// overwritten field's deadline clears (the HSET behavior); only a
	// single-pair write may carry it. A multi-frame emission returns its
	// last frame's seq, which covers the whole run under the per-group
	// monotone watermark.
	HashSet(key []byte, created bool, fieldsValues [][]byte, keepAtMs int64) (group uint16, seq uint64, err error)

	// HashDel records removed hash fields as one colldelta hdel, followed
	// by a colldrop when the removal emptied the hash. Post-decision:
	// fields lists only what actually left, and the HEXPIRE family's
	// set-to-the-past deletions land here too.
	HashDel(key []byte, fields [][]byte, dropped bool) (group uint16, seq uint64, err error)

	// HashExpire records a field-deadline change as one colldelta hexpire:
	// atMs is the absolute deadline the named fields now ride, 0 clears
	// (HPERSIST). Post-decision: the owner already applied the NX/XX/GT/LT
	// gate, so fields lists only the fields whose deadline changed.
	HashExpire(key []byte, atMs int64, fields [][]byte) (group uint16, seq uint64, err error)

	// SetAdd records applied set-member writes as one colldelta sadd,
	// preceded by a collnew when the write created the set.
	// Post-decision: members lists only what actually joined, so a
	// duplicate-only SADD frames nothing (the caller gates).
	SetAdd(key []byte, created bool, members [][]byte) (group uint16, seq uint64, err error)

	// SetRem records removed set members as one colldelta srem, followed
	// by a colldrop when the removal emptied the set. SPOP lands here as
	// the srem it is, its drawn members recorded post-decision.
	SetRem(key []byte, members [][]byte, dropped bool) (group uint16, seq uint64, err error)

	// SetStore records a STORE form's wholesale replacement of its
	// destination as one atomic run: a keydel when the string store held
	// the key, then for a non-empty result a collnew (replaying as
	// reset-to-empty over a set the destination already held) and the
	// result members as one sadd, or for an empty result a colldrop when
	// a set was there to drop. The caller gates the no-effect case, an
	// empty result over an absent destination.
	SetStore(key []byte, delString, hadSet bool, members [][]byte) (group uint16, seq uint64, err error)

	// SetMove records SMOVE's two-key effect: a collnew plus sadd side
	// on dst, an srem plus colldrop side on src. Both sides ride one
	// atomic run when the keys share a group, in which case srcSeq is 0
	// and the dst mark covers the run; otherwise the destination run
	// buffers first, then the source run, so a tail cut between them
	// duplicates the member rather than losing it, the disclosed
	// cross-group non-atomicity. A zero seq means that side buffered
	// nothing; a non-zero dstSeq rides even an errored call so the
	// caller marks what did land.
	SetMove(src, dst, member []byte, srcDropped, dstCreated bool) (dstGroup uint16, dstSeq uint64, srcGroup uint16, srcSeq uint64, err error)

	// ZSetAdd records applied sorted-set upserts as one colldelta zadd,
	// preceded by a collnew when the write created the sorted set. scores
	// parallels members (the seam speaks universe types), each pair
	// post-decision: the score the member now holds, so a ZADD flag miss
	// or a same-score write contributes no pair and an all-miss call
	// frames nothing (the caller gates). GEOADD lands here with geohash
	// scores.
	ZSetAdd(key []byte, created bool, scores []float64, members [][]byte) (group uint16, seq uint64, err error)

	// ZSetRem records removed sorted-set members as one colldelta zrem,
	// followed by a colldrop when the removal emptied the sorted set. The
	// pop family and the ZREMRANGEBY* verbs land here as the zrems they
	// are, members post-decision.
	ZSetRem(key []byte, members [][]byte, dropped bool) (group uint16, seq uint64, err error)

	// ZSetStore records a STORE form's wholesale replacement of its
	// destination as one atomic run, the sorted-set twin of SetStore: a
	// keydel when the string store held the key, then for a non-empty
	// result a collnew (replaying as reset-to-empty over a sorted set the
	// destination already held) and the result pairs as one zadd, or for
	// an empty result a colldrop when a sorted set was there to drop. The
	// caller gates the no-effect case, an empty result over an absent
	// destination.
	ZSetStore(key []byte, delString, hadZSet bool, scores []float64, members [][]byte) (group uint16, seq uint64, err error)

	// ListPush records pushed list values as one sided push (front picks
	// LPUSH's side), preceded by a collnew when the push created the list.
	// Post-decision: LPUSHX and RPUSHX emit only a push that happened (the
	// caller gates the miss).
	ListPush(key []byte, created, front bool, values [][]byte) (group uint16, seq uint64, err error)

	// ListPop records count removals from one end as a sided pop, followed
	// by a colldrop when the pop emptied the list. The frame carries only
	// the count: replay knows the elements it removes, so the values the
	// client saw never hit the WAL. A blocking serve lands here as the pop
	// it is.
	ListPop(key []byte, front bool, count int, dropped bool) (group uint16, seq uint64, err error)

	// ListSet records an element overwrite as one lset at the resolved
	// non-negative index; the handler normalizes a negative argument before
	// emitting.
	ListSet(key []byte, index int64, value []byte) (group uint16, seq uint64, err error)

	// ListTrim records LTRIM as the end cuts it is: an lpop of dropHead
	// and an rpop of dropTail as one run, or one bare colldrop when the
	// trim cleared the list (dropped, the clamp-fail form, in which case
	// the counts are ignored). The caller gates the no-effect trim.
	ListTrim(key []byte, dropHead, dropTail int, dropped bool) (group uint16, seq uint64, err error)

	// ListRem records LREM's removals as one lrem of the removed
	// positions, strictly ascending in the pre-removal list, followed by a
	// colldrop when the removal emptied it. Positions rather than the
	// value-count pair because replay applies overlay tombstones by
	// position (doc 08) and never rescans cold chunks for matches.
	ListRem(key []byte, indices []uint32, dropped bool) (group uint16, seq uint64, err error)

	// ListInsert records LINSERT as one lins carrying the resolved
	// position the value holds in the resulting list; the pivot search
	// happened in the handler and never hits the WAL.
	ListInsert(key []byte, index int64, value []byte) (group uint16, seq uint64, err error)

	// ListMove records LMOVE's two-key effect: a sided push on dst, a
	// sided pop on src. The same-key rotation is one run of pop then push
	// in command order. Distinct keys in one group ride one atomic run
	// destination first with srcSeq 0, the dst mark covering the run;
	// otherwise the destination run buffers first, then the source run, so
	// a tail cut between them duplicates the value rather than losing it,
	// the disclosed cross-group non-atomicity. A zero seq means that side
	// buffered nothing; a non-zero dstSeq rides even an errored call so
	// the caller marks what did land.
	ListMove(src, dst []byte, srcFront, dstFront bool, value []byte, srcDropped, dstCreated bool) (dstGroup uint16, dstSeq uint64, srcGroup uint16, srcSeq uint64, err error)

	// StreamAdd records one appended stream entry at the id the owner
	// assigned, as an xadd of the flat field-value pairs, with a collnew
	// ahead when the write created the stream and an xtrim of trimmed
	// behind when XADD's trim clause removed entries, one atomic run.
	StreamAdd(key []byte, created bool, idMs, idSeq uint64, fieldsValues [][]byte, trimmed uint64) (group uint16, seq uint64, err error)

	// StreamTrim records XTRIM's removals as one xtrim of the dropped
	// count; trims only ever remove a prefix of the oldest live entries
	// in id order, and never drop the stream, so no colldrop arm.
	StreamTrim(key []byte, removed uint64) (group uint16, seq uint64, err error)

	// StreamDel records XDEL's tombstones as one xdel of the ids that
	// actually removed, ms paralleling seqs in argument order; an emptied
	// stream persists, so no colldrop arm here either.
	StreamDel(key []byte, ms, seqs []uint64) (group uint16, seq uint64, err error)

	// StreamSetID records XSETID's resulting state as one xsetid of all
	// three values the stream now holds, the optional-argument merge
	// already done; the command requires the key, so a collnew never
	// leads.
	StreamSetID(key []byte, lastMs, lastSeq, entriesAdded, maxDelMs, maxDelSeq uint64) (group uint16, seq uint64, err error)

	// NotifyCommitted runs fn once a chain commit covering the marked
	// frame has landed and folded live (doc 04 section 3.2): it registers
	// on the committed watermark and raises barrier demand, so a pending
	// strict ack rides the barrier floor rather than the age trigger. An
	// already-covered mark fires fn before the call returns, on the
	// caller's goroutine; otherwise fn runs on the fold goroutine, so it
	// must not block (Conn.CompleteBlocked is safe from either). A mark
	// emitted under an epoch that has since been fenced never folds live,
	// so its fn never fires; the owner learns of the fence through the
	// lease machinery and fails the connection, not through this seam.
	NotifyCommitted(group uint16, seq uint64, fn func())

	// NotifyAllCommitted runs fn once chain commits cover every frame
	// emitted to any group before the call, the doc 04 section 3.3
	// chain commit barrier WAITAOF numlocal=1 maps to. A log with
	// nothing emitted, or nothing uncovered, fires fn before the call
	// returns on the caller's goroutine; otherwise fn runs on the fold
	// goroutine, so it must not block (Conn.CompleteBlocked is safe
	// from either). Like NotifyCommitted it raises barrier demand, and
	// a fenced group's frames never fold live, so fn then never fires.
	NotifyAllCommitted(fn func())
}

// WALMark names one emitted frame, the completion target a strict ack
// waits on: the group whose buffer took it and the per-group seq it drew.
type WALMark struct {
	Group uint16
	Seq   uint64
}

// LogStrSet emits a strset effect frame when a log is wired, and is free
// when none is. See WriteLog.StrSet for the contract; the mark lands on
// the Ctx when the connection asked for strict acks.
func (cx *Ctx) LogStrSet(key, value []byte, expireAtMs int64, counter bool) error {
	if cx.Log == nil {
		return nil
	}
	group, seq, err := cx.Log.StrSet(key, value, expireAtMs, counter)
	if err != nil {
		return err
	}
	cx.noteMark(group, seq)
	return nil
}

// LogKeyDel emits a keydel effect frame when a log is wired, and is free
// when none is.
func (cx *Ctx) LogKeyDel(key []byte) error {
	if cx.Log == nil {
		return nil
	}
	group, seq, err := cx.Log.KeyDel(key)
	if err != nil {
		return err
	}
	cx.noteMark(group, seq)
	return nil
}

// LogHashSet emits a hash write's effect frames when a log is wired, and
// is free when none is. See WriteLog.HashSet for the contract.
func (cx *Ctx) LogHashSet(key []byte, created bool, fieldsValues [][]byte, keepAtMs int64) error {
	if cx.Log == nil {
		return nil
	}
	group, seq, err := cx.Log.HashSet(key, created, fieldsValues, keepAtMs)
	if err != nil {
		return err
	}
	cx.noteMark(group, seq)
	return nil
}

// LogHashDel emits a hash removal's effect frames when a log is wired,
// and is free when none is. See WriteLog.HashDel for the contract.
func (cx *Ctx) LogHashDel(key []byte, fields [][]byte, dropped bool) error {
	if cx.Log == nil {
		return nil
	}
	group, seq, err := cx.Log.HashDel(key, fields, dropped)
	if err != nil {
		return err
	}
	cx.noteMark(group, seq)
	return nil
}

// LogHashExpire emits a field-deadline change's effect frame when a log
// is wired, and is free when none is. See WriteLog.HashExpire for the
// contract.
func (cx *Ctx) LogHashExpire(key []byte, atMs int64, fields [][]byte) error {
	if cx.Log == nil {
		return nil
	}
	group, seq, err := cx.Log.HashExpire(key, atMs, fields)
	if err != nil {
		return err
	}
	cx.noteMark(group, seq)
	return nil
}

// LogSetAdd emits a set write's effect frames when a log is wired, and
// is free when none is. See WriteLog.SetAdd for the contract.
func (cx *Ctx) LogSetAdd(key []byte, created bool, members [][]byte) error {
	if cx.Log == nil {
		return nil
	}
	group, seq, err := cx.Log.SetAdd(key, created, members)
	if err != nil {
		return err
	}
	cx.noteMark(group, seq)
	return nil
}

// LogSetRem emits a set removal's effect frames when a log is wired, and
// is free when none is. See WriteLog.SetRem for the contract.
func (cx *Ctx) LogSetRem(key []byte, members [][]byte, dropped bool) error {
	if cx.Log == nil {
		return nil
	}
	group, seq, err := cx.Log.SetRem(key, members, dropped)
	if err != nil {
		return err
	}
	cx.noteMark(group, seq)
	return nil
}

// LogSetStore emits a STORE form's destination replacement when a log is
// wired, and is free when none is. See WriteLog.SetStore for the
// contract.
func (cx *Ctx) LogSetStore(key []byte, delString, hadSet bool, members [][]byte) error {
	if cx.Log == nil {
		return nil
	}
	group, seq, err := cx.Log.SetStore(key, delString, hadSet, members)
	if err != nil {
		return err
	}
	cx.noteMark(group, seq)
	return nil
}

// LogSetMove emits SMOVE's two-key effect when a log is wired, and is
// free when none is. Marks land per side that buffered, even on an
// error, keeping the uniform rule that no reply on a strict connection
// races the frames emitted behind it. See WriteLog.SetMove for the
// contract.
func (cx *Ctx) LogSetMove(src, dst, member []byte, srcDropped, dstCreated bool) error {
	if cx.Log == nil {
		return nil
	}
	dg, ds, sg, ss, err := cx.Log.SetMove(src, dst, member, srcDropped, dstCreated)
	if ds != 0 {
		cx.noteMark(dg, ds)
	}
	if ss != 0 {
		cx.noteMark(sg, ss)
	}
	return err
}

// LogZSetAdd emits a sorted-set write's effect frames when a log is
// wired, and is free when none is. See WriteLog.ZSetAdd for the contract.
func (cx *Ctx) LogZSetAdd(key []byte, created bool, scores []float64, members [][]byte) error {
	if cx.Log == nil {
		return nil
	}
	group, seq, err := cx.Log.ZSetAdd(key, created, scores, members)
	if err != nil {
		return err
	}
	cx.noteMark(group, seq)
	return nil
}

// LogZSetRem emits a sorted-set removal's effect frames when a log is
// wired, and is free when none is. See WriteLog.ZSetRem for the contract.
func (cx *Ctx) LogZSetRem(key []byte, members [][]byte, dropped bool) error {
	if cx.Log == nil {
		return nil
	}
	group, seq, err := cx.Log.ZSetRem(key, members, dropped)
	if err != nil {
		return err
	}
	cx.noteMark(group, seq)
	return nil
}

// LogZSetStore emits a STORE form's destination replacement when a log
// is wired, and is free when none is. See WriteLog.ZSetStore for the
// contract.
func (cx *Ctx) LogZSetStore(key []byte, delString, hadZSet bool, scores []float64, members [][]byte) error {
	if cx.Log == nil {
		return nil
	}
	group, seq, err := cx.Log.ZSetStore(key, delString, hadZSet, scores, members)
	if err != nil {
		return err
	}
	cx.noteMark(group, seq)
	return nil
}

// LogListPush emits a list push's effect frames when a log is wired, and
// is free when none is. See WriteLog.ListPush for the contract.
func (cx *Ctx) LogListPush(key []byte, created, front bool, values [][]byte) error {
	if cx.Log == nil {
		return nil
	}
	group, seq, err := cx.Log.ListPush(key, created, front, values)
	if err != nil {
		return err
	}
	cx.noteMark(group, seq)
	return nil
}

// LogListPop emits a list pop's effect frames when a log is wired, and is
// free when none is. See WriteLog.ListPop for the contract.
func (cx *Ctx) LogListPop(key []byte, front bool, count int, dropped bool) error {
	_, err := cx.LogListPopServe(key, front, count, dropped)
	return err
}

// LogListPopServe is LogListPop returning the marks the pop drew, for a
// blocking serve whose reply completes on the waiter's own connection: a
// strict waiter's reply must wait for these frames through CompleteServed,
// and the marks also land on the running command's Ctx, the uniform rule
// that a pusher's strict reply covers every frame its execution emitted.
func (cx *Ctx) LogListPopServe(key []byte, front bool, count int, dropped bool) ([]WALMark, error) {
	if cx.Log == nil {
		return nil, nil
	}
	group, seq, err := cx.Log.ListPop(key, front, count, dropped)
	if err != nil {
		return nil, err
	}
	cx.noteMark(group, seq)
	return []WALMark{{Group: group, Seq: seq}}, nil
}

// LogListSet emits an element overwrite's effect frame when a log is
// wired, and is free when none is. See WriteLog.ListSet for the contract.
func (cx *Ctx) LogListSet(key []byte, index int64, value []byte) error {
	if cx.Log == nil {
		return nil
	}
	group, seq, err := cx.Log.ListSet(key, index, value)
	if err != nil {
		return err
	}
	cx.noteMark(group, seq)
	return nil
}

// LogListTrim emits a trim's effect frames when a log is wired, and is
// free when none is. See WriteLog.ListTrim for the contract.
func (cx *Ctx) LogListTrim(key []byte, dropHead, dropTail int, dropped bool) error {
	if cx.Log == nil {
		return nil
	}
	group, seq, err := cx.Log.ListTrim(key, dropHead, dropTail, dropped)
	if err != nil {
		return err
	}
	cx.noteMark(group, seq)
	return nil
}

// LogListRem emits LREM's effect frames when a log is wired, and is free
// when none is. See WriteLog.ListRem for the contract.
func (cx *Ctx) LogListRem(key []byte, indices []uint32, dropped bool) error {
	if cx.Log == nil {
		return nil
	}
	group, seq, err := cx.Log.ListRem(key, indices, dropped)
	if err != nil {
		return err
	}
	cx.noteMark(group, seq)
	return nil
}

// LogListInsert emits LINSERT's effect frame when a log is wired, and is
// free when none is. See WriteLog.ListInsert for the contract.
func (cx *Ctx) LogListInsert(key []byte, index int64, value []byte) error {
	if cx.Log == nil {
		return nil
	}
	group, seq, err := cx.Log.ListInsert(key, index, value)
	if err != nil {
		return err
	}
	cx.noteMark(group, seq)
	return nil
}

// LogListMove emits LMOVE's two-key effect when a log is wired, and is
// free when none is. See WriteLog.ListMove for the contract.
func (cx *Ctx) LogListMove(src, dst []byte, srcFront, dstFront bool, value []byte, srcDropped, dstCreated bool) error {
	_, err := cx.LogListMoveServe(src, dst, srcFront, dstFront, value, srcDropped, dstCreated)
	return err
}

// LogListMoveServe is LogListMove returning the marks the move drew, the
// serve-side twin of LogListPopServe for a waiter completed through a
// move. Marks land per side that buffered, even on an error, the
// LogSetMove rule.
func (cx *Ctx) LogListMoveServe(src, dst []byte, srcFront, dstFront bool, value []byte, srcDropped, dstCreated bool) ([]WALMark, error) {
	if cx.Log == nil {
		return nil, nil
	}
	dg, ds, sg, ss, err := cx.Log.ListMove(src, dst, srcFront, dstFront, value, srcDropped, dstCreated)
	var marks []WALMark
	if ds != 0 {
		cx.noteMark(dg, ds)
		marks = append(marks, WALMark{Group: dg, Seq: ds})
	}
	if ss != 0 {
		cx.noteMark(sg, ss)
		marks = append(marks, WALMark{Group: sg, Seq: ss})
	}
	return marks, err
}

// LogStreamAdd emits a stream append's effect frames when a log is
// wired, and is free when none is. See WriteLog.StreamAdd for the
// contract.
func (cx *Ctx) LogStreamAdd(key []byte, created bool, idMs, idSeq uint64, fieldsValues [][]byte, trimmed uint64) error {
	if cx.Log == nil {
		return nil
	}
	group, seq, err := cx.Log.StreamAdd(key, created, idMs, idSeq, fieldsValues, trimmed)
	if err != nil {
		return err
	}
	cx.noteMark(group, seq)
	return nil
}

// LogStreamTrim emits XTRIM's effect frame when a log is wired, and is
// free when none is. See WriteLog.StreamTrim for the contract.
func (cx *Ctx) LogStreamTrim(key []byte, removed uint64) error {
	if cx.Log == nil {
		return nil
	}
	group, seq, err := cx.Log.StreamTrim(key, removed)
	if err != nil {
		return err
	}
	cx.noteMark(group, seq)
	return nil
}

// LogStreamDel emits XDEL's effect frame when a log is wired, and is
// free when none is. See WriteLog.StreamDel for the contract.
func (cx *Ctx) LogStreamDel(key []byte, ms, seqs []uint64) error {
	if cx.Log == nil {
		return nil
	}
	group, seq, err := cx.Log.StreamDel(key, ms, seqs)
	if err != nil {
		return err
	}
	cx.noteMark(group, seq)
	return nil
}

// LogStreamSetID emits XSETID's effect frame when a log is wired, and is
// free when none is. See WriteLog.StreamSetID for the contract.
func (cx *Ctx) LogStreamSetID(key []byte, lastMs, lastSeq, entriesAdded, maxDelMs, maxDelSeq uint64) error {
	if cx.Log == nil {
		return nil
	}
	group, seq, err := cx.Log.StreamSetID(key, lastMs, lastSeq, entriesAdded, maxDelMs, maxDelSeq)
	if err != nil {
		return err
	}
	cx.noteMark(group, seq)
	return nil
}

// CompleteServed completes a served waiter's parked reply under the
// waiter's own ack mode: the command that woke it runs on some other
// connection's Ctx, so the pusher's marks say nothing about how strictly
// this reply may land. A relaxed waiter, an unlogged runtime, or a serve
// that framed nothing completes immediately; a strict waiter's reply
// parks on every mark through NotifyCommitted and lands when the last
// covering commit folds live. The reply bytes are copied before parking
// because serve loops reuse their buffers; the waiter's timer is already
// cancelled by the time a serve completes, so the deferred completion
// cannot race a timeout.
func (cx *Ctx) CompleteServed(conn *Conn, seq uint32, rep []byte, marks []WALMark) {
	if len(marks) == 0 || cx.Log == nil || !conn.strictAck.Load() {
		conn.CompleteBlocked(seq, rep)
		return
	}
	held := append([]byte(nil), rep...)
	if len(marks) == 1 {
		cx.Log.NotifyCommitted(marks[0].Group, marks[0].Seq, func() {
			conn.CompleteBlocked(seq, held)
		})
		return
	}
	pending := new(atomic.Int32)
	pending.Store(int32(len(marks)))
	for _, m := range marks {
		cx.Log.NotifyCommitted(m.Group, m.Seq, func() {
			if pending.Add(-1) == 0 {
				conn.CompleteBlocked(seq, held)
			}
		})
	}
}

// LogStrReadBack frames a write whose resulting string lives only in the
// store (APPEND, SETRANGE, SETBIT, BITFIELD, BITOP, the HLL surface): the
// whole value is read back, chunked values assembled through the stream
// the giant band serves reads with, and the deadline that rode through the
// write is read beside it. Free when no log is wired. An absent key frames
// nothing: the caller's write just landed, so absence only means the write
// itself removed the key, and the caller emits the keydel. A non-nil
// error's text is the wire reply, the LogStrSet contract.
func (cx *Ctx) LogStrReadBack(key []byte) error {
	if cx.Log == nil {
		return nil
	}
	v, cs, ok := cx.St.GetStream(key, cx.NowMs, cx.Val)
	cx.Val = v
	if !ok {
		return nil
	}
	if cs != nil {
		total := int(cs.Total())
		if cap(cx.Val) < total {
			cx.Val = make([]byte, total)
		}
		buf := cx.Val[:total]
		filled := 0
		for filled < total {
			n, err := cs.Next(buf[filled:])
			if err != nil || n == 0 {
				cs.Release()
				if err != nil {
					return errors.New("ERR " + err.Error())
				}
				break
			}
			filled += n
		}
		cs.Release()
		v = buf[:filled]
		cx.Val = v
	}
	return cx.LogStrSet(key, v, cx.St.ExpireAt(key, cx.NowMs), false)
}

// noteMark records an emitted frame on the running command when its
// connection asked for strict acks, one mark per touched group holding
// the group's highest seq: per-group seqs are monotone under the single
// owner, so waiting on the last frame covers every earlier one. A relaxed
// connection (the default, and every fan partial on one today) skips the
// append, so the relaxed path pays one atomic load per emission.
func (cx *Ctx) noteMark(group uint16, seq uint64) {
	if cx.curConn == nil || !cx.curConn.strictAck.Load() {
		return
	}
	for i := range cx.marks {
		if cx.marks[i].Group == group {
			cx.marks[i].Seq = seq
			return
		}
	}
	cx.marks = append(cx.marks, WALMark{Group: group, Seq: seq})
}

// GroupOfSlot maps a hash slot to its contiguous group among g groups,
// the doc 02 section 1.2 route Runtime.GroupOf uses. Exported so a
// WriteLog built outside a runtime (the server layer, tests) keys its
// per-group state on the same mapping the dispatcher routes by.
func GroupOfSlot(slot, g int) int {
	return groupOfSlot(slot, g)
}

// SetWriteLog wires the durability seam into every owner Ctx. Fixed
// before Start like the handler table: workers read cx.Log with plain
// loads on their own goroutines.
func (r *Runtime) SetWriteLog(l WriteLog) {
	if r.started {
		panic("shard: SetWriteLog after Start")
	}
	r.wlog = l
	for _, w := range r.workers {
		w.cx.Log = l
	}
}

// WriteLogged reports whether the connection's runtime has a durability
// pipeline wired, the dispatch-side guard that refuses a strict-ack
// request on a volatile node: with no log there are no frames and no
// watermark, so a strict write would hang forever instead of being
// stricter. Reads a fixed-before-Start field, safe from any goroutine.
func (c *Conn) WriteLogged() bool { return c.rt.wlog != nil }

// SetStrictAck flips the connection's ack mode (doc 04 section 3.2,
// AKI.DURABILITY): strict holds each write's reply until the chain
// commit covering its frames, relaxed (the default) acks on the buffer
// append. The dispatcher sets it reader-side before enqueueing the
// toggle's own reply, so every command it dispatches afterwards observes
// the new mode; a write already in flight on an owner may observe it
// early, which upgrades (or on a relaxed flip, relaxes) that one
// command's ack, the documented pipelining caveat.
func (c *Conn) SetStrictAck(on bool) { c.strictAck.Store(on) }

// StrictAck reports the connection's ack mode.
func (c *Conn) StrictAck() bool { return c.strictAck.Load() }

// SetWALInfo registers the durability INFO renderer: the function
// receives the stats text built so far and appends its own section
// (obs1.WriteLog.AppendInfo renders "# Durability"). Fixed before Start
// like SetNetInfo; it renders after the f3 section and before the
// transport's.
func (r *Runtime) SetWALInfo(f func([]byte) []byte) {
	if r.started {
		panic("shard: SetWALInfo after Start")
	}
	r.walInfo = f
}
