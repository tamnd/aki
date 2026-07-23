package sqlo1

// The PEL, doc 10's kind 5: XREADGROUP delivery appends pending
// entries to ID-partitioned segments behind the group record's PEL
// fence, XACK removes them, and the history form re-reads a consumer's
// own pending IDs. X-I3 is the slice's law: PEL churn never touches
// entry runs and entry reads never touch PEL segments; the group
// record plays the root-discipline role for its fence, so a fresh
// segment flushes before the record that references it and everything
// else rides one atomic batch (X-I5). The xpel lab (#1270) bakes the
// caps: segments cut at 4096 encoded bytes with a 1024-entry backstop
// that never binds at that byte cap, and the fence stays inline in the
// record until the paging follow-up, refusing deliveries past the
// inline budget the way the flat run fence refused before kind 3.

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"
)

// streamSubkindPelSeg is the stream plane's PEL segment kind, doc 10's
// kind 5. Segment IDs are minted from the root's next_segid like runs
// and fence pages, so a cut writes the root, which coalesces in the
// hot tier like every root-adjacent write.
const streamSubkindPelSeg uint8 = 5

// streamPelFenceEntLen is the on-disk PEL fence entry: base ms and
// seq, the segment ID, and the segment's entry count.
const streamPelFenceEntLen = 28

// streamPelSegHdrLen is the segment payload header: u16 n, u16
// reserved.
const streamPelSegHdrLen = 4

// The xpel caps. Vars, not consts, so tests reach the cut and refusal
// paths at test-sized PELs; nothing outside tests writes them.
var (
	// streamPelSegMaxBytes cuts a segment at 4096 encoded bytes, the
	// xpel verdict: 2048 edges it by 10 percent on the FIFO write bill
	// but 4096 wins random-order ACK, the full walk, and the segment
	// count, and lines up with every other 4 KiB record.
	streamPelSegMaxBytes = 4096

	// streamPelSegMaxEnts is the entry backstop, which never binds at
	// the byte cap (a 4096-byte segment holds ~250 entries).
	streamPelSegMaxEnts = 1024

	// streamPelFenceMax bounds the inline fence to the same record
	// budget the flat run fence used, ~18K pending entries at the
	// production caps; the paging follow-up lifts it, the kind 3
	// ladder at the group record.
	streamPelFenceMax = (listInlineMax - streamGroupHdrLen) / streamPelFenceEntLen
)

// errStreamPelFenceFull is the inline fence's temporary refusal, the
// 70-run flat fence precedent: a refused delivery is side-effect free
// and the fence paging slice lifts the cap.
var errStreamPelFenceFull = errors.New("sqlo1: stream group PEL fence is full until the paging slice")

// errStreamPelConsumerCap refuses a delivery for a consumer past the
// PEL's u8 index, 256 consumers per group with pending entries.
var errStreamPelConsumerCap = errors.New("sqlo1: stream group PEL is capped at 256 consumers")

// streamPelFenceEnt is one fence slot: the segment's first pending ID,
// its segid, and its live entry count.
type streamPelFenceEnt struct {
	base  streamID
	segid uint64
	count uint32
}

// streamPelEnt is one pending entry: the entry ID, the owning
// consumer's index into the group's consumer table, the delivery
// count, and the last delivery time. flags is reserved zero.
type streamPelEnt struct {
	id     streamID
	cidx   uint8
	flags  uint8
	dcount uint32
	dtime  int64
}

// Segment payload: u16 n, u16 reserved, then n entries of { varint
// dms, varint dseq, u8 consumer_idx, u8 flags, u32 delivery_count,
// u64 delivery_time } in strict ID order. The delta rule matches the
// run codec: dms is the gap from the previous entry's ms (the first
// entry's from zero), and dseq is the absolute seq when dms is
// nonzero, the seq gap when dms is zero. Canonical form: exact length,
// minimal varints, strictly increasing IDs, zero flags.

// streamPelEntLen is the encoded width of e after prev.
func streamPelEntLen(prev, id streamID) int {
	dms := id.ms - prev.ms
	dseq := id.seq
	if dms == 0 {
		dseq = id.seq - prev.seq
	}
	return streamUvarintLen(dms) + streamUvarintLen(dseq) + 14
}

// appendStreamPelSeg encodes ents as one segment payload onto dst. The
// contract violations are writer bugs, so they panic: one entry at
// least, strict ID order, zero flags, delivery times in int64 range.
func appendStreamPelSeg(dst []byte, ents []streamPelEnt) []byte {
	if len(ents) == 0 || len(ents) > math.MaxUint16 {
		panic("sqlo1: stream PEL segment entry count out of range")
	}
	dst = binary.LittleEndian.AppendUint16(dst, uint16(len(ents)))
	dst = binary.LittleEndian.AppendUint16(dst, 0)
	prev := streamID{}
	for i := range ents {
		e := &ents[i]
		if !prev.less(e.id) {
			panic("sqlo1: stream PEL segment out of ID order")
		}
		if e.flags != 0 || e.dtime < 0 {
			panic("sqlo1: stream PEL entry has reserved bits set")
		}
		dms := e.id.ms - prev.ms
		dseq := e.id.seq
		if dms == 0 {
			dseq = e.id.seq - prev.seq
		}
		dst = binary.AppendUvarint(dst, dms)
		dst = binary.AppendUvarint(dst, dseq)
		dst = append(dst, e.cidx, e.flags)
		dst = binary.LittleEndian.AppendUint32(dst, e.dcount)
		dst = binary.LittleEndian.AppendUint64(dst, uint64(e.dtime))
		prev = e.id
	}
	return dst
}

// decodeStreamPelSeg validates v and decodes it into the ents scratch,
// rejecting every non-canonical byte string so decode-then-reencode
// equality holds.
func decodeStreamPelSeg(v []byte, ents []streamPelEnt) ([]streamPelEnt, error) {
	if len(v) < streamPelSegHdrLen {
		return nil, fmt.Errorf("sqlo1: stream PEL segment of %d bytes has no header", len(v))
	}
	n := int(binary.LittleEndian.Uint16(v[0:]))
	if n == 0 {
		return nil, errors.New("sqlo1: stream PEL segment is empty")
	}
	if binary.LittleEndian.Uint16(v[2:]) != 0 {
		return nil, errors.New("sqlo1: stream PEL segment has reserved header bits set")
	}
	p := v[streamPelSegHdrLen:]
	prev := streamID{}
	for i := range n {
		dms, rest, err := streamRunUvarint(p, i, "PEL dms")
		if err != nil {
			return nil, err
		}
		dseq, rest, err := streamRunUvarint(rest, i, "PEL dseq")
		if err != nil {
			return nil, err
		}
		var e streamPelEnt
		if dms == 0 {
			if dseq == 0 {
				return nil, fmt.Errorf("sqlo1: stream PEL entry %d does not advance", i)
			}
			if dseq > math.MaxUint64-prev.seq {
				return nil, fmt.Errorf("sqlo1: stream PEL entry %d overflows seq", i)
			}
			e.id = streamID{ms: prev.ms, seq: prev.seq + dseq}
		} else {
			if dms > math.MaxUint64-prev.ms {
				return nil, fmt.Errorf("sqlo1: stream PEL entry %d overflows ms", i)
			}
			e.id = streamID{ms: prev.ms + dms, seq: dseq}
		}
		if len(rest) < 14 {
			return nil, fmt.Errorf("sqlo1: stream PEL segment truncates entry %d", i)
		}
		e.cidx = rest[0]
		e.flags = rest[1]
		if e.flags != 0 {
			return nil, fmt.Errorf("sqlo1: stream PEL entry %d has reserved flags %#x", i, e.flags)
		}
		e.dcount = binary.LittleEndian.Uint32(rest[2:])
		e.dtime = int64(binary.LittleEndian.Uint64(rest[6:]))
		if e.dtime < 0 {
			return nil, fmt.Errorf("sqlo1: stream PEL entry %d has a negative delivery time", i)
		}
		ents = append(ents, e)
		prev = e.id
		p = rest[14:]
	}
	if len(p) != 0 {
		return nil, fmt.Errorf("sqlo1: stream PEL segment has %d trailing bytes", len(p))
	}
	return ents, nil
}

// putStreamPelKey writes the subkey of PEL segment segid under rooth,
// the doc 03 6.3 layout with kind streamSubkindPelSeg.
func putStreamPelKey(dst []byte, rooth uint64, segid uint64) {
	binary.LittleEndian.PutUint64(dst, rooth)
	dst[8] = streamSubkindPelSeg
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], segid)
	copy(dst[9:SubkeySize], b[:7])
}

// readPelSeg reads segment segid into the pelEnts scratch.
func (x *Stream) readPelSeg(ctx context.Context, segid uint64) ([]streamPelEnt, error) {
	putStreamPelKey(x.pelKbuf[:], x.root.rooth, segid)
	v, ok, err := x.t.Get(ctx, x.pelKbuf[:])
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("sqlo1: stream PEL segment %d of rooth %#x is missing", segid, x.root.rooth)
	}
	ents, err := decodeStreamPelSeg(v, x.pelEnts[:0])
	if err != nil {
		return nil, err
	}
	x.pelEnts = ents
	return ents, nil
}

// writePelSeg encodes ents and lands them at segid.
func (x *Stream) writePelSeg(ctx context.Context, segid uint64, ents []streamPelEnt) error {
	x.pelBuf = appendStreamPelSeg(x.pelBuf[:0], ents)
	putStreamPelKey(x.pelKbuf[:], x.root.rooth, segid)
	return x.t.SetGen(ctx, x.pelKbuf[:], x.pelBuf, TagStream, x.root.rootgen)
}

// delPelSeg drops segment segid, always after the record write that
// stopped referencing it, in the same batch.
func (x *Stream) delPelSeg(ctx context.Context, segid uint64) error {
	putStreamPelKey(x.pelKbuf[:], x.root.rooth, segid)
	_, err := x.t.Del(ctx, x.pelKbuf[:])
	return err
}

// copyGroupOwned deep-copies a decoded group so it survives the reads
// an op does before writing the record back.
func (x *Stream) copyGroupOwned(g *streamGroup) streamGroup {
	out := streamGroup{
		name: append([]byte(nil), g.name...),
		last: g.last,
		read: g.read,
	}
	if len(g.cons) > 0 {
		out.cons = make([]streamConsumer, len(g.cons))
		for i := range g.cons {
			out.cons[i] = g.cons[i]
			out.cons[i].name = append([]byte(nil), g.cons[i].name...)
		}
	}
	if len(g.pelf) > 0 {
		out.pelf = append([]streamPelFenceEnt(nil), g.pelf...)
	}
	return out
}

// pelFenceSeek finds the fence index whose segment may hold id, the
// last base at or below it; -1 when id sits below the first base.
func pelFenceSeek(pelf []streamPelFenceEnt, id streamID) int {
	return sort.Search(len(pelf), func(i int) bool {
		return id.less(pelf[i].base)
	}) - 1
}

// groupConsumer finds consumer in g's table, appending an observed row
// when absent, both XREADGROUP forms' auto-create. The returned index
// is stable for the op: rows are only appended.
func groupConsumer(g *streamGroup, consumer []byte, nowMs int64) (int, error) {
	for i := range g.cons {
		if bytes.Equal(g.cons[i].name, consumer) {
			return i, nil
		}
	}
	if len(g.cons) >= math.MaxUint16 {
		return 0, fmt.Errorf("sqlo1: stream group %q is at the %d consumer cap", g.name, math.MaxUint16)
	}
	g.cons = append(g.cons, streamConsumer{name: append([]byte(nil), consumer...), seenMs: nowMs, activeMs: -1})
	return len(g.cons) - 1, nil
}

// pelDeliver appends ids to the PEL for consumer cidx, cutting fresh
// segments at the caps. All ids sit above every pending ID (they are
// above the group's old last-delivered ID), so this only amends the
// tail segment and cuts after it. Fresh segments flush before the
// record write the caller does next; the amended tail rides the
// caller's batch. rootDirty reports minted segids the caller must land
// with a root write. The refusals run before any write.
func (x *Stream) pelDeliver(ctx context.Context, g *streamGroup, ids []streamID, cidx int, nowMs int64) (rootDirty bool, err error) {
	if cidx > math.MaxUint8 {
		return false, errStreamPelConsumerCap
	}
	// Plan the segment layout first: the amended tail, then fresh
	// cuts, sized by the encoded-byte and entry caps.
	type segPlan struct {
		ents  []streamPelEnt
		fresh bool
	}
	var plan []segPlan
	var size int
	if n := len(g.pelf); n > 0 {
		tail, err := x.readPelSeg(ctx, g.pelf[n-1].segid)
		if err != nil {
			return false, err
		}
		plan = append(plan, segPlan{ents: append([]streamPelEnt(nil), tail...)})
		size = streamPelSegHdrLen
		prev := streamID{}
		for i := range tail {
			size += streamPelEntLen(prev, tail[i].id)
			prev = tail[i].id
		}
	}
	for _, id := range ids {
		e := streamPelEnt{id: id, cidx: uint8(cidx), dcount: 1, dtime: nowMs}
		cur := len(plan) - 1
		if cur >= 0 {
			prev := streamID{}
			if n := len(plan[cur].ents); n > 0 {
				prev = plan[cur].ents[n-1].id
			}
			w := streamPelEntLen(prev, id)
			if size+w <= streamPelSegMaxBytes && len(plan[cur].ents) < streamPelSegMaxEnts {
				plan[cur].ents = append(plan[cur].ents, e)
				size += w
				continue
			}
		}
		plan = append(plan, segPlan{ents: []streamPelEnt{e}, fresh: true})
		size = streamPelSegHdrLen + streamPelEntLen(streamID{}, id)
	}
	fresh := 0
	for i := range plan {
		if plan[i].fresh {
			fresh++
		}
	}
	if len(g.pelf)+fresh > streamPelFenceMax {
		return false, errStreamPelFenceFull
	}
	// Fresh segments land and flush first, so a crash prefix without
	// the record leaves only orphans; the amended tail rides the
	// record's batch.
	for i := range plan {
		if !plan[i].fresh {
			continue
		}
		segid := x.root.nextSegid
		x.root.nextSegid++
		rootDirty = true
		if err := x.writePelSeg(ctx, segid, plan[i].ents); err != nil {
			return false, err
		}
		g.pelf = append(g.pelf, streamPelFenceEnt{base: plan[i].ents[0].id, segid: segid, count: uint32(len(plan[i].ents))})
	}
	if fresh > 0 {
		if err := x.t.Flush(ctx); err != nil {
			return false, err
		}
	}
	if len(plan) > 0 && !plan[0].fresh {
		ti := len(g.pelf) - 1 - fresh
		if err := x.writePelSeg(ctx, g.pelf[ti].segid, plan[0].ents); err != nil {
			return false, err
		}
		g.pelf[ti].count = uint32(len(plan[0].ents))
	}
	return rootDirty, nil
}

// pelRemove drops ids from the PEL, the XACK core: segments rewrite or
// die per touched fence slot, duplicate ids count once, and ids not
// pending count zero. removed reports per-consumer removals for the
// pel counters; the dropped segids land in x.pelDrops for the caller
// to delete after its record write, same batch.
func (x *Stream) pelRemove(ctx context.Context, g *streamGroup, ids []streamID, removed []int) (int64, error) {
	x.pelDrops = x.pelDrops[:0]
	bySeg := map[int][]streamID{}
	for _, id := range ids {
		if i := pelFenceSeek(g.pelf, id); i >= 0 {
			bySeg[i] = append(bySeg[i], id)
		}
	}
	idxs := make([]int, 0, len(bySeg))
	for i := range bySeg {
		idxs = append(idxs, i)
	}
	sort.Ints(idxs)
	var total int64
	for _, fi := range idxs {
		want := bySeg[fi]
		ents, err := x.readPelSeg(ctx, g.pelf[fi].segid)
		if err != nil {
			return 0, err
		}
		kept := ents[:0]
		for i := range ents {
			hit := false
			for j := range want {
				if want[j] == ents[i].id {
					hit = true
					want[j] = want[len(want)-1]
					want = want[:len(want)-1]
					break
				}
			}
			if hit {
				removed[ents[i].cidx]++
				total++
				continue
			}
			kept = append(kept, ents[i])
		}
		if total == 0 {
			continue
		}
		if len(kept) == 0 {
			g.pelf[fi].count = 0
			x.pelDrops = append(x.pelDrops, g.pelf[fi].segid)
			continue
		}
		if len(kept) != len(ents) {
			if err := x.writePelSeg(ctx, g.pelf[fi].segid, kept); err != nil {
				return 0, err
			}
			g.pelf[fi].base = kept[0].id
			g.pelf[fi].count = uint32(len(kept))
		}
	}
	live := g.pelf[:0]
	for i := range g.pelf {
		if g.pelf[i].count > 0 {
			live = append(live, g.pelf[i])
		}
	}
	g.pelf = live
	return total, nil
}

// pelDropPending deletes the segments pelRemove emptied, after the
// record write that stopped referencing them, one batch.
func (x *Stream) pelDropPending(ctx context.Context) error {
	for _, segid := range x.pelDrops {
		if err := x.delPelSeg(ctx, segid); err != nil {
			return err
		}
	}
	return nil
}

// pelCollect walks the PEL from the segment that may hold ids above
// after, handing entries in ID order until fn returns false. fn sees a
// value copy; the walk stops early on false.
func (x *Stream) pelCollect(ctx context.Context, g *streamGroup, after streamID, fn func(e streamPelEnt) bool) error {
	start := max(pelFenceSeek(g.pelf, after), 0)
	for fi := start; fi < len(g.pelf); fi++ {
		ents, err := x.readPelSeg(ctx, g.pelf[fi].segid)
		if err != nil {
			return err
		}
		for i := range ents {
			if !after.less(ents[i].id) {
				continue
			}
			if !fn(ents[i]) {
				return nil
			}
		}
	}
	return nil
}

// pelBump rewrites the delivery count and time of ids, the history
// form's re-delivery bookkeeping. ids are pending and ID-sorted (they
// came off a pelCollect walk).
func (x *Stream) pelBump(ctx context.Context, g *streamGroup, ids []streamID, nowMs int64) error {
	for lo := 0; lo < len(ids); {
		fi := pelFenceSeek(g.pelf, ids[lo])
		hi := lo + 1
		for hi < len(ids) && pelFenceSeek(g.pelf, ids[hi]) == fi {
			hi++
		}
		ents, err := x.readPelSeg(ctx, g.pelf[fi].segid)
		if err != nil {
			return err
		}
		for i := range ents {
			if slices.Contains(ids[lo:hi], ents[i].id) {
				ents[i].dcount++
				ents[i].dtime = nowMs
			}
		}
		if err := x.writePelSeg(ctx, g.pelf[fi].segid, ents); err != nil {
			return err
		}
		lo = hi
	}
	return nil
}

// pelSweepConsumer removes every entry owned by consumer index ci and
// reindexes the owners above it, XGROUP DELCONSUMER's discard-pending
// rule. The dropped segids land in x.pelDrops like pelRemove's.
func (x *Stream) pelSweepConsumer(ctx context.Context, g *streamGroup, ci int) error {
	x.pelDrops = x.pelDrops[:0]
	for fi := 0; fi < len(g.pelf); fi++ {
		ents, err := x.readPelSeg(ctx, g.pelf[fi].segid)
		if err != nil {
			return err
		}
		kept := ents[:0]
		touched := false
		for i := range ents {
			if int(ents[i].cidx) == ci {
				touched = true
				continue
			}
			if int(ents[i].cidx) > ci {
				ents[i].cidx--
				touched = true
			}
			kept = append(kept, ents[i])
		}
		if !touched {
			continue
		}
		if len(kept) == 0 {
			g.pelf[fi].count = 0
			x.pelDrops = append(x.pelDrops, g.pelf[fi].segid)
			continue
		}
		if err := x.writePelSeg(ctx, g.pelf[fi].segid, kept); err != nil {
			return err
		}
		g.pelf[fi].base = kept[0].id
		g.pelf[fi].count = uint32(len(kept))
	}
	live := g.pelf[:0]
	for i := range g.pelf {
		if g.pelf[i].count > 0 {
			live = append(live, g.pelf[i])
		}
	}
	g.pelf = live
	return nil
}

// streamIDNext is the successor ID, the exclusive-start step; ok false
// at the ID space's end.
func streamIDNext(id streamID) (streamID, bool) {
	if id.seq < math.MaxUint64 {
		return streamID{ms: id.ms, seq: id.seq + 1}, true
	}
	if id.ms < math.MaxUint64 {
		return streamID{ms: id.ms + 1}, true
	}
	return streamID{}, false
}

// ReadGroupCheck is XREADGROUP's first pass over its keys: every key
// and group resolves before any serves, Redis's order, with the shared
// NOGROUP sentinel for a missing key or group and WRONGTYPE winning
// over both.
func (x *Stream) ReadGroupCheck(ctx context.Context, key, group []byte) error {
	exists, _, err := x.stateOf(ctx, key)
	if err != nil {
		return err
	}
	if !exists {
		return errStreamNoGroup
	}
	ord, _, err := x.findGroup(ctx, group)
	if err != nil {
		return err
	}
	if ord < 0 {
		return errStreamNoGroup
	}
	return nil
}

// ReadGroupNew is the XREADGROUP > form on one key: read entries above
// the group's last-delivered ID, append them to the PEL for consumer
// (unless noack), and advance the group cursor. begin runs once with
// the delivery count, entries follow through emit, and the pinned
// bookkeeping lands even at zero entries: the consumer auto-creates
// and its seen time resets on an empty poll. The entries-read counter
// repairs at the edges the way Redis 8.8 does: a position below the
// first entry rebases it to entries-added minus the live count, and a
// delivery that reaches the tail pins it to entries-added.
func (x *Stream) ReadGroupNew(ctx context.Context, key, group, consumer []byte, count int64, noack bool, nowMs int64, begin func(n int), emit func(id streamID, fv [][]byte)) error {
	exists, expMs, err := x.stateOf(ctx, key)
	if err != nil {
		return err
	}
	if !exists {
		return errStreamNoGroup
	}
	ord, g0, err := x.findGroup(ctx, group)
	if err != nil {
		return err
	}
	if ord < 0 {
		return errStreamNoGroup
	}
	g := x.copyGroupOwned(&g0)
	ci, err := groupConsumer(&g, consumer, nowMs)
	if err != nil {
		return err
	}
	x.pelIDs = x.pelIDs[:0]
	start, more := streamIDNext(g.last)
	if more && g.last.less(x.root.last) {
		end := streamID{ms: math.MaxUint64, seq: math.MaxUint64}
		err := x.Range(ctx, key, start, end, count, false, begin, func(id streamID, fv [][]byte) {
			x.pelIDs = append(x.pelIDs, id)
			emit(id, fv)
		})
		if err != nil {
			return err
		}
	} else {
		begin(0)
	}
	n := len(x.pelIDs)
	g.cons[ci].seenMs = nowMs
	rootDirty := false
	if n > 0 {
		g.cons[ci].activeMs = nowMs
		// The front repair runs against the pre-delivery position,
		// cgLag's edge guard: no tombstone at or below the first
		// entry, and the cursor below or on it.
		r := &x.root
		if g.read < 0 && r.count > 0 {
			var first streamID
			if r.paged {
				first = r.pidx[0].base
			} else {
				first = x.fence[0].base
			}
			if r.maxDel == (streamID{}) || r.maxDel.less(first) {
				if g.last.less(first) {
					g.read = int64(r.added - r.count)
				} else if g.last == first {
					g.read = int64(r.added-r.count) + 1
				}
			}
		}
		if g.read >= 0 {
			g.read += int64(n)
		}
		g.last = x.pelIDs[n-1]
		if g.read < 0 && g.last == r.last {
			g.read = int64(r.added)
		}
		g.read = clampGroupRead(g.read, r.added)
		if !noack {
			g.cons[ci].pel += uint64(n)
			rootDirty, err = x.pelDeliver(ctx, &g, x.pelIDs, ci, nowMs)
			if err != nil {
				return err
			}
		}
	}
	if err := x.writeGroupRec(ctx, uint32(ord), &g); err != nil {
		return err
	}
	if rootDirty {
		if err := x.writeRoot(ctx, key); err != nil {
			return err
		}
		return x.restamp(ctx, key, expMs)
	}
	return nil
}

// ReadGroupHistory is the XREADGROUP specific-ID form on one key:
// re-read the calling consumer's own pending entries strictly above
// after, echoing the key even when empty, the pinned shape. Every
// re-read bumps the entry's delivery count and time, NOACK included,
// and a pending ID whose entry was deleted or trimmed emits missing.
// The consumer auto-creates and its seen time resets; its active time
// does not move, the pinned inactivity rule.
func (x *Stream) ReadGroupHistory(ctx context.Context, key, group, consumer []byte, after streamID, count int64, nowMs int64, begin func(n int), emit func(id streamID, fv [][]byte, missing bool)) error {
	exists, _, err := x.stateOf(ctx, key)
	if err != nil {
		return err
	}
	if !exists {
		return errStreamNoGroup
	}
	ord, g0, err := x.findGroup(ctx, group)
	if err != nil {
		return err
	}
	if ord < 0 {
		return errStreamNoGroup
	}
	g := x.copyGroupOwned(&g0)
	ci, err := groupConsumer(&g, consumer, nowMs)
	if err != nil {
		return err
	}
	x.pelIDs = x.pelIDs[:0]
	err = x.pelCollect(ctx, &g, after, func(e streamPelEnt) bool {
		if int(e.cidx) != ci {
			return true
		}
		x.pelIDs = append(x.pelIDs, e.id)
		return count <= 0 || int64(len(x.pelIDs)) < count
	})
	if err != nil {
		return err
	}
	begin(len(x.pelIDs))
	for _, id := range x.pelIDs {
		found := false
		err := x.Range(ctx, key, id, id, 1, false, func(int) {}, func(rid streamID, fv [][]byte) {
			found = true
			emit(rid, fv, false)
		})
		if err != nil {
			return err
		}
		if !found {
			emit(id, nil, true)
		}
	}
	if len(x.pelIDs) > 0 {
		if err := x.pelBump(ctx, &g, x.pelIDs, nowMs); err != nil {
			return err
		}
	}
	g.cons[ci].seenMs = nowMs
	return x.writeGroupRec(ctx, uint32(ord), &g)
}

// AckPrecheck is XACK's key gate, run before the command layer parses
// IDs so WRONGTYPE outranks a malformed ID, Redis's order.
func (x *Stream) AckPrecheck(ctx context.Context, key []byte) (bool, error) {
	exists, _, err := x.stateOf(ctx, key)
	return exists, err
}

// Ack is XACK: remove ids from the group's PEL and answer how many
// were pending, duplicates counting once. A missing key or group
// answers zero; ownership does not matter, any consumer's pending
// entry acks.
func (x *Stream) Ack(ctx context.Context, key, group []byte, ids []streamID) (int64, error) {
	exists, _, err := x.stateOf(ctx, key)
	if err != nil {
		return 0, err
	}
	if !exists {
		return 0, nil
	}
	ord, g0, err := x.findGroup(ctx, group)
	if err != nil {
		return 0, err
	}
	if ord < 0 {
		return 0, nil
	}
	g := x.copyGroupOwned(&g0)
	removed := make([]int, len(g.cons))
	total, err := x.pelRemove(ctx, &g, ids, removed)
	if err != nil {
		return 0, err
	}
	if total == 0 {
		return 0, nil
	}
	for i := range removed {
		g.cons[i].pel -= uint64(removed[i])
	}
	if err := x.writeGroupRec(ctx, uint32(ord), &g); err != nil {
		return 0, err
	}
	return total, x.pelDropPending(ctx)
}

// streamPelRow is one pending entry surfaced to XINFO STREAM FULL: the
// ID, the owning consumer's index, and the delivery bookkeeping.
type streamPelRow struct {
	id     streamID
	cidx   int
	dtime  int64
	dcount uint32
}

// FullGroupsInfo drives the XINFO STREAM FULL groups array: begin once
// with the row count, then emit per group in name order with the
// group's first count pending rows and each consumer's first count
// pending rows, count -1 unbounded, Redis's COUNT rule for the FULL
// lists. The emitted group is an owned copy.
func (x *Stream) FullGroupsInfo(ctx context.Context, key []byte, count int64, begin func(n int), emit func(g *streamGroup, pending uint64, lag int64, lagOK bool, rows []streamPelRow, consRows [][]streamPelRow)) error {
	exists, _, err := x.stateOf(ctx, key)
	if err != nil {
		return err
	}
	if !exists {
		return errStreamNoKey
	}
	n := int(x.root.groupCount)
	begin(n)
	if n == 0 {
		return nil
	}
	names := make([][]byte, n)
	order := make([]int, n)
	for ord := range n {
		v, err := x.readGroupRec(ctx, uint32(ord))
		if err != nil {
			return err
		}
		g, err := decodeStreamGroup(v, x.grpCons[:0], x.grpFence[:0])
		if err != nil {
			return err
		}
		names[ord] = append([]byte(nil), g.name...)
		order[ord] = ord
	}
	sort.Slice(order, func(i, j int) bool {
		return bytes.Compare(names[order[i]], names[order[j]]) < 0
	})
	for _, ord := range order {
		v, err := x.readGroupRec(ctx, uint32(ord))
		if err != nil {
			return err
		}
		g0, err := decodeStreamGroup(v, x.grpCons[:0], x.grpFence[:0])
		if err != nil {
			return err
		}
		g := x.copyGroupOwned(&g0)
		pending := uint64(0)
		for i := range g.cons {
			pending += g.cons[i].pel
		}
		lag, lagOK := x.cgLag(&g)
		var rows []streamPelRow
		consRows := make([][]streamPelRow, len(g.cons))
		filled := 0
		err = x.pelCollect(ctx, &g, streamID{}, func(e streamPelEnt) bool {
			if count < 0 || int64(len(rows)) < count {
				rows = append(rows, streamPelRow{id: e.id, cidx: int(e.cidx), dtime: e.dtime, dcount: e.dcount})
			}
			cr := &consRows[e.cidx]
			if count < 0 || int64(len(*cr)) < count {
				*cr = append(*cr, streamPelRow{id: e.id, cidx: int(e.cidx), dtime: e.dtime, dcount: e.dcount})
				if count >= 0 && int64(len(*cr)) == count {
					filled++
				}
			}
			return count < 0 || int64(len(rows)) < count || filled < len(g.cons)
		})
		if err != nil {
			return err
		}
		emit(&g, pending, lag, lagOK, rows, consRows)
	}
	return nil
}
