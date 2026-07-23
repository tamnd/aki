package sqlo1

// Stream consumer group records, doc 10's kind 4: XGROUP CREATE,
// SETID, DESTROY, CREATECONSUMER, and DELCONSUMER over one record per
// group, plus the XINFO GROUPS and CONSUMERS reads. A group's segid is
// its dense ordinal in [0, group_count): the root already counts the
// groups, so lookup by name reads the ordinals in order (groups are
// few) and DESTROY compacts by moving the last ordinal into the hole,
// no allocator and no root format change. The record carries the
// group's last delivered ID, its entries-read counter, and the
// consumer table; the PEL fence field stays empty until the kind 5
// slice.

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"
)

// The XGROUP sentinels. The no-key text rides storeErr's ERR prefix;
// BUSYGROUP and NOGROUP carry their own prefixes and the group's
// NOGROUP text needs the key and group names, so the command layer
// renders both. errStreamBadArgID marks an ID argument whose parse
// failure the key and group lookups outrank, Redis's order.
var (
	errXgroupNoKey     = errors.New("The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.")
	errStreamBusyGroup = errors.New("sqlo1: consumer group exists")
	errStreamNoGroup   = errors.New("sqlo1: no such consumer group")
	errStreamBadArgID  = errors.New("sqlo1: malformed stream ID argument")
)

// streamConsumer is one consumer row in a group record. activeMs is -1
// until a delivery marks the consumer active; pel counts its pending
// entries, zero until the PEL slice writes any.
type streamConsumer struct {
	name     []byte
	seenMs   int64
	activeMs int64
	pel      uint64
}

// streamGroup is a decoded group record. read is the entries-read
// counter, -1 unknown; it is clamped to entries-added at every store,
// the Redis 8.8 behavior the lag math depends on. Decoded byte fields
// alias the record read and die on the next Tiered call.
type streamGroup struct {
	name []byte
	last streamID
	read int64
	cons []streamConsumer
}

// Group record payload:
//
//	u64 last_ms, last_seq
//	u64 read_raw          // all-ones unknown, else <= MaxInt64
//	u16 consumer_n
//	u16 pel_fence_n       // reserved 0 until the kind 5 slice
//	u32 name_len, name bytes
//	consumer_n x {
//		u32 name_len, name bytes
//		u64 seen_ms       // <= MaxInt64
//		u64 active_raw    // all-ones never, else <= MaxInt64
//		u64 pel_count
//	}
//
// Canonical form: exact length, unique consumer names in stored order.
const streamGroupHdrLen = 28

// streamSubkindGroup is the stream plane's group record kind, doc 10's
// kind 4; the kind byte is namespaced per root type (subkey.go), so
// the constant lives with the one type that reads it.
const streamSubkindGroup uint8 = 4

// appendStreamGroup encodes g onto dst.
func appendStreamGroup(dst []byte, g *streamGroup) []byte {
	var h [streamGroupHdrLen]byte
	binary.LittleEndian.PutUint64(h[0:], g.last.ms)
	binary.LittleEndian.PutUint64(h[8:], g.last.seq)
	binary.LittleEndian.PutUint64(h[16:], uint64(g.read))
	binary.LittleEndian.PutUint16(h[24:], uint16(len(g.cons)))
	dst = append(dst, h[:]...)
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(g.name)))
	dst = append(dst, g.name...)
	for i := range g.cons {
		c := &g.cons[i]
		dst = binary.LittleEndian.AppendUint32(dst, uint32(len(c.name)))
		dst = append(dst, c.name...)
		dst = binary.LittleEndian.AppendUint64(dst, uint64(c.seenMs))
		dst = binary.LittleEndian.AppendUint64(dst, uint64(c.activeMs))
		dst = binary.LittleEndian.AppendUint64(dst, c.pel)
	}
	return dst
}

// decodeStreamGroupBytes cuts a u32-prefixed byte field off p.
func decodeStreamGroupBytes(p []byte) (b, rest []byte, err error) {
	if len(p) < 4 {
		return nil, nil, errors.New("sqlo1: stream group record truncates a length")
	}
	n := int(binary.LittleEndian.Uint32(p))
	p = p[4:]
	if len(p) < n {
		return nil, nil, errors.New("sqlo1: stream group record truncates a name")
	}
	return p[:n], p[n:], nil
}

// decodeStreamGroup validates v and decodes it, consumer rows landing
// in the cons scratch. Name bytes alias v.
func decodeStreamGroup(v []byte, cons []streamConsumer) (streamGroup, error) {
	if len(v) < streamGroupHdrLen {
		return streamGroup{}, fmt.Errorf("sqlo1: stream group record of %d bytes has no header", len(v))
	}
	g := streamGroup{
		last: streamID{ms: binary.LittleEndian.Uint64(v[0:]), seq: binary.LittleEndian.Uint64(v[8:])},
		read: int64(binary.LittleEndian.Uint64(v[16:])),
	}
	if g.read < -1 {
		return streamGroup{}, fmt.Errorf("sqlo1: stream group record has entries-read raw %#x", uint64(g.read))
	}
	n := int(binary.LittleEndian.Uint16(v[24:]))
	if pel := binary.LittleEndian.Uint16(v[26:]); pel != 0 {
		return streamGroup{}, fmt.Errorf("sqlo1: stream group record has %d PEL fence entries before the PEL slice", pel)
	}
	p := v[streamGroupHdrLen:]
	var err error
	if g.name, p, err = decodeStreamGroupBytes(p); err != nil {
		return streamGroup{}, err
	}
	for i := range n {
		var c streamConsumer
		if c.name, p, err = decodeStreamGroupBytes(p); err != nil {
			return streamGroup{}, err
		}
		if len(p) < 24 {
			return streamGroup{}, fmt.Errorf("sqlo1: stream group record truncates consumer %d", i)
		}
		c.seenMs = int64(binary.LittleEndian.Uint64(p[0:]))
		c.activeMs = int64(binary.LittleEndian.Uint64(p[8:]))
		c.pel = binary.LittleEndian.Uint64(p[16:])
		if c.seenMs < 0 || c.activeMs < -1 {
			return streamGroup{}, fmt.Errorf("sqlo1: stream group record consumer %d has a bad time", i)
		}
		for j := range cons {
			if bytes.Equal(cons[j].name, c.name) {
				return streamGroup{}, fmt.Errorf("sqlo1: stream group record repeats consumer %q", c.name)
			}
		}
		cons = append(cons, c)
		p = p[24:]
	}
	if len(p) != 0 {
		return streamGroup{}, fmt.Errorf("sqlo1: stream group record has %d trailing bytes", len(p))
	}
	g.cons = cons
	return g, nil
}

// putStreamGroupKey writes the subkey of group ordinal ord under rooth,
// the doc 03 6.3 layout with kind streamSubkindGroup.
func putStreamGroupKey(dst []byte, rooth uint64, ord uint32) {
	binary.LittleEndian.PutUint64(dst, rooth)
	dst[8] = streamSubkindGroup
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], uint64(ord))
	copy(dst[9:SubkeySize], b[:7])
}

// readGroupRec reads the record at ordinal ord under the current root's
// plane. The bytes alias the read and die on the next Tiered call.
func (x *Stream) readGroupRec(ctx context.Context, ord uint32) ([]byte, error) {
	putStreamGroupKey(x.gkbuf[:], x.root.rooth, ord)
	v, ok, err := x.t.Get(ctx, x.gkbuf[:])
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("sqlo1: stream group %d of rooth %#x is missing", ord, x.root.rooth)
	}
	return v, nil
}

// writeGroupRec encodes g and lands it at ordinal ord. The encode runs
// before any other Tiered call, so g may alias a record read.
func (x *Stream) writeGroupRec(ctx context.Context, ord uint32, g *streamGroup) error {
	x.grpBuf = appendStreamGroup(x.grpBuf[:0], g)
	putStreamGroupKey(x.gkbuf[:], x.root.rooth, ord)
	return x.t.SetGen(ctx, x.gkbuf[:], x.grpBuf, TagStream, x.root.rootgen)
}

// delGroupRec drops the record at ordinal ord, always after the root
// that stopped referencing it.
func (x *Stream) delGroupRec(ctx context.Context, ord uint32) error {
	putStreamGroupKey(x.gkbuf[:], x.root.rooth, ord)
	_, err := x.t.Del(ctx, x.gkbuf[:])
	return err
}

// findGroup scans the ordinals for group by name. ord is -1 when the
// group is absent; the decoded record aliases the last read.
func (x *Stream) findGroup(ctx context.Context, group []byte) (int, streamGroup, error) {
	for ord := uint32(0); ord < x.root.groupCount; ord++ {
		v, err := x.readGroupRec(ctx, ord)
		if err != nil {
			return -1, streamGroup{}, err
		}
		g, err := decodeStreamGroup(v, x.grpCons[:0])
		if err != nil {
			return -1, streamGroup{}, err
		}
		if bytes.Equal(g.name, group) {
			return int(ord), g, nil
		}
	}
	return -1, streamGroup{}, nil
}

// clampGroupRead clamps a stored entries-read counter to the stream's
// entries-added at store time, the Redis 8.8 rule the lag estimate
// leans on; negative means unknown and stores as -1.
func clampGroupRead(read int64, added uint64) int64 {
	if read < 0 {
		return -1
	}
	if uint64(read) > added {
		return int64(added)
	}
	return read
}

// GroupCreate is XGROUP CREATE: land a fresh group record at the next
// ordinal, or the whole empty stream when MKSTREAM meets a missing key.
// idOK false marks an unparseable ID argument, which the key checks
// outrank but the group check does not, Redis's order for CREATE.
func (x *Stream) GroupCreate(ctx context.Context, key, group []byte, idOK bool, id streamID, dollar, mkstream bool, read int64) error {
	exists, expMs, err := x.stateOf(ctx, key)
	if err != nil {
		return err
	}
	if !exists {
		if !mkstream {
			return errXgroupNoKey
		}
		if !idOK {
			return errStreamBadArgID
		}
		return x.createEmptyWithGroup(ctx, key, group, id, dollar, read)
	}
	if !idOK {
		return errStreamBadArgID
	}
	ord, _, err := x.findGroup(ctx, group)
	if err != nil {
		return err
	}
	if ord >= 0 {
		return errStreamBusyGroup
	}
	if dollar {
		id = x.root.last
	}
	g := streamGroup{name: group, last: id, read: clampGroupRead(read, x.root.added)}
	if err := x.writeGroupRec(ctx, x.root.groupCount, &g); err != nil {
		return err
	}
	// The fresh record flushes before the root that references its
	// ordinal, the fence page rule: a crash prefix reads the old count
	// and the record is an orphan the plane retire cleans.
	if err := x.t.Flush(ctx); err != nil {
		return err
	}
	x.root.groupCount++
	if err := x.writeRoot(ctx, key); err != nil {
		return err
	}
	return x.restamp(ctx, key, expMs)
}

// createEmptyWithGroup is MKSTREAM's half of GroupCreate on a missing
// key: mint the plane, land group 0, and write the count 0 root after
// the plane flushes, create's fresh-plane rule. A $ ID on an empty
// stream is the zero ID.
func (x *Stream) createEmptyWithGroup(ctx context.Context, key, group []byte, id streamID, dollar bool, read int64) error {
	rooth, err := x.nextRooth(ctx)
	if err != nil {
		return err
	}
	x.root = streamRoot{rootgen: 1, rooth: rooth, groupCount: 1}
	x.fence = x.fence[:0]
	if dollar {
		id = streamID{}
	}
	g := streamGroup{name: group, last: id, read: clampGroupRead(read, 0)}
	if err := x.writeGroupRec(ctx, 0, &g); err != nil {
		return err
	}
	if err := x.t.Flush(ctx); err != nil {
		return err
	}
	return x.writeRoot(ctx, key)
}

// GroupSetID is XGROUP SETID: move the group's last delivered ID and
// restore entries-read, which resets to unknown when the option is
// absent since the position moved arbitrarily, the pinned 8.8 shape.
// The group lookup outranks the ID parse here, unlike CREATE.
func (x *Stream) GroupSetID(ctx context.Context, key, group []byte, idOK bool, id streamID, dollar bool, read int64) error {
	exists, _, err := x.stateOf(ctx, key)
	if err != nil {
		return err
	}
	if !exists {
		return errXgroupNoKey
	}
	ord, g, err := x.findGroup(ctx, group)
	if err != nil {
		return err
	}
	if ord < 0 {
		return errStreamNoGroup
	}
	if !idOK {
		return errStreamBadArgID
	}
	if dollar {
		id = x.root.last
	}
	g.last = id
	g.read = clampGroupRead(read, x.root.added)
	// An in-place record rewrite rides the current batch whole; the
	// root does not change.
	return x.writeGroupRec(ctx, uint32(ord), &g)
}

// GroupDestroy is XGROUP DESTROY: compact the ordinals by moving the
// last record into the destroyed slot, then drop the vacated tail
// after the root that stopped referencing it, all one batch. A missing
// group is destroyed false, not an error.
func (x *Stream) GroupDestroy(ctx context.Context, key, group []byte) (bool, error) {
	exists, expMs, err := x.stateOf(ctx, key)
	if err != nil {
		return false, err
	}
	if !exists {
		return false, errXgroupNoKey
	}
	ord, _, err := x.findGroup(ctx, group)
	if err != nil {
		return false, err
	}
	if ord < 0 {
		return false, nil
	}
	lastOrd := x.root.groupCount - 1
	if uint32(ord) != lastOrd {
		v, err := x.readGroupRec(ctx, lastOrd)
		if err != nil {
			return false, err
		}
		x.grpBuf = append(x.grpBuf[:0], v...)
		putStreamGroupKey(x.gkbuf[:], x.root.rooth, uint32(ord))
		if err := x.t.SetGen(ctx, x.gkbuf[:], x.grpBuf, TagStream, x.root.rootgen); err != nil {
			return false, err
		}
	}
	x.root.groupCount--
	if err := x.writeRoot(ctx, key); err != nil {
		return false, err
	}
	if err := x.delGroupRec(ctx, lastOrd); err != nil {
		return false, err
	}
	return true, x.restamp(ctx, key, expMs)
}

// GroupCreateConsumer is XGROUP CREATECONSUMER: an observed consumer
// with seen time now and no activity yet. created is false when the
// name already exists, with no write.
func (x *Stream) GroupCreateConsumer(ctx context.Context, key, group, consumer []byte, nowMs int64) (bool, error) {
	exists, _, err := x.stateOf(ctx, key)
	if err != nil {
		return false, err
	}
	if !exists {
		return false, errXgroupNoKey
	}
	ord, g, err := x.findGroup(ctx, group)
	if err != nil {
		return false, err
	}
	if ord < 0 {
		return false, errStreamNoGroup
	}
	for i := range g.cons {
		if bytes.Equal(g.cons[i].name, consumer) {
			return false, nil
		}
	}
	if len(g.cons) >= math.MaxUint16 {
		return false, fmt.Errorf("sqlo1: stream group %q is at the %d consumer cap", group, math.MaxUint16)
	}
	g.cons = append(g.cons, streamConsumer{name: consumer, seenMs: nowMs, activeMs: -1})
	x.grpCons = g.cons[:0]
	return true, x.writeGroupRec(ctx, uint32(ord), &g)
}

// GroupDelConsumer is XGROUP DELCONSUMER, replying the deleted
// consumer's pending count. A missing consumer is 0 with no write.
func (x *Stream) GroupDelConsumer(ctx context.Context, key, group, consumer []byte) (int64, error) {
	exists, _, err := x.stateOf(ctx, key)
	if err != nil {
		return 0, err
	}
	if !exists {
		return 0, errXgroupNoKey
	}
	ord, g, err := x.findGroup(ctx, group)
	if err != nil {
		return 0, err
	}
	if ord < 0 {
		return 0, errStreamNoGroup
	}
	for i := range g.cons {
		if !bytes.Equal(g.cons[i].name, consumer) {
			continue
		}
		pending := int64(g.cons[i].pel)
		g.cons = append(g.cons[:i], g.cons[i+1:]...)
		return pending, x.writeGroupRec(ctx, uint32(ord), &g)
	}
	return 0, nil
}

// cgLag is the group's lag pair for XINFO, the Redis 8.8 shape pinned
// live: an untouched or emptied stream answers 0; the stored counter
// answers entries-added minus entries-read bounded above by the live
// count, unless a tombstone sits at or past the group's position and
// makes it a lie; the edge estimate covers an unknown counter at or
// past the last ID, below the first, or exactly on it; anything else
// is unknown, the nil reply.
func (x *Stream) cgLag(g *streamGroup) (lag int64, lagOK bool) {
	r := &x.root
	if r.added == 0 || r.count == 0 {
		return 0, true
	}
	var first streamID
	if r.paged {
		first = r.pidx[0].base
	} else {
		first = x.fence[0].base
	}
	// A tombstone at or past the group's position means entries the
	// counter never saw may already be deleted.
	tombsPast := !r.maxDel.less(first) && !r.maxDel.less(g.last)
	if g.read >= 0 && !tombsPast {
		return min(int64(r.added)-g.read, int64(r.count)), true
	}
	if !g.last.less(r.last) {
		return 0, true
	}
	if r.maxDel == (streamID{}) || r.maxDel.less(first) {
		if g.last.less(first) {
			return int64(r.count), true
		}
		if g.last == first {
			return int64(r.count) - 1, true
		}
	}
	return 0, false
}

// GroupsInfo drives XINFO GROUPS and the FULL groups array: begin runs
// once with the row count, then emit runs per group in name order,
// Redis's rax iteration. The emitted record aliases its read and dies
// when emit returns; pending sums the consumer PEL counts, all zero
// until the PEL slice.
func (x *Stream) GroupsInfo(ctx context.Context, key []byte, begin func(n int), emit func(g *streamGroup, pending uint64, lag int64, lagOK bool)) error {
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
	// Two passes: names first, since sorting needs them all and a
	// record read invalidates the previous one.
	names := make([][]byte, n)
	order := make([]int, n)
	for ord := range n {
		v, err := x.readGroupRec(ctx, uint32(ord))
		if err != nil {
			return err
		}
		g, err := decodeStreamGroup(v, x.grpCons[:0])
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
		g, err := decodeStreamGroup(v, x.grpCons[:0])
		if err != nil {
			return err
		}
		pending := uint64(0)
		for i := range g.cons {
			pending += g.cons[i].pel
		}
		lag, lagOK := x.cgLag(&g)
		emit(&g, pending, lag, lagOK)
	}
	return nil
}

// ConsumersInfo drives XINFO CONSUMERS: begin once with the count,
// then emit per consumer in name order. Emitted names alias the record
// read and die when the walk returns.
func (x *Stream) ConsumersInfo(ctx context.Context, key, group []byte, begin func(n int), emit func(c *streamConsumer)) error {
	exists, _, err := x.stateOf(ctx, key)
	if err != nil {
		return err
	}
	if !exists {
		return errStreamNoKey
	}
	ord, g, err := x.findGroup(ctx, group)
	if err != nil {
		return err
	}
	if ord < 0 {
		return errStreamNoGroup
	}
	sort.Slice(g.cons, func(i, j int) bool {
		return bytes.Compare(g.cons[i].name, g.cons[j].name) < 0
	})
	begin(len(g.cons))
	for i := range g.cons {
		emit(&g.cons[i])
	}
	return nil
}
