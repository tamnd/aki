package obs1

// The consumer-group frame vocabulary (doc 04 section 2, doc 08's group
// and PEL chunk). Group deltas are their own top-level kind, not
// colldelta sub-kinds, because the fold routes them to the group chunk
// under the stream's collection key rather than to the entry chunks; the
// frame still carries the stream key, and the group name is the first
// item of every body. The post-decision rule holds throughout: a frame
// records resulting state (delivery times, delivery counts, the ids that
// actually moved), never the request, so replay is arithmetic-free.

import (
	"encoding/binary"
	"fmt"
)

// Group delta sub-kinds.
const (
	GSubNew         = 0x01
	GSubSetID       = 0x02
	GSubDrop        = 0x03
	GSubConsumerNew = 0x04
	GSubConsumerDel = 0x05
	GSubAck         = 0x06
	GSubDeliver     = 0x07
	GSubClaim       = 0x08
)

// GroupDelta (0x09) mutates one consumer group of the frame's stream.
type GroupDelta struct {
	Sub GroupSub
}

// GroupSub is one group delta sub-op.
type GroupSub interface {
	groupSubKind() uint8
	appendGroupBody(b []byte) ([]byte, error)
}

// GNew records XGROUP CREATE: the group exists with this read cursor.
// EntriesRead carries the lag basis and ReadValid its validity, the same
// pair XGROUP SETID keeps, since ENTRIESREAD is an optional argument and
// $ on an unknown length leaves the basis invalid.
type GNew struct {
	Group       []byte
	LastMs      uint64
	LastSeq     uint64
	EntriesRead uint64
	ReadValid   bool
}

// GSetID records XGROUP SETID: the resulting cursor and lag basis.
type GSetID struct {
	Group       []byte
	LastMs      uint64
	LastSeq     uint64
	EntriesRead uint64
	ReadValid   bool
}

// GDrop records XGROUP DESTROY of a group that existed.
type GDrop struct {
	Group []byte
}

// GConsumerNew records XGROUP CREATECONSUMER when it created one; SeenMs
// is the resulting seen time, and replay starts the consumer with no
// activity, the same shape the command leaves in RAM.
type GConsumerNew struct {
	Group    []byte
	Consumer []byte
	SeenMs   int64
}

// GConsumerDel records XGROUP DELCONSUMER: the consumer and its pending
// entries leave together. The frame carries no id list because replay
// drains by owner, the same walk the command ran.
type GConsumerDel struct {
	Group    []byte
	Consumer []byte
}

// GAck records PEL removals by id: XACK's acknowledged ids, and the
// entries a claim path dropped because their entries were deleted from
// the stream. Only ids that actually left the PEL appear.
type GAck struct {
	Group []byte
	IDMs  []uint64
	IDSeq []uint64
}

// GDeliver records an XREADGROUP new-message delivery: the cursor
// advances to the last id listed, the entries-read basis moves by the
// count, and each id enters the consumer's PEL at TimeMs with delivery
// count 1. NoAck deliveries advance the cursor without touching the PEL.
// Replay creates the consumer if the delivery was its first sighting,
// seen and active at TimeMs.
type GDeliver struct {
	Group    []byte
	Consumer []byte
	NoAck    bool
	TimeMs   int64
	IDMs     []uint64
	IDSeq    []uint64
}

// GClaim records resulting PEL entry state per id: owner, delivery time,
// and delivery count, exactly as XCLAIM, XAUTOCLAIM, or XNACK left them.
// The option soup (IDLE, TIME, RETRYCOUNT, FORCE, JUSTID) is resolved by
// the owner before the frame exists. Unowned marks the XNACK shape where
// the entry belongs to no consumer; it requires an empty consumer name
// explicitly rather than treating the empty string as a sentinel,
// because an empty consumer name is a legal name.
type GClaim struct {
	Group    []byte
	Consumer []byte
	Unowned  bool
	IDMs     []uint64
	IDSeq    []uint64
	TimeMs   []int64
	Counts   []uint16
}

func (GroupDelta) opKind() uint8  { return OpGroupDelta }
func (GroupDelta) opFlags() uint8 { return 0 }

func (o GroupDelta) appendPayload(b []byte) ([]byte, error) {
	if o.Sub == nil {
		return nil, fmt.Errorf("obs1: a group delta frame needs a sub-op")
	}
	return o.Sub.appendGroupBody(append(b, o.Sub.groupSubKind()))
}

func (GNew) groupSubKind() uint8         { return GSubNew }
func (GSetID) groupSubKind() uint8       { return GSubSetID }
func (GDrop) groupSubKind() uint8        { return GSubDrop }
func (GConsumerNew) groupSubKind() uint8 { return GSubConsumerNew }
func (GConsumerDel) groupSubKind() uint8 { return GSubConsumerDel }
func (GAck) groupSubKind() uint8         { return GSubAck }
func (GDeliver) groupSubKind() uint8     { return GSubDeliver }
func (GClaim) groupSubKind() uint8       { return GSubClaim }

func appendCursorBody(b, group []byte, lastMs, lastSeq, entriesRead uint64, readValid bool) ([]byte, error) {
	b, err := appendItem(b, group)
	if err != nil {
		return nil, err
	}
	b = binary.LittleEndian.AppendUint64(b, lastMs)
	b = binary.LittleEndian.AppendUint64(b, lastSeq)
	b = binary.LittleEndian.AppendUint64(b, entriesRead)
	if readValid {
		return append(b, 1), nil
	}
	return append(b, 0), nil
}

func (o GNew) appendGroupBody(b []byte) ([]byte, error) {
	return appendCursorBody(b, o.Group, o.LastMs, o.LastSeq, o.EntriesRead, o.ReadValid)
}

func (o GSetID) appendGroupBody(b []byte) ([]byte, error) {
	return appendCursorBody(b, o.Group, o.LastMs, o.LastSeq, o.EntriesRead, o.ReadValid)
}

func (o GDrop) appendGroupBody(b []byte) ([]byte, error) {
	return appendItem(b, o.Group)
}

func (o GConsumerNew) appendGroupBody(b []byte) ([]byte, error) {
	b, err := appendItem(b, o.Group)
	if err != nil {
		return nil, err
	}
	if b, err = appendItem(b, o.Consumer); err != nil {
		return nil, err
	}
	return binary.LittleEndian.AppendUint64(b, uint64(o.SeenMs)), nil
}

func (o GConsumerDel) appendGroupBody(b []byte) ([]byte, error) {
	b, err := appendItem(b, o.Group)
	if err != nil {
		return nil, err
	}
	return appendItem(b, o.Consumer)
}

func appendIDList(b []byte, ms, seqs []uint64) ([]byte, error) {
	if len(ms) == 0 {
		return nil, fmt.Errorf("obs1: a group delta id list records no effects")
	}
	if len(ms) != len(seqs) {
		return nil, fmt.Errorf("obs1: group delta id halves are %d and %d long", len(ms), len(seqs))
	}
	b = binary.LittleEndian.AppendUint32(b, uint32(len(ms)))
	for i := range ms {
		b = binary.LittleEndian.AppendUint64(b, ms[i])
		b = binary.LittleEndian.AppendUint64(b, seqs[i])
	}
	return b, nil
}

func (o GAck) appendGroupBody(b []byte) ([]byte, error) {
	b, err := appendItem(b, o.Group)
	if err != nil {
		return nil, err
	}
	return appendIDList(b, o.IDMs, o.IDSeq)
}

func (o GDeliver) appendGroupBody(b []byte) ([]byte, error) {
	b, err := appendItem(b, o.Group)
	if err != nil {
		return nil, err
	}
	if b, err = appendItem(b, o.Consumer); err != nil {
		return nil, err
	}
	if o.NoAck {
		b = append(b, 1)
	} else {
		b = append(b, 0)
	}
	b = binary.LittleEndian.AppendUint64(b, uint64(o.TimeMs))
	return appendIDList(b, o.IDMs, o.IDSeq)
}

func (o GClaim) appendGroupBody(b []byte) ([]byte, error) {
	if o.Unowned && len(o.Consumer) != 0 {
		return nil, fmt.Errorf("obs1: an unowned claim names a consumer")
	}
	if len(o.IDMs) == 0 {
		return nil, fmt.Errorf("obs1: a claim sub-op records no effects")
	}
	if len(o.IDMs) != len(o.IDSeq) || len(o.IDMs) != len(o.TimeMs) || len(o.IDMs) != len(o.Counts) {
		return nil, fmt.Errorf("obs1: claim columns are %d, %d, %d, and %d long",
			len(o.IDMs), len(o.IDSeq), len(o.TimeMs), len(o.Counts))
	}
	b, err := appendItem(b, o.Group)
	if err != nil {
		return nil, err
	}
	if b, err = appendItem(b, o.Consumer); err != nil {
		return nil, err
	}
	if o.Unowned {
		b = append(b, 1)
	} else {
		b = append(b, 0)
	}
	b = binary.LittleEndian.AppendUint32(b, uint32(len(o.IDMs)))
	for i := range o.IDMs {
		b = binary.LittleEndian.AppendUint64(b, o.IDMs[i])
		b = binary.LittleEndian.AppendUint64(b, o.IDSeq[i])
		b = binary.LittleEndian.AppendUint64(b, uint64(o.TimeMs[i]))
		b = binary.LittleEndian.AppendUint16(b, o.Counts[i])
	}
	return b, nil
}

func parseCursorBody(sub uint8, body []byte) ([]byte, uint64, uint64, uint64, bool, error) {
	group, rest, err := parseItem(body)
	if err != nil {
		return nil, 0, 0, 0, false, err
	}
	if len(rest) != 25 {
		return nil, 0, 0, 0, false, fmt.Errorf("obs1: group delta sub-kind 0x%02x cursor is %d bytes, want 25", sub, len(rest))
	}
	if rest[24] > 1 {
		return nil, 0, 0, 0, false, fmt.Errorf("obs1: group delta read-valid byte is 0x%02x", rest[24])
	}
	return group,
		binary.LittleEndian.Uint64(rest[0:8]),
		binary.LittleEndian.Uint64(rest[8:16]),
		binary.LittleEndian.Uint64(rest[16:24]),
		rest[24] == 1, nil
}

func parseGroupIDList(rest []byte) ([]uint64, []uint64, error) {
	n, b, err := parseCount(rest, 16)
	if err != nil {
		return nil, nil, err
	}
	if len(b) != n*16 {
		return nil, nil, fmt.Errorf("obs1: group delta id list is %d bytes, want %d", len(b), n*16)
	}
	ms := make([]uint64, n)
	seqs := make([]uint64, n)
	for i := range n {
		ms[i] = binary.LittleEndian.Uint64(b[i*16:])
		seqs[i] = binary.LittleEndian.Uint64(b[i*16+8:])
	}
	return ms, seqs, nil
}

func parseGroupDelta(p []byte) (Op, error) {
	if len(p) < 1 {
		return nil, fmt.Errorf("obs1: a group delta payload is empty")
	}
	sub, body := p[0], p[1:]
	switch sub {
	case GSubNew:
		group, lastMs, lastSeq, read, valid, err := parseCursorBody(sub, body)
		if err != nil {
			return nil, err
		}
		return GroupDelta{Sub: GNew{Group: group, LastMs: lastMs, LastSeq: lastSeq, EntriesRead: read, ReadValid: valid}}, nil
	case GSubSetID:
		group, lastMs, lastSeq, read, valid, err := parseCursorBody(sub, body)
		if err != nil {
			return nil, err
		}
		return GroupDelta{Sub: GSetID{Group: group, LastMs: lastMs, LastSeq: lastSeq, EntriesRead: read, ReadValid: valid}}, nil
	case GSubDrop:
		group, rest, err := parseItem(body)
		if err != nil {
			return nil, err
		}
		if len(rest) != 0 {
			return nil, fmt.Errorf("obs1: group drop carries %d trailing bytes", len(rest))
		}
		return GroupDelta{Sub: GDrop{Group: group}}, nil
	case GSubConsumerNew:
		group, rest, err := parseItem(body)
		if err != nil {
			return nil, err
		}
		consumer, rest, err := parseItem(rest)
		if err != nil {
			return nil, err
		}
		if len(rest) != 8 {
			return nil, fmt.Errorf("obs1: consumer-new tail is %d bytes, want 8", len(rest))
		}
		return GroupDelta{Sub: GConsumerNew{Group: group, Consumer: consumer, SeenMs: int64(binary.LittleEndian.Uint64(rest))}}, nil
	case GSubConsumerDel:
		group, rest, err := parseItem(body)
		if err != nil {
			return nil, err
		}
		consumer, rest, err := parseItem(rest)
		if err != nil {
			return nil, err
		}
		if len(rest) != 0 {
			return nil, fmt.Errorf("obs1: consumer-del carries %d trailing bytes", len(rest))
		}
		return GroupDelta{Sub: GConsumerDel{Group: group, Consumer: consumer}}, nil
	case GSubAck:
		group, rest, err := parseItem(body)
		if err != nil {
			return nil, err
		}
		ms, seqs, err := parseGroupIDList(rest)
		if err != nil {
			return nil, err
		}
		return GroupDelta{Sub: GAck{Group: group, IDMs: ms, IDSeq: seqs}}, nil
	case GSubDeliver:
		group, rest, err := parseItem(body)
		if err != nil {
			return nil, err
		}
		consumer, rest, err := parseItem(rest)
		if err != nil {
			return nil, err
		}
		if len(rest) < 9 {
			return nil, fmt.Errorf("obs1: deliver tail is %d bytes, want at least 9", len(rest))
		}
		if rest[0] > 1 {
			return nil, fmt.Errorf("obs1: deliver noack byte is 0x%02x", rest[0])
		}
		noAck := rest[0] == 1
		timeMs := int64(binary.LittleEndian.Uint64(rest[1:9]))
		ms, seqs, err := parseGroupIDList(rest[9:])
		if err != nil {
			return nil, err
		}
		return GroupDelta{Sub: GDeliver{Group: group, Consumer: consumer, NoAck: noAck, TimeMs: timeMs, IDMs: ms, IDSeq: seqs}}, nil
	case GSubClaim:
		group, rest, err := parseItem(body)
		if err != nil {
			return nil, err
		}
		consumer, rest, err := parseItem(rest)
		if err != nil {
			return nil, err
		}
		if len(rest) < 1 {
			return nil, fmt.Errorf("obs1: claim tail is empty")
		}
		if rest[0] > 1 {
			return nil, fmt.Errorf("obs1: claim unowned byte is 0x%02x", rest[0])
		}
		unowned := rest[0] == 1
		if unowned && len(consumer) != 0 {
			return nil, fmt.Errorf("obs1: an unowned claim names a consumer")
		}
		n, b, err := parseCount(rest[1:], 26)
		if err != nil {
			return nil, err
		}
		if len(b) != n*26 {
			return nil, fmt.Errorf("obs1: claim entry list is %d bytes, want %d", len(b), n*26)
		}
		ms := make([]uint64, n)
		seqs := make([]uint64, n)
		times := make([]int64, n)
		counts := make([]uint16, n)
		for i := range n {
			e := b[i*26:]
			ms[i] = binary.LittleEndian.Uint64(e[0:8])
			seqs[i] = binary.LittleEndian.Uint64(e[8:16])
			times[i] = int64(binary.LittleEndian.Uint64(e[16:24]))
			counts[i] = binary.LittleEndian.Uint16(e[24:26])
		}
		return GroupDelta{Sub: GClaim{Group: group, Consumer: consumer, Unowned: unowned, IDMs: ms, IDSeq: seqs, TimeMs: times, Counts: counts}}, nil
	default:
		return nil, fmt.Errorf("obs1: group delta sub-kind 0x%02x is not in the doc 04 table", sub)
	}
}
