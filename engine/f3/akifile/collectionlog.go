package akifile

import "encoding/binary"

// The collection payload codec (spec 2064/f3/M8-collection-durability-plan). A
// collection record frame is an ordinary record frame (recordlog.go) whose flag
// is RecFlagCollectionOp or RecFlagCollectionSnap and whose value slot holds one
// of the payloads framed here. akifile leaves the payload's meaning to the store
// the way it leaves a value word opaque: a kind byte tags the type so recovery
// routes the frame to the right rebuild, an op byte names the mutation, and the
// sub-key, sub-value, header, and element run are the store's own bytes carried
// verbatim. This file owns only the byte layout, so a round-trip test proves it
// without a store, exactly as recordlog.go frames a record without one.
//
// The effect payload is the small per-mutation frame a collection cuts on every
// mutating command, bounded to the one member, field, or entry the command
// touched. The snapshot payload is the whole-collection base a checkpoint writes,
// the type's cold-encoder element run plus the key-level header the cold frames
// leave out. Effect frames replay in append order through the type's mutation
// funnel; a snapshot frame resets the key then the later effect tail replays past
// it.

// CollKind tags the collection type a payload belongs to, matching the M7 cold
// codec kinds so a durable frame and a cold chunk name a type the same way. It is
// an opaque identifier to akifile: the codec carries it, recovery routes on it,
// akifile never acts on it.
type CollKind uint8

const (
	CollKindSet    CollKind = 0x01
	CollKindZset   CollKind = 0x02
	CollKindList   CollKind = 0x03
	CollKindHash   CollKind = 0x04
	CollKindStream CollKind = 0x05
)

// CollOpRow is the decoded form of one collection effect payload: the type, the
// op the store defines (add, remove, set an auxiliary attribute, trim, and so on,
// an opaque byte akifile carries but does not interpret), the sub-key the op
// targets (a member, a field, a stream id), and the sub-value it carries (a score,
// a field value, a trim bound, or nothing). SubKey and SubValue alias the payload
// they were parsed from.
type CollOpRow struct {
	Kind     CollKind
	Op       uint8
	SubKey   []byte
	SubValue []byte
}

// AppendCollOp frames one effect payload: kind, op, the uvarint sub-key length,
// the sub-key, then the sub-value as the trailing bytes. The sub-value needs no
// length because it runs to the end of the payload, whose length the record
// frame's value_len already bounds. It returns the grown buffer.
func AppendCollOp(dst []byte, row CollOpRow) []byte {
	dst = append(dst, byte(row.Kind), row.Op)
	var lb [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(lb[:], uint64(len(row.SubKey)))
	dst = append(dst, lb[:n]...)
	dst = append(dst, row.SubKey...)
	dst = append(dst, row.SubValue...)
	return dst
}

// ParseCollOp decodes an effect payload. A payload too short for the kind and op
// bytes, a torn sub-key length varint, or a sub-key length that outruns the
// payload is a corrupt frame, returned as ErrLength; the CRC on the record frame
// has already cleared a torn write, so this guards only a malformed but
// CRC-clean payload.
func ParseCollOp(payload []byte) (CollOpRow, error) {
	if len(payload) < 2 {
		return CollOpRow{}, ErrShort
	}
	row := CollOpRow{Kind: CollKind(payload[0]), Op: payload[1]}
	rest := payload[2:]
	klen, adv := binary.Uvarint(rest)
	if adv <= 0 {
		return CollOpRow{}, ErrLength
	}
	rest = rest[adv:]
	if klen > uint64(len(rest)) {
		return CollOpRow{}, ErrLength
	}
	row.SubKey = rest[:klen]
	row.SubValue = rest[klen:]
	return row, nil
}

// CollSnapRow is the decoded form of one whole-collection snapshot payload: the
// type, the key-level header the cold frames leave out (the key expiry, the
// encoding band, and for a stream the counters and the group and PEL ledger), and
// the element run the type's cold encoder frames verbatim. Header and ElementRun
// alias the payload.
type CollSnapRow struct {
	Kind       CollKind
	Header     []byte
	ElementRun []byte
}

// AppendCollSnap frames one snapshot payload: kind, the uvarint header length, the
// header, then the element run as the trailing bytes. The element run needs no
// length because it runs to the end of the payload. It returns the grown buffer.
func AppendCollSnap(dst []byte, row CollSnapRow) []byte {
	dst = append(dst, byte(row.Kind))
	var lb [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(lb[:], uint64(len(row.Header)))
	dst = append(dst, lb[:n]...)
	dst = append(dst, row.Header...)
	dst = append(dst, row.ElementRun...)
	return dst
}

// ParseCollSnap decodes a snapshot payload. A payload too short for the kind byte,
// a torn header length varint, or a header length that outruns the payload is a
// corrupt frame, returned as ErrLength.
func ParseCollSnap(payload []byte) (CollSnapRow, error) {
	if len(payload) < 1 {
		return CollSnapRow{}, ErrShort
	}
	row := CollSnapRow{Kind: CollKind(payload[0])}
	rest := payload[1:]
	hlen, adv := binary.Uvarint(rest)
	if adv <= 0 {
		return CollSnapRow{}, ErrLength
	}
	rest = rest[adv:]
	if hlen > uint64(len(rest)) {
		return CollSnapRow{}, ErrLength
	}
	row.Header = rest[:hlen]
	row.ElementRun = rest[hlen:]
	return row, nil
}
