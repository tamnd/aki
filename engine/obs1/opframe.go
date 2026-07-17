// Op frames (spec 2064/obs1 doc 04 section 2): the semantics layer over
// the doc 03 section 4 WAL frame, whose key and payload wal.go carries
// as opaque bytes. The WAL is logical: frames record effects, not
// commands, so replay never re-runs randomness, clocks, or arithmetic.
// SPOP writes an srem carrying the members it chose, the INCR family
// writes the resulting value as a strset with the counter hint, and
// expiry-relative commands write absolute milliseconds; that is why the
// sub-op vocabulary has no spop or incr entries.
//
// Structure is strict, hints are opaque. Unknown kinds, sub-kinds, and
// collection types are rejected because replay dispatches on them, and
// new ones arrive with an fversion bump, the chain-record rule. The
// strset ladder byte and the collnew hint bytes pass through untouched:
// doc 08 owns their vocabulary and replay correctness never depends on
// them.
//
// Doc 04 lists the sub-kind names and defers the byte encodings to doc
// 08, which never pins them, so this file does: every variable-length
// list is a u32 count followed by u32-length-prefixed items, scores are
// float64 bits, stream ids are the explicit ms/seq pair the owner
// already decided. DecodeOp enforces canonical form by re-encoding.
package obs1

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
)

// Op frame kinds (doc 04 section 2).
const (
	OpStrSet    = 0x01
	OpKeyDel    = 0x02
	OpExpire    = 0x03
	OpCollDelta = 0x04
	OpCollNew   = 0x05
	OpCollDrop  = 0x06
	OpTxn       = 0x07
	OpNoop      = 0x08
)

// Txn marker frame flags (doc 04 section 2): bit 0 opens a run, bit 1
// closes it, exactly one per marker. Every other kind keeps the frame's
// flags byte zero.
const (
	TxnBegin = 0x01
	TxnEnd   = 0x02
)

// LadderCounter marks a strset produced by the INCR family, doc 08's
// counter encoding hint. The rest of the ladder byte belongs to doc 08.
const LadderCounter = 0x01

// Collection types for collnew (doc 08's five heaps).
const (
	CollHash   = 0x01
	CollSet    = 0x02
	CollZSet   = 0x03
	CollList   = 0x04
	CollStream = 0x05
)

// Colldelta sub-kinds (doc 04 section 2's named set; hexpire filled the
// hash-field-expiry gap the table left open, #1023's next-slice item).
const (
	SubHSet    = 0x01
	SubHDel    = 0x02
	SubSAdd    = 0x03
	SubSRem    = 0x04
	SubZAdd    = 0x05
	SubZRem    = 0x06
	SubLPush   = 0x07
	SubRPush   = 0x08
	SubLPop    = 0x09
	SubRPop    = 0x0A
	SubLSet    = 0x0B
	SubXAdd    = 0x0C
	SubHExpire = 0x0D
)

// Op is one doc 04 op: exactly one of the eight kinds.
type Op interface {
	opKind() uint8
	opFlags() uint8
	appendPayload(b []byte) ([]byte, error)
}

// StrSet (0x01) sets a string key: value bytes, absolute expiry ms (0
// none), and the doc 08 ladder hint byte. INCR lands here as the
// resulting value with LadderCounter set.
type StrSet struct {
	Value    []byte
	ExpiryMS uint64
	Ladder   uint8
}

// KeyDel (0x02) removes a key of any type.
type KeyDel struct{}

// Expire (0x03) sets a key's absolute expiry ms; 0 means persist.
type Expire struct {
	ExpiryMS uint64
}

// CollDelta (0x04) applies one collection sub-op to an existing
// collection key.
type CollDelta struct {
	Sub CollSub
}

// CollNew (0x05) creates an empty collection; the hint bytes are doc
// 08's encoding hints, opaque here.
type CollNew struct {
	Type  uint8
	Hints []byte
}

// CollDrop (0x06) removes a collection key.
type CollDrop struct{}

// Txn (0x07) is a marker frame grouping a MULTI/EXEC or a
// single-command multi-effect run; replay applies the run atomically or
// not at all. Exactly one of Begin and End is set.
type Txn struct {
	Begin bool
	End   bool
}

// Noop (0x08) is padding and testing; it carries no key and its pad
// bytes mean nothing.
type Noop struct {
	Pad []byte
}

func (StrSet) opKind() uint8  { return OpStrSet }
func (StrSet) opFlags() uint8 { return 0 }

func (o StrSet) appendPayload(b []byte) ([]byte, error) {
	b = append(b, o.Value...)
	b = binary.LittleEndian.AppendUint64(b, o.ExpiryMS)
	return append(b, o.Ladder), nil
}

func (KeyDel) opKind() uint8  { return OpKeyDel }
func (KeyDel) opFlags() uint8 { return 0 }

func (KeyDel) appendPayload(b []byte) ([]byte, error) { return b, nil }

func (Expire) opKind() uint8  { return OpExpire }
func (Expire) opFlags() uint8 { return 0 }

func (o Expire) appendPayload(b []byte) ([]byte, error) {
	return binary.LittleEndian.AppendUint64(b, o.ExpiryMS), nil
}

func (CollDelta) opKind() uint8  { return OpCollDelta }
func (CollDelta) opFlags() uint8 { return 0 }

func (o CollDelta) appendPayload(b []byte) ([]byte, error) {
	if o.Sub == nil {
		return nil, fmt.Errorf("obs1: a colldelta frame needs a sub-op")
	}
	return o.Sub.appendBody(append(b, o.Sub.subKind()))
}

func (CollNew) opKind() uint8  { return OpCollNew }
func (CollNew) opFlags() uint8 { return 0 }

func (o CollNew) appendPayload(b []byte) ([]byte, error) {
	if o.Type < CollHash || o.Type > CollStream {
		return nil, fmt.Errorf("obs1: collnew type 0x%02x is not a doc 08 collection type", o.Type)
	}
	return append(append(b, o.Type), o.Hints...), nil
}

func (CollDrop) opKind() uint8  { return OpCollDrop }
func (CollDrop) opFlags() uint8 { return 0 }

func (CollDrop) appendPayload(b []byte) ([]byte, error) { return b, nil }

func (Txn) opKind() uint8 { return OpTxn }

func (o Txn) opFlags() uint8 {
	var f uint8
	if o.Begin {
		f |= TxnBegin
	}
	if o.End {
		f |= TxnEnd
	}
	return f
}

func (o Txn) appendPayload(b []byte) ([]byte, error) {
	if o.Begin == o.End {
		return nil, fmt.Errorf("obs1: a txn marker opens or closes a run, exactly one")
	}
	return b, nil
}

func (Noop) opKind() uint8  { return OpNoop }
func (Noop) opFlags() uint8 { return 0 }

func (o Noop) appendPayload(b []byte) ([]byte, error) { return append(b, o.Pad...), nil }

// CollSub is one colldelta sub-op: exactly one of the thirteen doc 04
// sub-kinds.
type CollSub interface {
	subKind() uint8
	appendBody(b []byte) ([]byte, error)
}

// FieldValue is one hash field or stream entry pair.
type FieldValue struct {
	Field []byte
	Value []byte
}

// ScoreMember is one zadd entry; the score travels as float64 bits, so
// negative zero and infinities survive byte-exactly.
type ScoreMember struct {
	Score  float64
	Member []byte
}

// HSet sets hash fields to values.
type HSet struct {
	Pairs []FieldValue
}

// HDel removes hash fields.
type HDel struct {
	Fields [][]byte
}

// SAdd adds set members.
type SAdd struct {
	Members [][]byte
}

// SRem removes set members; SPOP's post-decision form, carrying the
// members the owner chose.
type SRem struct {
	Members [][]byte
}

// ZAdd upserts scored members.
type ZAdd struct {
	Entries []ScoreMember
}

// ZRem removes sorted-set members.
type ZRem struct {
	Members [][]byte
}

// LPush prepends values, in push order.
type LPush struct {
	Values [][]byte
}

// RPush appends values, in push order.
type RPush struct {
	Values [][]byte
}

// LPop removes Count elements from the head; deterministic given prior
// state, so the frame records the decided count, not the values.
type LPop struct {
	Count uint32
}

// RPop removes Count elements from the tail.
type RPop struct {
	Count uint32
}

// LSet overwrites the element at Index (negative counts from the tail,
// the owner's already-validated index).
type LSet struct {
	Index int64
	Value []byte
}

// XAdd appends one stream entry at the explicit id the owner decided,
// so replay never re-runs the clock.
type XAdd struct {
	IDMs  uint64
	IDSeq uint64
	Pairs []FieldValue
}

// HExpire sets the named hash fields' expiry to the absolute deadline
// AtMs, or clears it when AtMs is 0 (HPERSIST), the same zero the expire
// kind uses. Post-decision like everything else: the owner already
// applied its NX/XX/GT/LT gate and unit conversion, and a set-to-the-past
// that deleted fields rides an hdel instead. It also restores a deadline
// a TTL-preserving write kept (HINCRBY on a field with one), because the
// hset replay rule clears an overwritten field's TTL, the HSET behavior.
type HExpire struct {
	AtMs   uint64
	Fields [][]byte
}

func (HSet) subKind() uint8    { return SubHSet }
func (HDel) subKind() uint8    { return SubHDel }
func (SAdd) subKind() uint8    { return SubSAdd }
func (SRem) subKind() uint8    { return SubSRem }
func (ZAdd) subKind() uint8    { return SubZAdd }
func (ZRem) subKind() uint8    { return SubZRem }
func (LPush) subKind() uint8   { return SubLPush }
func (RPush) subKind() uint8   { return SubRPush }
func (LPop) subKind() uint8    { return SubLPop }
func (RPop) subKind() uint8    { return SubRPop }
func (LSet) subKind() uint8    { return SubLSet }
func (XAdd) subKind() uint8    { return SubXAdd }
func (HExpire) subKind() uint8 { return SubHExpire }

func (o HSet) appendBody(b []byte) ([]byte, error)  { return appendPairList(b, o.Pairs) }
func (o HDel) appendBody(b []byte) ([]byte, error)  { return appendByteList(b, o.Fields) }
func (o SAdd) appendBody(b []byte) ([]byte, error)  { return appendByteList(b, o.Members) }
func (o SRem) appendBody(b []byte) ([]byte, error)  { return appendByteList(b, o.Members) }
func (o ZRem) appendBody(b []byte) ([]byte, error)  { return appendByteList(b, o.Members) }
func (o LPush) appendBody(b []byte) ([]byte, error) { return appendByteList(b, o.Values) }
func (o RPush) appendBody(b []byte) ([]byte, error) { return appendByteList(b, o.Values) }

func (o ZAdd) appendBody(b []byte) ([]byte, error) {
	if len(o.Entries) == 0 {
		return nil, fmt.Errorf("obs1: a zadd sub-op records no effects")
	}
	b = binary.LittleEndian.AppendUint32(b, uint32(len(o.Entries)))
	for _, e := range o.Entries {
		if math.IsNaN(e.Score) {
			return nil, fmt.Errorf("obs1: a zadd score cannot be NaN")
		}
		b = binary.LittleEndian.AppendUint64(b, math.Float64bits(e.Score))
		var err error
		if b, err = appendItem(b, e.Member); err != nil {
			return nil, err
		}
	}
	return b, nil
}

func (o LPop) appendBody(b []byte) ([]byte, error) {
	if o.Count == 0 {
		return nil, fmt.Errorf("obs1: an lpop sub-op records no effects")
	}
	return binary.LittleEndian.AppendUint32(b, o.Count), nil
}

func (o RPop) appendBody(b []byte) ([]byte, error) {
	if o.Count == 0 {
		return nil, fmt.Errorf("obs1: an rpop sub-op records no effects")
	}
	return binary.LittleEndian.AppendUint32(b, o.Count), nil
}

func (o LSet) appendBody(b []byte) ([]byte, error) {
	b = binary.LittleEndian.AppendUint64(b, uint64(o.Index))
	return append(b, o.Value...), nil
}

func (o XAdd) appendBody(b []byte) ([]byte, error) {
	b = binary.LittleEndian.AppendUint64(b, o.IDMs)
	b = binary.LittleEndian.AppendUint64(b, o.IDSeq)
	return appendPairList(b, o.Pairs)
}

func (o HExpire) appendBody(b []byte) ([]byte, error) {
	b = binary.LittleEndian.AppendUint64(b, o.AtMs)
	return appendByteList(b, o.Fields)
}

// appendItem writes one u32-length-prefixed item.
func appendItem(b, item []byte) ([]byte, error) {
	if int64(len(item)) > 0xFFFFFFFF {
		return nil, fmt.Errorf("obs1: a sub-op item is %d bytes, the format caps items at 4 GiB", len(item))
	}
	b = binary.LittleEndian.AppendUint32(b, uint32(len(item)))
	return append(b, item...), nil
}

// appendByteList writes a u32 count then length-prefixed items; a frame
// records effects, so an empty list is an error, not an encoding.
func appendByteList(b []byte, items [][]byte) ([]byte, error) {
	if len(items) == 0 {
		return nil, fmt.Errorf("obs1: a sub-op records no effects")
	}
	b = binary.LittleEndian.AppendUint32(b, uint32(len(items)))
	for _, it := range items {
		var err error
		if b, err = appendItem(b, it); err != nil {
			return nil, err
		}
	}
	return b, nil
}

func appendPairList(b []byte, pairs []FieldValue) ([]byte, error) {
	if len(pairs) == 0 {
		return nil, fmt.Errorf("obs1: a sub-op records no effects")
	}
	b = binary.LittleEndian.AppendUint32(b, uint32(len(pairs)))
	for _, p := range pairs {
		var err error
		if b, err = appendItem(b, p.Field); err != nil {
			return nil, err
		}
		if b, err = appendItem(b, p.Value); err != nil {
			return nil, err
		}
	}
	return b, nil
}

// parseItem reads one length-prefixed item and returns the remainder.
func parseItem(b []byte) ([]byte, []byte, error) {
	if len(b) < 4 {
		return nil, nil, fmt.Errorf("obs1: sub-op item truncated at its length")
	}
	n := int(binary.LittleEndian.Uint32(b))
	if n > len(b)-4 {
		return nil, nil, fmt.Errorf("obs1: sub-op item length %d overruns the payload", n)
	}
	return append([]byte(nil), b[4:4+n]...), b[4+n:], nil
}

// parseCount reads a list count, guarding the allocation against a
// count no remaining bytes could satisfy (every item needs 4 bytes).
func parseCount(b []byte, minItem int) (int, []byte, error) {
	if len(b) < 4 {
		return 0, nil, fmt.Errorf("obs1: sub-op list truncated at its count")
	}
	n := int(binary.LittleEndian.Uint32(b))
	if n == 0 {
		return 0, nil, fmt.Errorf("obs1: a sub-op records no effects")
	}
	b = b[4:]
	if uint64(n)*uint64(minItem) > uint64(len(b)) {
		return 0, nil, fmt.Errorf("obs1: sub-op list count %d overruns the payload", n)
	}
	return n, b, nil
}

// parseByteList reads a whole payload remainder as one item list; the
// list is always a sub-op's last field, so trailing bytes are an error.
func parseByteList(b []byte) ([][]byte, error) {
	n, b, err := parseCount(b, 4)
	if err != nil {
		return nil, err
	}
	items := make([][]byte, n)
	for i := range items {
		if items[i], b, err = parseItem(b); err != nil {
			return nil, err
		}
	}
	if len(b) != 0 {
		return nil, fmt.Errorf("obs1: sub-op list has %d trailing bytes", len(b))
	}
	return items, nil
}

func parsePairList(b []byte) ([]FieldValue, error) {
	n, b, err := parseCount(b, 8)
	if err != nil {
		return nil, err
	}
	pairs := make([]FieldValue, n)
	for i := range pairs {
		if pairs[i].Field, b, err = parseItem(b); err != nil {
			return nil, err
		}
		if pairs[i].Value, b, err = parseItem(b); err != nil {
			return nil, err
		}
	}
	if len(b) != 0 {
		return nil, fmt.Errorf("obs1: sub-op pair list has %d trailing bytes", len(b))
	}
	return pairs, nil
}

func parseCollDelta(p []byte) (Op, error) {
	if len(p) < 1 {
		return nil, fmt.Errorf("obs1: a colldelta payload is empty")
	}
	sub, body := p[0], p[1:]
	exact := func(n int) error {
		if len(body) != n {
			return fmt.Errorf("obs1: colldelta sub-kind 0x%02x body is %d bytes, want %d", sub, len(body), n)
		}
		return nil
	}
	switch sub {
	case SubHSet:
		pairs, err := parsePairList(body)
		if err != nil {
			return nil, err
		}
		return CollDelta{Sub: HSet{Pairs: pairs}}, nil
	case SubHDel:
		items, err := parseByteList(body)
		if err != nil {
			return nil, err
		}
		return CollDelta{Sub: HDel{Fields: items}}, nil
	case SubSAdd:
		items, err := parseByteList(body)
		if err != nil {
			return nil, err
		}
		return CollDelta{Sub: SAdd{Members: items}}, nil
	case SubSRem:
		items, err := parseByteList(body)
		if err != nil {
			return nil, err
		}
		return CollDelta{Sub: SRem{Members: items}}, nil
	case SubZAdd:
		n, b, err := parseCount(body, 12)
		if err != nil {
			return nil, err
		}
		entries := make([]ScoreMember, n)
		for i := range entries {
			if len(b) < 8 {
				return nil, fmt.Errorf("obs1: zadd entry truncated at its score")
			}
			entries[i].Score = math.Float64frombits(binary.LittleEndian.Uint64(b))
			if math.IsNaN(entries[i].Score) {
				return nil, fmt.Errorf("obs1: a zadd score cannot be NaN")
			}
			if entries[i].Member, b, err = parseItem(b[8:]); err != nil {
				return nil, err
			}
		}
		if len(b) != 0 {
			return nil, fmt.Errorf("obs1: zadd entry list has %d trailing bytes", len(b))
		}
		return CollDelta{Sub: ZAdd{Entries: entries}}, nil
	case SubZRem:
		items, err := parseByteList(body)
		if err != nil {
			return nil, err
		}
		return CollDelta{Sub: ZRem{Members: items}}, nil
	case SubLPush:
		items, err := parseByteList(body)
		if err != nil {
			return nil, err
		}
		return CollDelta{Sub: LPush{Values: items}}, nil
	case SubRPush:
		items, err := parseByteList(body)
		if err != nil {
			return nil, err
		}
		return CollDelta{Sub: RPush{Values: items}}, nil
	case SubLPop, SubRPop:
		if err := exact(4); err != nil {
			return nil, err
		}
		count := binary.LittleEndian.Uint32(body)
		if count == 0 {
			return nil, fmt.Errorf("obs1: a pop sub-op records no effects")
		}
		if sub == SubLPop {
			return CollDelta{Sub: LPop{Count: count}}, nil
		}
		return CollDelta{Sub: RPop{Count: count}}, nil
	case SubLSet:
		if len(body) < 8 {
			return nil, fmt.Errorf("obs1: lset body is %d bytes, want at least 8", len(body))
		}
		return CollDelta{Sub: LSet{
			Index: int64(binary.LittleEndian.Uint64(body)),
			Value: append([]byte(nil), body[8:]...),
		}}, nil
	case SubXAdd:
		if len(body) < 16 {
			return nil, fmt.Errorf("obs1: xadd body is %d bytes, want at least 16", len(body))
		}
		pairs, err := parsePairList(body[16:])
		if err != nil {
			return nil, err
		}
		return CollDelta{Sub: XAdd{
			IDMs:  binary.LittleEndian.Uint64(body[0:8]),
			IDSeq: binary.LittleEndian.Uint64(body[8:16]),
			Pairs: pairs,
		}}, nil
	case SubHExpire:
		if len(body) < 8 {
			return nil, fmt.Errorf("obs1: hexpire body is %d bytes, want at least 8", len(body))
		}
		items, err := parseByteList(body[8:])
		if err != nil {
			return nil, err
		}
		return CollDelta{Sub: HExpire{
			AtMs:   binary.LittleEndian.Uint64(body),
			Fields: items,
		}}, nil
	}
	// Unknown sub-kinds are rejected, not skipped: replay dispatches on
	// them, and a fourteenth arrives with an fversion bump.
	return nil, fmt.Errorf("obs1: colldelta sub-kind 0x%02x is not a doc 04 sub-kind", sub)
}

// opKeyless reports whether a kind carries no key: txn markers group
// frames and noop is padding, everything else names the key it changes.
func opKeyless(kind uint8) bool { return kind == OpTxn || kind == OpNoop }

// EncodeOp renders one op as a WAL frame ready for AppendWAL.
func EncodeOp(slot uint16, seq uint64, key []byte, op Op) (WALFrame, error) {
	kind := op.opKind()
	if opKeyless(kind) {
		if len(key) != 0 {
			return WALFrame{}, fmt.Errorf("obs1: op kind 0x%02x carries no key", kind)
		}
	} else if len(key) == 0 {
		return WALFrame{}, fmt.Errorf("obs1: op kind 0x%02x needs a key", kind)
	}
	if len(key) > 0xFFFF {
		return WALFrame{}, fmt.Errorf("obs1: op key is %d bytes, the format caps keys at 65535", len(key))
	}
	p, err := op.appendPayload(nil)
	if err != nil {
		return WALFrame{}, err
	}
	return WALFrame{Kind: kind, Flags: op.opFlags(), Slot: slot, Seq: seq, Key: key, Payload: p}, nil
}

// DecodeOp interprets a parsed WAL frame as its doc 04 op, enforcing
// canonical form by re-encoding what it accepted.
func DecodeOp(f WALFrame) (Op, error) {
	if f.Kind != OpTxn && f.Flags != 0 {
		return nil, fmt.Errorf("obs1: op kind 0x%02x has frame flags 0x%02x, only txn markers use them", f.Kind, f.Flags)
	}
	if opKeyless(f.Kind) {
		if len(f.Key) != 0 {
			return nil, fmt.Errorf("obs1: op kind 0x%02x carries no key, frame has one", f.Kind)
		}
	} else if len(f.Key) == 0 {
		return nil, fmt.Errorf("obs1: op kind 0x%02x needs a key, frame has none", f.Kind)
	}
	p := f.Payload
	exact := func(n int) error {
		if len(p) != n {
			return fmt.Errorf("obs1: op kind 0x%02x payload is %d bytes, want %d", f.Kind, len(p), n)
		}
		return nil
	}
	var op Op
	switch f.Kind {
	case OpStrSet:
		if len(p) < 9 {
			return nil, fmt.Errorf("obs1: strset payload is %d bytes, want at least 9", len(p))
		}
		op = StrSet{
			Value:    append([]byte(nil), p[:len(p)-9]...),
			ExpiryMS: binary.LittleEndian.Uint64(p[len(p)-9:]),
			Ladder:   p[len(p)-1],
		}
	case OpKeyDel:
		if err := exact(0); err != nil {
			return nil, err
		}
		op = KeyDel{}
	case OpExpire:
		if err := exact(8); err != nil {
			return nil, err
		}
		op = Expire{ExpiryMS: binary.LittleEndian.Uint64(p)}
	case OpCollDelta:
		var err error
		if op, err = parseCollDelta(p); err != nil {
			return nil, err
		}
	case OpCollNew:
		if len(p) < 1 {
			return nil, fmt.Errorf("obs1: a collnew payload is empty")
		}
		if p[0] < CollHash || p[0] > CollStream {
			return nil, fmt.Errorf("obs1: collnew type 0x%02x is not a doc 08 collection type", p[0])
		}
		op = CollNew{Type: p[0], Hints: append([]byte(nil), p[1:]...)}
	case OpCollDrop:
		if err := exact(0); err != nil {
			return nil, err
		}
		op = CollDrop{}
	case OpTxn:
		if err := exact(0); err != nil {
			return nil, err
		}
		if f.Flags != TxnBegin && f.Flags != TxnEnd {
			return nil, fmt.Errorf("obs1: txn marker flags 0x%02x, want begin 0x01 or end 0x02", f.Flags)
		}
		op = Txn{Begin: f.Flags == TxnBegin, End: f.Flags == TxnEnd}
	case OpNoop:
		op = Noop{Pad: append([]byte(nil), p...)}
	default:
		// Unknown kinds are rejected, not skipped: this build reads
		// fversion 1, where exactly eight kinds exist.
		return nil, fmt.Errorf("obs1: op kind 0x%02x is not a doc 04 kind", f.Kind)
	}
	again, err := EncodeOp(f.Slot, f.Seq, f.Key, op)
	if err != nil {
		return nil, fmt.Errorf("obs1: accepted op fails re-encode: %w", err)
	}
	if again.Flags != f.Flags || !bytes.Equal(again.Payload, f.Payload) {
		return nil, fmt.Errorf("obs1: op payload is not in canonical form")
	}
	return op, nil
}

// AppendStrSetFrame is the owner's hot-path append, the #956 lab encoder
// baked: one strset frame in the wal.go layout with the payload built in
// the same pass, no WALFrame between the shard and the group buffer. The
// bytes are pinned against EncodeOp plus AppendWAL by test, so the fast
// and slow paths cannot drift apart.
func AppendStrSetFrame(b []byte, slot uint16, seq uint64, key, value []byte, expiryMS uint64, ladder uint8) ([]byte, error) {
	if len(key) > 0xFFFF {
		return nil, fmt.Errorf("obs1: op key is %d bytes, the format caps keys at 65535", len(key))
	}
	flen := walFrameFixed + len(key) + len(value) + 9
	if int64(flen) > 0xFFFFFFFF {
		return nil, fmt.Errorf("obs1: WAL frame is %d bytes, the format caps frames at 4 GiB", flen)
	}
	b = binary.LittleEndian.AppendUint32(b, uint32(flen))
	b = append(b, OpStrSet, 0)
	b = binary.LittleEndian.AppendUint16(b, slot)
	b = binary.LittleEndian.AppendUint64(b, seq)
	b = binary.LittleEndian.AppendUint16(b, uint16(len(key)))
	b = append(b, key...)
	b = append(b, value...)
	b = binary.LittleEndian.AppendUint64(b, expiryMS)
	return append(b, ladder), nil
}
