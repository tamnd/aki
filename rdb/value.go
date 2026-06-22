package rdb

import (
	"errors"
	"sort"
	"strconv"

	"github.com/tamnd/aki/encoding"
)

// Version is the RDB payload version aki writes into the DUMP footer. Redis 7.x
// uses 11; we accept up to maxVersion on load so a payload from a slightly newer
// peer still restores.
const (
	Version    = 11
	maxVersion = 12
)

// RDB value type bytes. aki writes the smallest encoding for the current shape of
// a value and reads every form it might receive from a real Redis.
const (
	typeString         = 0
	typeSet            = 2
	typeHash           = 4
	typeZSet2          = 5
	typeListQuicklist2 = 13
	typeSetListpack    = 15
	typeZSetListpack   = 16
	typeHashListpack   = 17
	typeStream         = 19
	typeSetIntset      = 21

	typeListLegacy  = 1
	typeZSetLegacy  = 3
	typeListQL1     = 10
	typeHashZiplist = 12
)

// Listpack and intset size thresholds, matching the Redis defaults. A collection
// at or below these limits is written in its packed form.
const (
	maxListpackEntries = 128
	maxListpackValue   = 64
	maxIntsetEntries   = 512
)

// Kind tags the logical type a Value carries.
type Kind int

const (
	KindString Kind = iota
	KindList
	KindSet
	KindHash
	KindZSet
	KindStream
)

// Field is one hash entry.
type Field struct {
	Field []byte
	Value []byte
}

// Member is one sorted-set entry.
type Member struct {
	Member []byte
	Score  float64
}

// Value is a decoded logical value, the bridge between aki's keyspace and the RDB
// byte form. The command layer fills the field matching Kind and hands it to
// Marshal, or reads it back from Unmarshal.
type Value struct {
	Kind   Kind
	Str    []byte
	List   [][]byte
	Set    [][]byte
	Hash   []Field
	ZSet   []Member
	Stream *StreamData
}

// ErrUnsupported means the payload used a type aki does not deserialize yet, such
// as a module value. RESTORE maps it to the generic bad-payload reply.
var ErrUnsupported = errors.New("rdb: unsupported value type")

// Marshal serializes v into a DUMP payload: the type byte and value bytes, then a
// 2-byte little-endian version, then an 8-byte little-endian CRC-64 over all the
// preceding bytes.
func Marshal(v Value) ([]byte, error) {
	typeByte, body, err := encodeBody(v)
	if err != nil {
		return nil, err
	}
	out := append([]byte{typeByte}, body...)
	out = encoding.AppendU16(out, Version)
	out = appendCRC64(out, out)
	return out, nil
}

// encodeBody returns the RDB type byte and the value body for v, the two pieces a
// DUMP payload concatenates and a file record separates with the key. The body
// uses the smallest form for the value the way Redis does.
func encodeBody(v Value) (byte, []byte, error) {
	switch v.Kind {
	case KindString:
		return typeString, appendString(nil, v.Str), nil
	case KindList:
		return encodeList(v.List)
	case KindSet:
		return encodeSet(v.Set)
	case KindHash:
		return encodeHash(v.Hash)
	case KindZSet:
		return encodeZSet(v.ZSet)
	case KindStream:
		return encodeStream(v)
	default:
		return 0, nil, ErrUnsupported
	}
}

// encodeList writes a list as a quicklist v2 with a single packed listpack node,
// which is the form Redis uses for any list that fits one node.
func encodeList(elems [][]byte) (byte, []byte, error) {
	var body []byte
	body = appendLength(body, 1) // one node
	body = appendLength(body, 2) // container PACKED
	blob := listpackEncode(elems)
	body = appendLength(body, uint64(len(blob)))
	return typeListQuicklist2, append(body, blob...), nil
}

// encodeSet writes the smallest set form: intset when every member is an integer
// and the count is small, listpack when small and short, otherwise a plain
// hashtable list of strings.
func encodeSet(members [][]byte) (byte, []byte, error) {
	if vals, ok := intsetEncodable(members); ok && len(members) <= maxIntsetEntries {
		blob := intsetEncode(vals)
		body := appendLength(nil, uint64(len(blob)))
		return typeSetIntset, append(body, blob...), nil
	}
	if len(members) <= maxListpackEntries && allShort(members) {
		blob := listpackEncode(members)
		body := appendLength(nil, uint64(len(blob)))
		return typeSetListpack, append(body, blob...), nil
	}
	body := appendLength(nil, uint64(len(members)))
	for _, m := range members {
		body = appendString(body, m)
	}
	return typeSet, body, nil
}

// encodeHash writes a listpack hash when small, otherwise a plain hashtable hash.
func encodeHash(fields []Field) (byte, []byte, error) {
	small := len(fields) <= maxListpackEntries
	if small {
		for _, f := range fields {
			if len(f.Field) > maxListpackValue || len(f.Value) > maxListpackValue {
				small = false
				break
			}
		}
	}
	if small {
		flat := make([][]byte, 0, len(fields)*2)
		for _, f := range fields {
			flat = append(flat, f.Field, f.Value)
		}
		blob := listpackEncode(flat)
		body := appendLength(nil, uint64(len(blob)))
		return typeHashListpack, append(body, blob...), nil
	}
	body := appendLength(nil, uint64(len(fields)))
	for _, f := range fields {
		body = appendString(body, f.Field)
		body = appendString(body, f.Value)
	}
	return typeHash, body, nil
}

// encodeZSet writes a listpack sorted set when small, otherwise the binary-double
// ZSET_2 form. The listpack interleaves each member with its score as text.
func encodeZSet(members []Member) (byte, []byte, error) {
	small := len(members) <= maxListpackEntries
	if small {
		for _, m := range members {
			if len(m.Member) > maxListpackValue {
				small = false
				break
			}
		}
	}
	if small {
		flat := make([][]byte, 0, len(members)*2)
		for _, m := range members {
			flat = append(flat, m.Member, scoreText(m.Score))
		}
		blob := listpackEncode(flat)
		body := appendLength(nil, uint64(len(blob)))
		return typeZSetListpack, append(body, blob...), nil
	}
	body := appendLength(nil, uint64(len(members)))
	for _, m := range members {
		body = appendString(body, m.Member)
		body = encoding.AppendF64(body, m.Score)
	}
	return typeZSet2, body, nil
}

// Unmarshal validates a DUMP payload and decodes its value. It checks the CRC-64
// (unless the footer is all zeros, the rdbchecksum-off convention) and rejects a
// version above what aki supports, both reported by RESTORE as the same wrong
// payload error.
func Unmarshal(payload []byte) (Value, error) {
	if len(payload) < 11 {
		return Value{}, errTruncated
	}
	body := payload[:len(payload)-8]
	stored := encoding.U64(payload[len(payload)-8:])
	if stored != 0 && crc64(0, body) != stored {
		return Value{}, errTruncated
	}
	ver := encoding.U16(payload[len(payload)-10 : len(payload)-8])
	if ver > maxVersion {
		return Value{}, errTruncated
	}
	r := &reader{buf: payload[:len(payload)-10]}
	typeByte := r.readByte()
	v, err := decodeValue(r, typeByte)
	if err != nil {
		return Value{}, err
	}
	if r.err != nil {
		return Value{}, r.err
	}
	return v, nil
}

// decodeValue reads the value body for a given type byte.
func decodeValue(r *reader, typeByte byte) (Value, error) {
	switch typeByte {
	case typeString:
		return Value{Kind: KindString, Str: r.readString()}, r.err
	case typeListQuicklist2, typeListQL1:
		elems, err := decodeQuicklist(r)
		return Value{Kind: KindList, List: elems}, err
	case typeListLegacy:
		// Legacy ziplist list: a single ziplist blob. aki reads it as a listpack,
		// which shares the forward layout for the entries we emit.
		blob := r.readString()
		elems, err := listpackDecode(blob)
		return Value{Kind: KindList, List: elems}, err
	case typeSet:
		return decodeStringSet(r)
	case typeSetIntset:
		blob := r.readString()
		if r.err != nil {
			return Value{}, r.err
		}
		elems, err := intsetDecode(blob)
		return Value{Kind: KindSet, Set: elems}, err
	case typeSetListpack:
		blob := r.readString()
		if r.err != nil {
			return Value{}, r.err
		}
		elems, err := listpackDecode(blob)
		return Value{Kind: KindSet, Set: elems}, err
	case typeHash:
		return decodeStringHash(r)
	case typeHashListpack, typeHashZiplist:
		blob := r.readString()
		if r.err != nil {
			return Value{}, r.err
		}
		flat, err := listpackDecode(blob)
		if err != nil {
			return Value{}, err
		}
		return Value{Kind: KindHash, Hash: pairsToFields(flat)}, nil
	case typeZSet2:
		return decodeZSet2(r)
	case typeZSetLegacy:
		return decodeZSetLegacy(r)
	case typeZSetListpack:
		blob := r.readString()
		if r.err != nil {
			return Value{}, r.err
		}
		flat, err := listpackDecode(blob)
		if err != nil {
			return Value{}, err
		}
		m, err := pairsToMembers(flat)
		return Value{Kind: KindZSet, ZSet: m}, err
	case typeStream:
		return decodeStream(r)
	default:
		return Value{}, ErrUnsupported
	}
}

// decodeQuicklist reads a quicklist v2: a node count then each node's container
// flag and blob. A packed node is a listpack, a plain node is a single raw
// element.
func decodeQuicklist(r *reader) ([][]byte, error) {
	nodes, _, _ := r.readLength()
	var out [][]byte
	for range nodes {
		container, _, _ := r.readLength()
		blob := r.readString()
		if r.err != nil {
			return nil, r.err
		}
		if container == 1 { // PLAIN
			out = append(out, blob)
			continue
		}
		elems, err := listpackDecode(blob)
		if err != nil {
			return nil, err
		}
		out = append(out, elems...)
	}
	return out, nil
}

// sliceHint caps a count read from untrusted bytes before it sizes a slice. A
// corrupt length prefix can decode to a huge count, and make() panics with "cap
// out of range" if that count exceeds the max slice cap. The decode loop still
// appends every real element; this only bounds the preallocation.
func sliceHint(n uint64) int {
	const max = 1 << 16
	if n > max {
		return max
	}
	return int(n)
}

// decodeStringSet reads a plain hashtable set: a count then that many strings.
func decodeStringSet(r *reader) (Value, error) {
	n, _, _ := r.readLength()
	out := make([][]byte, 0, sliceHint(n))
	for range n {
		s := r.readString()
		if r.err != nil {
			return Value{}, r.err
		}
		out = append(out, s)
	}
	return Value{Kind: KindSet, Set: out}, r.err
}

// decodeStringHash reads a plain hashtable hash: a count then field/value strings.
func decodeStringHash(r *reader) (Value, error) {
	n, _, _ := r.readLength()
	out := make([]Field, 0, sliceHint(n))
	for range n {
		f := r.readString()
		v := r.readString()
		if r.err != nil {
			return Value{}, r.err
		}
		out = append(out, Field{Field: f, Value: v})
	}
	return Value{Kind: KindHash, Hash: out}, r.err
}

// decodeZSet2 reads the binary-double sorted set: a count then member string and
// 8-byte little-endian score per entry.
func decodeZSet2(r *reader) (Value, error) {
	n, _, _ := r.readLength()
	out := make([]Member, 0, sliceHint(n))
	for range n {
		m := r.readString()
		b := r.readBytes(8)
		if r.err != nil {
			return Value{}, r.err
		}
		out = append(out, Member{Member: m, Score: encoding.F64(b)})
	}
	return Value{Kind: KindZSet, ZSet: out}, r.err
}

// decodeZSetLegacy reads the old sorted set where the score is a length-prefixed
// ASCII double rather than binary.
func decodeZSetLegacy(r *reader) (Value, error) {
	n, _, _ := r.readLength()
	out := make([]Member, 0, sliceHint(n))
	for range n {
		m := r.readString()
		sb := r.readString()
		if r.err != nil {
			return Value{}, r.err
		}
		score, err := strconv.ParseFloat(string(sb), 64)
		if err != nil {
			return Value{}, err
		}
		out = append(out, Member{Member: m, Score: score})
	}
	return Value{Kind: KindZSet, ZSet: out}, r.err
}

// pairsToFields turns an interleaved field/value list into hash entries.
func pairsToFields(flat [][]byte) []Field {
	out := make([]Field, 0, len(flat)/2)
	for i := 0; i+1 < len(flat); i += 2 {
		out = append(out, Field{Field: flat[i], Value: flat[i+1]})
	}
	return out
}

// pairsToMembers turns an interleaved member/score list into sorted-set entries.
func pairsToMembers(flat [][]byte) ([]Member, error) {
	out := make([]Member, 0, len(flat)/2)
	for i := 0; i+1 < len(flat); i += 2 {
		score, err := strconv.ParseFloat(string(flat[i+1]), 64)
		if err != nil {
			return nil, err
		}
		out = append(out, Member{Member: flat[i], Score: score})
	}
	return out, nil
}

// allShort reports whether every element is within the listpack value limit.
func allShort(elems [][]byte) bool {
	for _, e := range elems {
		if len(e) > maxListpackValue {
			return false
		}
	}
	return true
}

// scoreText renders a score the way a listpack zset stores it: an integral score
// as its integer text so it packs as a listpack integer, otherwise the shortest
// round-tripping decimal.
func scoreText(score float64) []byte {
	if score == float64(int64(score)) {
		return []byte(strconv.FormatInt(int64(score), 10))
	}
	return []byte(strconv.FormatFloat(score, 'g', 17, 64))
}

// SortMembers orders sorted-set members by score then member bytes, the order
// aki's stored body expects. RESTORE calls it before re-encoding a decoded set.
func SortMembers(members []Member) {
	sort.SliceStable(members, func(i, j int) bool {
		if members[i].Score != members[j].Score {
			return members[i].Score < members[j].Score
		}
		return string(members[i].Member) < string(members[j].Member)
	})
}
