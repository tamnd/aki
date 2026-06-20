package resp

import (
	"math"
	"math/big"
	"strconv"
	"strings"
)

// Decode reads one complete RESP value from buf starting at pos and returns the
// value, the position just past it, and an error. The two error classes are
// load-bearing: ErrNeedMore means the buffer is incomplete and pos is returned
// unchanged so the caller can retry after reading more; a ProtocolError means
// the bytes are invalid and the connection must be closed. Decode handles all
// RESP2 and RESP3 types and is used both by the server to parse replies in
// tests and by the aki cli to decode server output (doc 06 §12).
func Decode(buf []byte, pos int) (RESPValue, int, error) {
	var zero RESPValue
	if pos >= len(buf) {
		return zero, pos, ErrNeedMore
	}
	typeByte := buf[pos]
	start := pos
	pos++

	switch RESPType(typeByte) {
	case TypeSimpleString:
		line, newPos, err := readLine(buf, pos)
		if err != nil {
			return zero, start, err
		}
		return RESPValue{Type: TypeSimpleString, Str: cloneBytes(line)}, newPos, nil

	case TypeError:
		line, newPos, err := readLine(buf, pos)
		if err != nil {
			return zero, start, err
		}
		return RESPValue{Type: TypeError, Err: string(line)}, newPos, nil

	case TypeInteger:
		line, newPos, err := readLine(buf, pos)
		if err != nil {
			return zero, start, err
		}
		n, perr := strconv.ParseInt(string(line), 10, 64)
		if perr != nil {
			return zero, start, ErrProtocol("invalid integer")
		}
		return RESPValue{Type: TypeInteger, Integer: n}, newPos, nil

	case TypeBulkString:
		return decodeBulk(buf, pos, start, TypeBulkString)

	case TypeArray:
		return decodeAggregate(buf, pos, start, TypeArray)

	case TypeNull:
		if pos+1 >= len(buf) {
			return zero, start, ErrNeedMore
		}
		if buf[pos] != '\r' || buf[pos+1] != '\n' {
			return zero, start, ErrProtocol("invalid null")
		}
		return RESPValue{Type: TypeNull, IsNull: true}, pos + 2, nil

	case TypeBool:
		if pos+2 >= len(buf) {
			return zero, start, ErrNeedMore
		}
		b := buf[pos]
		if (b != 't' && b != 'f') || buf[pos+1] != '\r' || buf[pos+2] != '\n' {
			return zero, start, ErrProtocol("invalid boolean")
		}
		return RESPValue{Type: TypeBool, Bool: b == 't'}, pos + 3, nil

	case TypeDouble:
		line, newPos, err := readLine(buf, pos)
		if err != nil {
			return zero, start, err
		}
		f, perr := parseDouble(string(line))
		if perr != nil {
			return zero, start, ErrProtocol("invalid double")
		}
		return RESPValue{Type: TypeDouble, Float: f}, newPos, nil

	case TypeBigNumber:
		line, newPos, err := readLine(buf, pos)
		if err != nil {
			return zero, start, err
		}
		n := new(big.Int)
		if _, ok := n.SetString(string(line), 10); !ok {
			return zero, start, ErrProtocol("invalid big number")
		}
		return RESPValue{Type: TypeBigNumber, BigInt: n}, newPos, nil

	case TypeBulkError:
		return decodeBulk(buf, pos, start, TypeBulkError)

	case TypeVerbatim:
		v, newPos, err := decodeBulk(buf, pos, start, TypeVerbatim)
		if err != nil {
			return zero, start, err
		}
		if len(v.Str) < 4 || v.Str[3] != ':' {
			return zero, start, ErrProtocol("invalid verbatim string")
		}
		v.VerbEnc = string(v.Str[:3])
		v.Str = v.Str[4:]
		return v, newPos, nil

	case TypeMap:
		return decodeMapLike(buf, pos, start, TypeMap)

	case TypeSet:
		return decodeAggregate(buf, pos, start, TypeSet)

	case TypeAttribute:
		line, midStart, err := readLine(buf, pos)
		if err != nil {
			return zero, start, err
		}
		if string(line) == "?" {
			return zero, start, ErrProtocol("streamed attribute unsupported")
		}
		n, perr := strconv.Atoi(string(line))
		if perr != nil || n < 0 {
			return zero, start, ErrProtocol("invalid attribute length")
		}
		attrs, bodyPos, err := decodeNPairs(buf, midStart, n)
		if err != nil {
			return zero, start, err
		}
		body, finalPos, err := Decode(buf, bodyPos)
		if err != nil {
			return zero, start, err
		}
		return RESPValue{Type: TypeAttribute, Attrs: attrs, AttrBody: &body}, finalPos, nil

	case TypePush:
		return decodeAggregate(buf, pos, start, TypePush)

	default:
		return zero, start, ErrProtocol("unexpected type byte '" + string(typeByte) + "'")
	}
}

// decodeBulk handles the length-prefixed binary-safe types ($, !, =). The null
// bulk form ($-1) is only legal for TypeBulkString.
func decodeBulk(buf []byte, pos, start int, t RESPType) (RESPValue, int, error) {
	var zero RESPValue
	line, lenPos, err := readLine(buf, pos)
	if err != nil {
		return zero, start, err
	}
	if string(line) == "?" && t == TypeBulkString {
		return decodeStreamedBulk(buf, lenPos, start)
	}
	n, perr := strconv.ParseInt(string(line), 10, 64)
	if perr != nil {
		return zero, start, ErrProtocol("invalid bulk length")
	}
	if n == -1 && t == TypeBulkString {
		return RESPValue{Type: TypeBulkString, IsNull: true}, lenPos, nil
	}
	if n < 0 {
		return zero, start, ErrProtocol("invalid bulk length")
	}
	if lenPos+int(n)+2 > len(buf) {
		return zero, start, ErrNeedMore
	}
	data := cloneBytes(buf[lenPos : lenPos+int(n)])
	if buf[lenPos+int(n)] != '\r' || buf[lenPos+int(n)+1] != '\n' {
		return zero, start, ErrProtocol("invalid bulk string CRLF")
	}
	v := RESPValue{Type: t}
	if t == TypeBulkError {
		v.Err = string(data)
	} else {
		v.Str = data
	}
	return v, lenPos + int(n) + 2, nil
}

// decodeAggregate handles the count-prefixed sequence types (*, ~, >). The null
// array form (*-1) is only legal for TypeArray.
func decodeAggregate(buf []byte, pos, start int, t RESPType) (RESPValue, int, error) {
	var zero RESPValue
	line, elemPos, err := readLine(buf, pos)
	if err != nil {
		return zero, start, err
	}
	if string(line) == "?" {
		if t != TypeArray {
			return zero, start, ErrProtocol("streamed aggregate unsupported")
		}
		return decodeStreamedArray(buf, elemPos, start)
	}
	n, perr := strconv.ParseInt(string(line), 10, 64)
	if perr != nil {
		return zero, start, ErrProtocol("invalid multibulk length")
	}
	if n == -1 && t == TypeArray {
		return RESPValue{Type: TypeArray, IsNull: true}, elemPos, nil
	}
	if n < 0 {
		return zero, start, ErrProtocol("invalid multibulk length")
	}
	elems, finalPos, err := decodeN(buf, elemPos, int(n))
	if err != nil {
		return zero, start, err
	}
	return RESPValue{Type: t, Elems: elems}, finalPos, nil
}

// decodeMapLike handles the % map type.
func decodeMapLike(buf []byte, pos, start int, t RESPType) (RESPValue, int, error) {
	var zero RESPValue
	line, pairPos, err := readLine(buf, pos)
	if err != nil {
		return zero, start, err
	}
	if string(line) == "?" {
		return decodeStreamedMap(buf, pairPos, start)
	}
	n, perr := strconv.ParseInt(string(line), 10, 64)
	if perr != nil || n < 0 {
		return zero, start, ErrProtocol("invalid map length")
	}
	pairs, finalPos, err := decodeNPairs(buf, pairPos, int(n))
	if err != nil {
		return zero, start, err
	}
	return RESPValue{Type: t, Map: pairs}, finalPos, nil
}

// readLine returns the bytes up to the next CRLF (without it) and the position
// just past the CRLF. It returns ErrNeedMore if no complete line is present. The
// returned slice aliases buf and must be copied by callers that retain it.
func readLine(buf []byte, pos int) ([]byte, int, error) {
	for i := pos; i+1 < len(buf); i++ {
		if buf[i] == '\r' && buf[i+1] == '\n' {
			return buf[pos:i], i + 2, nil
		}
	}
	return nil, pos, ErrNeedMore
}

// decodeN decodes n consecutive values.
func decodeN(buf []byte, pos, n int) ([]RESPValue, int, error) {
	elems := make([]RESPValue, 0, n)
	for range n {
		v, newPos, err := Decode(buf, pos)
		if err != nil {
			return nil, pos, err
		}
		elems = append(elems, v)
		pos = newPos
	}
	return elems, pos, nil
}

// decodeNPairs decodes n key-value pairs (2n values).
func decodeNPairs(buf []byte, pos, n int) ([][2]RESPValue, int, error) {
	pairs := make([][2]RESPValue, 0, n)
	for range n {
		k, p1, err := Decode(buf, pos)
		if err != nil {
			return nil, pos, err
		}
		v, p2, err := Decode(buf, p1)
		if err != nil {
			return nil, pos, err
		}
		pairs = append(pairs, [2]RESPValue{k, v})
		pos = p2
	}
	return pairs, pos, nil
}

// decodeStreamedBulk reassembles a chunked bulk string ($?) into a single value.
func decodeStreamedBulk(buf []byte, pos, start int) (RESPValue, int, error) {
	var zero RESPValue
	var result []byte
	for {
		if pos >= len(buf) {
			return zero, start, ErrNeedMore
		}
		if buf[pos] != ';' {
			return zero, start, ErrProtocol("expected ';' in streamed bulk")
		}
		line, dataPos, err := readLine(buf, pos+1)
		if err != nil {
			return zero, start, err
		}
		n, perr := strconv.ParseInt(string(line), 10, 64)
		if perr != nil || n < 0 {
			return zero, start, ErrProtocol("invalid chunk length")
		}
		if n == 0 {
			return RESPValue{Type: TypeBulkString, Str: result}, dataPos, nil
		}
		if dataPos+int(n)+2 > len(buf) {
			return zero, start, ErrNeedMore
		}
		result = append(result, buf[dataPos:dataPos+int(n)]...)
		pos = dataPos + int(n) + 2
	}
}

// decodeStreamedArray reads a streamed array (*?) terminated by ".\r\n".
func decodeStreamedArray(buf []byte, pos, start int) (RESPValue, int, error) {
	var zero RESPValue
	var elems []RESPValue
	for {
		if pos >= len(buf) {
			return zero, start, ErrNeedMore
		}
		if buf[pos] == '.' {
			if pos+2 >= len(buf) {
				return zero, start, ErrNeedMore
			}
			if buf[pos+1] == '\r' && buf[pos+2] == '\n' {
				return RESPValue{Type: TypeArray, Elems: elems}, pos + 3, nil
			}
		}
		v, newPos, err := Decode(buf, pos)
		if err != nil {
			return zero, start, err
		}
		elems = append(elems, v)
		pos = newPos
	}
}

// decodeStreamedMap reads a streamed map (%?) terminated by ".\r\n".
func decodeStreamedMap(buf []byte, pos, start int) (RESPValue, int, error) {
	var zero RESPValue
	var pairs [][2]RESPValue
	for {
		if pos >= len(buf) {
			return zero, start, ErrNeedMore
		}
		if buf[pos] == '.' {
			if pos+2 >= len(buf) {
				return zero, start, ErrNeedMore
			}
			if buf[pos+1] == '\r' && buf[pos+2] == '\n' {
				return RESPValue{Type: TypeMap, Map: pairs}, pos + 3, nil
			}
		}
		k, p1, err := Decode(buf, pos)
		if err != nil {
			return zero, start, err
		}
		v, p2, err := Decode(buf, p1)
		if err != nil {
			return zero, start, err
		}
		pairs = append(pairs, [2]RESPValue{k, v})
		pos = p2
	}
}

// parseDouble parses a RESP3 double, including the special inf/-inf/nan tokens
// (case-insensitive).
func parseDouble(s string) (float64, error) {
	switch strings.ToLower(s) {
	case "inf", "+inf":
		return math.Inf(1), nil
	case "-inf":
		return math.Inf(-1), nil
	case "nan":
		return math.NaN(), nil
	}
	return strconv.ParseFloat(s, 64)
}

func cloneBytes(b []byte) []byte {
	out := make([]byte, len(b))
	copy(out, b)
	return out
}
