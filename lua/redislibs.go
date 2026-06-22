package lua

import (
	"encoding/binary"
	"encoding/json"
	"io"
	"math"
	"strconv"
	"strings"
)

// This file implements the helper libraries Redis exposes to scripts beyond the
// redis.* table: cjson, cmsgpack, bit, and struct (spec 2064 doc 15 section 8).
// They live in package lua so they can reach the table value model directly, and
// they are pure Go with no external dependency. The host installs them into a
// fresh interpreter with OpenRedisLibs so both EVAL scripts and FUNCTION
// libraries see the same set.

// cjsonNull is the sentinel a decoded JSON null becomes, reachable as cjson.null.
// It keeps a distinct identity so a script can tell a real null apart from a
// missing key, and the encoder turns it back into "null".
var cjsonNull = NewTable()

// cjsonEmptyArray is the sentinel that forces an empty table to encode as a JSON
// array, reachable as cjson.empty_array. A plain empty table encodes as "{}" to
// match lua-cjson, so this is the way to ask for "[]".
var cjsonEmptyArray = NewTable()

// OpenRedisLibs installs cjson, cmsgpack, bit and struct as globals on the
// interpreter. It is called once per script interpreter, after the base stdlib.
func OpenRedisLibs(i *Interp) {
	g := i.Globals()
	g.Set(String("cjson"), buildCjson())
	g.Set(String("cmsgpack"), buildCmsgpack())
	g.Set(String("bit"), buildBit())
	g.Set(String("struct"), buildStruct())
}

func libTable(entries map[string]GoFunc) *Table {
	t := NewTable()
	for name, fn := range entries {
		t.Set(String(name), NewGoFunc(name, fn))
	}
	return t
}

// cjson ----------------------------------------------------------------------

func buildCjson() *Table {
	t := libTable(map[string]GoFunc{
		"encode": cjsonEncode,
		"decode": cjsonDecode,
	})
	t.Set(String("null"), cjsonNull)
	t.Set(String("empty_array"), cjsonEmptyArray)
	return t
}

func cjsonEncode(_ *Interp, args []Value) ([]Value, error) {
	if len(args) == 0 {
		return nil, runtimeErr("expected 1 argument")
	}
	var b strings.Builder
	if err := encodeJSON(&b, args[0]); err != nil {
		return nil, err
	}
	return []Value{String(b.String())}, nil
}

func encodeJSON(b *strings.Builder, v Value) error {
	switch x := v.(type) {
	case nilValue:
		b.WriteString("null")
	case Bool:
		if x {
			b.WriteString("true")
		} else {
			b.WriteString("false")
		}
	case Number:
		f := float64(x)
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return runtimeErr("Cannot serialise number: must not be NaN or Infinity")
		}
		b.WriteString(numberToString(f))
	case String:
		encodeJSONString(b, string(x))
	case *Table:
		return encodeJSONTable(b, x)
	default:
		return runtimeErr("Cannot serialise %s: type not supported", v.luaType())
	}
	return nil
}

func encodeJSONTable(b *strings.Builder, t *Table) error {
	if t == cjsonNull {
		b.WriteString("null")
		return nil
	}
	if t == cjsonEmptyArray {
		b.WriteString("[]")
		return nil
	}
	keys := t.Keys()
	if len(keys) == 0 {
		b.WriteString("{}")
		return nil
	}
	if isSequence(t, keys) {
		b.WriteByte('[')
		for idx := 1; idx <= len(keys); idx++ {
			if idx > 1 {
				b.WriteByte(',')
			}
			if err := encodeJSON(b, t.Get(Number(idx))); err != nil {
				return err
			}
		}
		b.WriteByte(']')
		return nil
	}
	b.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			b.WriteByte(',')
		}
		switch kk := k.(type) {
		case String:
			encodeJSONString(b, string(kk))
		case Number:
			encodeJSONString(b, numberToString(float64(kk)))
		default:
			return runtimeErr("Cannot serialise table: a key is neither string nor number")
		}
		b.WriteByte(':')
		if err := encodeJSON(b, t.Get(k)); err != nil {
			return err
		}
	}
	b.WriteByte('}')
	return nil
}

// isSequence reports whether the table's keys are exactly 1..n, so it encodes as
// a JSON array rather than an object.
func isSequence(t *Table, keys []Value) bool {
	if t.Len() != len(keys) {
		return false
	}
	for idx := 1; idx <= len(keys); idx++ {
		if _, ok := t.Get(Number(idx)).(nilValue); ok {
			return false
		}
	}
	return true
}

func encodeJSONString(b *strings.Builder, s string) {
	b.WriteByte('"')
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch c {
		case '"':
			b.WriteString(`\"`)
		case '\\':
			b.WriteString(`\\`)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case '\t':
			b.WriteString(`\t`)
		case '\b':
			b.WriteString(`\b`)
		case '\f':
			b.WriteString(`\f`)
		default:
			if c < 0x20 {
				const hex = "0123456789abcdef"
				b.WriteString(`\u00`)
				b.WriteByte(hex[c>>4])
				b.WriteByte(hex[c&0xf])
			} else {
				b.WriteByte(c)
			}
		}
	}
	b.WriteByte('"')
}

func cjsonDecode(_ *Interp, args []Value) ([]Value, error) {
	s, err := checkString(args, 1, "decode")
	if err != nil {
		return nil, err
	}
	dec := json.NewDecoder(strings.NewReader(s))
	dec.UseNumber()
	var raw any
	if err := dec.Decode(&raw); err != nil {
		return nil, runtimeErr("Expected value but found invalid token")
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, runtimeErr("Expected the end but found invalid token")
	}
	return []Value{jsonToLua(raw)}, nil
}

func jsonToLua(v any) Value {
	switch x := v.(type) {
	case nil:
		return cjsonNull
	case bool:
		return Bool(x)
	case json.Number:
		f, _ := strconv.ParseFloat(string(x), 64)
		return Number(f)
	case string:
		return String(x)
	case []any:
		t := NewTable()
		for _, e := range x {
			t.Append(jsonToLua(e))
		}
		return t
	case map[string]any:
		t := NewTable()
		for k, e := range x {
			t.Set(String(k), jsonToLua(e))
		}
		return t
	default:
		return Nil
	}
}

// cmsgpack -------------------------------------------------------------------

func buildCmsgpack() *Table {
	return libTable(map[string]GoFunc{
		"pack":   cmsgpackPack,
		"unpack": cmsgpackUnpack,
	})
}

func cmsgpackPack(_ *Interp, args []Value) ([]Value, error) {
	var b []byte
	for _, a := range args {
		var err error
		b, err = encodeMsgpack(b, a)
		if err != nil {
			return nil, err
		}
	}
	return []Value{String(b)}, nil
}

func encodeMsgpack(b []byte, v Value) ([]byte, error) {
	switch x := v.(type) {
	case nilValue:
		return append(b, 0xc0), nil
	case Bool:
		if x {
			return append(b, 0xc3), nil
		}
		return append(b, 0xc2), nil
	case Number:
		return encodeMsgpackNumber(b, float64(x)), nil
	case String:
		return encodeMsgpackString(b, string(x)), nil
	case *Table:
		return encodeMsgpackTable(b, x)
	default:
		return nil, runtimeErr("cannot pack %s", v.luaType())
	}
}

func encodeMsgpackNumber(b []byte, f float64) []byte {
	if f == math.Trunc(f) && !math.IsInf(f, 0) && f >= -9.2233720368547758e18 && f < 9.2233720368547758e18 {
		return encodeMsgpackInt(b, int64(f))
	}
	var tmp [8]byte
	binary.BigEndian.PutUint64(tmp[:], math.Float64bits(f))
	return append(append(b, 0xcb), tmp[:]...)
}

func encodeMsgpackInt(b []byte, n int64) []byte {
	switch {
	case n >= 0 && n <= 127:
		return append(b, byte(n))
	case n < 0 && n >= -32:
		return append(b, byte(n))
	case n >= -128 && n <= 127:
		return append(b, 0xd0, byte(n))
	case n >= -32768 && n <= 32767:
		var t [2]byte
		binary.BigEndian.PutUint16(t[:], uint16(n))
		return append(append(b, 0xd1), t[:]...)
	case n >= -2147483648 && n <= 2147483647:
		var t [4]byte
		binary.BigEndian.PutUint32(t[:], uint32(n))
		return append(append(b, 0xd2), t[:]...)
	default:
		var t [8]byte
		binary.BigEndian.PutUint64(t[:], uint64(n))
		return append(append(b, 0xd3), t[:]...)
	}
}

func encodeMsgpackString(b []byte, s string) []byte {
	n := len(s)
	switch {
	case n < 32:
		b = append(b, 0xa0|byte(n))
	case n < 256:
		b = append(b, 0xd9, byte(n))
	case n < 65536:
		var t [2]byte
		binary.BigEndian.PutUint16(t[:], uint16(n))
		b = append(append(b, 0xda), t[:]...)
	default:
		var t [4]byte
		binary.BigEndian.PutUint32(t[:], uint32(n))
		b = append(append(b, 0xdb), t[:]...)
	}
	return append(b, s...)
}

func encodeMsgpackTable(b []byte, t *Table) ([]byte, error) {
	keys := t.Keys()
	if isSequence(t, keys) {
		n := len(keys)
		switch {
		case n < 16:
			b = append(b, 0x90|byte(n))
		case n < 65536:
			var h [2]byte
			binary.BigEndian.PutUint16(h[:], uint16(n))
			b = append(append(b, 0xdc), h[:]...)
		default:
			var h [4]byte
			binary.BigEndian.PutUint32(h[:], uint32(n))
			b = append(append(b, 0xdd), h[:]...)
		}
		for idx := 1; idx <= n; idx++ {
			var err error
			b, err = encodeMsgpack(b, t.Get(Number(idx)))
			if err != nil {
				return nil, err
			}
		}
		return b, nil
	}
	n := len(keys)
	switch {
	case n < 16:
		b = append(b, 0x80|byte(n))
	case n < 65536:
		var h [2]byte
		binary.BigEndian.PutUint16(h[:], uint16(n))
		b = append(append(b, 0xde), h[:]...)
	default:
		var h [4]byte
		binary.BigEndian.PutUint32(h[:], uint32(n))
		b = append(append(b, 0xdf), h[:]...)
	}
	for _, k := range keys {
		var err error
		if b, err = encodeMsgpack(b, k); err != nil {
			return nil, err
		}
		if b, err = encodeMsgpack(b, t.Get(k)); err != nil {
			return nil, err
		}
	}
	return b, nil
}

func cmsgpackUnpack(_ *Interp, args []Value) ([]Value, error) {
	s, err := checkString(args, 1, "unpack")
	if err != nil {
		return nil, err
	}
	data := []byte(s)
	var out []Value
	pos := 0
	for pos < len(data) {
		v, next, err := decodeMsgpack(data, pos)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
		pos = next
	}
	return out, nil
}

func decodeMsgpack(b []byte, pos int) (Value, int, error) {
	if pos >= len(b) {
		return nil, 0, runtimeErr("Missing bytes in input")
	}
	c := b[pos]
	pos++
	switch {
	case c <= 0x7f:
		return Number(float64(c)), pos, nil
	case c >= 0xe0:
		return Number(float64(int8(c))), pos, nil
	case c >= 0xa0 && c <= 0xbf:
		return readMsgpackBytes(b, pos, int(c&0x1f))
	case c >= 0x90 && c <= 0x9f:
		return decodeMsgpackArray(b, pos, int(c&0x0f))
	case c >= 0x80 && c <= 0x8f:
		return decodeMsgpackMap(b, pos, int(c&0x0f))
	}
	switch c {
	case 0xc0:
		return Nil, pos, nil
	case 0xc2:
		return Bool(false), pos, nil
	case 0xc3:
		return Bool(true), pos, nil
	case 0xca:
		v, p, err := readUint(b, pos, 4)
		if err != nil {
			return nil, 0, err
		}
		return Number(float64(math.Float32frombits(uint32(v)))), p, nil
	case 0xcb:
		v, p, err := readUint(b, pos, 8)
		if err != nil {
			return nil, 0, err
		}
		return Number(math.Float64frombits(v)), p, nil
	case 0xcc:
		return readMsgpackUint(b, pos, 1)
	case 0xcd:
		return readMsgpackUint(b, pos, 2)
	case 0xce:
		return readMsgpackUint(b, pos, 4)
	case 0xcf:
		return readMsgpackUint(b, pos, 8)
	case 0xd0:
		return readMsgpackInt(b, pos, 1)
	case 0xd1:
		return readMsgpackInt(b, pos, 2)
	case 0xd2:
		return readMsgpackInt(b, pos, 4)
	case 0xd3:
		return readMsgpackInt(b, pos, 8)
	case 0xd9:
		return readMsgpackStr(b, pos, 1)
	case 0xda:
		return readMsgpackStr(b, pos, 2)
	case 0xdb:
		return readMsgpackStr(b, pos, 4)
	case 0xc4:
		return readMsgpackStr(b, pos, 1)
	case 0xc5:
		return readMsgpackStr(b, pos, 2)
	case 0xc6:
		return readMsgpackStr(b, pos, 4)
	case 0xdc:
		n, p, err := readUint(b, pos, 2)
		if err != nil {
			return nil, 0, err
		}
		return decodeMsgpackArray(b, p, int(n))
	case 0xdd:
		n, p, err := readUint(b, pos, 4)
		if err != nil {
			return nil, 0, err
		}
		return decodeMsgpackArray(b, p, int(n))
	case 0xde:
		n, p, err := readUint(b, pos, 2)
		if err != nil {
			return nil, 0, err
		}
		return decodeMsgpackMap(b, p, int(n))
	case 0xdf:
		n, p, err := readUint(b, pos, 4)
		if err != nil {
			return nil, 0, err
		}
		return decodeMsgpackMap(b, p, int(n))
	}
	return nil, 0, runtimeErr("Unsupported msgpack type 0x%02x", c)
}

func readUint(b []byte, pos, n int) (uint64, int, error) {
	if pos+n > len(b) {
		return 0, 0, runtimeErr("Missing bytes in input")
	}
	var v uint64
	for i := 0; i < n; i++ {
		v = v<<8 | uint64(b[pos+i])
	}
	return v, pos + n, nil
}

func readMsgpackUint(b []byte, pos, n int) (Value, int, error) {
	v, p, err := readUint(b, pos, n)
	if err != nil {
		return nil, 0, err
	}
	return Number(float64(v)), p, nil
}

func readMsgpackInt(b []byte, pos, n int) (Value, int, error) {
	v, p, err := readUint(b, pos, n)
	if err != nil {
		return nil, 0, err
	}
	shift := uint(64 - 8*n)
	return Number(float64(int64(v<<shift) >> shift)), p, nil
}

func readMsgpackStr(b []byte, pos, lenBytes int) (Value, int, error) {
	n, p, err := readUint(b, pos, lenBytes)
	if err != nil {
		return nil, 0, err
	}
	return readMsgpackBytes(b, p, int(n))
}

func readMsgpackBytes(b []byte, pos, n int) (Value, int, error) {
	if pos+n > len(b) {
		return nil, 0, runtimeErr("Missing bytes in input")
	}
	return String(b[pos : pos+n]), pos + n, nil
}

func decodeMsgpackArray(b []byte, pos, n int) (Value, int, error) {
	t := NewTable()
	for i := 0; i < n; i++ {
		v, next, err := decodeMsgpack(b, pos)
		if err != nil {
			return nil, 0, err
		}
		t.Append(v)
		pos = next
	}
	return t, pos, nil
}

func decodeMsgpackMap(b []byte, pos, n int) (Value, int, error) {
	t := NewTable()
	for i := 0; i < n; i++ {
		k, kpos, err := decodeMsgpack(b, pos)
		if err != nil {
			return nil, 0, err
		}
		v, vpos, err := decodeMsgpack(b, kpos)
		if err != nil {
			return nil, 0, err
		}
		t.Set(k, v)
		pos = vpos
	}
	return t, pos, nil
}

// bit ------------------------------------------------------------------------

func buildBit() *Table {
	return libTable(map[string]GoFunc{
		"tobit":   bitUnary(func(a int32) int32 { return a }),
		"bnot":    bitUnary(func(a int32) int32 { return ^a }),
		"band":    bitFold(func(a, b int32) int32 { return a & b }),
		"bor":     bitFold(func(a, b int32) int32 { return a | b }),
		"bxor":    bitFold(func(a, b int32) int32 { return a ^ b }),
		"lshift":  bitShift(func(a int32, n uint) int32 { return int32(uint32(a) << n) }),
		"rshift":  bitShift(func(a int32, n uint) int32 { return int32(uint32(a) >> n) }),
		"arshift": bitShift(func(a int32, n uint) int32 { return a >> n }),
		"rol":     bitShift(func(a int32, n uint) int32 { return int32(uint32(a)<<n | uint32(a)>>(32-n)) }),
		"ror":     bitShift(func(a int32, n uint) int32 { return int32(uint32(a)>>n | uint32(a)<<(32-n)) }),
		"tohex":   bitTohex,
	})
}

// toBit reduces a Lua number to a signed 32-bit value with two's-complement
// wraparound, the normalization every bit.* function applies to its arguments.
func toBit(f float64) int32 {
	t := math.Trunc(f)
	m := math.Mod(t, 4294967296)
	if m < 0 {
		m += 4294967296
	}
	return int32(uint32(m))
}

func bitArg(args []Value, n int, fname string) (int32, error) {
	f, err := checkNumber(args, n, fname)
	if err != nil {
		return 0, err
	}
	return toBit(f), nil
}

func bitUnary(op func(int32) int32) GoFunc {
	return func(_ *Interp, args []Value) ([]Value, error) {
		a, err := bitArg(args, 1, "bit")
		if err != nil {
			return nil, err
		}
		return []Value{Number(float64(op(a)))}, nil
	}
}

func bitFold(op func(a, b int32) int32) GoFunc {
	return func(_ *Interp, args []Value) ([]Value, error) {
		if len(args) == 0 {
			return nil, argError(1, "bit", "number expected, got no value")
		}
		acc, err := bitArg(args, 1, "bit")
		if err != nil {
			return nil, err
		}
		for n := 2; n <= len(args); n++ {
			b, err := bitArg(args, n, "bit")
			if err != nil {
				return nil, err
			}
			acc = op(acc, b)
		}
		return []Value{Number(float64(acc))}, nil
	}
}

func bitShift(op func(a int32, n uint) int32) GoFunc {
	return func(_ *Interp, args []Value) ([]Value, error) {
		a, err := bitArg(args, 1, "bit")
		if err != nil {
			return nil, err
		}
		sh, err := bitArg(args, 2, "bit")
		if err != nil {
			return nil, err
		}
		return []Value{Number(float64(op(a, uint(sh)&31)))}, nil
	}
}

func bitTohex(_ *Interp, args []Value) ([]Value, error) {
	a, err := bitArg(args, 1, "tohex")
	if err != nil {
		return nil, err
	}
	digits := 8
	if len(args) >= 2 {
		n, err := checkNumber(args, 2, "tohex")
		if err != nil {
			return nil, err
		}
		digits = int(n)
	}
	upper := false
	if digits < 0 {
		upper = true
		digits = -digits
	}
	if digits > 8 {
		digits = 8
	}
	const lo = "0123456789abcdef"
	const up = "0123456789ABCDEF"
	table := lo
	if upper {
		table = up
	}
	v := uint32(a)
	buf := make([]byte, digits)
	for i := digits - 1; i >= 0; i-- {
		buf[i] = table[v&0xf]
		v >>= 4
	}
	return []Value{String(buf)}, nil
}

// struct ---------------------------------------------------------------------

func buildStruct() *Table {
	return libTable(map[string]GoFunc{
		"pack":   structPack,
		"unpack": structUnpack,
		"size":   structSize,
	})
}

// structFmt walks a format string and calls op for each data item, threading the
// current endianness through the < and > switches. op gets the format char and
// any count after a c.
type structItem struct {
	code byte
	size int // bytes for fixed types, or count for c
	le   bool
}

func parseStructFmt(fmt string) ([]structItem, error) {
	le := true
	var items []structItem
	i := 0
	for i < len(fmt) {
		c := fmt[i]
		i++
		switch c {
		case ' ':
			continue
		case '<':
			le = true
		case '>':
			le = false
		case '=', '!':
			// native or alignment markers, treated as no-ops
		case 'b', 'B':
			items = append(items, structItem{c, 1, le})
		case 'h', 'H':
			items = append(items, structItem{c, 2, le})
		case 'i', 'I':
			items = append(items, structItem{c, 4, le})
		case 'l', 'L':
			items = append(items, structItem{c, 8, le})
		case 'f':
			items = append(items, structItem{c, 4, le})
		case 'd':
			items = append(items, structItem{c, 8, le})
		case 's':
			items = append(items, structItem{c, 0, le})
		case 'c':
			n := 0
			start := i
			for i < len(fmt) && fmt[i] >= '0' && fmt[i] <= '9' {
				n = n*10 + int(fmt[i]-'0')
				i++
			}
			if i == start {
				n = 1
			}
			items = append(items, structItem{c, n, le})
		default:
			return nil, runtimeErr("invalid format option '%c'", c)
		}
	}
	return items, nil
}

func putUint(b []byte, v uint64, n int, le bool) {
	if le {
		for i := 0; i < n; i++ {
			b[i] = byte(v >> (8 * uint(i)))
		}
		return
	}
	for i := 0; i < n; i++ {
		b[n-1-i] = byte(v >> (8 * uint(i)))
	}
}

func getUint(b []byte, n int, le bool) uint64 {
	var v uint64
	if le {
		for i := n - 1; i >= 0; i-- {
			v = v<<8 | uint64(b[i])
		}
		return v
	}
	for i := 0; i < n; i++ {
		v = v<<8 | uint64(b[i])
	}
	return v
}

func structPack(_ *Interp, args []Value) ([]Value, error) {
	fmt, err := checkString(args, 1, "pack")
	if err != nil {
		return nil, err
	}
	items, err := parseStructFmt(fmt)
	if err != nil {
		return nil, err
	}
	var out []byte
	argn := 2
	for _, it := range items {
		switch it.code {
		case 'b', 'B', 'h', 'H', 'i', 'I', 'l', 'L':
			n, err := checkNumber(args, argn, "pack")
			if err != nil {
				return nil, err
			}
			argn++
			buf := make([]byte, it.size)
			putUint(buf, uint64(int64(n)), it.size, it.le)
			out = append(out, buf...)
		case 'f':
			n, err := checkNumber(args, argn, "pack")
			if err != nil {
				return nil, err
			}
			argn++
			buf := make([]byte, 4)
			putUint(buf, uint64(math.Float32bits(float32(n))), 4, it.le)
			out = append(out, buf...)
		case 'd':
			n, err := checkNumber(args, argn, "pack")
			if err != nil {
				return nil, err
			}
			argn++
			buf := make([]byte, 8)
			putUint(buf, math.Float64bits(n), 8, it.le)
			out = append(out, buf...)
		case 's':
			s, err := checkString(args, argn, "pack")
			if err != nil {
				return nil, err
			}
			argn++
			hdr := make([]byte, 4)
			putUint(hdr, uint64(len(s)), 4, it.le)
			out = append(out, hdr...)
			out = append(out, s...)
		case 'c':
			s, err := checkString(args, argn, "pack")
			if err != nil {
				return nil, err
			}
			argn++
			buf := make([]byte, it.size)
			copy(buf, s)
			out = append(out, buf...)
		}
	}
	return []Value{String(out)}, nil
}

func structUnpack(_ *Interp, args []Value) ([]Value, error) {
	fmt, err := checkString(args, 1, "unpack")
	if err != nil {
		return nil, err
	}
	data, err := checkString(args, 2, "unpack")
	if err != nil {
		return nil, err
	}
	pos := 0
	if len(args) >= 3 {
		n, err := checkNumber(args, 3, "unpack")
		if err != nil {
			return nil, err
		}
		pos = int(n) - 1
	}
	items, err := parseStructFmt(fmt)
	if err != nil {
		return nil, err
	}
	b := []byte(data)
	var out []Value
	for _, it := range items {
		switch it.code {
		case 'b', 'h', 'i', 'l':
			if pos+it.size > len(b) {
				return nil, runtimeErr("data string too short")
			}
			u := getUint(b[pos:pos+it.size], it.size, it.le)
			shift := uint(64 - 8*it.size)
			out = append(out, Number(float64(int64(u<<shift)>>shift)))
			pos += it.size
		case 'B', 'H', 'I', 'L':
			if pos+it.size > len(b) {
				return nil, runtimeErr("data string too short")
			}
			out = append(out, Number(float64(getUint(b[pos:pos+it.size], it.size, it.le))))
			pos += it.size
		case 'f':
			if pos+4 > len(b) {
				return nil, runtimeErr("data string too short")
			}
			out = append(out, Number(float64(math.Float32frombits(uint32(getUint(b[pos:pos+4], 4, it.le))))))
			pos += 4
		case 'd':
			if pos+8 > len(b) {
				return nil, runtimeErr("data string too short")
			}
			out = append(out, Number(math.Float64frombits(getUint(b[pos:pos+8], 8, it.le))))
			pos += 8
		case 's':
			if pos+4 > len(b) {
				return nil, runtimeErr("data string too short")
			}
			n := int(getUint(b[pos:pos+4], 4, it.le))
			pos += 4
			if pos+n > len(b) {
				return nil, runtimeErr("data string too short")
			}
			out = append(out, String(b[pos:pos+n]))
			pos += n
		case 'c':
			if pos+it.size > len(b) {
				return nil, runtimeErr("data string too short")
			}
			out = append(out, String(b[pos:pos+it.size]))
			pos += it.size
		}
	}
	out = append(out, Number(float64(pos+1)))
	return out, nil
}

func structSize(_ *Interp, args []Value) ([]Value, error) {
	fmt, err := checkString(args, 1, "size")
	if err != nil {
		return nil, err
	}
	items, err := parseStructFmt(fmt)
	if err != nil {
		return nil, err
	}
	total := 0
	for _, it := range items {
		if it.code == 's' {
			return nil, runtimeErr("variable-size format in struct.size")
		}
		total += it.size
	}
	return []Value{Number(float64(total))}, nil
}
