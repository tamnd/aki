package command

import (
	"strings"
	"testing"
)

func TestCjsonRoundTrip(t *testing.T) {
	r, c := startData(t)
	got := sendArgs(t, r, c, "EVAL", "return cjson.encode({1,2,3})", "0")
	if got != "[1,2,3]" {
		t.Fatalf("encode array = %v", got)
	}
	got = sendArgs(t, r, c, "EVAL", "return cjson.encode({foo='bar'})", "0")
	if got != `{"foo":"bar"}` {
		t.Fatalf("encode object = %v", got)
	}
	// Empty table encodes as an object to match lua-cjson.
	got = sendArgs(t, r, c, "EVAL", "return cjson.encode({})", "0")
	if got != "{}" {
		t.Fatalf("encode empty = %v", got)
	}
	// The empty_array sentinel forces a JSON array.
	got = sendArgs(t, r, c, "EVAL", "return cjson.encode(cjson.empty_array)", "0")
	if got != "[]" {
		t.Fatalf("encode empty_array = %v", got)
	}
}

func TestCjsonDecode(t *testing.T) {
	r, c := startData(t)
	got := sendArgs(t, r, c, "EVAL", `return cjson.decode('[10,20,30]')[2]`, "0")
	if got != int64(20) {
		t.Fatalf("decode array index = %v", got)
	}
	got = sendArgs(t, r, c, "EVAL", `return cjson.decode('{"a":"b"}').a`, "0")
	if got != "b" {
		t.Fatalf("decode object field = %v", got)
	}
	// A decoded null is the cjson.null sentinel.
	got = sendArgs(t, r, c, "EVAL", `if cjson.decode('null') == cjson.null then return 'yes' end return 'no'`, "0")
	if got != "yes" {
		t.Fatalf("decode null = %v", got)
	}
}

func TestCjsonEncodeNaN(t *testing.T) {
	r, c := startData(t)
	got := sendArgs(t, r, c, "EVAL", "return cjson.encode(1/0)", "0")
	if e, ok := got.(cmdErr); !ok || !strings.Contains(string(e), "NaN or Infinity") {
		t.Fatalf("encode inf = %v (%T)", got, got)
	}
}

func TestCmsgpackRoundTrip(t *testing.T) {
	r, c := startData(t)
	got := sendArgs(t, r, c, "EVAL", `return cmsgpack.unpack(cmsgpack.pack('hello'))`, "0")
	if got != "hello" {
		t.Fatalf("string round trip = %v", got)
	}
	got = sendArgs(t, r, c, "EVAL", `return cmsgpack.unpack(cmsgpack.pack(42))`, "0")
	if got != int64(42) {
		t.Fatalf("int round trip = %v", got)
	}
	got = sendArgs(t, r, c, "EVAL", `return cmsgpack.unpack(cmsgpack.pack(-1000))`, "0")
	if got != int64(-1000) {
		t.Fatalf("negative round trip = %v", got)
	}
	got = sendArgs(t, r, c, "EVAL", `return cmsgpack.unpack(cmsgpack.pack({7,8,9}))[3]`, "0")
	if got != int64(9) {
		t.Fatalf("array round trip = %v", got)
	}
}

func TestBitOps(t *testing.T) {
	r, c := startData(t)
	cases := []struct {
		body string
		want int64
	}{
		{"return bit.band(6, 3)", 2},
		{"return bit.bor(4, 1)", 5},
		{"return bit.bxor(5, 1)", 4},
		{"return bit.lshift(1, 4)", 16},
		{"return bit.rshift(256, 4)", 16},
		{"return bit.arshift(-16, 1)", -8},
		{"return bit.tobit(4294967296)", 0},
		{"return bit.bnot(0)", -1},
	}
	for _, tc := range cases {
		got := sendArgs(t, r, c, "EVAL", tc.body, "0")
		if got != tc.want {
			t.Fatalf("%s = %v, want %d", tc.body, got, tc.want)
		}
	}
}

func TestBitTohex(t *testing.T) {
	r, c := startData(t)
	got := sendArgs(t, r, c, "EVAL", "return bit.tohex(255)", "0")
	if got != "000000ff" {
		t.Fatalf("tohex 255 = %v", got)
	}
	got = sendArgs(t, r, c, "EVAL", "return bit.tohex(255, -4)", "0")
	if got != "00FF" {
		t.Fatalf("tohex upper = %v", got)
	}
}

func TestStructPackUnpack(t *testing.T) {
	r, c := startData(t)
	// Pack two little-endian int32s, unpack the first.
	got := sendArgs(t, r, c, "EVAL", `local s = struct.pack('<i', 258); return struct.unpack('<i', s)`, "0")
	if got != int64(258) {
		t.Fatalf("int32 round trip = %v", got)
	}
	// Big-endian round trip.
	got = sendArgs(t, r, c, "EVAL", `return struct.unpack('>I', struct.pack('>I', 70000))`, "0")
	if got != int64(70000) {
		t.Fatalf("uint32 be = %v", got)
	}
	// size of a fixed format.
	got = sendArgs(t, r, c, "EVAL", `return struct.size('<ihb')`, "0")
	if got != int64(7) {
		t.Fatalf("size = %v", got)
	}
	// length-prefixed string field.
	got = sendArgs(t, r, c, "EVAL", `return struct.unpack('<s', struct.pack('<s', 'abc'))`, "0")
	if got != "abc" {
		t.Fatalf("string field = %v", got)
	}
}
