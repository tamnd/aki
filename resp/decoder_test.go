package resp

import (
	"bytes"
	"errors"
	"math"
	"testing"
)

func TestDecodeScalars(t *testing.T) {
	cases := []struct {
		in    string
		check func(t *testing.T, v RESPValue)
	}{
		{"+OK\r\n", func(t *testing.T, v RESPValue) {
			if v.Type != TypeSimpleString || string(v.Str) != "OK" {
				t.Fatalf("simple string: %+v", v)
			}
		}},
		{"-ERR bad\r\n", func(t *testing.T, v RESPValue) {
			if v.Type != TypeError || v.Err != "ERR bad" {
				t.Fatalf("error: %+v", v)
			}
		}},
		{":42\r\n", func(t *testing.T, v RESPValue) {
			if v.Type != TypeInteger || v.Integer != 42 {
				t.Fatalf("integer: %+v", v)
			}
		}},
		{"$5\r\nhello\r\n", func(t *testing.T, v RESPValue) {
			if v.Type != TypeBulkString || string(v.Str) != "hello" {
				t.Fatalf("bulk: %+v", v)
			}
		}},
		{"$-1\r\n", func(t *testing.T, v RESPValue) {
			if v.Type != TypeBulkString || !v.IsNull {
				t.Fatalf("null bulk: %+v", v)
			}
		}},
		{"$0\r\n\r\n", func(t *testing.T, v RESPValue) {
			if v.Type != TypeBulkString || v.IsNull || len(v.Str) != 0 {
				t.Fatalf("empty bulk: %+v", v)
			}
		}},
		{"_\r\n", func(t *testing.T, v RESPValue) {
			if v.Type != TypeNull || !v.IsNull {
				t.Fatalf("null: %+v", v)
			}
		}},
		{"#t\r\n", func(t *testing.T, v RESPValue) {
			if v.Type != TypeBool || !v.Bool {
				t.Fatalf("bool: %+v", v)
			}
		}},
		{",3.14\r\n", func(t *testing.T, v RESPValue) {
			if v.Type != TypeDouble || v.Float != 3.14 {
				t.Fatalf("double: %+v", v)
			}
		}},
		{",inf\r\n", func(t *testing.T, v RESPValue) {
			if v.Type != TypeDouble || !math.IsInf(v.Float, 1) {
				t.Fatalf("inf: %+v", v)
			}
		}},
		{"(12345678901234567890\r\n", func(t *testing.T, v RESPValue) {
			if v.Type != TypeBigNumber || v.BigInt.String() != "12345678901234567890" {
				t.Fatalf("bignum: %+v", v)
			}
		}},
		{"=16\r\ntxt:Hello World!\r\n", func(t *testing.T, v RESPValue) {
			if v.Type != TypeVerbatim || v.VerbEnc != "txt" || string(v.Str) != "Hello World!" {
				t.Fatalf("verbatim: %+v", v)
			}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			v, pos, err := Decode([]byte(tc.in), 0)
			if err != nil {
				t.Fatalf("Decode(%q) error: %v", tc.in, err)
			}
			if pos != len(tc.in) {
				t.Fatalf("Decode(%q) pos=%d want %d", tc.in, pos, len(tc.in))
			}
			tc.check(t, v)
		})
	}
}

func TestDecodeArrayAndMapAndSet(t *testing.T) {
	v, _, err := Decode([]byte("*3\r\n$3\r\nfoo\r\n$3\r\nbar\r\n:7\r\n"), 0)
	if err != nil {
		t.Fatal(err)
	}
	if v.Type != TypeArray || len(v.Elems) != 3 {
		t.Fatalf("array: %+v", v)
	}
	if string(v.Elems[0].Str) != "foo" || v.Elems[2].Integer != 7 {
		t.Fatalf("array elems: %+v", v.Elems)
	}

	m, _, err := Decode([]byte("%1\r\n$1\r\na\r\n:1\r\n"), 0)
	if err != nil {
		t.Fatal(err)
	}
	if m.Type != TypeMap || len(m.Map) != 1 || string(m.Map[0][0].Str) != "a" || m.Map[0][1].Integer != 1 {
		t.Fatalf("map: %+v", m)
	}

	s, _, err := Decode([]byte("~2\r\n$1\r\na\r\n$1\r\nb\r\n"), 0)
	if err != nil {
		t.Fatal(err)
	}
	if s.Type != TypeSet || len(s.Elems) != 2 {
		t.Fatalf("set: %+v", s)
	}

	nullArr, _, err := Decode([]byte("*-1\r\n"), 0)
	if err != nil {
		t.Fatal(err)
	}
	if nullArr.Type != TypeArray || !nullArr.IsNull {
		t.Fatalf("null array: %+v", nullArr)
	}
}

func TestDecodeNestedArray(t *testing.T) {
	v, _, err := Decode([]byte("*2\r\n*2\r\n:1\r\n:2\r\n+ok\r\n"), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(v.Elems) != 2 || v.Elems[0].Type != TypeArray || len(v.Elems[0].Elems) != 2 {
		t.Fatalf("nested: %+v", v)
	}
}

func TestDecodeAttribute(t *testing.T) {
	v, pos, err := Decode([]byte("|1\r\n+ttl\r\n:3600\r\n:42\r\n"), 0)
	if err != nil {
		t.Fatal(err)
	}
	if v.Type != TypeAttribute || len(v.Attrs) != 1 {
		t.Fatalf("attribute: %+v", v)
	}
	if v.AttrBody == nil || v.AttrBody.Integer != 42 {
		t.Fatalf("attribute body: %+v", v.AttrBody)
	}
	if pos != len("|1\r\n+ttl\r\n:3600\r\n:42\r\n") {
		t.Fatalf("pos=%d", pos)
	}
}

func TestDecodeStreamed(t *testing.T) {
	v, _, err := Decode([]byte("$?\r\n;5\r\nhello\r\n;6\r\n world\r\n;0\r\n"), 0)
	if err != nil {
		t.Fatal(err)
	}
	if string(v.Str) != "hello world" {
		t.Fatalf("streamed bulk: %q", v.Str)
	}

	arr, _, err := Decode([]byte("*?\r\n$3\r\nfoo\r\n$3\r\nbar\r\n.\r\n"), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(arr.Elems) != 2 || string(arr.Elems[1].Str) != "bar" {
		t.Fatalf("streamed array: %+v", arr)
	}
}

func TestDecodeNeedMore(t *testing.T) {
	// Every proper prefix of a complete value must report ErrNeedMore and leave
	// pos at the start so a retry from the same offset is safe.
	full := "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
	for i := 1; i < len(full); i++ {
		_, pos, err := Decode([]byte(full[:i]), 0)
		if !errors.Is(err, ErrNeedMore) {
			t.Fatalf("prefix len %d: err=%v want ErrNeedMore", i, err)
		}
		if pos != 0 {
			t.Fatalf("prefix len %d: pos=%d want 0", i, pos)
		}
	}
	v, pos, err := Decode([]byte(full), 0)
	if err != nil || pos != len(full) || len(v.Elems) != 2 {
		t.Fatalf("full decode: v=%+v pos=%d err=%v", v, pos, err)
	}
}

func TestDecodeProtocolErrors(t *testing.T) {
	bad := []string{
		":notanint\r\n",
		"#x\r\n",
		"$-2\r\n",
		"@nope\r\n",
		",notafloat\r\n",
	}
	for _, in := range bad {
		_, _, err := Decode([]byte(in), 0)
		var pe ProtocolError
		if !errors.As(err, &pe) {
			t.Fatalf("Decode(%q) err=%v want ProtocolError", in, err)
		}
	}
}

func TestDecodeBulkBadCRLF(t *testing.T) {
	_, _, err := Decode([]byte("$3\r\nfooXX"), 0)
	var pe ProtocolError
	if !errors.As(err, &pe) {
		t.Fatalf("err=%v want ProtocolError", err)
	}
}

// TestRoundTrip encodes a set of values and decodes them back, checking the
// decoder is the inverse of the encoder for the common reply shapes.
func TestRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	e := NewEncoder(&buf, 3)
	e.WriteArrayLen(4)
	e.WriteStatus("OK")
	e.WriteInteger(-9)
	e.WriteBulkString([]byte("payload"))
	e.WriteNull()

	v, pos, err := Decode(buf.Bytes(), 0)
	if err != nil || pos != buf.Len() {
		t.Fatalf("decode: pos=%d err=%v", pos, err)
	}
	if len(v.Elems) != 4 || v.Elems[1].Integer != -9 || string(v.Elems[2].Str) != "payload" || !v.Elems[3].IsNull {
		t.Fatalf("roundtrip: %+v", v)
	}
}
