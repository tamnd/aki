package rdb

import (
	"bytes"
	"slices"
	"testing"
)

// TestStringPayloadShape checks the DUMP byte layout for a short string against
// the worked example in the spec: type byte, length-prefixed value, version 11.
func TestStringPayloadShape(t *testing.T) {
	got, err := Marshal(Value{Kind: KindString, Str: []byte("hello")})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	wantPrefix := []byte{0x00, 0x05, 'h', 'e', 'l', 'l', 'o', 0x0b, 0x00}
	if !bytes.HasPrefix(got, wantPrefix) {
		t.Fatalf("payload prefix = % x want % x", got[:len(wantPrefix)], wantPrefix)
	}
	if len(got) != len(wantPrefix)+8 {
		t.Fatalf("payload len = %d want %d", len(got), len(wantPrefix)+8)
	}
}

// TestStringIntEncoding checks that an integer string packs into an INT8 special
// encoding rather than a raw run.
func TestStringIntEncoding(t *testing.T) {
	got, err := Marshal(Value{Kind: KindString, Str: []byte("100")})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	// type 0x00, then 0xC0 (INT8) and the byte 100.
	if got[0] != 0x00 || got[1] != 0xC0 || got[2] != 100 {
		t.Fatalf("int string head = % x", got[:3])
	}
	back, err := Unmarshal(got)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if string(back.Str) != "100" {
		t.Fatalf("round trip = %q", back.Str)
	}
}

// TestCRCRejectsTamper checks that flipping a payload byte fails the checksum.
func TestCRCRejectsTamper(t *testing.T) {
	got, _ := Marshal(Value{Kind: KindString, Str: []byte("hello")})
	got[2] ^= 0xFF
	if _, err := Unmarshal(got); err == nil {
		t.Fatal("tampered payload accepted")
	}
}

// TestVersionRejected checks that a payload claiming a newer version is refused.
func TestVersionRejected(t *testing.T) {
	got, _ := Marshal(Value{Kind: KindString, Str: []byte("x")})
	// The version is the two bytes before the 8-byte CRC. Bump it past maxVersion
	// and recompute the CRC so only the version check can reject it.
	got[len(got)-10] = maxVersion + 1
	fixed := appendCRC64(got[:len(got)-8], got[:len(got)-8])
	if _, err := Unmarshal(fixed); err == nil {
		t.Fatal("future version accepted")
	}
}

// TestRoundTripList checks a list round-trips through the quicklist form.
func TestRoundTripList(t *testing.T) {
	in := Value{Kind: KindList, List: [][]byte{[]byte("a"), []byte("1"), []byte("hello world")}}
	out := roundTrip(t, in)
	if !equalBytes(in.List, out.List) {
		t.Fatalf("list = %q want %q", out.List, in.List)
	}
}

// TestRoundTripIntSet checks an all-integer set takes the intset form and comes
// back with the same members, sorted as intset stores them.
func TestRoundTripIntSet(t *testing.T) {
	in := Value{Kind: KindSet, Set: [][]byte{[]byte("3"), []byte("1"), []byte("2")}}
	payload, _ := Marshal(in)
	if payload[0] != typeSetIntset {
		t.Fatalf("set type = %d want intset %d", payload[0], typeSetIntset)
	}
	out, err := Unmarshal(payload)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if !equalBytes(out.Set, [][]byte{[]byte("1"), []byte("2"), []byte("3")}) {
		t.Fatalf("intset members = %q", out.Set)
	}
}

// TestRoundTripListpackSet checks a set with a non-integer member takes the
// listpack form and round-trips.
func TestRoundTripListpackSet(t *testing.T) {
	in := Value{Kind: KindSet, Set: [][]byte{[]byte("apple"), []byte("7")}}
	payload, _ := Marshal(in)
	if payload[0] != typeSetListpack {
		t.Fatalf("set type = %d want listpack %d", payload[0], typeSetListpack)
	}
	out := roundTrip(t, in)
	if !sameMembers(out.Set, in.Set) {
		t.Fatalf("set = %q want %q", out.Set, in.Set)
	}
}

// TestRoundTripHash checks a hash round-trips with its fields and values intact.
func TestRoundTripHash(t *testing.T) {
	in := Value{Kind: KindHash, Hash: []Field{
		{Field: []byte("f1"), Value: []byte("v1")},
		{Field: []byte("count"), Value: []byte("42")},
	}}
	out := roundTrip(t, in)
	if len(out.Hash) != 2 || string(out.Hash[0].Field) != "f1" || string(out.Hash[1].Value) != "42" {
		t.Fatalf("hash = %+v", out.Hash)
	}
}

// TestRoundTripZSet checks a sorted set round-trips through the listpack form with
// integer and fractional scores.
func TestRoundTripZSet(t *testing.T) {
	in := Value{Kind: KindZSet, ZSet: []Member{
		{Member: []byte("a"), Score: 1},
		{Member: []byte("b"), Score: 2.5},
	}}
	payload, _ := Marshal(in)
	if payload[0] != typeZSetListpack {
		t.Fatalf("zset type = %d want listpack %d", payload[0], typeZSetListpack)
	}
	out := roundTrip(t, in)
	if len(out.ZSet) != 2 || out.ZSet[0].Score != 1 || out.ZSet[1].Score != 2.5 {
		t.Fatalf("zset = %+v", out.ZSet)
	}
}

// TestRoundTripBigZSet checks a sorted set past the listpack threshold uses the
// binary-double form and round-trips.
func TestRoundTripBigZSet(t *testing.T) {
	var members []Member
	for i := range maxListpackEntries + 5 {
		members = append(members, Member{Member: []byte{byte('a' + i%26), byte('0' + i/26)}, Score: float64(i)})
	}
	in := Value{Kind: KindZSet, ZSet: members}
	payload, _ := Marshal(in)
	if payload[0] != typeZSet2 {
		t.Fatalf("zset type = %d want zset2 %d", payload[0], typeZSet2)
	}
	out := roundTrip(t, in)
	if len(out.ZSet) != len(members) {
		t.Fatalf("zset len = %d want %d", len(out.ZSet), len(members))
	}
}

// TestListpackIntWidths checks every integer width packs and reads back exactly.
func TestListpackIntWidths(t *testing.T) {
	vals := []string{"0", "127", "-1", "4095", "-4096", "32767", "-32768",
		"8388607", "-8388608", "2147483647", "-2147483648", "9223372036854775807"}
	in := make([][]byte, len(vals))
	for i, v := range vals {
		in[i] = []byte(v)
	}
	blob := listpackEncode(in)
	out, err := listpackDecode(blob)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	got := make([]string, len(out))
	for i, b := range out {
		got[i] = string(b)
	}
	if !slices.Equal(got, vals) {
		t.Fatalf("listpack ints = %v want %v", got, vals)
	}
}

// roundTrip marshals then unmarshals a value and fails the test on any error.
func roundTrip(t *testing.T, v Value) Value {
	t.Helper()
	payload, err := Marshal(v)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	out, err := Unmarshal(payload)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	return out
}

func equalBytes(a, b [][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !bytes.Equal(a[i], b[i]) {
			return false
		}
	}
	return true
}

func sameMembers(a, b [][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	seen := map[string]int{}
	for _, x := range a {
		seen[string(x)]++
	}
	for _, x := range b {
		seen[string(x)]--
	}
	for _, n := range seen {
		if n != 0 {
			return false
		}
	}
	return true
}
