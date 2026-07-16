package store

import (
	"bytes"
	"math"
	"strconv"
	"testing"
)

func newTestStore() *Store { return New(4<<20, 1<<18) }

func TestParseIntCanonical(t *testing.T) {
	good := map[string]int64{
		"0":                    0,
		"7":                    7,
		"42":                   42,
		"-1":                   -1,
		"9223372036854775807":  math.MaxInt64,
		"-9223372036854775808": math.MinInt64,
	}
	for in, want := range good {
		got, ok := ParseInt([]byte(in))
		if !ok || got != want {
			t.Fatalf("ParseInt(%q) = %d, %v; want %d, true", in, got, ok, want)
		}
	}
	bad := []string{
		"", "-", "+1", "-0", "00", "01", " 1", "1 ", "1.0", "1e2", "abc",
		"9223372036854775808", "-9223372036854775809", "123456789012345678901",
	}
	for _, in := range bad {
		if _, ok := ParseInt([]byte(in)); ok {
			t.Fatalf("ParseInt(%q) accepted, want reject", in)
		}
	}
}

func TestDecLen(t *testing.T) {
	for _, n := range []int64{0, 1, -1, 9, 10, -10, 12345, math.MaxInt64, math.MinInt64} {
		want := uint32(len(strconv.FormatInt(n, 10)))
		if got := decLen(n); got != want {
			t.Fatalf("decLen(%d) = %d, want %d", n, got, want)
		}
	}
}

// TestIntBandRoundTrip pins the V_INT band: canonical integer text stores as
// a cell, reads back byte-identical, and StrLen reports the digit count.
func TestIntBandRoundTrip(t *testing.T) {
	s := newTestStore()
	for _, v := range []string{"0", "7", "-13", "9223372036854775807", "-9223372036854775808"} {
		if err := s.SetString([]byte("k"), []byte(v), 1, 0, false); err != nil {
			t.Fatal(err)
		}
		got, ok := s.GetString([]byte("k"), 1, nil)
		if !ok || string(got) != v {
			t.Fatalf("round trip %q came back %q, %v", v, got, ok)
		}
		n, ok := s.StrLen([]byte("k"), 1)
		if !ok || n != int64(len(v)) {
			t.Fatalf("StrLen after %q = %d, want %d", v, n, len(v))
		}
	}
	// Non-canonical spellings stay text.
	for _, v := range []string{"007", "+5", "-0", "1.5"} {
		if err := s.SetString([]byte("t"), []byte(v), 1, 0, false); err != nil {
			t.Fatal(err)
		}
		got, _ := s.GetString([]byte("t"), 1, nil)
		if string(got) != v {
			t.Fatalf("text %q came back %q", v, got)
		}
	}
}

func TestLazyExpiry(t *testing.T) {
	s := newTestStore()
	key := []byte("k")
	if err := s.SetString(key, []byte("v"), 1000, 2000, false); err != nil {
		t.Fatal(err)
	}
	if !s.Exists(key, 1500) {
		t.Fatal("live record reads as absent before its deadline")
	}
	// At the deadline the record is gone and the touch reaps it.
	if s.Exists(key, 2000) {
		t.Fatal("expired record still reads as present")
	}
	if s.Len() != 0 {
		t.Fatalf("lazy reap left count at %d", s.Len())
	}
	// Del of an expired record answers false, like any read.
	if err := s.SetString(key, []byte("v"), 1000, 2000, false); err != nil {
		t.Fatal(err)
	}
	if s.Del(key, 3000) {
		t.Fatal("Del reported an expired record as deleted")
	}
	if s.Len() != 0 {
		t.Fatalf("count %d after expired Del", s.Len())
	}
}

func TestSetStringTTLTransitions(t *testing.T) {
	s := newTestStore()
	key := []byte("k")
	// A plain SET over a deadline clears it.
	if err := s.SetString(key, []byte("a"), 1000, 5000, false); err != nil {
		t.Fatal(err)
	}
	if err := s.SetString(key, []byte("b"), 1000, 0, false); err != nil {
		t.Fatal(err)
	}
	if !s.Exists(key, 9000) {
		t.Fatal("plain SET did not clear the deadline")
	}
	// KEEPTTL carries the deadline through a value replace.
	if err := s.SetString(key, []byte("c"), 1000, 5000, false); err != nil {
		t.Fatal(err)
	}
	if err := s.SetString(key, []byte("dddddddddddddddddddd"), 1000, 0, true); err != nil {
		t.Fatal(err)
	}
	got, ok := s.GetString(key, 4000, nil)
	if !ok || string(got) != "dddddddddddddddddddd" {
		t.Fatalf("KEEPTTL replace read %q, %v", got, ok)
	}
	if s.Exists(key, 5000) {
		t.Fatal("KEEPTTL lost the deadline")
	}
}

func TestIncrByPath(t *testing.T) {
	s := newTestStore()
	key := []byte("n")
	// Create on miss.
	if n, err := s.IncrBy(key, 5, 1); err != nil || n != 5 {
		t.Fatalf("create = %d, %v", n, err)
	}
	if n, err := s.IncrBy(key, -7, 1); err != nil || n != -2 {
		t.Fatalf("decrement = %d, %v", n, err)
	}
	// Text that parses canonically converts in place.
	if err := s.SetString(key, []byte("100"), 1, 0, false); err != nil {
		t.Fatal(err)
	}
	if n, err := s.IncrBy(key, 1, 1); err != nil || n != 101 {
		t.Fatalf("text convert = %d, %v", n, err)
	}
	// Non-integer text refuses.
	if err := s.SetString(key, []byte("abc"), 1, 0, false); err != nil {
		t.Fatal(err)
	}
	if _, err := s.IncrBy(key, 1, 1); err != ErrNotInt {
		t.Fatalf("non-int err = %v, want ErrNotInt", err)
	}
	// Overflow both ways.
	if err := s.SetString(key, []byte("9223372036854775807"), 1, 0, false); err != nil {
		t.Fatal(err)
	}
	if _, err := s.IncrBy(key, 1, 1); err != ErrOverflow {
		t.Fatalf("overflow err = %v, want ErrOverflow", err)
	}
	if err := s.SetString(key, []byte("-9223372036854775808"), 1, 0, false); err != nil {
		t.Fatal(err)
	}
	if _, err := s.IncrBy(key, -1, 1); err != ErrOverflow {
		t.Fatalf("underflow err = %v, want ErrOverflow", err)
	}
}

func TestAppendGrowth(t *testing.T) {
	s := newTestStore()
	key := []byte("k")
	// Create on miss.
	if n, err := s.Append(key, []byte("hello"), 1); err != nil || n != 5 {
		t.Fatalf("create = %d, %v", n, err)
	}
	// Fresh create has zero headroom: 5 bytes round to one word.
	_, addr, _ := s.findEntry(Hash(key), key)
	if got := s.vcapBytes(addr); got != 8 {
		t.Fatalf("fresh vcap = %d, want 8", got)
	}
	// The next append misses vcap and doubles.
	if n, err := s.Append(key, []byte(" world"), 1); err != nil || n != 11 {
		t.Fatalf("grow = %d, %v", n, err)
	}
	_, addr, _ = s.findEntry(Hash(key), key)
	if got := s.vcapBytes(addr); got != 16 {
		t.Fatalf("grown vcap = %d, want 16", got)
	}
	if s.recFlags(addr)&flagRawSticky == 0 {
		t.Fatal("APPEND did not set the raw-sticky flag")
	}
	got, _ := s.GetString(key, 1, nil)
	if string(got) != "hello world" {
		t.Fatalf("value = %q", got)
	}
	// APPEND onto an int cell materializes the digits first.
	if _, err := s.IncrBy([]byte("n"), 12, 1); err != nil {
		t.Fatal(err)
	}
	if n, err := s.Append([]byte("n"), []byte("x"), 1); err != nil || n != 3 {
		t.Fatalf("append to int = %d, %v", n, err)
	}
	got, _ = s.GetString([]byte("n"), 1, nil)
	if string(got) != "12x" {
		t.Fatalf("int append value = %q", got)
	}
	// APPEND keeps a deadline across the republish.
	if err := s.SetString([]byte("t"), []byte("aaaaaaaa"), 1000, 9000, false); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Append([]byte("t"), []byte("bbbbbbbbbbbbbbbb"), 1000); err != nil {
		t.Fatal(err)
	}
	if s.Exists([]byte("t"), 9000) {
		t.Fatal("APPEND republish lost the deadline")
	}
	// Growth past the chunk threshold moves the value to the chunked band.
	big := make([]byte, maxVal)
	if err := s.SetString([]byte("b"), big, 1, 0, false); err != nil {
		t.Fatal(err)
	}
	if n, err := s.Append([]byte("b"), []byte("x"), 1); err != nil || n != maxVal+1 {
		t.Fatalf("append past threshold = %d, %v", n, err)
	}
	got, ok := s.GetString([]byte("b"), 1, nil)
	if !ok || len(got) != maxVal+1 || got[maxVal] != 'x' {
		t.Fatalf("chunk transition value: len %d, ok %v", len(got), ok)
	}
}

func TestSetRangeZeroFill(t *testing.T) {
	s := newTestStore()
	key := []byte("k")
	// Create with a gap: the pad must be zero bytes even on a reused arena.
	if n, err := s.SetRange(key, 3, []byte("xy"), 1); err != nil || n != 5 {
		t.Fatalf("create = %d, %v", n, err)
	}
	got, _ := s.GetString(key, 1, nil)
	if !bytes.Equal(got, []byte{0, 0, 0, 'x', 'y'}) {
		t.Fatalf("gap = %v", got)
	}
	// Overwrite inside the value.
	if n, err := s.SetRange(key, 1, []byte("AB"), 1); err != nil || n != 5 {
		t.Fatalf("overwrite = %d, %v", n, err)
	}
	got, _ = s.GetString(key, 1, nil)
	if !bytes.Equal(got, []byte{0, 'A', 'B', 'x', 'y'}) {
		t.Fatalf("overwrite = %v", got)
	}
	// Extend past the end with a fresh gap, forcing a republish.
	if n, err := s.SetRange(key, 10, []byte("z"), 1); err != nil || n != 11 {
		t.Fatalf("extend = %d, %v", n, err)
	}
	got, _ = s.GetString(key, 1, nil)
	want := []byte{0, 'A', 'B', 'x', 'y', 0, 0, 0, 0, 0, 'z'}
	if !bytes.Equal(got, want) {
		t.Fatalf("extend = %v, want %v", got, want)
	}
	// SETRANGE over an int cell goes through digits.
	if _, err := s.IncrBy([]byte("n"), 1234, 1); err != nil {
		t.Fatal(err)
	}
	if n, err := s.SetRange([]byte("n"), 1, []byte("9"), 1); err != nil || n != 4 {
		t.Fatalf("int setrange = %d, %v", n, err)
	}
	got, _ = s.GetString([]byte("n"), 1, nil)
	if string(got) != "1934" {
		t.Fatalf("int setrange value = %q", got)
	}
	if _, err := s.SetRange(key, maxValueLen, []byte("x"), 1); err != ErrTooBig {
		t.Fatalf("cap err = %v, want ErrTooBig", err)
	}
}

// TestDirtyArenaReuseZeroFill drives delete and rewrite cycles so records land
// on reused bytes, then checks a SETRANGE gap still reads zero.
func TestDirtyArenaReuseZeroFill(t *testing.T) {
	s := New(1<<20, 1<<18)
	junk := bytes.Repeat([]byte{0xff}, 1024)
	// Paint the arena with junk records until it wraps at least once.
	for i := 0; i < 4096; i++ {
		k := []byte{byte(i), byte(i >> 8), 'j'}
		if err := s.Set(k, junk); err != nil {
			break
		}
		s.Delete(k)
	}
	if n, err := s.SetRange([]byte("gap"), 8, []byte("end"), 1); err != nil || n != 11 {
		t.Fatalf("setrange on dirty arena = %d, %v", n, err)
	}
	got, _ := s.GetString([]byte("gap"), 1, nil)
	if !bytes.Equal(got[:8], make([]byte, 8)) {
		t.Fatalf("gap holds junk: %v", got)
	}
}
