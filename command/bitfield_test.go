package command

import (
	"bufio"
	"net"
	"strconv"
	"strings"
	"testing"

	"github.com/tamnd/aki/keyspace"
	"github.com/tamnd/aki/pager"
	"github.com/tamnd/aki/vfs"
)

// intArray sends cmd, reads the *N array header, then returns the N element
// lines verbatim (with CRLF stripped). BITFIELD elements are RESP integers, or
// a null bulk ($-1) for an INCRBY that failed, so each element is one line.
func intArray(t *testing.T, r *bufio.Reader, c net.Conn, cmd string) []string {
	t.Helper()
	line := sendLine(t, r, c, cmd)
	if line == "" || line[0] != '*' {
		t.Fatalf("expected array header after %q, got %q", cmd, line)
	}
	n, err := strconv.Atoi(line[1:])
	if err != nil {
		t.Fatalf("parse array len %q: %v", line, err)
	}
	out := make([]string, n)
	for i := range out {
		el, err := r.ReadString('\n')
		if err != nil {
			t.Fatalf("read element %d after %q: %v", i, cmd, err)
		}
		out[i] = strings.TrimRight(el, "\r\n")
	}
	return out
}

func TestBitFieldGet(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, `SET myfield "\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09"`)
	cases := []struct {
		cmd  string
		want string
	}{
		{"BITFIELD myfield GET u8 0", ":0"},
		{"BITFIELD myfield GET u8 8", ":1"},
		{"BITFIELD myfield GET u8 16", ":2"},
		{"BITFIELD myfield GET i8 8", ":1"},
		{"BITFIELD myfield GET u4 0", ":0"},
		{"BITFIELD myfield GET u4 12", ":1"},
	}
	for _, tc := range cases {
		got := intArray(t, r, c, tc.cmd)
		if len(got) != 1 || got[0] != tc.want {
			t.Fatalf("%s = %v want [%s]", tc.cmd, got, tc.want)
		}
	}
}

func TestBitFieldSignedGet(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, `SET sigfield "\xff"`)
	if got := intArray(t, r, c, "BITFIELD sigfield GET i8 0"); got[0] != ":-1" {
		t.Fatalf("GET i8 0 = %v want -1", got)
	}
	if got := intArray(t, r, c, "BITFIELD sigfield GET u8 0"); got[0] != ":255" {
		t.Fatalf("GET u8 0 = %v want 255", got)
	}
	if got := intArray(t, r, c, "BITFIELD sigfield GET i4 0"); got[0] != ":-1" {
		t.Fatalf("GET i4 0 = %v want -1", got)
	}
}

func TestBitFieldSet(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, `SET mykey "\x00\x00"`)
	if got := intArray(t, r, c, "BITFIELD mykey SET u8 0 200"); got[0] != ":0" {
		t.Fatalf("SET returns old = %v want 0", got)
	}
	got := intArray(t, r, c, "BITFIELD mykey SET u8 0 200 GET u8 0")
	if len(got) != 2 || got[0] != ":200" || got[1] != ":200" {
		t.Fatalf("SET+GET = %v want [200 200]", got)
	}
}

func TestBitFieldCounterArraySat(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "DEL counters")
	// The example packs three u16 counters; set each slot by its # index.
	_ = intArray(t, r, c, "BITFIELD counters SET u16 #0 1000")
	_ = intArray(t, r, c, "BITFIELD counters SET u16 #1 2000")
	_ = intArray(t, r, c, "BITFIELD counters SET u16 #2 3000")
	if got := intArray(t, r, c, "BITFIELD counters GET u16 #0"); got[0] != ":1000" {
		t.Fatalf("GET u16 #0 = %v want 1000", got)
	}
	if got := intArray(t, r, c, "BITFIELD counters GET u16 #1"); got[0] != ":2000" {
		t.Fatalf("GET u16 #1 = %v want 2000", got)
	}
	if got := intArray(t, r, c, "BITFIELD counters GET u16 #2"); got[0] != ":3000" {
		t.Fatalf("GET u16 #2 = %v want 3000", got)
	}
	// 1000 + 65000 saturates to 65535.
	if got := intArray(t, r, c, "BITFIELD counters OVERFLOW SAT INCRBY u16 #0 65000"); got[0] != ":65535" {
		t.Fatalf("SAT INCRBY = %v want 65535", got)
	}
}

func TestBitFieldSignedWrap(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "DEL sf")
	if got := intArray(t, r, c, "BITFIELD sf OVERFLOW WRAP INCRBY i8 0 127"); got[0] != ":127" {
		t.Fatalf("INCRBY to 127 = %v", got)
	}
	if got := intArray(t, r, c, "BITFIELD sf OVERFLOW WRAP INCRBY i8 0 1"); got[0] != ":-128" {
		t.Fatalf("INCRBY wrap = %v want -128", got)
	}
}

func TestBitFieldFail(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "DEL ff")
	_ = intArray(t, r, c, "BITFIELD ff SET u8 0 250")
	// 250 + 10 = 260 > 255, FAIL returns null and does not modify.
	got := intArray(t, r, c, "BITFIELD ff OVERFLOW FAIL INCRBY u8 0 10")
	if len(got) != 1 || got[0] != "$-1" {
		t.Fatalf("FAIL INCRBY = %v want null", got)
	}
	if got := intArray(t, r, c, "BITFIELD ff GET u8 0"); got[0] != ":250" {
		t.Fatalf("value after FAIL = %v want 250 unchanged", got)
	}
}

func TestBitFieldCrossByte(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "DEL xb")
	if got := intArray(t, r, c, "BITFIELD xb SET u16 4 43981"); got[0] != ":0" {
		t.Fatalf("SET cross-byte = %v want 0", got)
	}
	if got := intArray(t, r, c, "BITFIELD xb GET u16 4"); got[0] != ":43981" {
		t.Fatalf("GET cross-byte = %v want 43981", got)
	}
}

func TestBitFieldOverflowPersists(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "DEL p")
	// OVERFLOW SAT applies to both INCRBY that follow it.
	got := intArray(t, r, c, "BITFIELD p OVERFLOW SAT INCRBY i8 0 200 GET u8 0")
	if len(got) != 2 || got[0] != ":127" || got[1] != ":127" {
		t.Fatalf("SAT then GET = %v want [127 127]", got)
	}
}

func TestBitFieldEmpty(t *testing.T) {
	r, c := startData(t)
	if got := sendLine(t, r, c, "BITFIELD nokey"); got != "*0" {
		t.Fatalf("BITFIELD with no ops = %q want *0", got)
	}
}

func TestBitFieldBadType(t *testing.T) {
	r, c := startData(t)
	if got := sendLine(t, r, c, "BITFIELD k GET u64 0"); got != "-"+bitfieldTypeError {
		t.Fatalf("u64 = %q", got)
	}
	if got := sendLine(t, r, c, "BITFIELD k GET i65 0"); got != "-"+bitfieldTypeError {
		t.Fatalf("i65 = %q", got)
	}
	if got := sendLine(t, r, c, "BITFIELD k GET x8 0"); got != "-"+bitfieldTypeError {
		t.Fatalf("x8 = %q", got)
	}
}

func TestBitFieldRO(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, `SET f "\x01"`)
	if got := intArray(t, r, c, "BITFIELD_RO f GET u8 0"); got[0] != ":1" {
		t.Fatalf("BITFIELD_RO GET = %v want 1", got)
	}
	if got := sendLine(t, r, c, "BITFIELD_RO f SET u8 0 9"); got != "-ERR BITFIELD_RO only supports the GET subcommand" {
		t.Fatalf("BITFIELD_RO SET = %q", got)
	}
}

func TestBitFieldWrongType(t *testing.T) {
	fs := vfs.NewMem()
	p, err := pager.Create(fs, "data.aki", pager.Options{PageSize: 4096, DBCount: 16})
	if err != nil {
		t.Fatalf("create pager: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	ks, err := keyspace.Open(p)
	if err != nil {
		t.Fatalf("open keyspace: %v", err)
	}
	eng := NewEngine(ks)
	if err := eng.update(0, func(db *keyspace.DB) error {
		return db.Set([]byte("ml"), []byte("x"), keyspace.TypeList, keyspace.EncRaw, -1)
	}); err != nil {
		t.Fatalf("seed list: %v", err)
	}
	r, c := start(t, Config{Engine: eng})
	if got := sendLine(t, r, c, "BITFIELD ml GET u8 0"); got != "-"+wrongTypeError {
		t.Fatalf("BITFIELD on list = %q", got)
	}
	if got := sendLine(t, r, c, "BITFIELD_RO ml GET u8 0"); got != "-"+wrongTypeError {
		t.Fatalf("BITFIELD_RO on list = %q", got)
	}
}
