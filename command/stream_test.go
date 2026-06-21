package command

import (
	"bufio"
	"net"
	"strconv"
	"strings"
	"testing"
)

// readBulkRaw reads a single bulk reply already waiting in the buffer.
func readBulkRaw(t *testing.T, r *bufio.Reader) string {
	t.Helper()
	line := sendLineRead(t, r)
	if line == "$-1" || line == "_" {
		return "<nil>"
	}
	if line == "" || line[0] != '$' {
		t.Fatalf("expected bulk header, got %q", line)
	}
	payload, err := r.ReadString('\n')
	if err != nil {
		t.Fatalf("read bulk payload: %v", err)
	}
	return payload[:len(payload)-2]
}

// xentries sends a command and parses the [id, [f, v, ...]] entry-array reply
// into a flat list per entry: ["id", "f", "v", ...].
func xentries(t *testing.T, r *bufio.Reader, c net.Conn, cmd string) [][]string {
	t.Helper()
	hdr := sendLine(t, r, c, cmd)
	if hdr == "*-1" || hdr == "_" {
		return nil
	}
	n := arrayLen(t, hdr)
	out := make([][]string, 0, n)
	for range n {
		if got := sendLineRead(t, r); got != "*2" {
			t.Fatalf("entry header = %q want *2", got)
		}
		id := readBulkRaw(t, r)
		fhdr := sendLineRead(t, r)
		fn := arrayLen(t, fhdr)
		row := []string{id}
		for range fn {
			row = append(row, readBulkRaw(t, r))
		}
		out = append(out, row)
	}
	return out
}

func arrayLen(t *testing.T, line string) int {
	t.Helper()
	if line == "" || line[0] != '*' {
		t.Fatalf("expected array header, got %q", line)
	}
	n, err := strconv.Atoi(line[1:])
	if err != nil {
		t.Fatalf("bad array length %q", line)
	}
	return n
}

func TestXAddAndLen(t *testing.T) {
	r, c := startData(t)
	if got := bulk(t, r, c, "XADD s 1-1 a 1"); got != "1-1" {
		t.Fatalf("XADD 1-1 = %q", got)
	}
	if got := bulk(t, r, c, "XADD s 1-2 b 2"); got != "1-2" {
		t.Fatalf("XADD 1-2 = %q", got)
	}
	if got := sendLine(t, r, c, "XLEN s"); got != ":2" {
		t.Fatalf("XLEN = %q want :2", got)
	}
	if got := sendLine(t, r, c, "XLEN missing"); got != ":0" {
		t.Fatalf("XLEN missing = %q want :0", got)
	}
}

func TestXAddAutoID(t *testing.T) {
	r, c := startData(t)
	id1 := bulk(t, r, c, "XADD s * a 1")
	if !strings.Contains(id1, "-") {
		t.Fatalf("auto ID has no seq: %q", id1)
	}
	id2 := bulk(t, r, c, "XADD s * b 2")
	// Second ID must be strictly greater than the first.
	if !idLess(id1, id2) {
		t.Fatalf("auto IDs not increasing: %q then %q", id1, id2)
	}
}

func idLess(a, b string) bool {
	pa := strings.SplitN(a, "-", 2)
	pb := strings.SplitN(b, "-", 2)
	ams, _ := strconv.ParseUint(pa[0], 10, 64)
	aseq, _ := strconv.ParseUint(pa[1], 10, 64)
	bms, _ := strconv.ParseUint(pb[0], 10, 64)
	bseq, _ := strconv.ParseUint(pb[1], 10, 64)
	if ams != bms {
		return ams < bms
	}
	return aseq < bseq
}

func TestXAddPartialSeq(t *testing.T) {
	r, c := startData(t)
	if got := bulk(t, r, c, "XADD s 5-* a 1"); got != "5-0" {
		t.Fatalf("XADD 5-* = %q want 5-0", got)
	}
	if got := bulk(t, r, c, "XADD s 5-* b 2"); got != "5-1" {
		t.Fatalf("XADD 5-* again = %q want 5-1", got)
	}
}

func TestXAddMonotonicError(t *testing.T) {
	r, c := startData(t)
	_ = bulk(t, r, c, "XADD s 5-5 a 1")
	if got := sendLine(t, r, c, "XADD s 5-5 b 2"); got != "-"+errStreamIDSmaller {
		t.Fatalf("XADD equal ID = %q", got)
	}
	if got := sendLine(t, r, c, "XADD s 0-0 b 2"); got != "-"+errStreamIDNotGT0 {
		t.Fatalf("XADD 0-0 = %q", got)
	}
}

func TestXAddNoMkStream(t *testing.T) {
	r, c := startData(t)
	if got := bulk(t, r, c, "XADD missing NOMKSTREAM * a 1"); got != "<nil>" {
		t.Fatalf("XADD NOMKSTREAM missing = %q want nil", got)
	}
	if got := sendLine(t, r, c, "XLEN missing"); got != ":0" {
		t.Fatalf("stream created despite NOMKSTREAM: %q", got)
	}
}

func TestXAddWrongFields(t *testing.T) {
	r, c := startData(t)
	// Odd field/value list.
	if got := sendLine(t, r, c, "XADD s 1-1 a 1 b"); got != "-ERR wrong number of arguments for 'xadd' command" {
		t.Fatalf("XADD odd fields = %q", got)
	}
}

func TestXRange(t *testing.T) {
	r, c := startData(t)
	_ = bulk(t, r, c, "XADD s 1-1 a 1")
	_ = bulk(t, r, c, "XADD s 2-1 b 2")
	_ = bulk(t, r, c, "XADD s 3-1 c 3")
	got := xentries(t, r, c, "XRANGE s - +")
	if len(got) != 3 || got[0][0] != "1-1" || got[2][0] != "3-1" {
		t.Fatalf("XRANGE - + = %v", got)
	}
	if got[0][1] != "a" || got[0][2] != "1" {
		t.Fatalf("first entry fields = %v", got[0])
	}
	// Inclusive partial range.
	got = xentries(t, r, c, "XRANGE s 2 2")
	if len(got) != 1 || got[0][0] != "2-1" {
		t.Fatalf("XRANGE 2 2 = %v", got)
	}
	// Exclusive low bound.
	got = xentries(t, r, c, "XRANGE s (1-1 +")
	if len(got) != 2 || got[0][0] != "2-1" {
		t.Fatalf("XRANGE (1-1 + = %v", got)
	}
	// COUNT.
	got = xentries(t, r, c, "XRANGE s - + COUNT 2")
	if len(got) != 2 {
		t.Fatalf("XRANGE COUNT 2 = %v", got)
	}
}

func TestXRevRange(t *testing.T) {
	r, c := startData(t)
	_ = bulk(t, r, c, "XADD s 1-1 a 1")
	_ = bulk(t, r, c, "XADD s 2-1 b 2")
	_ = bulk(t, r, c, "XADD s 3-1 c 3")
	got := xentries(t, r, c, "XREVRANGE s + -")
	if len(got) != 3 || got[0][0] != "3-1" || got[2][0] != "1-1" {
		t.Fatalf("XREVRANGE + - = %v", got)
	}
	got = xentries(t, r, c, "XREVRANGE s + - COUNT 1")
	if len(got) != 1 || got[0][0] != "3-1" {
		t.Fatalf("XREVRANGE COUNT 1 = %v", got)
	}
}

func TestXDel(t *testing.T) {
	r, c := startData(t)
	_ = bulk(t, r, c, "XADD s 1-1 a 1")
	_ = bulk(t, r, c, "XADD s 2-1 b 2")
	if got := sendLine(t, r, c, "XDEL s 1-1 9-9"); got != ":1" {
		t.Fatalf("XDEL = %q want :1", got)
	}
	if got := sendLine(t, r, c, "XLEN s"); got != ":1" {
		t.Fatalf("XLEN after XDEL = %q want :1", got)
	}
	// last_id stays put, so re-adding the deleted ID is still rejected.
	if got := sendLine(t, r, c, "XADD s 1-1 a 1"); got != "-"+errStreamIDSmaller {
		t.Fatalf("XADD reused ID after XDEL = %q", got)
	}
}

func TestXRead(t *testing.T) {
	r, c := startData(t)
	_ = bulk(t, r, c, "XADD s 1-1 a 1")
	_ = bulk(t, r, c, "XADD s 2-1 b 2")
	// Read all after 0.
	hdr := sendLine(t, r, c, "XREAD STREAMS s 0")
	if hdr != "*1" {
		t.Fatalf("XREAD outer = %q want *1", hdr)
	}
	if got := sendLineRead(t, r); got != "*2" {
		t.Fatalf("XREAD pair header = %q", got)
	}
	if got := readBulkRaw(t, r); got != "s" {
		t.Fatalf("XREAD stream name = %q", got)
	}
	// Then the entry array: 2 entries.
	if got := sendLineRead(t, r); got != "*2" {
		t.Fatalf("XREAD entries header = %q", got)
	}
	// Drain the two entries.
	for range 2 {
		if got := sendLineRead(t, r); got != "*2" {
			t.Fatalf("entry header = %q", got)
		}
		_ = readBulkRaw(t, r) // id
		fh := sendLineRead(t, r)
		for range arrayLen(t, fh) {
			_ = readBulkRaw(t, r)
		}
	}
	// Nothing new after the last ID returns null.
	if got := sendLine(t, r, c, "XREAD STREAMS s 2-1"); got != "*-1" {
		t.Fatalf("XREAD after last = %q want *-1", got)
	}
	// $ means only entries after now, so it returns null on a static stream.
	if got := sendLine(t, r, c, "XREAD STREAMS s $"); got != "*-1" {
		t.Fatalf("XREAD $ = %q want *-1", got)
	}
}

func TestXReadCount(t *testing.T) {
	r, c := startData(t)
	_ = bulk(t, r, c, "XADD s 1-1 a 1")
	_ = bulk(t, r, c, "XADD s 2-1 b 2")
	_ = bulk(t, r, c, "XADD s 3-1 c 3")
	if got := sendLine(t, r, c, "XREAD COUNT 2 STREAMS s 0"); got != "*1" {
		t.Fatalf("XREAD COUNT outer = %q", got)
	}
	_ = sendLineRead(t, r) // *2 pair
	_ = readBulkRaw(t, r)  // stream name
	if got := sendLineRead(t, r); got != "*2" {
		t.Fatalf("XREAD COUNT 2 entries header = %q want *2", got)
	}
}

func TestXReadUnbalanced(t *testing.T) {
	r, c := startData(t)
	_ = bulk(t, r, c, "XADD s 1-1 a 1")
	// Two keys but only one ID is an unbalanced STREAMS clause.
	if got := sendLine(t, r, c, "XREAD STREAMS s s2 0"); got != "-"+errStreamUnbalanced {
		t.Fatalf("XREAD unbalanced = %q", got)
	}
}

func TestStreamWrongType(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET k v")
	for _, cmd := range []string{
		"XADD k 1-1 a 1", "XLEN k", "XRANGE k - +", "XREVRANGE k + -", "XDEL k 1-1",
	} {
		if got := sendLine(t, r, c, cmd); got != "-"+wrongTypeError {
			t.Fatalf("%s = %q want WRONGTYPE", cmd, got)
		}
	}
}
