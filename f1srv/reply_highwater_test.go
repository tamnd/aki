package f1srv

import (
	"bufio"
	"strconv"
	"testing"
)

// TestReplyHighWaterStreaming proves the mid-drain high-water flush streams a pipeline of
// large materialize replies out byte-correct and complete. It builds one hash whose HGETALL
// reply is several times the outHighWater mark, pipelines several HGETALLs in a single write,
// and checks every reply carries the full field set with the right value for each field. The
// point is the flush path: a pipeline of these replies used to grow one unbounded c.out and
// reallocate it through the whole pipeline before a single write, and the streaming flush must
// not drop, duplicate, or split a reply when it hands the buffer to the socket at a command
// boundary partway through the drain.
func TestReplyHighWaterStreaming(t *testing.T) {
	rw, cleanup := dialTestServerMode(t, "go")
	defer cleanup()

	// 5000 fields of 64-byte values frames an HGETALL reply well past outHighWater (256 KiB):
	// each field is a "$len\r\nfNNNN\r\n$64\r\n<64 bytes>\r\n" pair, roughly 90 bytes, so the
	// whole reply is near 450 KiB and trips the flush more than once mid-reply-set.
	const fields = 5000
	val := make([]byte, 64)
	for i := range val {
		val[i] = 'x'
	}
	valStr := string(val)

	// Preload the hash one field per HSET, pipelined so the build is one flush.
	for i := 0; i < fields; i++ {
		writeCmd(t, rw, "HSET", "bighash", "f"+strconv.Itoa(i), valStr)
	}
	if err := rw.Flush(); err != nil {
		t.Fatalf("preload flush: %v", err)
	}
	for i := 0; i < fields; i++ {
		if got := readReply(t, rw); got != ":1" {
			t.Fatalf("HSET %d reply %q, want :1", i, got)
		}
	}

	// Pipeline several HGETALLs at once so more than one large reply is in flight through the
	// same drain, which is exactly the P16 case the high-water flush changes.
	const repeats = 4
	for r := 0; r < repeats; r++ {
		writeCmd(t, rw, "HGETALL", "bighash")
	}
	if err := rw.Flush(); err != nil {
		t.Fatalf("hgetall flush: %v", err)
	}

	for r := 0; r < repeats; r++ {
		got := readArrayMap(t, rw)
		if len(got) != fields {
			t.Fatalf("HGETALL %d returned %d fields, want %d", r, len(got), fields)
		}
		// Spot-check the boundary fields and a middle one carry the right value, which catches a
		// flush that split a field across two writes or dropped one.
		for _, idx := range []int{0, 1, fields / 2, fields - 2, fields - 1} {
			f := "f" + strconv.Itoa(idx)
			if got[f] != valStr {
				t.Fatalf("HGETALL %d field %s value len %d, want the 64-byte value", r, f, len(got[f]))
			}
		}
	}
}

// readArrayMap reads one RESP array reply of alternating field/value bulk strings and returns
// it as a field to value map, the shape an HGETALL reply carries. It fails the test on a
// non-array header or an odd element count, so a truncated or malformed streamed reply surfaces
// here rather than as a silent short map.
func readArrayMap(t *testing.T, rw *bufio.ReadWriter) map[string]string {
	t.Helper()
	head := readReply(t, rw)
	if len(head) == 0 || head[0] != '*' {
		t.Fatalf("want array header, got %q", head)
	}
	n, err := strconv.Atoi(head[1:])
	if err != nil {
		t.Fatalf("bad array count %q: %v", head, err)
	}
	if n%2 != 0 {
		t.Fatalf("odd array count %d for a field/value reply", n)
	}
	out := make(map[string]string, n/2)
	for i := 0; i < n; i += 2 {
		out[readBulk(t, rw)] = readBulk(t, rw)
	}
	return out
}
