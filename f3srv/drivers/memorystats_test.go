package drivers

import (
	"bufio"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
)

// TestMemoryStatsAndDoctor drives the keyless MEMORY subcommands through the
// real fan. STATS scatters to every shard, sums the counter blob, and renders a
// flat field-value array; the test checks the fields it reports are present,
// keys.count tracks the live key count, and total.allocated is positive once the
// dataset holds data. DOCTOR folds the aggregate used-memory figure into a bulk
// verdict, which on a near-empty instance is the small-dataset line.
func TestMemoryStatsAndDoctor(t *testing.T) {
	_, nc, br := startServer(t)

	// A fresh instance reports zero keys and a doctor verdict that parses as a
	// non-empty bulk string.
	stats := readMemStats(t, nc, br)
	if stats["keys.count"] != 0 {
		t.Fatalf("fresh keys.count = %d, want 0", stats["keys.count"])
	}
	for _, want := range []string{"total.allocated", "dataset.bytes", "index.bytes", "vlog.bytes", "keys.bytes-per-key"} {
		if _, ok := stats[want]; !ok {
			t.Fatalf("MEMORY STATS missing field %q", want)
		}
	}

	send(t, nc, "MEMORY", "DOCTOR")
	doctor := readBulk(t, br)
	if doctor == "" {
		t.Fatalf("MEMORY DOCTOR returned an empty verdict")
	}

	// Load some keys across types and re-read: keys.count reflects them and
	// per-key bytes is total/keys.
	send(t, nc, "SET", "k1", "v1")
	expect(t, br, "+OK\r\n")
	send(t, nc, "SET", "k2", "v2")
	expect(t, br, "+OK\r\n")
	send(t, nc, "RPUSH", "k3", "a", "b")
	expect(t, br, ":2\r\n")
	send(t, nc, "HSET", "k4", "f", "v")
	expect(t, br, ":1\r\n")

	stats = readMemStats(t, nc, br)
	if stats["keys.count"] != 4 {
		t.Fatalf("keys.count = %d, want 4", stats["keys.count"])
	}
	if stats["total.allocated"] <= 0 {
		t.Fatalf("total.allocated = %d, want positive", stats["total.allocated"])
	}
	if want := stats["total.allocated"] / 4; stats["keys.bytes-per-key"] != want {
		t.Fatalf("keys.bytes-per-key = %d, want total/keys = %d", stats["keys.bytes-per-key"], want)
	}

	// A malformed keyless subcommand stays on the point path and answers the
	// unknown-subcommand error rather than fanning.
	send(t, nc, "MEMORY", "BOGUS")
	line, err := br.ReadString('\n')
	if err != nil {
		t.Fatalf("read BOGUS reply: %v", err)
	}
	if !strings.HasPrefix(line, "-ERR Unknown MEMORY subcommand") {
		t.Fatalf("MEMORY BOGUS reply = %q, want unknown-subcommand error", line)
	}
}

// readMemStats sends MEMORY STATS and decodes its flat field-value array into a
// map. Each pair is a bulk name followed by an integer value.
func readMemStats(t *testing.T, nc net.Conn, br *bufio.Reader) map[string]int64 {
	t.Helper()
	send(t, nc, "MEMORY", "STATS")
	head, err := br.ReadString('\n')
	if err != nil {
		t.Fatalf("read STATS array header: %v", err)
	}
	if len(head) == 0 || head[0] != '*' {
		t.Fatalf("STATS header = %q, want array", head)
	}
	n, err := strconv.Atoi(strings.TrimSuffix(head[1:], "\r\n"))
	if err != nil || n%2 != 0 {
		t.Fatalf("STATS array length %q: %v", head, err)
	}
	out := make(map[string]int64, n/2)
	for i := 0; i < n/2; i++ {
		name := readBulkFrom(t, br)
		out[name] = readIntFrom(t, br)
	}
	return out
}

// readBulkFrom reads one RESP bulk string off br.
func readBulkFrom(t *testing.T, br *bufio.Reader) string {
	t.Helper()
	hdr, err := br.ReadString('\n')
	if err != nil {
		t.Fatalf("read bulk header: %v", err)
	}
	if len(hdr) == 0 || hdr[0] != '$' {
		t.Fatalf("bulk header = %q", hdr)
	}
	blen, err := strconv.Atoi(strings.TrimSuffix(hdr[1:], "\r\n"))
	if err != nil {
		t.Fatalf("bulk length %q: %v", hdr, err)
	}
	buf := make([]byte, blen+2)
	if _, err := io.ReadFull(br, buf); err != nil {
		t.Fatalf("read bulk payload: %v", err)
	}
	return string(buf[:blen])
}

// readIntFrom reads one RESP integer reply off br.
func readIntFrom(t *testing.T, br *bufio.Reader) int64 {
	t.Helper()
	line, err := br.ReadString('\n')
	if err != nil {
		t.Fatalf("read int: %v", err)
	}
	if len(line) == 0 || line[0] != ':' {
		t.Fatalf("int reply = %q", line)
	}
	n, err := strconv.ParseInt(strings.TrimSuffix(line[1:], "\r\n"), 10, 64)
	if err != nil {
		t.Fatalf("parse int %q: %v", line, err)
	}
	return n
}
