package drivers

import (
	"bufio"
	"bytes"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"
)

// startLTMServer is startServer with the larger-than-memory knobs on: every
// shard gets a value log under a temp dir and a resident cap small enough
// that the test working set must outgrow it.
func startLTMServer(t *testing.T, residentCap uint64) (net.Conn, *bufio.Reader) {
	t.Helper()
	srv, err := Listen(Options{
		Addr:             "127.0.0.1:0",
		Shards:           2,
		ArenaBytes:       16 << 20,
		SegBytes:         1 << 18,
		VlogDir:          t.TempDir(),
		ResidentCapBytes: residentCap,
		ConnShape:        testConnShape(),
		NetDriver:        testNetDriver(),
	})
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve()
	nc, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		srv.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		nc.Close()
		srv.Close()
	})
	return nc, bufio.NewReader(nc)
}

// readInfo sends INFO and parses the summed counter bulk into a map.
func readInfo(t *testing.T, nc net.Conn, br *bufio.Reader) map[string]uint64 {
	t.Helper()
	send(t, nc, "INFO")
	hdr, err := br.ReadString('\n')
	if err != nil || len(hdr) < 4 || hdr[0] != '$' {
		t.Fatalf("info header %q: %v", hdr, err)
	}
	n, err := strconv.Atoi(strings.TrimSuffix(hdr[1:], "\r\n"))
	if err != nil {
		t.Fatalf("info header %q: %v", hdr, err)
	}
	body := make([]byte, n+2)
	if _, err := io.ReadFull(br, body); err != nil {
		t.Fatal(err)
	}
	stats := make(map[string]uint64)
	for _, line := range strings.Split(string(body[:n]), "\r\n") {
		name, val, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		v, err := strconv.ParseUint(val, 10, 64)
		if err != nil {
			// String-valued lines (net_driver) are not counters.
			continue
		}
		stats[name] = v
	}
	return stats
}

// TestInfoMemoryOnly checks the INFO fan on a log-less server: band counts
// move with writes and the log counters stay zero.
func TestInfoMemoryOnly(t *testing.T) {
	_, nc, br := startServer(t)
	send(t, nc, "SET", "n", "12345")
	expect(t, br, "+OK\r\n")
	send(t, nc, "SET", "s", "hello")
	expect(t, br, "+OK\r\n")
	send(t, nc, "SET", "big", strings.Repeat("x", 2048))
	expect(t, br, "+OK\r\n")

	stats := readInfo(t, nc, br)
	if stats["keys"] != 3 || stats["band_int"] != 1 || stats["band_embedded"] != 1 || stats["band_separated"] != 1 {
		t.Fatalf("stats = %v", stats)
	}
	if stats["vlog_bytes"] != 0 || stats["vlog_runs"] != 0 {
		t.Fatalf("log counters on a memory-only server: %v", stats)
	}
	if stats["arena_total_bytes"] == 0 || stats["arena_used_bytes"] == 0 {
		t.Fatalf("arena counters missing: %v", stats)
	}
}

// TestInfoBackpressureCounters checks the block-not-drop counters ride the INFO
// fan (backpressure.go, M7 slice 5b). A server that never crossed its resident
// cap parked no write, so both counters must render and read zero, the L9
// no-pressure account: the fields are present for a client to poll, not omitted,
// and no write parked on a store with room to spare.
func TestInfoBackpressureCounters(t *testing.T) {
	_, nc, br := startServer(t)
	send(t, nc, "SET", "k", "v")
	expect(t, br, "+OK\r\n")

	stats := readInfo(t, nc, br)
	if _, ok := stats["backpressure_waits"]; !ok {
		t.Fatalf("backpressure_waits missing from INFO: %v", stats)
	}
	if _, ok := stats["backpressure_stalls"]; !ok {
		t.Fatalf("backpressure_stalls missing from INFO: %v", stats)
	}
	if stats["backpressure_waits"] != 0 || stats["backpressure_stalls"] != 0 {
		t.Fatalf("counters nonzero on a store that never filled: waits=%d stalls=%d",
			stats["backpressure_waits"], stats["backpressure_stalls"])
	}
}

// TestInfoRAMExceeded is the string LTM evidence run of doc 09 section 8: with
// a resident cap far under the working set, separated-band values must land
// in the per-shard value logs and INFO must show it, while every value still
// reads back exactly.
func TestInfoRAMExceeded(t *testing.T) {
	nc, br := startLTMServer(t, 128<<10)

	val := func(i int) []byte {
		v := bytes.Repeat([]byte{byte(i)}, 4096)
		copy(v, "value-"+strconv.Itoa(i))
		return v
	}
	const keys = 100
	for i := 0; i < keys; i++ {
		send(t, nc, "SET", "ltm:"+strconv.Itoa(i), string(val(i)))
		expect(t, br, "+OK\r\n")
	}

	stats := readInfo(t, nc, br)
	if stats["keys"] != keys {
		t.Fatalf("keys = %d, want %d", stats["keys"], keys)
	}
	if stats["band_separated"] != keys {
		t.Fatalf("band_separated = %d, want %d", stats["band_separated"], keys)
	}
	if stats["vlog_bytes"] == 0 || stats["vlog_runs"] == 0 {
		t.Fatalf("no spill: vlog_bytes=%d vlog_runs=%d", stats["vlog_bytes"], stats["vlog_runs"])
	}
	// The working set is ~200KiB of values per shard against a 128KiB cap;
	// records and early writes stay resident, the log takes the rest.
	if stats["arena_used_bytes"] == 0 {
		t.Fatal("arena_used_bytes = 0")
	}

	// Served, not just stored: every value reads back byte for byte.
	for i := 0; i < keys; i++ {
		send(t, nc, "GET", "ltm:"+strconv.Itoa(i))
		expectBulk(t, br, val(i))
	}
}

// TestCompactionTrigger drives the owner-scheduled compaction: churn logged
// values well past the trigger volume and check the log stayed bounded. The
// trigger runs at every idle boundary, so with a synchronous client it fires
// as soon as a shard's dead bytes cross the floor; the evidence that it ran
// is that the log ends far smaller than the churn and the residual dead bytes
// sit under the per-shard floor.
func TestCompactionTrigger(t *testing.T) {
	nc, br := startLTMServer(t, 64<<10)

	// Each overwrite of a logged value strands its old run. 60 rounds over 4
	// keys churns ~7.9MiB through the logs against ~130KB live, several times
	// the compactMinDead floor on every shard.
	const rounds, keys = 60, 4
	val := bytes.Repeat([]byte("dead-weight"), 3000) // 33000 bytes
	for round := 0; round < rounds; round++ {
		for k := 0; k < keys; k++ {
			send(t, nc, "SET", "churn:"+strconv.Itoa(k), string(val))
			expect(t, br, "+OK\r\n")
		}
	}
	churned := uint64(rounds * keys * len(val))

	stats := readInfo(t, nc, br)
	if stats["vlog_bytes"] == 0 {
		t.Fatal("no log traffic; resident cap did not bite")
	}
	if stats["vlog_bytes"] >= churned/2 {
		t.Fatalf("log never compacted: vlog_bytes=%d of %d churned",
			stats["vlog_bytes"], churned)
	}

	// Residual dead bytes must sit under the trigger: one floor per shard
	// plus at most a run in flight, or the trigger is not firing.
	const shards = 2
	limit := uint64(shards * (1<<20 + len(val)))
	deadline := time.Now().Add(5 * time.Second)
	for stats["vlog_dead_bytes"] >= limit {
		if time.Now().After(deadline) {
			t.Fatalf("dead bytes above trigger: vlog_dead_bytes=%d limit=%d",
				stats["vlog_dead_bytes"], limit)
		}
		time.Sleep(10 * time.Millisecond)
		stats = readInfo(t, nc, br)
	}

	// The rewrites kept the live set intact.
	for k := 0; k < keys; k++ {
		send(t, nc, "GET", "churn:"+strconv.Itoa(k))
		expectBulk(t, br, val)
	}
}
