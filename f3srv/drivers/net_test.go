package drivers

import (
	"fmt"
	"io"
	"strconv"
	"strings"
	"testing"
	"time"
)

// netDelta is the counter movement between two snapshots.
func netDelta(a, b NetStats) NetStats {
	return NetStats{
		ReadSyscalls:  b.ReadSyscalls - a.ReadSyscalls,
		WriteSyscalls: b.WriteSyscalls - a.WriteSyscalls,
		Batches:       b.Batches - a.Batches,
		Commands:      b.Commands - a.Commands,
		WorkerWakes:   b.WorkerWakes - a.WorkerWakes,
		ConnWakes:     b.ConnWakes - a.ConnWakes,
		WorkerParks:   b.WorkerParks - a.WorkerParks,
		ConnParks:     b.ConnParks - a.ConnParks,
	}
}

// TestNetCountersMoveUnderPipeline is the doc 08 section 9.5 anti-rot check:
// a scripted pipeline must move every akinet counter. Commands is exact (the
// server dispatched precisely what the script sent); batches is bounded, not
// exact, because TCP may split one client write across reads, but every round
// is at least one boundary and a boundary needs at least one command; the
// syscall and wake counters are monotonic and nonzero because the traffic
// cannot have flowed without them.
func TestNetCountersMoveUnderPipeline(t *testing.T) {
	srv, nc, br := startServer(t)

	base := srv.NetStats()
	if base.Driver != wantNetDriver() {
		t.Fatalf("net driver = %q, want %q", base.Driver, wantNetDriver())
	}

	// Round one: 16 pipelined SETs in one write.
	var req strings.Builder
	for i := 0; i < 16; i++ {
		req.WriteString(cmd("SET", fmt.Sprintf("k%02d", i), "v"))
	}
	if _, err := nc.Write([]byte(req.String())); err != nil {
		t.Fatal(err)
	}
	expect(t, br, strings.Repeat("+OK\r\n", 16))

	// Round two: 16 pipelined GETs in one write.
	req.Reset()
	for i := 0; i < 16; i++ {
		req.WriteString(cmd("GET", fmt.Sprintf("k%02d", i)))
	}
	if _, err := nc.Write([]byte(req.String())); err != nil {
		t.Fatal(err)
	}
	expect(t, br, strings.Repeat("$1\r\nv\r\n", 16))

	d := netDelta(base, srv.NetStats())
	if d.Commands != 32 {
		t.Fatalf("net_commands moved %d, want exactly 32", d.Commands)
	}
	if d.Batches < 2 || d.Batches > 32 {
		t.Fatalf("net_batches moved %d, want 2..32 for two pipelined rounds", d.Batches)
	}
	if d.ReadSyscalls < 2 {
		t.Fatalf("net_read_syscalls moved %d, want >= 2", d.ReadSyscalls)
	}
	if d.WriteSyscalls < 2 {
		t.Fatalf("net_write_syscalls moved %d, want >= 2 (one flush per round)", d.WriteSyscalls)
	}
	if d.WorkerWakes == 0 {
		t.Fatal("net_worker_wakes did not move; the reader never woke a parked worker")
	}

	// Wakes and parks on the worker side are monotonic nonzero, not exact:
	// the worker spin window is zero so an idle worker parks at once, and the
	// connection writer parks after its spin window, but how many turns the
	// traffic allowed is timing, and the worker-side counter adds land just
	// after the wake tokens themselves. Poll past the settling.
	deadline := time.Now().Add(5 * time.Second)
	for {
		d = netDelta(base, srv.NetStats())
		if d.ConnWakes > 0 && d.WorkerParks > 0 && d.ConnParks > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("wake/park counters never moved: conn wakes %d, worker parks %d, conn parks %d",
				d.ConnWakes, d.WorkerParks, d.ConnParks)
		}
		time.Sleep(time.Millisecond)
	}
}

// TestNetCountersFoldOnClose checks a closed connection's traffic stays in
// the aggregate: counters are per-connection while it lives and fold into the
// totals when it goes, so NetStats never loses history to churn.
func TestNetCountersFoldOnClose(t *testing.T) {
	srv, nc, br := startServer(t)

	base := srv.NetStats()
	send(t, nc, "SET", "k", "v")
	expect(t, br, "+OK\r\n")
	send(t, nc, "GET", "k")
	expect(t, br, "$1\r\nv\r\n")
	_ = nc.Close()

	deadline := time.Now().Add(5 * time.Second)
	for {
		d := netDelta(base, srv.NetStats())
		if d.Commands == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("net_commands = %d after close, want the folded 2", d.Commands)
		}
		time.Sleep(time.Millisecond)
	}
}

// TestInfoNetSection checks INFO carries the "# Net" section: the driver name
// as a string and every counter as a numeric line, so a harness can verify
// the running config off the wire without trusting launch flags.
func TestInfoNetSection(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "SET", "k", "v")
	expect(t, br, "+OK\r\n")

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
	text := string(body[:n])

	if !strings.Contains(text, "\r\n# Net\r\n") {
		t.Fatalf("INFO missing # Net section:\n%s", text)
	}
	if !strings.Contains(text, "net_driver:"+wantNetDriver()+"\r\n") {
		t.Fatalf("INFO missing net_driver line:\n%s", text)
	}
	shape := testConnShape()
	if shape == "" {
		shape = ShapeSingle
	}
	if !strings.Contains(text, "net_conn_shape:"+shape+"\r\n") {
		t.Fatalf("INFO missing net_conn_shape:%s line:\n%s", shape, text)
	}
	stats := make(map[string]uint64)
	for _, line := range strings.Split(text, "\r\n") {
		name, val, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		if v, err := strconv.ParseUint(val, 10, 64); err == nil {
			stats[name] = v
		}
	}
	for _, k := range []string{"net_read_syscalls", "net_write_syscalls", "net_batches", "net_commands", "net_worker_wakes"} {
		if stats[k] == 0 {
			t.Fatalf("%s = 0 in INFO after traffic (%v)", k, stats)
		}
	}
	for _, k := range []string{"net_conn_wakes", "net_worker_parks", "net_conn_parks", "net_loop_wakes"} {
		if _, ok := stats[k]; !ok {
			t.Fatalf("%s missing from INFO (%v)", k, stats)
		}
	}
}
