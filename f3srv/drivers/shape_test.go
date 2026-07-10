package drivers

import (
	"bufio"
	"net"
	"strings"
	"testing"
)

// TestConnShapePair pins the pair shape stays selectable for the lab 15 A/B:
// an explicit ShapePair server answers a pipelined round in order and reports
// its shape through NetStats, whatever AKI_CONN_SHAPE says.
func TestConnShapePair(t *testing.T) {
	srv, err := Listen(Options{Addr: "127.0.0.1:0", Shards: 2, ArenaBytes: 4 << 20, SegBytes: 1 << 18, ConnShape: ShapePair})
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve()
	t.Cleanup(func() { srv.Close() })
	nc, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { nc.Close() })
	br := bufio.NewReader(nc)

	req := cmd("SET", "k", "v") + cmd("GET", "k") + "PING\r\n"
	if _, err := nc.Write([]byte(req)); err != nil {
		t.Fatal(err)
	}
	expect(t, br, "+OK\r\n$1\r\nv\r\n+PONG\r\n")

	if got := srv.NetStats().Shape; got != ShapePair {
		t.Fatalf("NetStats().Shape = %q, want %q", got, ShapePair)
	}
}

// TestConnShapeUnknown pins the knob's error path: an unrecognized shape must
// fail Listen instead of silently running some default.
func TestConnShapeUnknown(t *testing.T) {
	if _, err := Listen(Options{Addr: "127.0.0.1:0", ConnShape: "reactorish"}); err == nil {
		t.Fatal("Listen accepted an unknown conn shape")
	}
}

// TestPipelineWindowOverrun sends one write holding several times more
// commands than the reply reorder ring, so the parse pass runs the reader a
// full window ahead mid-batch. On the single shape that is the Do throttle's
// inline drain (the one spot the connection goroutine must drain its own
// replies before the boundary); on the pair shape it is the yield-to-writer
// path. Either way every reply must come back, in order.
func TestPipelineWindowOverrun(t *testing.T) {
	_, nc, br := startServer(t)

	const n = 3000 // replyRing is 1024
	if _, err := nc.Write([]byte(strings.Repeat("PING\r\n", n))); err != nil {
		t.Fatal(err)
	}
	expect(t, br, strings.Repeat("+PONG\r\n", n))

	// The same overrun with ordered, distinguishable replies: ECHO carries
	// its index back, so a reordering inside the inline drain cannot hide.
	var req strings.Builder
	var want strings.Builder
	for i := 0; i < n; i++ {
		p := string('a'+byte(i%26)) + "-" + string('a'+byte(i%7))
		req.WriteString(cmd("ECHO", p))
		want.WriteString("$3\r\n" + p + "\r\n")
	}
	if _, err := nc.Write([]byte(req.String())); err != nil {
		t.Fatal(err)
	}
	expect(t, br, want.String())
}

// TestPipelineWindowOverrunFan is the same window overrun through the fan-out
// enqueue path: DoFan shares the throttle with Do, so a burst of MGETs past
// the reorder ring must drain inline on the single shape too, each gathered
// reply holding its slot in the pipeline order.
func TestPipelineWindowOverrunFan(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "SET", "k1", "v1")
	expect(t, br, "+OK\r\n")
	send(t, nc, "SET", "k2", "v2")
	expect(t, br, "+OK\r\n")

	const n = 1500 // replyRing is 1024
	if _, err := nc.Write([]byte(strings.Repeat(cmd("MGET", "k1", "k2"), n))); err != nil {
		t.Fatal(err)
	}
	expect(t, br, strings.Repeat("*2\r\n$2\r\nv1\r\n$2\r\nv2\r\n", n))
}
