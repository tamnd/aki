package shard

import (
	"bytes"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tamnd/aki/engine/obs1/store"
)

// opStream is a test op whose handler answers with a streamed reply from a
// source the test controls.
const opStream byte = opIncr + 1

// patSource serves total bytes of a position-dependent pattern in full
// chunks, counting Next calls so the footprint test can bound how far the
// producer runs ahead of the consumer.
type patSource struct {
	total int64
	pos   int64
	nexts atomic.Int64
	fail  int64 // fail the source once pos reaches this, 0 for never
}

func (p *patSource) Next(dst []byte) (int, error) {
	p.nexts.Add(1)
	if p.fail > 0 && p.pos >= p.fail {
		return 0, errors.New("source failure")
	}
	n := p.total - p.pos
	if n > int64(len(dst)) {
		n = int64(len(dst))
	}
	for i := int64(0); i < n; i++ {
		dst[i] = byte((p.pos + i) * 31)
	}
	p.pos += n
	return int(n), nil
}

func streamRuntime(src StreamSource, total int64) *Runtime {
	h := testHandlers()
	for len(h) <= int(opStream) {
		h = append(h, nil)
	}
	h[opStream] = func(cx *Ctx, args [][]byte, r Reply) {
		r.Stream(total, src)
	}
	rt := New(1, testArena, testSeg)
	rt.Use(h)
	return rt
}

// TestStreamWindowBounded is the streaming footprint proof: a deliberately
// slow consumer drains a multi-megabyte streamed reply and asserts at every
// chunk that the producer never ran more than the window (plus the one chunk
// it may have in flight) ahead of the consumer, so the reply's peak memory is
// the ring, not the value. It also pins the wire shape and that a pipelined
// point op behind the stream stays behind it.
func TestStreamWindowBounded(t *testing.T) {
	const chunks = 48
	total := int64(chunks * store.ChunkSize)
	src := &patSource{total: total}
	rt := streamRuntime(src, total)
	rt.Start()
	defer rt.Stop()

	c := rt.NewConn()
	if err := c.Do(opStream, false, nil); err != nil {
		t.Fatal(err)
	}
	if err := c.Do(opPing, false, nil); err != nil {
		t.Fatal(err)
	}
	c.Flush()

	var out []byte
	consumed := int64(0)
	emit := func(rep []byte) {
		if len(rep) == store.ChunkSize {
			consumed++
			if ahead := src.nexts.Load() - consumed; ahead > streamWindow+1 {
				t.Errorf("producer %d chunks ahead of the consumer, window is %d", ahead, streamWindow)
			}
			time.Sleep(200 * time.Microsecond)
		}
		out = append(out, rep...)
	}
	deadline := time.Now().Add(20 * time.Second)
	n := 0
	for n < 2 {
		n += c.DrainReplies(emit)
		if time.Now().After(deadline) {
			t.Fatalf("timed out with %d of 2 replies", n)
		}
	}
	if c.Failed() {
		t.Fatal("connection failed on a healthy stream")
	}

	want := []byte("$" + itoa(total) + "\r\n")
	if !bytes.HasPrefix(out, want) {
		t.Fatalf("reply starts %q, want %q", out[:min(len(out), 16)], want)
	}
	body := out[len(want):]
	if !bytes.HasSuffix(body, []byte("\r\n+PONG\r\n")) {
		t.Fatalf("reply tail = %q, want the trailer then the pipelined PONG", body[max(0, len(body)-16):])
	}
	body = body[:len(body)-len("\r\n+PONG\r\n")]
	if int64(len(body)) != total {
		t.Fatalf("streamed %d bytes, want %d", len(body), total)
	}
	for i := range body {
		if body[i] != byte(int64(i)*31) {
			t.Fatalf("byte %d = %#x, want %#x", i, body[i], byte(int64(i)*31))
		}
	}
}

// rawSource serves total bytes of self-framed RESP (a position pattern stands
// in for the real array-and-bulks). It counts Next calls so the window bound
// can be checked, the same way patSource does for the wrapped path.
type rawSource struct {
	total int64
	pos   int64
	nexts atomic.Int64
}

func (p *rawSource) Next(dst []byte) (int, error) {
	p.nexts.Add(1)
	n := p.total - p.pos
	if n > int64(len(dst)) {
		n = int64(len(dst))
	}
	for i := int64(0); i < n; i++ {
		dst[i] = byte((p.pos+i)*17 + 3)
	}
	p.pos += n
	return int(n), nil
}

// TestStreamRawWireShape drives a StreamRaw reply (SMEMBERS's path) and asserts
// two things the raw mode owes: the writer emits the source bytes verbatim with
// no $total bulk header and no trailing crlf, and the producer still honors the
// ring window so a huge enumeration holds the window, not the whole reply. A
// pipelined PING behind it must still land after the raw bytes, proving the
// reorder cursor committed to total even without a header on the wire.
func TestStreamRawWireShape(t *testing.T) {
	const chunks = 40
	total := int64(chunks * store.ChunkSize)
	src := &rawSource{total: total}

	h := testHandlers()
	for len(h) <= int(opStream) {
		h = append(h, nil)
	}
	h[opStream] = func(cx *Ctx, args [][]byte, r Reply) {
		r.StreamRaw(total, src)
	}
	rt := New(1, testArena, testSeg)
	rt.Use(h)
	rt.Start()
	defer rt.Stop()

	c := rt.NewConn()
	if err := c.Do(opStream, false, nil); err != nil {
		t.Fatal(err)
	}
	if err := c.Do(opPing, false, nil); err != nil {
		t.Fatal(err)
	}
	c.Flush()

	var out []byte
	consumed := int64(0)
	emit := func(rep []byte) {
		if len(rep) == store.ChunkSize {
			consumed++
			if ahead := src.nexts.Load() - consumed; ahead > streamWindow+1 {
				t.Errorf("producer %d chunks ahead, window is %d", ahead, streamWindow)
			}
			time.Sleep(200 * time.Microsecond)
		}
		out = append(out, rep...)
	}
	deadline := time.Now().Add(20 * time.Second)
	n := 0
	for n < 2 {
		n += c.DrainReplies(emit)
		if time.Now().After(deadline) {
			t.Fatalf("timed out with %d of 2 replies", n)
		}
	}
	if c.Failed() {
		t.Fatal("connection failed on a healthy raw stream")
	}
	if out[0] != byte(3) {
		t.Fatalf("raw reply starts %#x, want the source's first byte with no bulk header", out[0])
	}
	if !bytes.HasSuffix(out, []byte("+PONG\r\n")) {
		t.Fatalf("reply tail = %q, want the pipelined PONG right after the raw bytes", out[max(0, len(out)-16):])
	}
	body := out[:len(out)-len("+PONG\r\n")]
	if int64(len(body)) != total {
		t.Fatalf("raw body is %d bytes, want %d with no trailer", len(body), total)
	}
	for i := range body {
		if body[i] != byte(int64(i)*17+3) {
			t.Fatalf("raw byte %d = %#x, want %#x", i, body[i], byte(int64(i)*17+3))
		}
	}
}

// TestStreamRawStepConsumer drives the raw stream through the reactor's step
// consumer (SetStreamStep), the path production takes. It proves the step
// completion emits no trailer for raw and the reorder cursor still advances to
// the pipelined PONG once the raw bytes are out.
func TestStreamRawStepConsumer(t *testing.T) {
	const chunks = 16
	total := int64(chunks * store.ChunkSize)
	src := &rawSource{total: total}

	h := testHandlers()
	for len(h) <= int(opStream) {
		h = append(h, nil)
	}
	h[opStream] = func(cx *Ctx, args [][]byte, r Reply) {
		r.StreamRaw(total, src)
	}
	rt := New(1, testArena, testSeg)
	rt.Use(h)
	rt.Start()
	defer rt.Stop()

	nl := newNotifyLoop()
	c := rt.NewConn()
	c.SetStreamStep()
	c.SetWriterNotify(nl.notify)
	defer c.Close()

	if err := c.Do(opStream, false, nil); err != nil {
		t.Fatal(err)
	}
	if err := c.Do(opPing, false, nil); err != nil {
		t.Fatal(err)
	}
	c.Flush()

	var out []byte
	emit := func(rep []byte) { out = append(out, rep...) }
	deadline := time.Now().Add(20 * time.Second)
	for c.Owes() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out with %d bytes of %d", len(out), total)
		}
		c.DrainReplies(emit)
		for c.StreamReady() && !c.Failed() {
			c.StreamStep(emit, store.ChunkSize)
		}
		if c.Failed() {
			t.Fatal("connection failed on a healthy raw stream")
		}
		if !c.Owes() {
			break
		}
		if c.ParkWriter() {
			if !nl.wait(10 * time.Second) {
				t.Fatal("no wake with the raw stream mid-flight")
			}
		}
	}
	if !bytes.HasSuffix(out, []byte("+PONG\r\n")) {
		t.Fatalf("reply tail = %q, want the pipelined PONG right after the raw bytes", out[max(0, len(out)-16):])
	}
	body := out[:len(out)-len("+PONG\r\n")]
	if int64(len(body)) != total {
		t.Fatalf("raw body is %d bytes, want %d with no trailer", len(body), total)
	}
	if body[0] != byte(3) {
		t.Fatalf("raw body starts %#x, want no bulk header", body[0])
	}
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

// TestStreamFailurePoisonsConn fails the source mid-stream: the bulk header
// is already on the wire, so the connection must report Failed and the
// transport drops it.
func TestStreamFailurePoisonsConn(t *testing.T) {
	total := int64(8 * store.ChunkSize)
	src := &patSource{total: total, fail: 2 * store.ChunkSize}
	rt := streamRuntime(src, total)
	rt.Start()
	defer rt.Stop()

	c := rt.NewConn()
	if err := c.Do(opStream, false, nil); err != nil {
		t.Fatal(err)
	}
	c.Flush()

	deadline := time.Now().Add(10 * time.Second)
	for !c.Failed() {
		c.DrainReplies(func([]byte) {})
		if time.Now().After(deadline) {
			t.Fatal("connection never reported the stream failure")
		}
	}
}

// TestStreamThroughStore drives the real chunked band end to end at the shard
// layer: SET a giant value through the hop, GET it back as a streamed reply,
// and check the exact bytes.
func TestStreamThroughStore(t *testing.T) {
	h := testHandlers()
	for len(h) <= int(opStream) {
		h = append(h, nil)
	}
	h[opStream] = func(cx *Ctx, args [][]byte, r Reply) {
		v, cs, ok := cx.St.GetStream(args[0], cx.NowMs, cx.Val)
		if !ok {
			cx.Val = v
			r.Null()
			return
		}
		if cs != nil {
			r.Stream(cs.Total(), cs)
			return
		}
		cx.Val = v
		r.Bulk(v)
	}
	rt := New(1, 16<<20, testSeg)
	rt.Use(h)
	rt.Start()
	defer rt.Stop()

	val := make([]byte, 3*store.ChunkSize+777)
	for i := range val {
		val[i] = byte(i*13 + 7)
	}
	c := rt.NewConn()
	if err := c.Do(opSet, true, [][]byte{[]byte("giant"), val}); err != nil {
		t.Fatal(err)
	}
	if err := c.Do(opStream, true, [][]byte{[]byte("giant")}); err != nil {
		t.Fatal(err)
	}
	c.Flush()

	var out []byte
	deadline := time.Now().Add(10 * time.Second)
	n := 0
	for n < 2 {
		n += c.DrainReplies(func(rep []byte) { out = append(out, rep...) })
		if time.Now().After(deadline) {
			t.Fatalf("timed out with %d of 2 replies", n)
		}
	}
	want := append([]byte("+OK\r\n$"+itoa(int64(len(val)))+"\r\n"), val...)
	want = append(want, '\r', '\n')
	if !bytes.Equal(out, want) {
		t.Fatalf("wire bytes differ: got %d bytes, want %d", len(out), len(want))
	}
}

// TestStreamStepConsumer drives a streamed reply the way the reactor loop
// does (SetStreamStep): the consumer never blocks on the chunk ring. It
// drains what is there, steps the stream cursor while chunks are ready, parks
// the writer, and blocks on the notify seam like the loop blocks in
// epoll_wait. The producer's pump owes a wake for every chunk publish, so a
// lost wake on that edge shows up here as the watchdog deadline firing with
// the stream mid-flight.
func TestStreamStepConsumer(t *testing.T) {
	const chunks = 24
	total := int64(chunks * store.ChunkSize)
	src := &patSource{total: total}
	rt := streamRuntime(src, total)
	rt.Start()
	defer rt.Stop()

	nl := newNotifyLoop()
	c := rt.NewConn()
	c.SetStreamStep()
	c.SetWriterNotify(nl.notify)
	defer c.Close()

	if err := c.Do(opStream, false, nil); err != nil {
		t.Fatal(err)
	}
	if err := c.Do(opPing, false, nil); err != nil {
		t.Fatal(err)
	}
	c.Flush()

	var out []byte
	emit := func(rep []byte) { out = append(out, rep...) }
	deadline := time.Now().Add(20 * time.Second)
	for c.Owes() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out with %d bytes emitted of %d", len(out), total)
		}
		c.DrainReplies(emit)
		for c.StreamReady() && !c.Failed() {
			// The loop's quantum: bounded emit, never a spin on the ring.
			c.StreamStep(emit, store.ChunkSize)
		}
		if c.Failed() {
			t.Fatal("connection failed on a healthy stream")
		}
		if !c.Owes() {
			break
		}
		if c.ParkWriter() {
			if !nl.wait(10 * time.Second) {
				t.Fatal("no wake with the stream mid-flight; a parked consumer would hang")
			}
		}
	}

	want := []byte("$" + itoa(total) + "\r\n")
	if !bytes.HasPrefix(out, want) {
		t.Fatalf("reply starts %q, want %q", out[:min(len(out), 16)], want)
	}
	body := out[len(want):]
	if !bytes.HasSuffix(body, []byte("\r\n+PONG\r\n")) {
		t.Fatalf("reply tail = %q, want the trailer then the pipelined PONG", body[max(0, len(body)-16):])
	}
	body = body[:len(body)-len("\r\n+PONG\r\n")]
	if int64(len(body)) != total {
		t.Fatalf("streamed %d bytes, want %d", len(body), total)
	}
	for i := range body {
		if body[i] != byte(int64(i)*31) {
			t.Fatalf("byte %d = %#x, want %#x", i, body[i], byte(int64(i)*31))
		}
	}
}

// TestStreamStepFailureUnparks fails the source mid-stream against a step
// consumer parked on the notify seam: the pump's failure wake must bring the
// consumer back so StreamStep observes the failure and flips Failed, instead
// of the consumer waiting forever on chunks that will never come. This is the
// same unwind path a shard-shutdown abortStreams takes to a loop-owned
// connection.
func TestStreamStepFailureUnparks(t *testing.T) {
	total := int64(8 * store.ChunkSize)
	src := &patSource{total: total, fail: 2 * store.ChunkSize}
	rt := streamRuntime(src, total)
	rt.Start()
	defer rt.Stop()

	nl := newNotifyLoop()
	c := rt.NewConn()
	c.SetStreamStep()
	c.SetWriterNotify(nl.notify)
	defer c.Close()

	if err := c.Do(opStream, false, nil); err != nil {
		t.Fatal(err)
	}
	c.Flush()

	emit := func([]byte) {}
	deadline := time.Now().Add(20 * time.Second)
	for !c.Failed() {
		if time.Now().After(deadline) {
			t.Fatal("connection never reported the stream failure")
		}
		c.DrainReplies(emit)
		for c.StreamReady() && !c.Failed() {
			c.StreamStep(emit, store.ChunkSize)
		}
		if c.Failed() {
			break
		}
		if c.ParkWriter() {
			if !nl.wait(10 * time.Second) {
				t.Fatal("no wake for the stream failure; a parked consumer would hang")
			}
		}
	}
	if c.StreamAborted() {
		t.Fatal("StreamAborted still set after the failure was consumed")
	}
}
