package drivers

import (
	"bufio"
	"bytes"
	"io"
	"strconv"
	"testing"
)

// TestGiantValueRoundtrip drives the chunked band over a real socket: SET a
// value past a megabyte, read it back as one streamed bulk reply, and check
// the bytes exactly. The server side never materializes the value on the read
// path; the wire cannot tell, which is the point.
func TestGiantValueRoundtrip(t *testing.T) {
	srv, nc, br := startServer(t)
	_ = srv

	val := make([]byte, 3*(1<<19)+12345) // ~1.6MiB, a ragged chunk count
	for i := range val {
		val[i] = byte(i*11 + 3)
	}

	var req bytes.Buffer
	req.WriteString("*3\r\n$3\r\nSET\r\n$5\r\ngiant\r\n$")
	req.WriteString(strconv.Itoa(len(val)))
	req.WriteString("\r\n")
	req.Write(val)
	req.WriteString("\r\n")
	if _, err := nc.Write(req.Bytes()); err != nil {
		t.Fatal(err)
	}
	expect(t, br, "+OK\r\n")

	send(t, nc, "STRLEN", "giant")
	expect(t, br, ":"+strconv.Itoa(len(val))+"\r\n")

	send(t, nc, "GET", "giant")
	expectBulk(t, br, val)
}

// TestGiantValuePipelined pipelines point ops around a giant GET on one
// socket: replies must come back in exact request order, the streamed bulk in
// its slot.
func TestGiantValuePipelined(t *testing.T) {
	_, nc, br := startServer(t)

	val := bytes.Repeat([]byte("streamers"), 150000) // 1.35MB
	var req bytes.Buffer
	req.WriteString("*3\r\n$3\r\nSET\r\n$1\r\ng\r\n$")
	req.WriteString(strconv.Itoa(len(val)))
	req.WriteString("\r\n")
	req.Write(val)
	req.WriteString("\r\n")
	req.WriteString(cmd("SET", "a", "1"))
	req.WriteString(cmd("GET", "g"))
	req.WriteString(cmd("GET", "a"))
	req.WriteString(cmd("PING"))
	if _, err := nc.Write(req.Bytes()); err != nil {
		t.Fatal(err)
	}
	expect(t, br, "+OK\r\n")
	expect(t, br, "+OK\r\n")
	expectBulk(t, br, val)
	expect(t, br, "$1\r\n1\r\n")
	expect(t, br, "+PONG\r\n")

	// The value stays writable through the chunk-bounded paths.
	send(t, nc, "APPEND", "g", "-tail")
	expect(t, br, ":"+strconv.Itoa(len(val)+5)+"\r\n")
	send(t, nc, "GET", "g")
	expectBulk(t, br, append(append([]byte{}, val...), []byte("-tail")...))
}

// expectBulk reads one bulk reply and compares it byte for byte.
func expectBulk(t *testing.T, br *bufio.Reader, want []byte) {
	t.Helper()
	hdr, err := br.ReadString('\n')
	if err != nil {
		t.Fatalf("bulk header: %v", err)
	}
	if hdr != "$"+strconv.Itoa(len(want))+"\r\n" {
		t.Fatalf("bulk header = %q, want $%d", hdr, len(want))
	}
	got := make([]byte, len(want)+2)
	if _, err := io.ReadFull(br, got); err != nil {
		t.Fatalf("bulk body: %v", err)
	}
	if !bytes.HasSuffix(got, []byte("\r\n")) {
		t.Fatalf("bulk trailer = %q", got[len(got)-2:])
	}
	got = got[:len(got)-2]
	if !bytes.Equal(got, want) {
		for i := range got {
			if got[i] != want[i] {
				t.Fatalf("bulk body differs at byte %d: %#x want %#x", i, got[i], want[i])
			}
		}
		t.Fatalf("bulk body length %d, want %d", len(got), len(want))
	}
}
