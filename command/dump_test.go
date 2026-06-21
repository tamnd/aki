package command

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"testing"
)

// sendCmd writes a command as a RESP array of bulk strings, which is the only way
// to carry the binary DUMP payload back into RESTORE without the inline parser
// choking on its bytes.
func sendCmd(t *testing.T, c net.Conn, args ...[]byte) {
	t.Helper()
	var b []byte
	b = append(b, []byte(fmt.Sprintf("*%d\r\n", len(args)))...)
	for _, a := range args {
		b = append(b, []byte(fmt.Sprintf("$%d\r\n", len(a)))...)
		b = append(b, a...)
		b = append(b, '\r', '\n')
	}
	if _, err := c.Write(b); err != nil {
		t.Fatalf("write command: %v", err)
	}
}

// readBulkBytes reads one RESP bulk-string reply by its declared length, so a
// payload with embedded CR or LF bytes comes back whole. A null bulk returns nil.
func readBulkBytes(t *testing.T, r *bufio.Reader) []byte {
	t.Helper()
	line, err := r.ReadString('\n')
	if err != nil {
		t.Fatalf("read bulk header: %v", err)
	}
	line = line[:len(line)-2]
	if line == "$-1" || line == "_" {
		return nil
	}
	if len(line) == 0 || line[0] != '$' {
		t.Fatalf("expected bulk header, got %q", line)
	}
	n, err := strconv.Atoi(line[1:])
	if err != nil {
		t.Fatalf("bad bulk length %q: %v", line, err)
	}
	buf := make([]byte, n+2)
	if _, err := io.ReadFull(r, buf); err != nil {
		t.Fatalf("read bulk body: %v", err)
	}
	return buf[:n]
}

// readSimple reads one line reply such as +OK or an error.
func readSimple(t *testing.T, r *bufio.Reader) string {
	t.Helper()
	line, err := r.ReadString('\n')
	if err != nil {
		t.Fatalf("read line: %v", err)
	}
	return line[:len(line)-2]
}

// dumpRestoreRoundTrip dumps a key, deletes it, restores the payload, and returns
// the restored payload so the test can compare it byte for byte with the first.
func dumpRestoreRoundTrip(t *testing.T, r *bufio.Reader, c net.Conn, key string) []byte {
	t.Helper()
	sendCmd(t, c, []byte("DUMP"), []byte(key))
	payload := readBulkBytes(t, r)
	if payload == nil {
		t.Fatalf("DUMP %s returned nil", key)
	}
	_ = sendLine(t, r, c, "DEL "+key)
	sendCmd(t, c, []byte("RESTORE"), []byte(key), []byte("0"), payload)
	if got := readSimple(t, r); got != "+OK" {
		t.Fatalf("RESTORE %s = %q", key, got)
	}
	sendCmd(t, c, []byte("DUMP"), []byte(key))
	return readBulkBytes(t, r)
}

// TestDumpMissingKey checks DUMP of an absent key is a nil bulk.
func TestDumpMissingKey(t *testing.T) {
	r, c := startData(t)
	sendCmd(t, c, []byte("DUMP"), []byte("nope"))
	if got := readBulkBytes(t, r); got != nil {
		t.Fatalf("DUMP missing = %q want nil", got)
	}
}

// TestDumpRestoreString round-trips a string value and checks the second dump
// matches the first byte for byte.
func TestDumpRestoreString(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET k hello")
	sendCmd(t, c, []byte("DUMP"), []byte("k"))
	first := readBulkBytes(t, r)
	second := dumpRestoreRoundTrip(t, r, c, "k")
	if string(first) != string(second) {
		t.Fatalf("string dump differs after restore: % x vs % x", first, second)
	}
	if got := bulk(t, r, c, "GET k"); got != "hello" {
		t.Fatalf("GET after restore = %q", got)
	}
}

// TestDumpRestoreList round-trips a list and checks the elements survive in order.
func TestDumpRestoreList(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "RPUSH l a b c 1 2 3")
	_ = dumpRestoreRoundTrip(t, r, c, "l")
	if got := readArray(t, r, c, "LRANGE l 0 -1"); fmt.Sprint(got) != "[a b c 1 2 3]" {
		t.Fatalf("list after restore = %v", got)
	}
}

// TestDumpRestoreHashAndZSet round-trips a hash and a sorted set.
func TestDumpRestoreHashAndZSet(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "HSET h f1 v1 count 42")
	_ = dumpRestoreRoundTrip(t, r, c, "h")
	if got := bulk(t, r, c, "HGET h count"); got != "42" {
		t.Fatalf("HGET after restore = %q", got)
	}

	_ = sendLine(t, r, c, "ZADD z 1 a 2.5 b")
	_ = dumpRestoreRoundTrip(t, r, c, "z")
	if got := bulk(t, r, c, "ZSCORE z b"); got != "2.5" {
		t.Fatalf("ZSCORE after restore = %q", got)
	}
}

// TestRestoreBusyKey checks RESTORE refuses an existing key without REPLACE and
// overwrites it with REPLACE.
func TestRestoreBusyKey(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET k hello")
	sendCmd(t, c, []byte("DUMP"), []byte("k"))
	payload := readBulkBytes(t, r)

	sendCmd(t, c, []byte("RESTORE"), []byte("k"), []byte("0"), payload)
	if got := readSimple(t, r); got != "-BUSYKEY Target key name already exists" {
		t.Fatalf("RESTORE busy = %q", got)
	}
	sendCmd(t, c, []byte("RESTORE"), []byte("k"), []byte("0"), payload, []byte("REPLACE"))
	if got := readSimple(t, r); got != "+OK" {
		t.Fatalf("RESTORE REPLACE = %q", got)
	}
}

// TestRestoreBadChecksum checks a corrupted payload is rejected.
func TestRestoreBadChecksum(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET k hello")
	sendCmd(t, c, []byte("DUMP"), []byte("k"))
	payload := readBulkBytes(t, r)
	payload[1] ^= 0xFF // corrupt the value byte, leaving the stored CRC stale

	sendCmd(t, c, []byte("RESTORE"), []byte("k2"), []byte("0"), payload)
	if got := readSimple(t, r); got != "-ERR DUMP payload version or checksum are wrong" {
		t.Fatalf("RESTORE corrupt = %q", got)
	}
}

// TestRestoreTTLAndNegative checks a relative TTL sets an expiry and a negative
// TTL is rejected.
func TestRestoreTTLAndNegative(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET k hello")
	sendCmd(t, c, []byte("DUMP"), []byte("k"))
	payload := readBulkBytes(t, r)
	_ = sendLine(t, r, c, "DEL k")

	sendCmd(t, c, []byte("RESTORE"), []byte("k"), []byte("100000"), payload)
	if got := readSimple(t, r); got != "+OK" {
		t.Fatalf("RESTORE with ttl = %q", got)
	}
	if got := sendLine(t, r, c, "PTTL k"); got == ":-1" || got == ":-2" {
		t.Fatalf("PTTL after restore = %q want a positive value", got)
	}

	sendCmd(t, c, []byte("RESTORE"), []byte("k2"), []byte("-5"), payload)
	if got := readSimple(t, r); got != "-ERR Invalid argument: ttl must be a positive integer or zero" {
		t.Fatalf("RESTORE negative ttl = %q", got)
	}
}
