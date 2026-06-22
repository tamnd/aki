package command

import (
	"strings"
	"testing"
)

// TestProtoMaxBulkLenValueLimit checks that the value-size cap on APPEND and
// SETRANGE tracks CONFIG SET proto-max-bulk-len instead of a fixed constant.
func TestProtoMaxBulkLenValueLimit(t *testing.T) {
	r, c := startData(t)

	if got := sendLine(t, r, c, "CONFIG SET proto-max-bulk-len 10"); got != "+OK" {
		t.Fatalf("CONFIG SET proto-max-bulk-len = %q", got)
	}

	// A five byte value is well under the cap.
	if got := sendLine(t, r, c, "SET k 12345"); got != "+OK" {
		t.Fatalf("SET = %q", got)
	}

	// Growing it to exactly the cap is allowed.
	if got := sendLine(t, r, c, "APPEND k 67890"); got != ":10" {
		t.Fatalf("APPEND to cap = %q want :10", got)
	}

	// One more byte trips the limit.
	if got := sendLine(t, r, c, "APPEND k x"); !strings.HasPrefix(got, "-ERR") || !strings.Contains(got, "proto-max-bulk-len") {
		t.Fatalf("APPEND past cap = %q want proto-max-bulk-len error", got)
	}

	// SETRANGE past the cap is rejected the same way.
	if got := sendLine(t, r, c, "SETRANGE k 20 z"); !strings.HasPrefix(got, "-ERR") || !strings.Contains(got, "proto-max-bulk-len") {
		t.Fatalf("SETRANGE past cap = %q want proto-max-bulk-len error", got)
	}

	// Raising the cap lets the same write through.
	if got := sendLine(t, r, c, "CONFIG SET proto-max-bulk-len 1000"); got != "+OK" {
		t.Fatalf("CONFIG SET raise = %q", got)
	}
	if got := sendLine(t, r, c, "APPEND k x"); got != ":11" {
		t.Fatalf("APPEND after raise = %q want :11", got)
	}
}

// TestProtoMaxBulkLenParserLimit checks that CONFIG SET proto-max-bulk-len pushes
// the new cap to the request parser, so a bulk argument larger than the cap is
// rejected as a protocol error before the command runs.
func TestProtoMaxBulkLenParserLimit(t *testing.T) {
	r, c := startData(t)

	if got := sendLine(t, r, c, "CONFIG SET proto-max-bulk-len 10"); got != "+OK" {
		t.Fatalf("CONFIG SET proto-max-bulk-len = %q", got)
	}

	// A SET whose value bulk declares 20 bytes, over the cap of 10.
	big := strings.Repeat("a", 20)
	req := "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$20\r\n" + big + "\r\n"
	if _, err := c.Write([]byte(req)); err != nil {
		t.Fatalf("write oversized request: %v", err)
	}

	line, err := r.ReadString('\n')
	if err != nil {
		t.Fatalf("read reply: %v", err)
	}
	line = strings.TrimRight(line, "\r\n")
	if !strings.HasPrefix(line, "-ERR Protocol error") || !strings.Contains(line, "invalid bulk length") {
		t.Fatalf("oversized bulk reply = %q want invalid bulk length protocol error", line)
	}
}
