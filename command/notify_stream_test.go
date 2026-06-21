package command

import (
	"slices"
	"testing"
)

func TestNotifyStreamAddTrim(t *testing.T) {
	r1, c1, r2, c2 := startDataTwo(t)
	if _, err := c2.Write([]byte("PSUBSCRIBE __keyevent@0__:x*\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = readResp(t, r2)
	if got := sendLine(t, r1, c1, "CONFIG SET notify-keyspace-events KEA"); got != "+OK" {
		t.Fatalf("CONFIG SET = %q", got)
	}

	// A plain XADD fires xadd.
	if _, err := c1.Write([]byte("XADD s * f v\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = readResp(t, r1)
	if msg := readResp(t, r2); !slices.Contains(msg, "__keyevent@0__:xadd") {
		t.Fatalf("xadd push = %v", msg)
	}

	// A few more entries so the next MAXLEN actually trims.
	for range 3 {
		if _, err := c1.Write([]byte("XADD s * f v\r\n")); err != nil {
			t.Fatal(err)
		}
		_ = readResp(t, r1)
		_ = readResp(t, r2) // xadd
	}

	// XADD with MAXLEN fires xadd then xtrim.
	if _, err := c1.Write([]byte("XADD s MAXLEN 2 * f v\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = readResp(t, r1)
	if msg := readResp(t, r2); !slices.Contains(msg, "__keyevent@0__:xadd") {
		t.Fatalf("xadd push = %v", msg)
	}
	if msg := readResp(t, r2); !slices.Contains(msg, "__keyevent@0__:xtrim") {
		t.Fatalf("xtrim push = %v", msg)
	}
}

func TestNotifyStreamDel(t *testing.T) {
	r1, c1, r2, c2 := startDataTwo(t)
	if _, err := c2.Write([]byte("PSUBSCRIBE __keyevent@0__:x*\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = readResp(t, r2)
	if got := sendLine(t, r1, c1, "CONFIG SET notify-keyspace-events KEA"); got != "+OK" {
		t.Fatalf("CONFIG SET = %q", got)
	}
	if _, err := c1.Write([]byte("XADD s 1-1 f v\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = readResp(t, r1)
	_ = readResp(t, r2) // xadd

	if got := sendLine(t, r1, c1, "XDEL s 1-1"); got != ":1" {
		t.Fatalf("XDEL = %q", got)
	}
	if msg := readResp(t, r2); !slices.Contains(msg, "__keyevent@0__:xdel") {
		t.Fatalf("xdel push = %v", msg)
	}
}

func TestNotifyStreamTrimSetID(t *testing.T) {
	r1, c1, r2, c2 := startDataTwo(t)
	if _, err := c2.Write([]byte("PSUBSCRIBE __keyevent@0__:x*\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = readResp(t, r2)
	if got := sendLine(t, r1, c1, "CONFIG SET notify-keyspace-events KEA"); got != "+OK" {
		t.Fatalf("CONFIG SET = %q", got)
	}
	if _, err := c1.Write([]byte("XADD s 1-1 f v\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = readResp(t, r1)
	_ = readResp(t, r2) // xadd
	if _, err := c1.Write([]byte("XADD s 2-1 f v\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = readResp(t, r1)
	_ = readResp(t, r2) // xadd

	// XTRIM fires xtrim when it removes an entry.
	if got := sendLine(t, r1, c1, "XTRIM s MAXLEN 1"); got != ":1" {
		t.Fatalf("XTRIM = %q", got)
	}
	if msg := readResp(t, r2); !slices.Contains(msg, "__keyevent@0__:xtrim") {
		t.Fatalf("xtrim push = %v", msg)
	}

	// XSETID fires xsetid.
	if got := sendLine(t, r1, c1, "XSETID s 5-0"); got != "+OK" {
		t.Fatalf("XSETID = %q", got)
	}
	if msg := readResp(t, r2); !slices.Contains(msg, "__keyevent@0__:xsetid") {
		t.Fatalf("xsetid push = %v", msg)
	}
}

func TestNotifyStreamGroup(t *testing.T) {
	r1, c1, r2, c2 := startDataTwo(t)
	if _, err := c2.Write([]byte("PSUBSCRIBE __keyevent@0__:xgroup*\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = readResp(t, r2)
	if got := sendLine(t, r1, c1, "CONFIG SET notify-keyspace-events KEA"); got != "+OK" {
		t.Fatalf("CONFIG SET = %q", got)
	}
	if _, err := c1.Write([]byte("XADD s 1-1 f v\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = readResp(t, r1)

	// XGROUP CREATE fires xgroup-create.
	if got := sendLine(t, r1, c1, "XGROUP CREATE s g 0"); got != "+OK" {
		t.Fatalf("XGROUP CREATE = %q", got)
	}
	if msg := readResp(t, r2); !slices.Contains(msg, "__keyevent@0__:xgroup-create") {
		t.Fatalf("xgroup-create push = %v", msg)
	}

	// XGROUP CREATECONSUMER fires xgroup-createconsumer.
	if got := sendLine(t, r1, c1, "XGROUP CREATECONSUMER s g c"); got != ":1" {
		t.Fatalf("XGROUP CREATECONSUMER = %q", got)
	}
	if msg := readResp(t, r2); !slices.Contains(msg, "__keyevent@0__:xgroup-createconsumer") {
		t.Fatalf("xgroup-createconsumer push = %v", msg)
	}

	// XGROUP SETID fires xgroup-setid.
	if got := sendLine(t, r1, c1, "XGROUP SETID s g 0"); got != "+OK" {
		t.Fatalf("XGROUP SETID = %q", got)
	}
	if msg := readResp(t, r2); !slices.Contains(msg, "__keyevent@0__:xgroup-setid") {
		t.Fatalf("xgroup-setid push = %v", msg)
	}

	// XGROUP DELCONSUMER fires xgroup-delconsumer.
	if got := sendLine(t, r1, c1, "XGROUP DELCONSUMER s g c"); got != ":0" {
		t.Fatalf("XGROUP DELCONSUMER = %q", got)
	}
	if msg := readResp(t, r2); !slices.Contains(msg, "__keyevent@0__:xgroup-delconsumer") {
		t.Fatalf("xgroup-delconsumer push = %v", msg)
	}

	// XGROUP DESTROY fires xgroup-destroy.
	if got := sendLine(t, r1, c1, "XGROUP DESTROY s g"); got != ":1" {
		t.Fatalf("XGROUP DESTROY = %q", got)
	}
	if msg := readResp(t, r2); !slices.Contains(msg, "__keyevent@0__:xgroup-destroy") {
		t.Fatalf("xgroup-destroy push = %v", msg)
	}
}

func TestNotifyStreamClaim(t *testing.T) {
	r1, c1, r2, c2 := startDataTwo(t)
	if _, err := c2.Write([]byte("PSUBSCRIBE __keyevent@0__:xclaim\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = readResp(t, r2)
	if got := sendLine(t, r1, c1, "CONFIG SET notify-keyspace-events KEA"); got != "+OK" {
		t.Fatalf("CONFIG SET = %q", got)
	}
	if _, err := c1.Write([]byte("XADD s 1-1 f v\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = readResp(t, r1)
	if got := sendLine(t, r1, c1, "XGROUP CREATE s g 0"); got != "+OK" {
		t.Fatalf("XGROUP CREATE = %q", got)
	}
	// Deliver the entry to consumer a so it lands in the PEL.
	if _, err := c1.Write([]byte("XREADGROUP GROUP g a COUNT 1 STREAMS s >\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = readResp(t, r1)

	// XCLAIM with min-idle 0 hands the entry to consumer b and fires xclaim.
	if _, err := c1.Write([]byte("XCLAIM s g b 0 1-1\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = readResp(t, r1)
	if msg := readResp(t, r2); !slices.Contains(msg, "__keyevent@0__:xclaim") {
		t.Fatalf("xclaim push = %v", msg)
	}
}

func TestNotifyStreamAutoClaim(t *testing.T) {
	r1, c1, r2, c2 := startDataTwo(t)
	if _, err := c2.Write([]byte("PSUBSCRIBE __keyevent@0__:xautoclaim\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = readResp(t, r2)
	if got := sendLine(t, r1, c1, "CONFIG SET notify-keyspace-events KEA"); got != "+OK" {
		t.Fatalf("CONFIG SET = %q", got)
	}
	if _, err := c1.Write([]byte("XADD s 1-1 f v\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = readResp(t, r1)
	if got := sendLine(t, r1, c1, "XGROUP CREATE s g 0"); got != "+OK" {
		t.Fatalf("XGROUP CREATE = %q", got)
	}
	if _, err := c1.Write([]byte("XREADGROUP GROUP g a COUNT 1 STREAMS s >\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = readResp(t, r1)

	// XAUTOCLAIM from 0 hands the pending entry to b and fires xautoclaim.
	if _, err := c1.Write([]byte("XAUTOCLAIM s g b 0 0\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = readResp(t, r1)
	if msg := readResp(t, r2); !slices.Contains(msg, "__keyevent@0__:xautoclaim") {
		t.Fatalf("xautoclaim push = %v", msg)
	}
}
