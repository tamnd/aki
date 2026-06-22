package command

import (
	"testing"
	"time"
)

func TestXreadBlockServedByXadd(t *testing.T) {
	addr := startBlockingServer(t)
	ra, ca := dialBlocking(t, addr)
	rb, cb := dialBlocking(t, addr)

	writeCmd(t, ca, "XREAD BLOCK 5000 STREAMS s $")
	time.Sleep(100 * time.Millisecond)
	_ = bulk(t, rb, cb, "XADD s 1-1 f v")

	// ca receives the payload: outer *1, pair *2, key, entries.
	if got := readLineWait(t, ra, ca); got != "*1" {
		t.Fatalf("XREAD outer = %q want *1", got)
	}
	if got := readLineWait(t, ra, ca); got != "*2" {
		t.Fatalf("XREAD pair = %q want *2", got)
	}
	_ = readLineWait(t, ra, ca) // $1 key header
	if got := readLineWait(t, ra, ca); got != "s" {
		t.Fatalf("XREAD key = %q want s", got)
	}
}

func TestXreadBlockImmediateWhenDataPresent(t *testing.T) {
	r, c := startData(t)
	_ = bulk(t, r, c, "XADD s 1-1 f v")
	if got := sendLine(t, r, c, "XREAD BLOCK 5000 STREAMS s 0"); got != "*1" {
		t.Fatalf("XREAD immediate outer = %q want *1", got)
	}
}

func TestXreadBlockTimeout(t *testing.T) {
	r, c := startData(t)
	start := time.Now()
	if got := sendLine(t, r, c, "XREAD BLOCK 200 STREAMS s $"); got != "*-1" {
		t.Fatalf("XREAD timeout = %q want *-1", got)
	}
	if elapsed := time.Since(start); elapsed < 150*time.Millisecond {
		t.Fatalf("XREAD returned too soon: %v", elapsed)
	}
}

func TestXreadBlockFanout(t *testing.T) {
	addr := startBlockingServer(t)
	ra, ca := dialBlocking(t, addr)
	rb, cb := dialBlocking(t, addr)
	rc, cc := dialBlocking(t, addr)

	// Two readers block on the same stream. One XADD must reach both.
	writeCmd(t, ca, "XREAD BLOCK 5000 STREAMS s $")
	writeCmd(t, cb, "XREAD BLOCK 5000 STREAMS s $")
	time.Sleep(120 * time.Millisecond)
	_ = bulk(t, rc, cc, "XADD s 1-1 f v")

	if got := readLineWait(t, ra, ca); got != "*1" {
		t.Fatalf("reader A outer = %q want *1", got)
	}
	if got := readLineWait(t, rb, cb); got != "*1" {
		t.Fatalf("reader B outer = %q want *1", got)
	}
}

func TestXreadgroupBlockServedByXadd(t *testing.T) {
	addr := startBlockingServer(t)
	ra, ca := dialBlocking(t, addr)
	rb, cb := dialBlocking(t, addr)

	_ = bulk(t, ra, ca, "XADD s 1-1 a 1")
	if got := sendLine(t, ra, ca, "XGROUP CREATE s g1 $"); got != "+OK" {
		t.Fatalf("XGROUP CREATE = %q", got)
	}

	writeCmd(t, ca, "XREADGROUP GROUP g1 c1 BLOCK 5000 STREAMS s >")
	time.Sleep(100 * time.Millisecond)
	_ = bulk(t, rb, cb, "XADD s 2-2 b 2")

	if got := readLineWait(t, ra, ca); got != "*1" {
		t.Fatalf("XREADGROUP outer = %q want *1", got)
	}
	if got := readLineWait(t, ra, ca); got != "*2" {
		t.Fatalf("XREADGROUP pair = %q want *2", got)
	}
	_ = readLineWait(t, ra, ca) // $1 key header
	if got := readLineWait(t, ra, ca); got != "s" {
		t.Fatalf("XREADGROUP key = %q want s", got)
	}
}

func TestXreadgroupBlockTimeout(t *testing.T) {
	r, c := startData(t)
	_ = bulk(t, r, c, "XADD s 1-1 a 1")
	if got := sendLine(t, r, c, "XGROUP CREATE s g1 $"); got != "+OK" {
		t.Fatalf("XGROUP CREATE = %q", got)
	}
	if got := sendLine(t, r, c, "XREADGROUP GROUP g1 c1 BLOCK 150 STREAMS s >"); got != "*-1" {
		t.Fatalf("XREADGROUP timeout = %q want *-1", got)
	}
}

func TestXreadgroupBlockExplicitNeverBlocks(t *testing.T) {
	r, c := startData(t)
	_ = bulk(t, r, c, "XADD s 1-1 a 1")
	if got := sendLine(t, r, c, "XGROUP CREATE s g1 0"); got != "+OK" {
		t.Fatalf("XGROUP CREATE = %q", got)
	}
	_ = flatCmd(t, r, c, "XREADGROUP GROUP g1 c1 STREAMS s >")
	// An explicit-ID read of the PEL returns at once even with BLOCK.
	if got := sendLine(t, r, c, "XREADGROUP GROUP g1 c1 BLOCK 5000 STREAMS s 0"); got != "*1" {
		t.Fatalf("XREADGROUP explicit outer = %q want *1", got)
	}
}

func TestXreadBlockNegativeTimeout(t *testing.T) {
	r, c := startData(t)
	if got := sendLine(t, r, c, "XREAD BLOCK -1 STREAMS s $"); got != "-"+errStreamTimeoutNeg {
		t.Fatalf("XREAD BLOCK -1 = %q", got)
	}
}

func TestXreadBlockInsideMultiDoesNotBlock(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "MULTI")
	_ = sendLine(t, r, c, "XREAD BLOCK 0 STREAMS s $")
	if got := sendLine(t, r, c, "EXEC"); got != "*1" {
		t.Fatalf("EXEC header = %q want *1", got)
	}
	if got := sendLineRead(t, r); got != "*-1" {
		t.Fatalf("XREAD in MULTI = %q want *-1", got)
	}
}

func TestClientUnblockXread(t *testing.T) {
	addr := startBlockingServer(t)
	ra, ca := dialBlocking(t, addr)
	rb, cb := dialBlocking(t, addr)

	id := sendLine(t, ra, ca, "CLIENT ID")[1:]
	writeCmd(t, ca, "XREAD BLOCK 0 STREAMS s $")
	time.Sleep(100 * time.Millisecond)
	if got := sendLine(t, rb, cb, "CLIENT UNBLOCK "+id); got != ":1" {
		t.Fatalf("CLIENT UNBLOCK = %q want :1", got)
	}
	if got := readLineWait(t, ra, ca); got != "*-1" {
		t.Fatalf("unblocked XREAD = %q want *-1", got)
	}
}
