package command

import (
	"strings"
	"testing"
	"time"
)

func TestBzpopminServedByZadd(t *testing.T) {
	addr := startBlockingServer(t)
	ra, ca := dialBlocking(t, addr)
	rb, cb := dialBlocking(t, addr)

	writeCmd(t, ca, "BZPOPMIN z 5")
	time.Sleep(100 * time.Millisecond)
	if got := sendLine(t, rb, cb, "ZADD z 1 a 2 b"); got != ":2" {
		t.Fatalf("ZADD = %q want :2", got)
	}
	got := readArrayWait(t, ra, ca)
	if len(got) != 3 || got[0] != "z" || got[1] != "a" || got[2] != "1" {
		t.Fatalf("BZPOPMIN = %v want [z a 1]", got)
	}
}

func TestBzpopmaxServedByZadd(t *testing.T) {
	addr := startBlockingServer(t)
	ra, ca := dialBlocking(t, addr)
	rb, cb := dialBlocking(t, addr)

	writeCmd(t, ca, "BZPOPMAX z 5")
	time.Sleep(100 * time.Millisecond)
	_ = sendLine(t, rb, cb, "ZADD z 1 a 2 b")
	got := readArrayWait(t, ra, ca)
	if len(got) != 3 || got[0] != "z" || got[1] != "b" || got[2] != "2" {
		t.Fatalf("BZPOPMAX = %v want [z b 2]", got)
	}
}

func TestBzpopminImmediateWhenDataPresent(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "ZADD z 5 a 10 b")
	got := array(t, r, c, "BZPOPMIN z 0")
	if len(got) != 3 || got[0] != "z" || got[1] != "a" || got[2] != "5" {
		t.Fatalf("BZPOPMIN present = %v want [z a 5]", got)
	}
}

func TestBzpopminMultiKeyFirstNonEmpty(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "ZADD second 3 v")
	got := array(t, r, c, "BZPOPMIN first second 0")
	if len(got) != 3 || got[0] != "second" || got[1] != "v" || got[2] != "3" {
		t.Fatalf("BZPOPMIN multi = %v want [second v 3]", got)
	}
}

func TestBzpopminTimeout(t *testing.T) {
	r, c := startData(t)
	start := time.Now()
	if got := sendLine(t, r, c, "BZPOPMIN z 0.2"); got != "*-1" {
		t.Fatalf("BZPOPMIN timeout = %q want *-1", got)
	}
	if elapsed := time.Since(start); elapsed < 150*time.Millisecond {
		t.Fatalf("BZPOPMIN returned too soon: %v", elapsed)
	}
}

func TestBzpopminFifoFairness(t *testing.T) {
	addr := startBlockingServer(t)
	ra, ca := dialBlocking(t, addr)
	rb, cb := dialBlocking(t, addr)
	rc, cc := dialBlocking(t, addr)

	writeCmd(t, ca, "BZPOPMIN z 5")
	time.Sleep(80 * time.Millisecond)
	writeCmd(t, cb, "BZPOPMIN z 5")
	time.Sleep(80 * time.Millisecond)

	_ = sendLine(t, rc, cc, "ZADD z 1 a 2 b")

	gotA := readArrayWait(t, ra, ca)
	gotB := readArrayWait(t, rb, cb)
	if gotA[1] != "a" {
		t.Fatalf("first blocker got %v want member a", gotA)
	}
	if gotB[1] != "b" {
		t.Fatalf("second blocker got %v want member b", gotB)
	}
}

func TestBzmpopServedByZadd(t *testing.T) {
	addr := startBlockingServer(t)
	ra, ca := dialBlocking(t, addr)
	rb, cb := dialBlocking(t, addr)

	writeCmd(t, ca, "BZMPOP 5 2 z1 z2 MIN COUNT 10")
	time.Sleep(100 * time.Millisecond)
	_ = sendLine(t, rb, cb, "ZADD z2 1 a 2 b 3 c")

	if head := readLineWait(t, ra, ca); head != "*2" {
		t.Fatalf("BZMPOP header = %q want *2", head)
	}
	if got := readLineWait(t, ra, ca); got != "$2" {
		t.Fatalf("BZMPOP key header = %q want $2", got)
	}
	if got := readLineWait(t, ra, ca); got != "z2" {
		t.Fatalf("BZMPOP key = %q want z2", got)
	}
	if got := readLineWait(t, ra, ca); got != "*6" {
		t.Fatalf("BZMPOP pairs header = %q want *6 (RESP2 flat)", got)
	}
	for _, want := range []string{"a", "1", "b", "2", "c", "3"} {
		_ = readLineWait(t, ra, ca) // bulk header
		if got := readLineWait(t, ra, ca); got != want {
			t.Fatalf("BZMPOP elem = %q want %q", got, want)
		}
	}
}

func TestBzmpopMaxImmediate(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "ZADD z 1 a 2 b 3 c")
	// RESP2 reply is [key, flat pairs]: *2, z, then *4 with c 3 b 2.
	writeCmd(t, c, "BZMPOP 0 1 z MAX COUNT 2")
	if got := readLineWait(t, r, c); got != "*2" {
		t.Fatalf("BZMPOP MAX header = %q want *2", got)
	}
	_ = readLineWait(t, r, c) // $1
	if got := readLineWait(t, r, c); got != "z" {
		t.Fatalf("BZMPOP MAX key = %q want z", got)
	}
	if got := readLineWait(t, r, c); got != "*4" {
		t.Fatalf("BZMPOP MAX pairs header = %q want *4", got)
	}
	for _, want := range []string{"c", "3", "b", "2"} {
		_ = readLineWait(t, r, c) // bulk header
		if got := readLineWait(t, r, c); got != want {
			t.Fatalf("BZMPOP MAX elem = %q want %q", got, want)
		}
	}
}

func TestBzmpopTimeout(t *testing.T) {
	r, c := startData(t)
	if got := sendLine(t, r, c, "BZMPOP 0.1 1 z MIN"); got != "*-1" {
		t.Fatalf("BZMPOP timeout = %q want *-1", got)
	}
}

func TestBzpopminInsideMultiDoesNotBlock(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "MULTI")
	_ = sendLine(t, r, c, "BZPOPMIN z 0")
	if got := sendLine(t, r, c, "EXEC"); got != "*1" {
		t.Fatalf("EXEC header = %q want *1", got)
	}
	if got := sendLineRead(t, r); got != "*-1" {
		t.Fatalf("BZPOPMIN in MULTI = %q want *-1", got)
	}
}

func TestBzpopminNegativeTimeout(t *testing.T) {
	r, c := startData(t)
	if got := sendLine(t, r, c, "BZPOPMIN z -1"); got != "-ERR timeout is negative" {
		t.Fatalf("BZPOPMIN -1 = %q", got)
	}
}

func TestBzpopminWrongType(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET z v")
	if got := sendLine(t, r, c, "BZPOPMIN z 0"); got != "-"+wrongTypeError {
		t.Fatalf("BZPOPMIN wrongtype = %q", got)
	}
}

func TestClientUnblockBzpopmin(t *testing.T) {
	addr := startBlockingServer(t)
	ra, ca := dialBlocking(t, addr)
	rb, cb := dialBlocking(t, addr)

	id := sendLine(t, ra, ca, "CLIENT ID")[1:]
	writeCmd(t, ca, "BZPOPMIN z 0")
	time.Sleep(100 * time.Millisecond)
	if got := sendLine(t, rb, cb, "CLIENT UNBLOCK "+id+" ERROR"); got != ":1" {
		t.Fatalf("CLIENT UNBLOCK ERROR = %q want :1", got)
	}
	if got := readLineWait(t, ra, ca); !strings.HasPrefix(got, "-UNBLOCKED") {
		t.Fatalf("unblocked BZPOPMIN = %q want -UNBLOCKED...", got)
	}
}
