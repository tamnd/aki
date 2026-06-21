package command

import (
	"bufio"
	"net"
	"strconv"
	"testing"
)

// codeArray reads an array reply header (*N) and then N integer elements (:V),
// returning each value as its decimal string with the prefix stripped.
func codeArray(t *testing.T, r *bufio.Reader, c net.Conn, cmd string) []string {
	t.Helper()
	line := sendLine(t, r, c, cmd)
	if line == "" || line[0] != '*' {
		t.Fatalf("expected array header after %q, got %q", cmd, line)
	}
	n, err := strconv.Atoi(line[1:])
	if err != nil {
		t.Fatalf("parse array len %q: %v", line, err)
	}
	out := make([]string, n)
	for i := range out {
		el := sendLineRead(t, r)
		if el == "" || el[0] != ':' {
			t.Fatalf("expected integer element, got %q", el)
		}
		out[i] = el[1:]
	}
	return out
}

func TestHExpireBasic(t *testing.T) {
	r, c := startData(t)
	if got := sendLine(t, r, c, "HSET h f1 v1 f2 v2"); got != ":2" {
		t.Fatalf("HSET = %q", got)
	}
	// One field, code 1.
	toks := codeArray(t, r, c, "HEXPIRE h 100 FIELDS 1 f1")
	if len(toks) != 1 || toks[0] != "1" {
		t.Fatalf("HEXPIRE = %v want [1]", toks)
	}
	// HTTL returns roughly the set value for f1 and -1 for the persistent f2.
	toks = codeArray(t, r, c, "HTTL h FIELDS 2 f1 f2")
	n, _ := strconv.Atoi(toks[0])
	if n <= 0 || n > 100 {
		t.Fatalf("HTTL f1 = %v want 1..100", toks[0])
	}
	if toks[1] != "-1" {
		t.Fatalf("HTTL f2 = %q want -1", toks[1])
	}
}

func TestHExpireMissingField(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "HSET h f1 v1")
	toks := codeArray(t, r, c, "HEXPIRE h 100 FIELDS 2 f1 nope")
	if len(toks) != 2 || toks[0] != "1" || toks[1] != "-2" {
		t.Fatalf("HEXPIRE missing field = %v want [1 -2]", toks)
	}
}

func TestHExpireMissingKey(t *testing.T) {
	r, c := startData(t)
	toks := codeArray(t, r, c, "HEXPIRE nope 100 FIELDS 1 f")
	if len(toks) != 1 || toks[0] != "-2" {
		t.Fatalf("HEXPIRE missing key = %v want [-2]", toks)
	}
}

func TestHExpirePastDeletesField(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "HSET h f1 v1 f2 v2")
	// HEXPIRE with 0 seconds resolves to now, so the field is deleted with code 2.
	toks := codeArray(t, r, c, "HEXPIRE h 0 FIELDS 1 f1")
	if len(toks) != 1 || toks[0] != "2" {
		t.Fatalf("HEXPIRE 0 = %v want [2]", toks)
	}
	if got := sendLine(t, r, c, "HEXISTS h f1"); got != ":0" {
		t.Fatalf("HEXISTS f1 = %q want :0", got)
	}
	if got := sendLine(t, r, c, "HLEN h"); got != ":1" {
		t.Fatalf("HLEN = %q want :1", got)
	}
}

func TestHExpireLastFieldDeletesKey(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "HSET h f1 v1")
	toks := codeArray(t, r, c, "HPEXPIREAT h 1 FIELDS 1 f1")
	if toks[0] != "2" {
		t.Fatalf("HPEXPIREAT past = %v want [2]", toks)
	}
	if got := sendLine(t, r, c, "EXISTS h"); got != ":0" {
		t.Fatalf("EXISTS h after last field gone = %q want :0", got)
	}
}

func TestHExpireConditions(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "HSET h f v")
	big := strconv.Itoa(farFuture)
	if toks := codeArray(t, r, c, "HPEXPIREAT h "+big+" FIELDS 1 f"); toks[0] != "1" {
		t.Fatalf("set ttl = %v", toks)
	}
	// NX fails because the field already has a TTL.
	if toks := codeArray(t, r, c, "HPEXPIREAT h 123 NX FIELDS 1 f"); toks[0] != "0" {
		t.Fatalf("NX = %v want [0]", toks)
	}
	// GT fails for a smaller deadline.
	if toks := codeArray(t, r, c, "HPEXPIREAT h 123 GT FIELDS 1 f"); toks[0] != "0" {
		t.Fatalf("GT smaller = %v want [0]", toks)
	}
	// GT passes for a larger deadline.
	if toks := codeArray(t, r, c, "HPEXPIREAT h "+strconv.Itoa(farFuture+1)+" GT FIELDS 1 f"); toks[0] != "1" {
		t.Fatalf("GT larger = %v want [1]", toks)
	}
	// LT passes for a smaller deadline.
	if toks := codeArray(t, r, c, "HPEXPIREAT h "+big+" LT FIELDS 1 f"); toks[0] != "1" {
		t.Fatalf("LT smaller = %v want [1]", toks)
	}
}

func TestHPersist(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "HSET h f1 v1 f2 v2")
	_ = codeArray(t, r, c, "HEXPIRE h 100 FIELDS 1 f1")
	toks := codeArray(t, r, c, "HPERSIST h FIELDS 3 f1 f2 nope")
	// f1 had a TTL → 1, f2 persistent → 0, nope missing → -2.
	if len(toks) != 3 || toks[0] != "1" || toks[1] != "0" || toks[2] != "-2" {
		t.Fatalf("HPERSIST = %v want [1 0 -2]", toks)
	}
	if toks := codeArray(t, r, c, "HTTL h FIELDS 1 f1"); toks[0] != "-1" {
		t.Fatalf("HTTL after persist = %v want [-1]", toks)
	}
}

func TestHPersistMissingKey(t *testing.T) {
	r, c := startData(t)
	toks := codeArray(t, r, c, "HPERSIST nope FIELDS 2 a b")
	if len(toks) != 2 || toks[0] != "-2" || toks[1] != "-2" {
		t.Fatalf("HPERSIST missing key = %v want [-2 -2]", toks)
	}
}

func TestHExpireTimeReaders(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "HSET h f v")
	big := strconv.Itoa(farFuture)
	_ = codeArray(t, r, c, "HPEXPIREAT h "+big+" FIELDS 1 f")
	if toks := codeArray(t, r, c, "HPEXPIRETIME h FIELDS 1 f"); toks[0] != big {
		t.Fatalf("HPEXPIRETIME = %v want %s", toks, big)
	}
	if toks := codeArray(t, r, c, "HEXPIRETIME h FIELDS 1 f"); toks[0] != strconv.Itoa(farFuture/1000) {
		t.Fatalf("HEXPIRETIME = %v", toks)
	}
	if toks := codeArray(t, r, c, "HEXPIRETIME h FIELDS 1 nope"); toks[0] != "-2" {
		t.Fatalf("HEXPIRETIME missing field = %v want [-2]", toks)
	}
}

func TestHTTLMissingKey(t *testing.T) {
	r, c := startData(t)
	toks := codeArray(t, r, c, "HTTL nope FIELDS 1 f")
	if len(toks) != 1 || toks[0] != "-2" {
		t.Fatalf("HTTL missing key = %v want [-2]", toks)
	}
}

func TestHExpireErrors(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "HSET h f v")
	if got := sendLine(t, r, c, "HEXPIRE h 100 FIELDS 0 x"); got != "-ERR Parameter `numFields` should be greater than 0" {
		t.Fatalf("numfields 0 = %q", got)
	}
	if got := sendLine(t, r, c, "HEXPIRE h 100 FIELDS 2 f"); got != "-ERR Parameter `numFields` is more than number of arguments" {
		t.Fatalf("count mismatch = %q", got)
	}
	if got := sendLine(t, r, c, "HEXPIRE h 100 BOGUS 1 f"); got != "-ERR Mandatory keyword FIELDS is missing or not at the right position" {
		t.Fatalf("missing FIELDS = %q", got)
	}
	if got := sendLine(t, r, c, "SET s str"); got != "+OK" {
		t.Fatalf("SET = %q", got)
	}
	if got := sendLine(t, r, c, "HEXPIRE s 100 FIELDS 1 f"); got != "-"+wrongTypeError {
		t.Fatalf("HEXPIRE wrongtype = %q", got)
	}
}
