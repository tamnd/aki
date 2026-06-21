package command

import (
	"strconv"
	"testing"
)

func TestHGetDel(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "HSET h f1 v1 f2 v2 f3 v3")
	got := array(t, r, c, "HGETDEL h FIELDS 2 f1 nope")
	if len(got) != 2 || got[0] != "v1" || got[1] != "<nil>" {
		t.Fatalf("HGETDEL = %v want [v1 <nil>]", got)
	}
	if got := sendLine(t, r, c, "HEXISTS h f1"); got != ":0" {
		t.Fatalf("HEXISTS f1 after HGETDEL = %q want :0", got)
	}
	if got := sendLine(t, r, c, "HLEN h"); got != ":2" {
		t.Fatalf("HLEN = %q want :2", got)
	}
}

func TestHGetDelLastFieldDeletesKey(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "HSET h f1 v1")
	got := array(t, r, c, "HGETDEL h FIELDS 1 f1")
	if got[0] != "v1" {
		t.Fatalf("HGETDEL = %v", got)
	}
	if got := sendLine(t, r, c, "EXISTS h"); got != ":0" {
		t.Fatalf("EXISTS after last field = %q want :0", got)
	}
}

func TestHGetDelMissingKey(t *testing.T) {
	r, c := startData(t)
	got := array(t, r, c, "HGETDEL nope FIELDS 2 a b")
	if len(got) != 2 || got[0] != "<nil>" || got[1] != "<nil>" {
		t.Fatalf("HGETDEL missing key = %v want two nils", got)
	}
}

func TestHGetExRead(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "HSET h f1 v1 f2 v2")
	// No option reads the values without touching TTL.
	got := array(t, r, c, "HGETEX h FIELDS 2 f1 f2")
	if len(got) != 2 || got[0] != "v1" || got[1] != "v2" {
		t.Fatalf("HGETEX read = %v", got)
	}
	if toks := codeArray(t, r, c, "HTTL h FIELDS 1 f1"); toks[0] != "-1" {
		t.Fatalf("HTTL after plain HGETEX = %v want [-1]", toks)
	}
}

func TestHGetExSetTTL(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "HSET h f1 v1")
	got := array(t, r, c, "HGETEX h EX 100 FIELDS 1 f1")
	if got[0] != "v1" {
		t.Fatalf("HGETEX EX = %v", got)
	}
	toks := codeArray(t, r, c, "HTTL h FIELDS 1 f1")
	n, _ := strconv.Atoi(toks[0])
	if n <= 0 || n > 100 {
		t.Fatalf("HTTL after HGETEX EX = %v want 1..100", toks[0])
	}
}

func TestHGetExSetAbsolute(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "HSET h f1 v1")
	big := strconv.Itoa(farFuture)
	got := array(t, r, c, "HGETEX h PXAT "+big+" FIELDS 1 f1")
	if got[0] != "v1" {
		t.Fatalf("HGETEX PXAT = %v", got)
	}
	if toks := codeArray(t, r, c, "HPEXPIRETIME h FIELDS 1 f1"); toks[0] != big {
		t.Fatalf("HPEXPIRETIME after HGETEX PXAT = %v want %s", toks, big)
	}
}

func TestHGetExPersist(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "HSET h f1 v1")
	_ = codeArray(t, r, c, "HEXPIRE h 100 FIELDS 1 f1")
	got := array(t, r, c, "HGETEX h PERSIST FIELDS 1 f1")
	if got[0] != "v1" {
		t.Fatalf("HGETEX PERSIST = %v", got)
	}
	if toks := codeArray(t, r, c, "HTTL h FIELDS 1 f1"); toks[0] != "-1" {
		t.Fatalf("HTTL after HGETEX PERSIST = %v want [-1]", toks)
	}
}

func TestHGetExPastDeletesField(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "HSET h f1 v1 f2 v2")
	// A past absolute deadline returns the value and deletes the field.
	got := array(t, r, c, "HGETEX h PXAT 1 FIELDS 1 f1")
	if got[0] != "v1" {
		t.Fatalf("HGETEX past = %v", got)
	}
	if got := sendLine(t, r, c, "HEXISTS h f1"); got != ":0" {
		t.Fatalf("HEXISTS f1 after past HGETEX = %q want :0", got)
	}
}

func TestHGetExMissingKey(t *testing.T) {
	r, c := startData(t)
	got := array(t, r, c, "HGETEX nope EX 10 FIELDS 1 f")
	if len(got) != 1 || got[0] != "<nil>" {
		t.Fatalf("HGETEX missing key = %v want [<nil>]", got)
	}
}

func TestHGetExErrors(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "HSET h f v")
	if got := sendLine(t, r, c, "HGETEX h EX notanint FIELDS 1 f"); got != "-ERR value is not an integer or out of range" {
		t.Fatalf("HGETEX bad int = %q", got)
	}
	if got := sendLine(t, r, c, "HGETEX h BOGUS FIELDS 1 f"); got != "-ERR Mandatory keyword FIELDS is missing or not at the right position" {
		t.Fatalf("HGETEX bad option = %q", got)
	}
	_ = sendLine(t, r, c, "SET s str")
	if got := sendLine(t, r, c, "HGETEX s FIELDS 1 f"); got != "-"+wrongTypeError {
		t.Fatalf("HGETEX wrongtype = %q", got)
	}
	if got := sendLine(t, r, c, "HGETDEL s FIELDS 1 f"); got != "-"+wrongTypeError {
		t.Fatalf("HGETDEL wrongtype = %q", got)
	}
}
