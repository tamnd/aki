package command

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
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
	// f1 had a TTL → 1, f2 persistent → -1, nope missing → -2.
	if len(toks) != 3 || toks[0] != "1" || toks[1] != "-1" || toks[2] != "-2" {
		t.Fatalf("HPERSIST = %v want [1 -1 -2]", toks)
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

// TestHashFieldTTLCollForm exercises the whole 7.4 hash-field expiry family on a
// hash large enough to live in the btree-backed (hashtable) form, not the inline
// blob. That form stores one TTL per element row, a separate code path, and it
// once decoded the metadata cell as if it were an inline blob and failed every
// command with a truncated-input error. The test builds a 200-field hash, asserts
// it reports hashtable, then walks HEXPIREAT/HEXPIRETIME/HPERSIST/HGETEX/HGETDEL
// and the past-deadline delete, all with absolute deadlines so the values are
// exact rather than a live countdown.
func TestHashFieldTTLCollForm(t *testing.T) {
	r, c := startData(t)

	var b strings.Builder
	b.WriteString("HSET h")
	for i := 0; i < 200; i++ {
		fmt.Fprintf(&b, " f%03d v%03d", i, i)
	}
	if got := sendLine(t, r, c, b.String()); got != ":200" {
		t.Fatalf("HSET 200 fields = %q want :200", got)
	}
	if enc := readBulk(t, r, sendLine(t, r, c, "OBJECT ENCODING h")); enc != "hashtable" {
		t.Fatalf("OBJECT ENCODING = %q want hashtable (test must hit the coll path)", enc)
	}

	// A seconds-granularity deadline reads back as that many seconds, and as
	// seconds*1000 in millis.
	atSec := farFuture / 1000
	if toks := codeArray(t, r, c, "HEXPIREAT h "+strconv.Itoa(atSec)+" FIELDS 1 f100"); toks[0] != "1" {
		t.Fatalf("HEXPIREAT = %v want [1]", toks)
	}
	if toks := codeArray(t, r, c, "HEXPIRETIME h FIELDS 1 f100"); toks[0] != strconv.Itoa(atSec) {
		t.Fatalf("HEXPIRETIME = %v want %d", toks, atSec)
	}
	if toks := codeArray(t, r, c, "HPEXPIRETIME h FIELDS 1 f100"); toks[0] != strconv.Itoa(atSec*1000) {
		t.Fatalf("HPEXPIRETIME = %v want %d", toks, atSec*1000)
	}
	// A field with no TTL reads -1, a missing field -2.
	if toks := codeArray(t, r, c, "HEXPIRETIME h FIELDS 2 f101 nope"); toks[0] != "-1" || toks[1] != "-2" {
		t.Fatalf("HEXPIRETIME no-ttl/missing = %v want [-1 -2]", toks)
	}
	// HPERSIST: 1 cleared, -1 already persistent, -2 missing.
	if toks := codeArray(t, r, c, "HPERSIST h FIELDS 3 f100 f101 nope"); toks[0] != "1" || toks[1] != "-1" || toks[2] != "-2" {
		t.Fatalf("HPERSIST = %v want [1 -1 -2]", toks)
	}
	if toks := codeArray(t, r, c, "HTTL h FIELDS 1 f100"); toks[0] != "-1" {
		t.Fatalf("HTTL after persist = %v want [-1]", toks)
	}
	// HGETEX PERSIST reads the value and clears the TTL it just set.
	_ = codeArray(t, r, c, "HEXPIREAT h "+strconv.Itoa(farFuture/1000)+" FIELDS 1 f102")
	if got := sendLine(t, r, c, "HGETEX h PERSIST FIELDS 1 f102"); got != "*1" {
		t.Fatalf("HGETEX header = %q want *1", got)
	}
	if v := readBulk(t, r, sendLineRead(t, r)); v != "v102" {
		t.Fatalf("HGETEX value = %q want v102", v)
	}
	if toks := codeArray(t, r, c, "HTTL h FIELDS 1 f102"); toks[0] != "-1" {
		t.Fatalf("HTTL after HGETEX PERSIST = %v want [-1]", toks)
	}
	// HGETDEL reads then removes a field.
	if got := sendLine(t, r, c, "HGETDEL h FIELDS 1 f103"); got != "*1" {
		t.Fatalf("HGETDEL header = %q want *1", got)
	}
	if v := readBulk(t, r, sendLineRead(t, r)); v != "v103" {
		t.Fatalf("HGETDEL value = %q want v103", v)
	}
	if got := sendLine(t, r, c, "HEXISTS h f103"); got != ":0" {
		t.Fatalf("HEXISTS after HGETDEL = %q want :0", got)
	}
	// A past deadline deletes the field and returns 2.
	if toks := codeArray(t, r, c, "HEXPIREAT h 1 FIELDS 1 f104"); toks[0] != "2" {
		t.Fatalf("HEXPIREAT past = %v want [2]", toks)
	}
	if got := sendLine(t, r, c, "HEXISTS h f104"); got != ":0" {
		t.Fatalf("HEXISTS after past-deadline = %q want :0", got)
	}
	// Three fields gone (f103 HGETDEL, f104 past-delete... f100/f101/f102 persist).
	if got := sendLine(t, r, c, "HLEN h"); got != ":198" {
		t.Fatalf("HLEN = %q want :198", got)
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
