package sqlo1

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
)

// compatWire turns a decoded fixture reply into wire bytes: scalars
// through wantWire, arrays recursively.
func compatWire(v any) string {
	switch x := v.(type) {
	case string:
		if x == "*-1" {
			return "*-1\r\n"
		}
		return wantWire(x)
	case []any:
		var b strings.Builder
		fmt.Fprintf(&b, "*%d\r\n", len(x))
		for _, e := range x {
			b.WriteString(compatWire(e))
		}
		return b.String()
	}
	panic(fmt.Sprintf("unhandled fixture reply %T", v))
}

// TestCompatRedisParity replays testdata/compat/fixtures.txt,
// generated against a real redis-server 8.8.0 by
// testdata/compat/gen.py: the STRING, BITMAP, HLL, HASH, SET, ZSET,
// GEO, LIST, STREAM, and EXPIRY manifest rows from spec doc 12, one
// live-captured reply per line, diffed against the dispatch path byte
// for byte.
func TestCompatRedisParity(t *testing.T) {
	f, err := os.Open("testdata/compat/fixtures.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	do, _ := dispatchServer(t)
	section := ""
	n := 0
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if line == "" {
			continue
		}
		if rest, ok := strings.CutPrefix(line, "S "); ok {
			section = rest
			continue
		}
		rest, ok := strings.CutPrefix(line, "C ")
		if !ok {
			t.Fatalf("unhandled fixture line %q", line)
		}
		jargs, jrep, ok := strings.Cut(rest, " -> ")
		if !ok {
			t.Fatalf("fixture line without reply: %q", line)
		}
		var argStrs []string
		if err := json.Unmarshal([]byte(jargs), &argStrs); err != nil {
			t.Fatalf("bad args %q: %v", jargs, err)
		}
		var rep any
		if err := json.Unmarshal([]byte(jrep), &rep); err != nil {
			t.Fatalf("bad reply %q: %v", jrep, err)
		}
		args := make([]string, len(argStrs))
		for i, a := range argStrs {
			args[i] = string(latin1(a))
		}
		if got, want := do(args...), compatWire(rep); got != want {
			t.Errorf("[%s] %v = %q, want %q", section, argStrs, got, want)
		}
		n++
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	if n < 800 {
		t.Fatalf("replayed only %d commands, fixture looks truncated", n)
	}
}
