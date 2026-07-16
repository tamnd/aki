package sqlo1

import (
	"bufio"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
)

// TestHLLDenseCodec pins the 6-bit register packing against a plain
// byte-per-register mirror, including the last register, whose high
// bits fall off the end of the array.
func TestHLLDenseCodec(t *testing.T) {
	regs := make([]byte, hllDenseSize-hllHdrSize)
	var mirror [hllRegisters]byte
	seed := uint64(42)
	for round := range 4 {
		for i := range hllRegisters {
			seed = seed*6364136223846793005 + 1442695040888963407
			r := uint8(seed>>33) & hllRegMax
			mirror[i] = r
			hllDenseSetReg(regs, i, r)
		}
		for i := range hllRegisters {
			if got := hllDenseGet(regs, i); got != mirror[i] {
				t.Fatalf("round %d: register %d = %d, want %d", round, i, got, mirror[i])
			}
		}
	}
}

// TestHLLSelfTest runs the PFSELFTEST body, which also cross-checks
// the sparse update path (with natural promotion) against pure dense
// adds over the same element stream.
func TestHLLSelfTest(t *testing.T) {
	if err := hllSelfTest(); err != nil {
		t.Fatal(err)
	}
}

// latin1 undoes the fixture generator's latin-1 JSON encoding: every
// rune below 256 is one payload byte.
func latin1(s string) []byte {
	out := make([]byte, 0, len(s))
	for _, r := range s {
		if r > 0xff {
			panic("fixture payload rune out of latin-1 range")
		}
		out = append(out, byte(r))
	}
	return out
}

// wantWire turns a fixture reply ("+OK", ":3", "-ERR x", "$payload",
// "$-1") into the exact wire bytes.
func wantWire(rep string) string {
	switch rep[0] {
	case '+', ':', '-':
		return rep + "\r\n"
	case '$':
		if rep == "$-1" {
			return "$-1\r\n"
		}
		p := latin1(rep[1:])
		return fmt.Sprintf("$%d\r\n%s\r\n", len(p), p)
	}
	panic("unhandled fixture reply " + rep)
}

// TestHLLRedisParity replays testdata/hll/fixtures.txt, generated
// against a real redis-server 8.8.0 by testdata/hll/gen.py, and
// demands the same reply for every command and the same value bytes
// at every snapshot. The snapshots after PFCOUNT pin the estimator
// through the cached cardinality it writes back.
func TestHLLRedisParity(t *testing.T) {
	f, err := os.Open("testdata/hll/fixtures.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	do, _ := dispatchServer(t)
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	lineno := 0
	for sc.Scan() {
		lineno++
		line := sc.Text()
		switch {
		case strings.HasPrefix(line, "C "):
			argPart, repPart, found := strings.Cut(line[2:], " -> ")
			if !found {
				t.Fatalf("line %d: no reply separator", lineno)
			}
			var args []string
			if err := json.Unmarshal([]byte(argPart), &args); err != nil {
				t.Fatalf("line %d: %v", lineno, err)
			}
			var rep string
			if err := json.Unmarshal([]byte(repPart), &rep); err != nil {
				t.Fatalf("line %d: %v", lineno, err)
			}
			if got, want := do(args...), wantWire(rep); got != want {
				t.Fatalf("line %d: %v = %q, want %q", lineno, args, got, want)
			}
		case strings.HasPrefix(line, "V "):
			parts := strings.SplitN(line[2:], " ", 2)
			want, err := hex.DecodeString(parts[1])
			if err != nil {
				t.Fatalf("line %d: %v", lineno, err)
			}
			got := do("GET", parts[0])
			exp := fmt.Sprintf("$%d\r\n%s\r\n", len(want), want)
			if got != exp {
				t.Fatalf("line %d: %s holds %q, want %q", lineno, parts[0], got, exp)
			}
		default:
			t.Fatalf("line %d: unparseable fixture line %q", lineno, line)
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
}

// TestServerPfTTL pins that PFADD, PFCOUNT's cache write-back, and
// PFMERGE all preserve a live expiry on the key they rewrite, unlike
// SET.
func TestServerPfTTL(t *testing.T) {
	do, _ := dispatchServer(t)

	do("PFADD", "h", "a", "b", "c")
	do("EXPIRE", "h", "100")
	if got := do("PFADD", "h", "d"); got != ":1\r\n" {
		t.Fatalf("PFADD = %q", got)
	}
	if got := do("TTL", "h"); got != ":100\r\n" {
		t.Fatalf("TTL after PFADD = %q", got)
	}
	if got := do("PFCOUNT", "h"); got != ":4\r\n" {
		t.Fatalf("PFCOUNT = %q", got)
	}
	if got := do("TTL", "h"); got != ":100\r\n" {
		t.Fatalf("TTL after PFCOUNT = %q", got)
	}
	do("PFADD", "src", "x", "y")
	if got := do("PFMERGE", "h", "src"); got != "+OK\r\n" {
		t.Fatalf("PFMERGE = %q", got)
	}
	if got := do("TTL", "h"); got != ":100\r\n" {
		t.Fatalf("TTL after PFMERGE = %q", got)
	}
}

// TestServerPfArity pins the command-level arity errors.
func TestServerPfArity(t *testing.T) {
	do, _ := dispatchServer(t)
	for _, c := range [][]string{
		{"PFADD"},
		{"PFCOUNT"},
		{"PFMERGE"},
		{"PFDEBUG", "ENCODING"},
		{"PFDEBUG", "GETREG", "k", "extra"},
		{"PFSELFTEST", "x"},
	} {
		want := fmt.Sprintf("-ERR wrong number of arguments for '%s' command\r\n", strings.ToLower(c[0]))
		if got := do(c...); got != want {
			t.Fatalf("%v = %q, want %q", c, got, want)
		}
	}
}

// TestStrPfColdReopen writes HLLs through the layer, flushes, and
// reopens cold: the value bytes and the counts must survive the trip
// through the store, sparse and dense alike.
func TestStrPfColdReopen(t *testing.T) {
	r := newStrRig(t)
	ctx := context.Background()

	small := make([][]byte, 30)
	for i := range small {
		small[i] = fmt.Appendf(nil, "s%d", i)
	}
	big := make([][]byte, 5000)
	for i := range big {
		big[i] = fmt.Appendf(nil, "b%d", i)
	}
	if _, err := r.s.PfAdd(ctx, []byte("sp"), small); err != nil {
		t.Fatal(err)
	}
	if _, err := r.s.PfAdd(ctx, []byte("dn"), big); err != nil {
		t.Fatal(err)
	}
	spCount, err := r.s.PfCount(ctx, [][]byte{[]byte("sp")})
	if err != nil {
		t.Fatal(err)
	}
	dnCount, err := r.s.PfCount(ctx, [][]byte{[]byte("dn")})
	if err != nil {
		t.Fatal(err)
	}
	// Snapshots after the counts, so the cached cardinality bytes
	// are part of what must survive the reopen.
	spWant, _ := r.get(r.s, "sp")
	spWant = append([]byte(nil), spWant...)
	dnWant, _ := r.get(r.s, "dn")
	dnWant = append([]byte(nil), dnWant...)
	if len(dnWant) != hllDenseSize {
		t.Fatalf("dense value is %d bytes, want %d", len(dnWant), hllDenseSize)
	}
	r.flush()

	s2 := r.reopen()
	r.want(s2, "sp", spWant)
	r.want(s2, "dn", dnWant)
	if got, err := s2.PfCount(ctx, [][]byte{[]byte("sp")}); err != nil || got != spCount {
		t.Fatalf("cold sparse count = %d (%v), want %d", got, err, spCount)
	}
	if got, err := s2.PfCount(ctx, [][]byte{[]byte("dn")}); err != nil || got != dnCount {
		t.Fatalf("cold dense count = %d (%v), want %d", got, err, dnCount)
	}
	if got, err := s2.PfCount(ctx, [][]byte{[]byte("sp"), []byte("dn")}); err != nil || got < dnCount {
		t.Fatalf("cold union count = %d (%v), want at least %d", got, err, dnCount)
	}
}
