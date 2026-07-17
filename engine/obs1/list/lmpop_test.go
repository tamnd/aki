package list

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/tamnd/aki/engine/obs1/shard"
)

// LMPOP (spec 2064/f3/13 M3 slice 8), the non-blocking multi-key list pop. The
// suite proves the popped key, the popped elements, and the post-pop list state
// match a naive reference model across numkeys, both ends, COUNT clamping, and
// first-non-empty-key selection, over the inline and native bands, plus the parse
// and WRONGTYPE corners and a byte-exact live parity replay.

// --- the reference model --------------------------------------------------

// lmpopModel is the naive multi-key pop over a map of key to element slice: walk
// keys in order, pop min(count, len) off the head when front is true and the tail
// otherwise, drop an emptied key, and report the chosen key with the popped
// elements in pop order. ok is false when every key is missing or empty.
func lmpopModel(m map[string][]string, keys []string, front bool, count int) (key string, popped []string, ok bool) {
	for _, k := range keys {
		xs := m[k]
		if len(xs) == 0 {
			continue
		}
		n := count
		if n > len(xs) {
			n = len(xs)
		}
		if front {
			popped = append([]string(nil), xs[:n]...)
			m[k] = append([]string(nil), xs[n:]...)
		} else {
			for i := 0; i < n; i++ {
				popped = append(popped, xs[len(xs)-1-i])
			}
			m[k] = append([]string(nil), xs[:len(xs)-n]...)
		}
		if len(m[k]) == 0 {
			delete(m, k)
		}
		return k, popped, true
	}
	return "", nil, false
}

// --- the differential -----------------------------------------------------

func TestLmpopOracle(t *testing.T) {
	// Inline values stay in the listpack band; the wide values push a seed of a few
	// hundred into the native chunked deque, so the pop walks past chunk boundaries.
	bands := []struct {
		name string
		val  func(i int) string
		big  int // seed length that reaches the native band
	}{
		{"inline", func(i int) string { return fmt.Sprintf("v%d", i) }, 6},
		{"native", func(i int) string { return fmt.Sprintf("%04d:", i) + strings.Repeat("x", 100) }, 300},
	}
	dirs := []struct {
		name  string
		front bool
	}{
		{"LEFT", true},
		{"RIGHT", false},
	}
	for _, band := range bands {
		for _, dir := range dirs {
			t.Run(band.name+" "+dir.name, func(t *testing.T) {
				g, cx := newMoveReg()
				seed := make([]string, band.big)
				for i := range seed {
					seed[i] = band.val(i)
				}
				// Two keys: the first is missing so selection skips to the second,
				// which proves both first-non-empty-key selection and the pop itself.
				g.m["b"] = seedList(seed...)
				if band.name == "native" && g.m["b"].nat == nil {
					t.Fatal("seed did not reach the native band")
				}
				model := map[string][]string{"b": append([]string(nil), seed...)}
				keys := [][]byte{[]byte("a"), []byte("b")}
				keyNames := []string{"a", "b"}

				// A count larger than the list clamps to the length and drains it, so
				// the last pop empties and drops the key. Walk counts that pop part of
				// the list, then all of it, then a missing-everywhere pop.
				for _, count := range []int{1, 3, band.big, 2} {
					wantKey, wantPopped, wantOk := lmpopModel(model, keyNames, dir.front, count)
					gotKey, gotPopped, ok, wrong := runLmpop(g, cx, keys, dir.front, count)
					if wrong {
						t.Fatalf("count %d: unexpected WRONGTYPE", count)
					}
					if ok != wantOk {
						t.Fatalf("count %d: ok=%v, want %v", count, ok, wantOk)
					}
					if !wantOk {
						continue
					}
					if gotKey != wantKey {
						t.Fatalf("count %d: key %q, want %q", count, gotKey, wantKey)
					}
					eqState(t, fmt.Sprintf("count %d popped", count), gotPopped, wantPopped)
					eqState(t, fmt.Sprintf("count %d remainder", count), listState(g, "b"), model["b"])
				}
				// After the walk the key is drained and dropped, so the registry no
				// longer holds it, matching Redis deleting an emptied list.
				if _, present := g.m["b"]; present {
					t.Fatal("drained key still in the registry")
				}
			})
		}
	}
}

// runLmpop drives the core and decodes its reply into the chosen key and the
// popped elements, so a test can compare against the model without touching RESP.
func runLmpop(g *reg, cx *shard.Ctx, keys [][]byte, front bool, count int) (key string, popped []string, ok, wrong bool) {
	out, ok, wrong, _ := lmpop(g, cx, nil, keys, front, count)
	if !ok || wrong {
		return "", nil, ok, wrong
	}
	key, popped = decodeKeyElems(out)
	return key, popped, true, false
}

// decodeKeyElems reads the [key, [elem, ...]] RESP the core builds. It is a small
// two-level reader for the one reply shape LMPOP emits, so the model tests do not
// pull in the full harness decoder.
func decodeKeyElems(b []byte) (key string, elems []string) {
	// Outer header *2.
	_, b = readLine(b)
	key, b = readBulk(b)
	// Inner array header.
	line, b := readLine(b)
	n, _ := strconv.Atoi(string(line[1:]))
	for i := 0; i < n; i++ {
		var v string
		v, b = readBulk(b)
		elems = append(elems, v)
	}
	return key, elems
}

func readLine(b []byte) (line, rest []byte) {
	for i := 0; i+1 < len(b); i++ {
		if b[i] == '\r' && b[i+1] == '\n' {
			return b[:i], b[i+2:]
		}
	}
	return b, nil
}

func readBulk(b []byte) (string, []byte) {
	line, rest := readLine(b) // $n
	n, _ := strconv.Atoi(string(line[1:]))
	return string(rest[:n]), rest[n+2:]
}

// An emptied key is dropped from the registry and the string store, both bands.
func TestLmpopDropsEmptiedKey(t *testing.T) {
	for _, band := range []struct {
		name string
		seed []string
	}{
		{"inline", []string{"x", "y", "z"}},
		{"native", bigVals("k", 200)},
	} {
		t.Run(band.name, func(t *testing.T) {
			g, cx := newMoveReg()
			g.m["k"] = seedList(band.seed...)
			_, ok, wrong, _ := lmpop(g, cx, nil, [][]byte{[]byte("k")}, true, len(band.seed))
			if !ok || wrong {
				t.Fatalf("ok=%v wrong=%v, want a full pop", ok, wrong)
			}
			if _, present := g.m["k"]; present {
				t.Fatal("drained key still in the registry")
			}
			if cx.St.Exists([]byte("k"), cx.NowMs) {
				t.Fatal("drained key still in the string store")
			}
		})
	}
}

// A wrong-typed key probed before any poppable key aborts with WRONGTYPE, and a
// poppable key reached first pops and never probes a later wrong-typed key, the
// same order Redis's mpopGenericCommand checks types in.
func TestLmpopWrongType(t *testing.T) {
	t.Run("string before poppable aborts", func(t *testing.T) {
		g, cx := newMoveReg()
		if err := cx.St.Set([]byte("s"), []byte("astring")); err != nil {
			t.Fatalf("seed string: %v", err)
		}
		g.m["b"] = seedList("x")
		_, _, wrong, _ := lmpop(g, cx, nil, [][]byte{[]byte("s"), []byte("b")}, true, 1)
		if !wrong {
			t.Fatal("expected WRONGTYPE for a string probed first")
		}
		eqState(t, "poppable key untouched", listState(g, "b"), []string{"x"})
	})
	t.Run("poppable before string pops", func(t *testing.T) {
		g, cx := newMoveReg()
		g.m["a"] = seedList("x", "y")
		if err := cx.St.Set([]byte("s"), []byte("astring")); err != nil {
			t.Fatalf("seed string: %v", err)
		}
		key, popped, ok, wrong := runLmpop(g, cx, [][]byte{[]byte("a"), []byte("s")}, true, 1)
		if wrong {
			t.Fatal("a poppable first key must not report WRONGTYPE on a later key")
		}
		if !ok || key != "a" {
			t.Fatalf("key %q ok=%v, want a", key, ok)
		}
		eqState(t, "popped", popped, []string{"x"})
	})
}

// --- the command harness --------------------------------------------------

// The LMPOP surface as a self-contained harness so the parse-error and parity
// tests drive LMPOP, RPUSH, LRANGE, and SET without touching the shared list
// harness.
const (
	mpLmpop byte = iota + 1
	mpRpush
	mpLrange
	mpSet
	mpLast
)

func lmpopHarness(t *testing.T) *shard.Runtime {
	t.Helper()
	h := make([]shard.Handler, mpLast)
	h[mpLmpop] = Lmpop
	h[mpRpush] = Rpush
	h[mpLrange] = Lrange
	h[mpSet] = func(cx *shard.Ctx, args [][]byte, r shard.Reply) {
		if err := cx.St.Set(args[0], args[1]); err != nil {
			r.Err("ERR " + err.Error())
			return
		}
		r.Status("OK")
	}
	rt := shard.New(1, 8<<20, 1<<18)
	rt.Use(h)
	rt.Start()
	t.Cleanup(rt.Stop)
	return rt
}

// The reply shape and the null array through the wired handler.
func TestLmpopReplyShape(t *testing.T) {
	rt := lmpopHarness(t)
	c := rt.NewConn()
	do(t, c, mpRpush, "k", "a", "b", "c", "d")

	// The two-element array: the key, then the popped elements in pop order.
	got := decodeReply(t, do(t, c, mpLmpop, "1", "k", "LEFT", "COUNT", "2"))
	arr, okArr := got.([]any)
	if !okArr || len(arr) != 2 {
		t.Fatalf("reply = %v, want a two-element array", render(got))
	}
	if s, ok := arr[0].(string); !ok || s != "k" {
		t.Fatalf("reply key = %v, want k", arr[0])
	}
	elems, okElems := arr[1].([]any)
	if !okElems || len(elems) != 2 || elems[0] != "a" || elems[1] != "b" {
		t.Fatalf("reply elems = %v, want [a b]", arr[1])
	}

	// RIGHT pops from the tail, newest first.
	got = decodeReply(t, do(t, c, mpLmpop, "1", "k", "RIGHT"))
	arr, _ = got.([]any)
	if elems, _ := arr[1].([]any); len(elems) != 1 || elems[0] != "d" {
		t.Fatalf("RIGHT reply = %v, want [d]", render(got))
	}

	// Every listed key missing or empty: the null array.
	wantNil(t, do(t, c, mpLmpop, "2", "missing1", "missing2", "LEFT"))
}

// Arity and tail parse errors report Redis's exact texts.
func TestLmpopParseErrors(t *testing.T) {
	rt := lmpopHarness(t)
	c := rt.NewConn()
	do(t, c, mpRpush, "k", "a", "b")

	const errNumkeys = "ERR numkeys should be greater than 0"
	const errCount = "ERR count should be greater than 0"

	// numkeys must be a positive integer.
	wantErr(t, do(t, c, mpLmpop, "0", "k", "LEFT"), errNumkeys)
	wantErr(t, do(t, c, mpLmpop, "-1", "k", "LEFT"), errNumkeys)
	wantErr(t, do(t, c, mpLmpop, "x", "k", "LEFT"), errNumkeys)

	// A numkeys past the tail is a syntax error, not an out-of-range panic.
	wantErr(t, do(t, c, mpLmpop, "5", "k", "LEFT"), errSyntax)

	// A missing or misspelled direction token.
	wantErr(t, do(t, c, mpLmpop, "1", "k", "UP"), errSyntax)

	// COUNT count must be positive and well formed.
	wantErr(t, do(t, c, mpLmpop, "1", "k", "LEFT", "COUNT", "0"), errCount)
	wantErr(t, do(t, c, mpLmpop, "1", "k", "LEFT", "COUNT", "-2"), errCount)
	wantErr(t, do(t, c, mpLmpop, "1", "k", "LEFT", "COUNT", "x"), errCount)

	// A malformed tail past the direction token.
	wantErr(t, do(t, c, mpLmpop, "1", "k", "LEFT", "EXTRA"), errSyntax)
	wantErr(t, do(t, c, mpLmpop, "1", "k", "LEFT", "NOPE", "1"), errSyntax)
	wantErr(t, do(t, c, mpLmpop, "1", "k", "LEFT", "COUNT", "1", "TAIL"), errSyntax)
}

// --- live redis parity ----------------------------------------------------

// lmpopDiffer pairs the LMPOP harness with a live redis for a byte-exact replay,
// the same shape the move parity suite uses.
type lmpopDiffer struct {
	t *testing.T
	c *shard.Conn
	r *redisConn
}

func newLmpopDiffer(t *testing.T) *lmpopDiffer {
	addr := os.Getenv("AKI_REDIS_ADDR")
	if addr == "" {
		t.Skip("set AKI_REDIS_ADDR=host:port to replay LMPOP against a live Redis")
	}
	rc, err := dialRedis(addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	t.Cleanup(rc.close)
	rt := lmpopHarness(t)
	return &lmpopDiffer{t: t, c: rt.NewConn(), r: rc}
}

// agree runs one command on both backends and fails unless the replies decode to
// the same value, so both the reply and, through the LRANGE checks, the post-pop
// state are byte-exact against Redis.
func (d *lmpopDiffer) agree(op byte, verb string, args ...string) any {
	d.t.Helper()
	mine := decodeReply(d.t, do(d.t, d.c, op, args...))
	theirs, err := d.r.cmdReply(append([]string{verb}, args...)...)
	if err != nil {
		d.t.Fatalf("%s %v: redis transport error: %v", verb, args, err)
	}
	if !equalReply(mine, theirs) {
		d.t.Fatalf("%s %v: aki %v, redis %v", verb, args, render(mine), render(theirs))
	}
	return mine
}

func (d *lmpopDiffer) freshKey(name string) string {
	k := "aki:lmpop:" + name
	d.r.cmd("DEL", k)
	return k
}

func TestLMpopAgainstRedis(t *testing.T) {
	d := newLmpopDiffer(t)

	// numkeys 1, both ends, with and without COUNT, and a COUNT that clamps.
	k := d.freshKey("basic")
	d.agree(mpRpush, "RPUSH", k, "a", "b", "c", "d", "e")
	d.agree(mpLmpop, "LMPOP", "1", k, "LEFT")
	d.agree(mpLmpop, "LMPOP", "1", k, "RIGHT")
	d.agree(mpLmpop, "LMPOP", "1", k, "LEFT", "COUNT", "2")
	d.agree(mpLmpop, "LMPOP", "1", k, "RIGHT", "COUNT", "9") // clamps to the rest
	d.agree(mpLrange, "LRANGE", k, "0", "-1")

	// First-non-empty selection across many keys with earlier missing ones.
	a := d.freshKey("a")
	b := d.freshKey("b")
	cc := d.freshKey("c")
	d.agree(mpRpush, "RPUSH", b, "one", "two", "three")
	d.agree(mpLmpop, "LMPOP", "3", a, b, cc, "LEFT", "COUNT", "2")
	d.agree(mpLrange, "LRANGE", b, "0", "-1")

	// Every key missing or empty: the null array, both forms.
	e1 := d.freshKey("e1")
	e2 := d.freshKey("e2")
	d.agree(mpLmpop, "LMPOP", "2", e1, e2, "RIGHT")
	d.agree(mpLmpop, "LMPOP", "2", e1, e2, "LEFT", "COUNT", "3")

	// A native-band, many-chunk list popped across chunk boundaries and drained.
	big := d.freshKey("big")
	block := strings.Repeat("q", 100)
	for i := 0; i < 300; i++ {
		d.agree(mpRpush, "RPUSH", big, fmt.Sprintf("%04d:", i)+block)
	}
	d.agree(mpLmpop, "LMPOP", "1", big, "LEFT", "COUNT", "150")
	d.agree(mpLmpop, "LMPOP", "1", big, "RIGHT", "COUNT", "100")
	d.agree(mpLrange, "LRANGE", big, "0", "-1")
	d.agree(mpLmpop, "LMPOP", "1", big, "LEFT", "COUNT", "1000") // clamps, drains, deletes
	d.agree(mpLmpop, "LMPOP", "1", big, "LEFT")                  // now missing: null array

	// WRONGTYPE on a probed key: a string reached first aborts, a poppable key
	// reached first pops and never probes the string.
	str := d.freshKey("str")
	lst := d.freshKey("lst")
	d.agree(mpSet, "SET", str, "v")
	d.agree(mpRpush, "RPUSH", lst, "x")
	d.agree(mpLmpop, "LMPOP", "2", str, lst, "LEFT")
	d.agree(mpLmpop, "LMPOP", "2", lst, str, "LEFT")

	// numkeys and count bound errors agree on both sides.
	d.agree(mpLmpop, "LMPOP", "0", k, "LEFT")
	d.agree(mpLmpop, "LMPOP", "1", k, "LEFT", "COUNT", "0")
}
