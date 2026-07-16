package list

import (
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/tamnd/aki/engine/obs1/shard"
	"github.com/tamnd/aki/engine/obs1/store"
)

// LMOVE and RPOPLPUSH (spec 2064/f3/13 M3 slice 6). The suite proves the moved
// element and both keys' post-move order match a naive reference model across
// every direction combination and edge: all four LMOVE directions, RPOPLPUSH,
// same-key rotation, a missing or empty source, WRONGTYPE on either side, a
// source drained to empty (the key is deleted), a destination created fresh, and
// values spanning the inline and native bands so a move exercises the promotion.
// A separate regression test carries the P9 ordered-commit lesson.

// newMoveReg builds an isolated registry and a context over a fresh store, the
// way set/smove_test.go's newCtx does, so the core lmove can be driven directly
// without a runtime. Shared with the move benchmarks.
func newMoveReg() (*reg, *shard.Ctx) {
	cx := &shard.Ctx{St: store.New(16<<20, 1<<20), NowMs: 1}
	return &reg{m: make(map[string]*list)}, cx
}

// seedList builds a list and pushes vals onto the tail, the same order an RPUSH
// stream lands them, so a wide-enough seed promotes to the native band exactly as
// it would live.
func seedList(vals ...string) *list {
	l := newList()
	for _, v := range vals {
		l.pushBack([]byte(v))
	}
	return l
}

// bigVals returns n distinct values wide enough that a list of them crosses the
// listpack budget into the native chunked band, with several chunks so a move
// walks past a chunk boundary.
func bigVals(prefix string, n int) []string {
	out := make([]string, n)
	for i := 0; i < n; i++ {
		out[i] = fmt.Sprintf("%s%04d:", prefix, i) + strings.Repeat("x", 100)
	}
	return out
}

// listState reads a key's list into a []string, nil for a dropped or absent key,
// so it compares cleanly against the model.
func listState(g *reg, key string) []string {
	l := g.m[key]
	if l == nil {
		return nil
	}
	return decode(l)
}

// --- the reference model --------------------------------------------------

// popStr pops the head when left is true and the tail otherwise, returning the
// popped value and a fresh remainder slice.
func popStr(xs []string, left bool) (string, []string) {
	if left {
		return xs[0], append([]string(nil), xs[1:]...)
	}
	return xs[len(xs)-1], append([]string(nil), xs[:len(xs)-1]...)
}

// pushStr pushes onto the head when left is true and the tail otherwise,
// returning a fresh slice.
func pushStr(xs []string, v string, left bool) []string {
	if left {
		return append([]string{v}, xs...)
	}
	return append(append([]string(nil), xs...), v)
}

// lmoveModel is the naive move over a map of key to element slice: pop the source
// end, push the destination end, drop an emptied distinct source, and rotate in
// place for the same-key case. moved is the element and ok is false when the
// source is missing or empty.
func lmoveModel(m map[string][]string, srcKey, dstKey string, srcLeft, dstLeft bool) (moved string, ok bool) {
	src := m[srcKey]
	if len(src) == 0 {
		return "", false
	}
	v, rest := popStr(src, srcLeft)
	if srcKey == dstKey {
		m[srcKey] = pushStr(rest, v, dstLeft)
		return v, true
	}
	m[dstKey] = pushStr(m[dstKey], v, dstLeft)
	if len(rest) == 0 {
		delete(m, srcKey)
	} else {
		m[srcKey] = rest
	}
	return v, true
}

// --- the differential -----------------------------------------------------

func TestLmoveOracle(t *testing.T) {
	dirs := []struct {
		name             string
		srcLeft, dstLeft bool
	}{
		{"L L", true, true},
		{"L R", true, false},
		{"R L", false, true}, // the RPOPLPUSH move
		{"R R", false, false},
	}
	// Both a short inline seed and a wide native seed, so the move exercises the
	// listpack band and the chunked deque and the two agree with the model.
	seeds := []struct {
		name       string
		src, dst   []string
		wantNative bool
	}{
		{"inline", []string{"a", "b", "c", "d"}, []string{"x", "y"}, false},
		{"native", bigVals("s", 200), bigVals("d", 200), true},
	}
	for _, sd := range seeds {
		for _, dir := range dirs {
			t.Run(sd.name+" "+dir.name, func(t *testing.T) {
				g, cx := newMoveReg()
				g.m["s"] = seedList(sd.src...)
				g.m["d"] = seedList(sd.dst...)
				if sd.wantNative && (g.m["s"].nat == nil || g.m["d"].nat == nil) {
					t.Fatal("seed did not promote to the native band")
				}
				model := map[string][]string{
					"s": append([]string(nil), sd.src...),
					"d": append([]string(nil), sd.dst...),
				}
				wantMoved, wantOk := lmoveModel(model, "s", "d", dir.srcLeft, dir.dstLeft)
				gotMoved, ok, wrong := lmove(g, cx, []byte("s"), []byte("d"), dir.srcLeft, dir.dstLeft)
				if wrong {
					t.Fatal("unexpected WRONGTYPE")
				}
				if ok != wantOk || string(gotMoved) != wantMoved {
					t.Fatalf("moved %q,%v want %q,%v", gotMoved, ok, wantMoved, wantOk)
				}
				eqState(t, "source", listState(g, "s"), model["s"])
				eqState(t, "destination", listState(g, "d"), model["d"])
			})
		}
	}
}

// Same-key rotation: every direction combination rotates exactly as the model
// says, across both bands. LEFT LEFT and RIGHT RIGHT are identities, LEFT RIGHT
// rotates left, and RIGHT LEFT is the RPOPLPUSH rotation.
func TestLmoveSameKeyRotation(t *testing.T) {
	dirs := []struct {
		name             string
		srcLeft, dstLeft bool
	}{
		{"L L", true, true},
		{"L R", true, false},
		{"R L", false, true},
		{"R R", false, false},
	}
	for _, band := range []struct {
		name       string
		vals       []string
		wantNative bool
	}{
		{"inline", []string{"a", "b", "c", "d", "e"}, false},
		{"native", bigVals("k", 200), true},
	} {
		for _, dir := range dirs {
			t.Run(band.name+" "+dir.name, func(t *testing.T) {
				g, cx := newMoveReg()
				g.m["k"] = seedList(band.vals...)
				if band.wantNative && g.m["k"].nat == nil {
					t.Fatal("seed did not promote")
				}
				model := map[string][]string{"k": append([]string(nil), band.vals...)}
				wantMoved, _ := lmoveModel(model, "k", "k", dir.srcLeft, dir.dstLeft)
				gotMoved, ok, wrong := lmove(g, cx, []byte("k"), []byte("k"), dir.srcLeft, dir.dstLeft)
				if wrong || !ok {
					t.Fatalf("ok=%v wrong=%v, want a move", ok, wrong)
				}
				if string(gotMoved) != wantMoved {
					t.Fatalf("moved %q, want %q", gotMoved, wantMoved)
				}
				eqState(t, "same key", listState(g, "k"), model["k"])
				// The key is never dropped by a same-key move: the element stays in
				// the one list it rotates within.
				if g.m["k"] == nil {
					t.Fatal("same-key move dropped the key")
				}
			})
		}
	}
}

// A missing or empty source moves nothing, replies not-ok (a null bulk at the
// command layer), and leaves the destination untouched.
func TestLmoveMissingSource(t *testing.T) {
	g, cx := newMoveReg()
	g.m["d"] = seedList("x", "y")
	moved, ok, wrong := lmove(g, cx, []byte("missing"), []byte("d"), false, true)
	if wrong {
		t.Fatal("unexpected WRONGTYPE")
	}
	if ok || moved != nil {
		t.Fatalf("moved %q,%v want no move", moved, ok)
	}
	eqState(t, "destination untouched", listState(g, "d"), []string{"x", "y"})
	if g.m["missing"] != nil {
		t.Fatal("a missing source must not be created")
	}
}

// The last element leaving a distinct source deletes the key (Redis deletes an
// emptied list): it is gone from the registry and the string store.
func TestLmoveDrainsSource(t *testing.T) {
	g, cx := newMoveReg()
	g.m["s"] = seedList("only")
	moved, ok, wrong := lmove(g, cx, []byte("s"), []byte("d"), false, true)
	if wrong || !ok || string(moved) != "only" {
		t.Fatalf("moved %q ok=%v wrong=%v", moved, ok, wrong)
	}
	if _, present := g.m["s"]; present {
		t.Fatal("source still in the registry after its last element moved")
	}
	if cx.St.Exists([]byte("s"), cx.NowMs) {
		t.Fatal("source still exists in the string store")
	}
	eqState(t, "destination", listState(g, "d"), []string{"only"})
}

// A move into a missing destination creates it, the same create path the pushes
// use.
func TestLmoveCreatesDestination(t *testing.T) {
	g, cx := newMoveReg()
	g.m["s"] = seedList("a", "b", "c")
	moved, ok, wrong := lmove(g, cx, []byte("s"), []byte("d"), true, true)
	if wrong || !ok || string(moved) != "a" {
		t.Fatalf("moved %q ok=%v wrong=%v", moved, ok, wrong)
	}
	if g.m["d"] == nil {
		t.Fatal("destination not created")
	}
	eqState(t, "created destination", listState(g, "d"), []string{"a"})
	eqState(t, "source", listState(g, "s"), []string{"b", "c"})
}

// A move whose element crosses the destination's byte budget promotes the
// destination one way to the native band, and a native source stays native after
// a pop (F4, no downward conversion).
func TestLmovePromotesDestination(t *testing.T) {
	g, cx := newMoveReg()
	// Fill the destination to just under the listpack budget with 80-byte values:
	// 98 in stays listpack (the live boundary is 99, TestPromotionAtBudget).
	dst := newList()
	val := strings.Repeat("z", 80)
	for i := 0; i < 98; i++ {
		dst.pushBack([]byte(val))
	}
	if dst.encoding() != encListpack {
		t.Fatalf("seed dst is %s, want listpack", dst.encoding())
	}
	g.m["d"] = dst
	g.m["s"] = seedList(val) // one more 80-byte element to push across the budget
	if _, ok, _ := lmove(g, cx, []byte("s"), []byte("d"), false, false); !ok {
		t.Fatal("expected a move")
	}
	if g.m["d"].encoding() != encQuicklist {
		t.Fatalf("dst is %s, want quicklist after the move crossed the budget", g.m["d"].encoding())
	}

	// A native source stays native after a pop drains one element.
	g2, cx2 := newMoveReg()
	g2.m["s"] = seedList(bigVals("s", 200)...)
	if g2.m["s"].encoding() != encQuicklist {
		t.Fatalf("seed src is %s, want quicklist", g2.m["s"].encoding())
	}
	g2.m["d"] = seedList("1")
	lmove(g2, cx2, []byte("s"), []byte("d"), false, true)
	if g2.m["s"].encoding() != encQuicklist {
		t.Fatalf("src is %s, want sticky quicklist after a pop (F4)", g2.m["s"].encoding())
	}
}

// WRONGTYPE on either side follows Redis's lmoveGenericCommand order: a string
// source errors, a string destination behind a present source errors, but a
// string destination behind a missing source does not (Redis replies the null
// bulk without ever checking the destination type), so no half-move is possible.
func TestLmoveWrongType(t *testing.T) {
	t.Run("source is a string", func(t *testing.T) {
		g, cx := newMoveReg()
		if err := cx.St.Set([]byte("s"), []byte("astring")); err != nil {
			t.Fatalf("seed string: %v", err)
		}
		g.m["d"] = seedList("1")
		_, _, wrong := lmove(g, cx, []byte("s"), []byte("d"), false, true)
		if !wrong {
			t.Fatal("expected WRONGTYPE for a string source")
		}
	})
	t.Run("destination is a string, source present", func(t *testing.T) {
		g, cx := newMoveReg()
		g.m["s"] = seedList("a", "b")
		if err := cx.St.Set([]byte("d"), []byte("astring")); err != nil {
			t.Fatalf("seed string: %v", err)
		}
		_, _, wrong := lmove(g, cx, []byte("s"), []byte("d"), false, true)
		if !wrong {
			t.Fatal("expected WRONGTYPE for a string destination")
		}
		// The source is untouched: the type check preceded the pop.
		eqState(t, "source untouched", listState(g, "s"), []string{"a", "b"})
	})
	t.Run("destination is a string, source missing", func(t *testing.T) {
		g, cx := newMoveReg()
		if err := cx.St.Set([]byte("d"), []byte("astring")); err != nil {
			t.Fatalf("seed string: %v", err)
		}
		// Redis stops at the missing source and replies the null bulk, never
		// checking the destination type, so this is not a WRONGTYPE.
		_, ok, wrong := lmove(g, cx, []byte("s"), []byte("d"), false, true)
		if wrong {
			t.Fatal("a missing source must not report WRONGTYPE on the destination")
		}
		if ok {
			t.Fatal("a missing source must not move anything")
		}
	})
}

// TestMovePhantomHoleOrderedCommit encodes the P9 / L15 lesson (spec 2064/f3/19
// the P9 row, 01-f1-estate-and-lessons.md line 376, 03-execution-model.md the
// push-window section): reserve-then-fill without an ordered commit exposes a
// phantom hole, a slot inside the live range that resolves to no element, and
// pure-push benchmarks hide it. Under single ownership the v1 multi-writer
// reservation deflates to a plain ordered deque append, so after a move and a
// churn of moves the list must never expose a hole: every index in [0, len)
// resolves to a real element, the lengths stay consistent, and the element
// multiset is conserved, so each element leaves its source exactly once and
// enters its destination exactly once, never in neither and never duplicated.
// This is the same ordered-commit discipline the durability-log append inherits
// at M8 (the log itself is not built yet), carried here as the move's regression
// guard.
func TestMovePhantomHoleOrderedCommit(t *testing.T) {
	g, cx := newMoveReg()
	// Two native-band lists deep enough to span several chunks, so a rotation
	// walks past a chunk boundary (chunkElemCap 128, chunkBlobCap 4096).
	aVals := bigVals("a", 300)
	bVals := bigVals("b", 300)
	g.m["a"] = seedList(aVals...)
	g.m["b"] = seedList(bVals...)
	if g.m["a"].nat == nil || g.m["b"].nat == nil {
		t.Fatal("seed did not reach the native band")
	}
	want := multiset(aVals, bVals)
	check := func(step string) {
		t.Helper()
		assertNoHole(t, step, g.m["a"])
		assertNoHole(t, step, g.m["b"])
		got := multiset(listState(g, "a"), listState(g, "b"))
		if !reflect.DeepEqual(want, got) {
			t.Fatalf("%s: element multiset drifted, a move dropped or duplicated an element", step)
		}
	}
	// Same-key RPOPLPUSH rotations: the tail chunk drains and the head chunk fills,
	// crossing chunk boundaries. A phantom hole would surface as a length mismatch
	// or an index that resolves to nothing.
	for i := 0; i < 300; i++ {
		lmove(g, cx, []byte("a"), []byte("a"), false, true)
		check(fmt.Sprintf("same-key rotate %d", i))
	}
	// Cross-key churn both directions, which drains one end while filling another.
	for i := 0; i < 300; i++ {
		if i%2 == 0 {
			lmove(g, cx, []byte("a"), []byte("b"), false, true)
		} else {
			lmove(g, cx, []byte("b"), []byte("a"), true, false)
		}
		check(fmt.Sprintf("cross churn %d", i))
	}
}

// assertNoHole checks the list exposes no phantom hole: its decoded length equals
// length(), and every index in [0, len) resolves through get to exactly the
// element the ordered walk sees.
func assertNoHole(t *testing.T, step string, l *list) {
	t.Helper()
	if l == nil {
		return
	}
	n := l.length()
	dec := decode(l)
	if len(dec) != n {
		t.Fatalf("%s: length() = %d but the list decodes to %d elements", step, n, len(dec))
	}
	for i := 0; i < n; i++ {
		got := l.get(i)
		if got == nil {
			t.Fatalf("%s: index %d in [0,%d) resolves to no element (phantom hole)", step, i, n)
		}
		if string(got) != dec[i] {
			t.Fatalf("%s: index %d = %q but the ordered walk has %q", step, i, got, dec[i])
		}
	}
}

func multiset(groups ...[]string) map[string]int {
	m := make(map[string]int)
	for _, grp := range groups {
		for _, s := range grp {
			m[s]++
		}
	}
	return m
}

func eqState(t *testing.T, what string, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: length %d, want %d (got %v, want %v)", what, len(got), len(want), got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("%s: elem %d = %q, want %q (got %v, want %v)", what, i, got[i], want[i], got, want)
		}
	}
}

// --- live redis parity ----------------------------------------------------

// The move ops as a self-contained harness so the parity tests drive LMOVE,
// RPOPLPUSH, RPUSH, LRANGE, and SET without touching the shared list harness.
const (
	mvLmove byte = iota + 1
	mvRpoplpush
	mvRpush
	mvLrange
	mvSet
	mvLast
)

func moveHarness(t *testing.T) *shard.Runtime {
	t.Helper()
	h := make([]shard.Handler, mvLast)
	h[mvLmove] = Lmove
	h[mvRpoplpush] = Rpoplpush
	h[mvRpush] = Rpush
	h[mvLrange] = Lrange
	h[mvSet] = func(cx *shard.Ctx, args [][]byte, r shard.Reply) {
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

// moveDiffer pairs the move harness with a live redis for a byte-exact replay.
type moveDiffer struct {
	t *testing.T
	c *shard.Conn
	r *redisConn
}

func newMoveDiffer(t *testing.T) *moveDiffer {
	addr := os.Getenv("AKI_REDIS_ADDR")
	if addr == "" {
		t.Skip("set AKI_REDIS_ADDR=host:port to replay the move commands against a live Redis")
	}
	rc, err := dialRedis(addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	t.Cleanup(rc.close)
	rt := moveHarness(t)
	return &moveDiffer{t: t, c: rt.NewConn(), r: rc}
}

// agree runs one command on both backends and fails unless the replies decode to
// the same value, so both the reply and, through the LRANGE checks, the post-move
// state are byte-exact against Redis.
func (d *moveDiffer) agree(op byte, verb string, args ...string) any {
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

func (d *moveDiffer) freshKey(name string) string {
	k := "aki:lmove:" + name
	d.r.cmd("DEL", k)
	return k
}

func TestLMoveAgainstRedis(t *testing.T) {
	d := newMoveDiffer(t)
	dirs := [][2]string{{"LEFT", "LEFT"}, {"LEFT", "RIGHT"}, {"RIGHT", "LEFT"}, {"RIGHT", "RIGHT"}}
	for _, dir := range dirs {
		src := d.freshKey("src:" + dir[0] + dir[1])
		dst := d.freshKey("dst:" + dir[0] + dir[1])
		d.agree(mvRpush, "RPUSH", src, "a", "b", "c", "d")
		d.agree(mvLmove, "LMOVE", src, dst, dir[0], dir[1])
		d.agree(mvLmove, "LMOVE", src, dst, dir[0], dir[1])
		d.agree(mvLrange, "LRANGE", src, "0", "-1")
		d.agree(mvLrange, "LRANGE", dst, "0", "-1")
	}

	// Same-key rotation, all four combos.
	for _, dir := range dirs {
		key := d.freshKey("rot:" + dir[0] + dir[1])
		d.agree(mvRpush, "RPUSH", key, "1", "2", "3", "4", "5")
		d.agree(mvLmove, "LMOVE", key, key, dir[0], dir[1])
		d.agree(mvLmove, "LMOVE", key, key, dir[0], dir[1])
		d.agree(mvLrange, "LRANGE", key, "0", "-1")
	}

	// Empty and missing source: a null bulk, no side effect.
	absent := d.freshKey("absent")
	dst := d.freshKey("nulldst")
	d.agree(mvLmove, "LMOVE", absent, dst, "LEFT", "RIGHT")
	d.agree(mvLrange, "LRANGE", dst, "0", "-1")

	// Draining a source to empty deletes it: the second move finds it gone.
	one := d.freshKey("one")
	oneDst := d.freshKey("onedst")
	d.agree(mvRpush, "RPUSH", one, "solo")
	d.agree(mvLmove, "LMOVE", one, oneDst, "LEFT", "LEFT")
	d.agree(mvLmove, "LMOVE", one, oneDst, "LEFT", "LEFT")
	d.agree(mvLrange, "LRANGE", one, "0", "-1")
	d.agree(mvLrange, "LRANGE", oneDst, "0", "-1")

	// WRONGTYPE on the source and on the destination (present source).
	strKey := d.freshKey("str")
	d.agree(mvSet, "SET", strKey, "v")
	listKey := d.freshKey("list")
	d.agree(mvRpush, "RPUSH", listKey, "a")
	d.agree(mvLmove, "LMOVE", strKey, listKey, "LEFT", "RIGHT")
	d.agree(mvLmove, "LMOVE", listKey, strKey, "LEFT", "RIGHT")

	// Invalid direction is a syntax error.
	d.agree(mvLmove, "LMOVE", listKey, listKey, "SIDEWAYS", "LEFT")

	// A native-band, many-chunk move: build a wide list and move across the ring.
	big := d.freshKey("big")
	bigDst := d.freshKey("bigdst")
	block := strings.Repeat("q", 100)
	for i := 0; i < 300; i++ {
		d.agree(mvRpush, "RPUSH", big, fmt.Sprintf("%04d:", i)+block)
	}
	for i := 0; i < 50; i++ {
		d.agree(mvLmove, "LMOVE", big, bigDst, "RIGHT", "LEFT")
	}
	d.agree(mvLrange, "LRANGE", big, "0", "-1")
	d.agree(mvLrange, "LRANGE", bigDst, "0", "-1")
}

func TestRpoplpushAgainstRedis(t *testing.T) {
	d := newMoveDiffer(t)

	// Cross-key RPOPLPUSH: pops the source tail, pushes the destination head.
	src := d.freshKey("src")
	dst := d.freshKey("dst")
	d.agree(mvRpush, "RPUSH", src, "a", "b", "c")
	d.agree(mvRpoplpush, "RPOPLPUSH", src, dst)
	d.agree(mvRpoplpush, "RPOPLPUSH", src, dst)
	d.agree(mvLrange, "LRANGE", src, "0", "-1")
	d.agree(mvLrange, "LRANGE", dst, "0", "-1")

	// Same-key rotation moves the tail to the head.
	rot := d.freshKey("rot")
	d.agree(mvRpush, "RPUSH", rot, "1", "2", "3", "4")
	for i := 0; i < 4; i++ {
		d.agree(mvRpoplpush, "RPOPLPUSH", rot, rot)
		d.agree(mvLrange, "LRANGE", rot, "0", "-1")
	}

	// Missing source: a null bulk with no side effect.
	absent := d.freshKey("absent")
	nullDst := d.freshKey("nulldst")
	d.agree(mvRpoplpush, "RPOPLPUSH", absent, nullDst)
	d.agree(mvLrange, "LRANGE", nullDst, "0", "-1")

	// WRONGTYPE on the source and on the destination.
	strKey := d.freshKey("str")
	d.agree(mvSet, "SET", strKey, "v")
	listKey := d.freshKey("list")
	d.agree(mvRpush, "RPUSH", listKey, "a")
	d.agree(mvRpoplpush, "RPOPLPUSH", strKey, listKey)
	d.agree(mvRpoplpush, "RPOPLPUSH", listKey, strKey)

	// A native-band, many-chunk same-key rotation.
	big := d.freshKey("big")
	block := strings.Repeat("q", 100)
	for i := 0; i < 300; i++ {
		d.agree(mvRpush, "RPUSH", big, fmt.Sprintf("%04d:", i)+block)
	}
	for i := 0; i < 50; i++ {
		d.agree(mvRpoplpush, "RPOPLPUSH", big, big)
	}
	d.agree(mvLrange, "LRANGE", big, "0", "-1")
}
