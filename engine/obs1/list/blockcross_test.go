package list

import (
	"bytes"
	"fmt"
	"reflect"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/tamnd/aki/engine/obs1/shard"
)

// Cross-shard blocking pops (blockcross.go): BLPOP, BRPOP, and BLMPOP over keys
// that span shards. The differential suite holds the DoBlockCross path
// byte-identical to the co-located point handler across the serve, first-non-empty,
// drain, and WRONGTYPE cases: the co-located arm reads every key off one shard, the
// cross arm reads them off distinct owners under one barrier, and a client must not
// be able to tell them apart. The Redis-exactness of the co-located pop itself is
// blocking_test.go's job; here the co-located reply is the oracle. The park suite
// then proves the parts the co-located form never exercises: a waiter parked on
// several owners at once, served exactly once by whichever owner's push or timeout
// wins the shared claim, with the losers tearing down their own dead nodes.

// blockList reads a key's whole list through the block harness LRANGE handler into
// a []string, nil for a dropped or absent key. The package's other listAll reader
// is bound to the cross-move harness op table, so the block suite needs its own.
func blockList(t *testing.T, c *shard.Conn, key string) []string {
	t.Helper()
	got := decodeReply(t, do(t, c, bkLrange, key, "0", "-1"))
	arr, ok := got.([]any)
	if !ok {
		return nil
	}
	out := make([]string, len(arr))
	for i := range arr {
		out[i], _ = arr[i].(string)
	}
	return out
}

// crossBlockRuntime is a multi-shard runtime wired with the block harness handlers,
// so a cross pop really rides several owners.
func crossBlockRuntime(t *testing.T, shards int) *shard.Runtime {
	t.Helper()
	rt := shard.New(shards, 8<<20, 1<<18)
	rt.Use(blockHandlers())
	rt.Start()
	t.Cleanup(rt.Stop)
	return rt
}

// strBytes copies a []string into the [][]byte a DoBlockCross intent list and a
// blockCross argument tail take.
func strBytes(a []string) [][]byte {
	out := make([][]byte, len(a))
	for i := range a {
		out[i] = []byte(a[i])
	}
	return out
}

// crossBlpopFire sends a cross-shard BLPOP (front) or BRPOP (!front) the way
// dispatchBlockCross does: DoBlockCross holds an intent on every key while the body
// runs the ordered serve-or-park scan, and ArmBlock guards the reorder slot. args
// is the argument tail, the listed keys then the trailing timeout. It returns
// without waiting, so an immediate serve is read with drainOne and a park is left
// open for a later push or the timeout.
func crossBlpopFire(t *testing.T, c *shard.Conn, front bool, args ...string) {
	t.Helper()
	tail := strBytes(args)
	keys := tail[:len(tail)-1]
	err := c.DoBlockCross(keys, func(tx *shard.Txn, conn *shard.Conn, seq uint32) []byte {
		if front {
			return BlpopCross(tx, conn, seq, tail)
		}
		return BrpopCross(tx, conn, seq, tail)
	})
	if err != nil {
		t.Fatal(err)
	}
	c.ArmBlock()
	c.Flush()
}

// crossBlmpopFire sends a cross-shard BLMPOP the same way: args is the tail, the
// leading timeout then the numkeys/keys/direction/COUNT run BLMPOP shares with
// LMPOP. BlmpopKeys pulls the intent keys out of that tail, the same parse the
// dispatcher's co-location check runs.
func crossBlmpopFire(t *testing.T, c *shard.Conn, args ...string) {
	t.Helper()
	tail := strBytes(args)
	keys := BlmpopKeys(tail)
	if keys == nil {
		t.Fatalf("BlmpopKeys rejected a tail the test meant to be valid: %v", args)
	}
	err := c.DoBlockCross(keys, func(tx *shard.Txn, conn *shard.Conn, seq uint32) []byte {
		return BlmpopCross(tx, conn, seq, tail)
	})
	if err != nil {
		t.Fatal(err)
	}
	c.ArmBlock()
	c.Flush()
}

// pushDrain runs one RPUSH and drains its own reply, so the caller knows the push
// was processed by the owner (and served any waiter it wakes) before it returns. It
// is goroutine-safe: it never calls t.Fatal, so a racing pusher can run it in a
// separate goroutine, and it leaves the connection idle for reuse. vals is one or
// more elements pushed to the tail.
func pushDrain(c *shard.Conn, key string, vals ...string) {
	args := make([][]byte, 0, len(vals)+1)
	args = append(args, []byte(key))
	for _, v := range vals {
		args = append(args, []byte(v))
	}
	_ = c.DoAt(bkRpush, 0, args)
	c.Flush()
	for {
		got := false
		c.DrainReplies(func(b []byte) { got = true })
		if got {
			return
		}
		runtime.Gosched()
	}
}

// crossPopServe fires a cross pop that resolves immediately and returns the one
// reply, the immediate-serve arm of the differential.
func crossPopServe(t *testing.T, c *shard.Conn, front bool, args ...string) []byte {
	t.Helper()
	crossBlpopFire(t, c, front, args...)
	return drainOne(t, c)
}

// eqList fails unless two keys hold the same list, read through LRANGE 0 -1, so a
// cross pop's post-state matches the co-located pop's byte for byte. A string key
// (the WRONGTYPE seed) makes both sides answer the same WRONGTYPE error, which is
// still an exact match.
func eqList(t *testing.T, c *shard.Conn, what, co, x string) {
	t.Helper()
	coR := do(t, c, bkLrange, co, "0", "-1")
	xR := do(t, c, bkLrange, x, "0", "-1")
	if !bytes.Equal(coR, xR) {
		t.Fatalf("%s LRANGE drift on %s/%s: co-located %q, cross-shard %q", what, co, x, coR, xR)
	}
}

// normPop decodes a BLPOP/BRPOP/BLMPOP reply and rewrites the served key (the
// array head) to its index among keys, so a co-located reply and a cross-shard
// reply over the paired keys compare equal even though the two paths name
// different keys. A null array or an error line decodes to a value with no key to
// rewrite and compares as is, so timeout and WRONGTYPE outcomes still match
// exactly. The rewrite also pins the first-non-empty priority: both paths must
// serve the key at the same index.
func normPop(t *testing.T, rep []byte, keys [3]string) any {
	t.Helper()
	v := decodeReply(t, rep)
	arr, ok := v.([]any)
	if !ok || len(arr) == 0 {
		return v
	}
	k, ok := arr[0].(string)
	if !ok {
		return v
	}
	for i := range keys {
		if keys[i] == k {
			arr[0] = fmt.Sprintf("#%d", i)
			break
		}
	}
	return v
}

// eqPop fails unless a co-located reply and a cross-shard reply describe the same
// outcome once the served key is mapped to its index.
func eqPop(t *testing.T, coRep, xRep []byte, co, x [3]string) {
	t.Helper()
	coN := normPop(t, coRep, co)
	xN := normPop(t, xRep, x)
	if !reflect.DeepEqual(coN, xN) {
		t.Fatalf("reply drift: co-located %q -> %v, cross-shard %q -> %v", coRep, render(coN), xRep, render(xN))
	}
}

// popCase is one seeding of three keys for the differential: per-key list values
// (nil is a missing key) or a planted string (the WRONGTYPE seed).
type popCase struct {
	name string
	vals [3][]string
	strs [3]bool
}

var crossPopCases = []popCase{
	{name: "first present", vals: [3][]string{{"a", "b"}, nil, nil}},
	{name: "second present", vals: [3][]string{nil, {"x", "y"}, nil}},
	{name: "third present", vals: [3][]string{nil, nil, {"z"}}},
	{name: "all present first wins", vals: [3][]string{{"a"}, {"b"}, {"c"}}},
	{name: "drains first", vals: [3][]string{{"solo"}, {"x"}, nil}},
	{name: "wrongtype first", strs: [3]bool{true, false, false}},
	{name: "empty then wrongtype", strs: [3]bool{false, true, false}},
	{name: "poppable before wrongtype", vals: [3][]string{{"p"}, nil, nil}, strs: [3]bool{false, true, false}},
	{name: "native band first", vals: [3][]string{bigVals("s", 200), nil, nil}},
}

// seedPop plants one key's contents on both a co-located and a cross-shard key so
// the two paths run over identical data.
func seedPop(t *testing.T, c *shard.Conn, coKey, xKey string, vals []string, str bool) {
	t.Helper()
	if str {
		do(t, c, bkSet, coKey, "notalist")
		do(t, c, bkSet, xKey, "notalist")
		return
	}
	for _, v := range vals {
		do(t, c, bkRpush, coKey, v)
		do(t, c, bkRpush, xKey, v)
	}
}

// TestBlpopCrossImmediateServeDifferential replays every serve-or-error shape on
// both paths for BLPOP and BRPOP: three co-located keys on one shard through the
// point handler, three cross-shard keys on distinct owners through BlpopCross under
// DoBlockCross. The reply bytes and the post-pop LRANGE of all three key pairs must
// agree exactly, so the cross pop is proven identical to the co-located pop.
func TestBlpopCrossImmediateServeDifferential(t *testing.T) {
	rt := crossBlockRuntime(t, 4)
	c := rt.NewConn()

	for ci, tc := range crossPopCases {
		for _, front := range []bool{true, false} {
			dir := "BRPOP"
			bkOp := bkBrpop
			if front {
				dir = "BLPOP"
				bkOp = bkBlpop
			}
			t.Run(tc.name+" "+dir, func(t *testing.T) {
				p := fmt.Sprintf("p%d%s", ci, dir)
				co := [3]string{
					keyOnShard(t, rt, 3, p+"co0"),
					keyOnShard(t, rt, 3, p+"co1"),
					keyOnShard(t, rt, 3, p+"co2"),
				}
				x := [3]string{
					keyOnShard(t, rt, 0, p+"x0"),
					keyOnShard(t, rt, 1, p+"x1"),
					keyOnShard(t, rt, 2, p+"x2"),
				}
				for i := 0; i < 3; i++ {
					seedPop(t, c, co[i], x[i], tc.vals[i], tc.strs[i])
				}

				coRep := do(t, c, bkOp, co[0], co[1], co[2], "0")
				xRep := crossPopServe(t, c, front, x[0], x[1], x[2], "0")
				eqPop(t, coRep, xRep, co, x)
				for i := 0; i < 3; i++ {
					eqList(t, c, fmt.Sprintf("key %d", i), co[i], x[i])
				}
			})
		}
	}
}

// TestBlmpopCrossImmediateServeDifferential is the BLMPOP arm: the cross pop must
// match the co-located point handler's reply and post-pop state across directions
// and counts, including a count larger than the served list (clamped to what is
// there) and the first-non-empty priority across a missing key.
func TestBlmpopCrossImmediateServeDifferential(t *testing.T) {
	rt := crossBlockRuntime(t, 4)
	c := rt.NewConn()

	cases := []struct {
		name  string
		vals  [3][]string
		strs  [3]bool
		dir   string
		count string // "" omits the COUNT clause
	}{
		{name: "left count one", vals: [3][]string{{"a", "b", "c"}, nil, nil}, dir: "LEFT"},
		{name: "right count one", vals: [3][]string{{"a", "b", "c"}, nil, nil}, dir: "RIGHT"},
		{name: "left count two", vals: [3][]string{{"a", "b", "c"}, nil, nil}, dir: "LEFT", count: "2"},
		{name: "right count two", vals: [3][]string{{"a", "b", "c"}, nil, nil}, dir: "RIGHT", count: "2"},
		{name: "count over length", vals: [3][]string{{"a", "b"}, nil, nil}, dir: "LEFT", count: "9"},
		{name: "second wins", vals: [3][]string{nil, {"x", "y", "z"}, nil}, dir: "RIGHT", count: "2"},
		{name: "third wins", vals: [3][]string{nil, nil, {"m", "n"}}, dir: "LEFT", count: "2"},
		{name: "wrongtype first", strs: [3]bool{true, false, false}, dir: "LEFT"},
		{name: "empty then wrongtype", strs: [3]bool{false, true, false}, dir: "LEFT", count: "3"},
	}
	for ci, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := fmt.Sprintf("mp%d", ci)
			co := [3]string{
				keyOnShard(t, rt, 3, p+"co0"),
				keyOnShard(t, rt, 3, p+"co1"),
				keyOnShard(t, rt, 3, p+"co2"),
			}
			x := [3]string{
				keyOnShard(t, rt, 0, p+"x0"),
				keyOnShard(t, rt, 1, p+"x1"),
				keyOnShard(t, rt, 2, p+"x2"),
			}
			for i := 0; i < 3; i++ {
				seedPop(t, c, co[i], x[i], tc.vals[i], tc.strs[i])
			}

			coArgs := []string{"0", "3", co[0], co[1], co[2], tc.dir}
			xArgs := []string{"0", "3", x[0], x[1], x[2], tc.dir}
			if tc.count != "" {
				coArgs = append(coArgs, "COUNT", tc.count)
				xArgs = append(xArgs, "COUNT", tc.count)
			}

			coRep := do(t, c, bkBlmpop, coArgs...)
			crossBlmpopFire(t, c, xArgs...)
			xRep := drainOne(t, c)
			eqPop(t, coRep, xRep, co, x)
			for i := 0; i < 3; i++ {
				eqList(t, c, fmt.Sprintf("key %d", i), co[i], x[i])
			}
		})
	}
}

// TestBlpopCrossTimeoutErrorsDifferential proves the timeout guards answer in place
// on the cross path too: a negative or non-numeric timeout is an immediate error
// reply, byte-identical to the co-located point handler, with no park.
func TestBlpopCrossTimeoutErrorsDifferential(t *testing.T) {
	rt := crossBlockRuntime(t, 4)
	c := rt.NewConn()
	k0 := keyOnShard(t, rt, 0, "terr0")
	k1 := keyOnShard(t, rt, 1, "terr1")

	for _, bad := range []string{"-1", "-0.5", "notanumber", "nan", "inf"} {
		coRep := do(t, c, bkBlpop, k0, k1, bad)
		xRep := crossPopServe(t, c, true, k0, k1, bad)
		if !bytes.Equal(coRep, xRep) {
			t.Fatalf("timeout %q drift: co-located %q, cross-shard %q", bad, coRep, xRep)
		}
	}
}

// TestBlpopCrossParkThenServe parks a cross-shard BLPOP on three keys on three
// owners, then a push on the middle key's owner serves it once and unlinks the
// waiter from the other two owners. Later pushes on those two keys must not wake the
// already served waiter, and their pushed values stay in place, the cross analog of
// the co-located sibling-unlink test.
func TestBlpopCrossParkThenServe(t *testing.T) {
	rt := crossBlockRuntime(t, 4)
	a := rt.NewConn()
	b := rt.NewConn()
	k0 := keyOnShard(t, rt, 0, "psk0")
	k1 := keyOnShard(t, rt, 1, "psk1")
	k2 := keyOnShard(t, rt, 2, "psk2")

	crossBlpopFire(t, a, true, k0, k1, k2, "0")
	noReply(t, a, 30*time.Millisecond) // all three empty, the waiter is parked
	wantInt(t, do(t, b, bkRpush, k1, "v"), 1)
	wantArray(t, drainOne(t, a), k1, "v")

	// The served waiter is gone from k0 and k2: pushes there stay put and never wake it.
	wantInt(t, do(t, b, bkRpush, k0, "w"), 1)
	wantInt(t, do(t, b, bkRpush, k2, "z"), 1)
	noReply(t, a, 60*time.Millisecond)
	wantArray(t, do(t, b, bkLrange, k0, "0", "-1"), "w")
	wantArray(t, do(t, b, bkLrange, k2, "0", "-1"), "z")
}

// TestBrpopCrossParkThenServePopsTail proves a parked cross BRPOP pops the served
// key's tail when a push finally feeds it, the end the front flag threads through
// the waiter node to the serve.
func TestBrpopCrossParkThenServePopsTail(t *testing.T) {
	rt := crossBlockRuntime(t, 4)
	a := rt.NewConn()
	b := rt.NewConn()
	k0 := keyOnShard(t, rt, 0, "tail0")
	k1 := keyOnShard(t, rt, 1, "tail1")

	crossBlpopFire(t, a, false, k0, k1, "0")
	noReply(t, a, 30*time.Millisecond)
	wantInt(t, do(t, b, bkRpush, k1, "x", "y", "z"), 3)
	wantArray(t, drainOne(t, a), k1, "z") // tail
	wantArray(t, do(t, b, bkLrange, k1, "0", "-1"), "x", "y")
}

// TestBlmpopCrossParkThenServe parks a cross BLMPOP and proves a later push on one
// of its owners wakes it and pops up to its count off the recorded end.
func TestBlmpopCrossParkThenServe(t *testing.T) {
	rt := crossBlockRuntime(t, 4)
	a := rt.NewConn()
	b := rt.NewConn()
	k0 := keyOnShard(t, rt, 0, "mppark0")
	k1 := keyOnShard(t, rt, 1, "mppark1")

	crossBlmpopFire(t, a, "0", "2", k0, k1, "LEFT", "COUNT", "2")
	noReply(t, a, 30*time.Millisecond)
	wantInt(t, do(t, b, bkRpush, k0, "a", "b", "c"), 3)
	// [k0, [a, b]] off the head, count 2, leaving c.
	got := decodeReply(t, drainOne(t, a))
	arr, ok := got.([]any)
	if !ok || len(arr) != 2 || arr[0] != k0 {
		t.Fatalf("reply = %v, want [%s [a b]]", render(got), k0)
	}
	inner, ok := arr[1].([]any)
	if !ok || len(inner) != 2 || inner[0] != "a" || inner[1] != "b" {
		t.Fatalf("popped = %v, want [a b]", render(arr[1]))
	}
	wantArray(t, do(t, b, bkLrange, k0, "0", "-1"), "c")
}

// TestBlpopCrossTimeoutNullArray parks a cross BLPOP with a finite timeout on
// all-missing keys spanning shards and proves the timer on the first owner fires
// and delivers the RESP2 null array, cancelling the sibling nodes on the others.
func TestBlpopCrossTimeoutNullArray(t *testing.T) {
	rt := crossBlockRuntime(t, 4)
	a := rt.NewConn()
	k0 := keyOnShard(t, rt, 0, "toa0")
	k1 := keyOnShard(t, rt, 1, "toa1")

	start := time.Now()
	crossBlpopFire(t, a, true, k0, k1, "0.1")
	wantNil(t, drainOne(t, a))
	if elapsed := time.Since(start); elapsed < 80*time.Millisecond {
		t.Fatalf("timeout fired after %v, want at least ~100ms", elapsed)
	}
	// A push after the timeout stays put: the waiter is gone from every owner.
	b := rt.NewConn()
	wantInt(t, do(t, b, bkRpush, k1, "late"), 1)
	noReply(t, a, 40*time.Millisecond)
	wantArray(t, do(t, b, bkLrange, k1, "0", "-1"), "late")
}

// TestBlpopCrossTimeoutRaceServe pits a finite timeout against a push arriving at
// about the same moment: exactly one of them completes the waiter and the other
// steps aside, so the client sees either the served pair or the null array but
// never both. Repeated under the race detector this is the timeout-versus-serve arm
// of the one-winner claim.
func TestBlpopCrossTimeoutRaceServe(t *testing.T) {
	rt := crossBlockRuntime(t, 4)
	for iter := 0; iter < 60; iter++ {
		a := rt.NewConn()
		b := rt.NewConn()
		k0 := keyOnShard(t, rt, 0, fmt.Sprintf("tr%d_0", iter))
		k1 := keyOnShard(t, rt, 1, fmt.Sprintf("tr%d_1", iter))

		crossBlpopFire(t, a, true, k0, k1, "0.03")
		done := make(chan struct{})
		go func() { pushDrain(b, k1, "v"); close(done) }()

		got := decodeReply(t, drainOne(t, a))
		<-done // the push is fully processed, so the list state is settled
		switch v := got.(type) {
		case nil:
			// Timeout won. The pushed element stays in the list.
			wantArray(t, do(t, b, bkLrange, k1, "0", "-1"), "v")
		case []any:
			if len(v) != 2 || v[0] != k1 || v[1] != "v" {
				t.Fatalf("iter %d served %v, want [%s v]", iter, render(got), k1)
			}
			// Serve won. The element is gone.
			wantEmptyArray(t, do(t, b, bkLrange, k1, "0", "-1"))
		default:
			t.Fatalf("iter %d reply = %v, want null array or served pair", iter, render(got))
		}
		noReply(t, a, 5*time.Millisecond) // never a second reply
	}
}

// TestBlpopCrossExactlyOneWinner is the central race the atomic claim exists for: a
// waiter parked on two owners, then two pushes fired at once, one onto each owner.
// Exactly one push serves the client and the other leaves its element in place, so
// the reply names one key and the other key retains its push. Repeated over many
// iterations under the race detector, this proves the CAS admits one winner and the
// loser tears down only its own dead node.
func TestBlpopCrossExactlyOneWinner(t *testing.T) {
	rt := crossBlockRuntime(t, 4)
	for iter := 0; iter < 150; iter++ {
		a := rt.NewConn()
		b := rt.NewConn()
		d := rt.NewConn()
		k0 := keyOnShard(t, rt, 0, fmt.Sprintf("ow%d_0", iter))
		k1 := keyOnShard(t, rt, 1, fmt.Sprintf("ow%d_1", iter))

		crossBlpopFire(t, a, true, k0, k1, "0")

		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); pushDrain(b, k0, "x0") }()
		go func() { defer wg.Done(); pushDrain(d, k1, "x1") }()
		wg.Wait()

		got := decodeReply(t, drainOne(t, a))
		arr, ok := got.([]any)
		if !ok || len(arr) != 2 {
			t.Fatalf("iter %d reply = %v, want a served pair", iter, render(got))
		}
		servedKey, _ := arr[0].(string)
		switch servedKey {
		case k0:
			if arr[1] != "x0" {
				t.Fatalf("iter %d served %v, want [%s x0]", iter, render(got), k0)
			}
			wantEmptyArray(t, do(t, b, bkLrange, k0, "0", "-1"))
			wantArray(t, do(t, d, bkLrange, k1, "0", "-1"), "x1")
		case k1:
			if arr[1] != "x1" {
				t.Fatalf("iter %d served %v, want [%s x1]", iter, render(got), k1)
			}
			wantEmptyArray(t, do(t, d, bkLrange, k1, "0", "-1"))
			wantArray(t, do(t, b, bkLrange, k0, "0", "-1"), "x0")
		default:
			t.Fatalf("iter %d served an unexpected key %q", iter, servedKey)
		}
		noReply(t, a, 3*time.Millisecond) // exactly one delivery, never two
	}
}

// TestBlpopCrossManyWaitersConcurrent parks many cross waiters on the same two
// owners, then feeds both owners concurrently, and proves every waiter is served
// exactly once with nothing lost or duplicated: the pushed elements are conserved
// across the delivered replies and the leftover lists. Under the race detector this
// exercises the claim and the cancel fan-out at contention.
func TestBlpopCrossManyWaitersConcurrent(t *testing.T) {
	rt := crossBlockRuntime(t, 4)
	k0 := keyOnShard(t, rt, 0, "mw0")
	k1 := keyOnShard(t, rt, 1, "mw1")

	const waiters = 40
	conns := make([]*shard.Conn, waiters)
	for i := 0; i < waiters; i++ {
		conns[i] = rt.NewConn()
		crossBlpopFire(t, conns[i], true, k0, k1, "0")
	}
	noReply(t, conns[0], 30*time.Millisecond) // all parked, none served yet

	// Feed both owners at once: 30 elements on k0, 30 on k1, 60 in all for 40
	// waiters, so 40 are served and 20 elements are left across the two lists.
	const per = 30
	b := rt.NewConn()
	d := rt.NewConn()
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < per; i++ {
			pushDrain(b, k0, fmt.Sprintf("a%d", i))
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < per; i++ {
			pushDrain(d, k1, fmt.Sprintf("b%d", i))
		}
	}()
	wg.Wait()

	// Every waiter is served exactly one pair, and the served elements plus the two
	// leftover lists reconstruct the full pushed multiset with no duplicates.
	served := make(map[string]int)
	for i := 0; i < waiters; i++ {
		got := decodeReply(t, drainOne(t, conns[i]))
		arr, ok := got.([]any)
		if !ok || len(arr) != 2 {
			t.Fatalf("waiter %d reply = %v, want a served pair", i, render(got))
		}
		elem, _ := arr[1].(string)
		served[elem]++
		noReply(t, conns[i], 2*time.Millisecond)
	}

	all := multiset(blockList(t, b, k0), blockList(t, b, k1))
	for e, cnt := range served {
		all[e] += cnt
	}
	want := make(map[string]int)
	for i := 0; i < per; i++ {
		want[fmt.Sprintf("a%d", i)]++
		want[fmt.Sprintf("b%d", i)]++
	}
	if len(all) != len(want) {
		t.Fatalf("reconstructed %d distinct elements, want %d", len(all), len(want))
	}
	for e, cnt := range want {
		if all[e] != cnt {
			t.Fatalf("element %q counted %d, want %d", e, all[e], cnt)
		}
	}
	if len(served) != waiters {
		t.Fatalf("served %d distinct elements across %d waiters, want %d", len(served), waiters, waiters)
	}
}
