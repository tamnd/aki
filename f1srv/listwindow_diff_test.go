package f1srv

import (
	"bufio"
	"math/rand"
	"net"
	"sort"
	"sync"
	"testing"
	"time"
)

// dialNGo starts one server on the goroutine driver and returns n independent client
// connections to it plus a cleanup, so a test can drive many connections at one hot key. The
// hot-list window is the point of these tests, and it lives on the same server across every
// connection, which dialTestServer (one server per call) cannot express.
func dialNGo(t *testing.T, n int) (conns []*bufio.ReadWriter, cleanup func()) {
	t.Helper()
	cfg := Config{Addr: "127.0.0.1:0", IndexBuckets: 1 << 12, ArenaBytes: 1 << 20, ReadBufSize: 4 << 10, IncrStripes: 64, NetMode: "go"}
	srv := New(cfg)
	if err := srv.Listen(); err != nil {
		t.Fatalf("listen: %v", err)
	}
	go srv.ListenAndServe()
	var raw []net.Conn
	for i := 0; i < n; i++ {
		conn, err := net.DialTimeout("tcp", srv.Addr(), 2*time.Second)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		raw = append(raw, conn)
		conns = append(conns, bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn)))
	}
	cleanup = func() {
		for _, c := range raw {
			c.Close()
		}
		srv.Close()
	}
	return conns, cleanup
}

// TestListWindowDifferential drives a long random sequence of list mutators and reads at a
// handful of reused keys and checks the server's full LRANGE against an independent in-test deque
// model after every step. The reuse is deliberate: a second push to a key admits its hot-list
// window, so from then on the pushes take the lock-free fast path and every read and every
// evicting mutator (pop, LSET, LTRIM, DEL) must agree with the pre-window semantics the model
// encodes. A mismatch means the window overlay diverged from the header path it stands in for.
func TestListWindowDifferential(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	keys := []string{"a", "b", "c", "d"}
	model := map[string][]string{}
	rng := rand.New(rand.NewSource(0x1234abcd))

	// full reads the server's whole list for a key as a flat slice, the same view the model holds.
	full := func(k string) []string { return lrangeCall(t, rw, "LRANGE", k, "0", "-1") }

	const steps = 3000
	seq := 0
	for s := 0; s < steps; s++ {
		k := keys[rng.Intn(len(keys))]
		switch rng.Intn(9) {
		case 0, 1: // RPUSH one or a few, matching the coalesced-run fast path
			m := 1 + rng.Intn(3)
			args := []string{"RPUSH", k}
			for i := 0; i < m; i++ {
				seq++
				v := "v" + itoa(seq)
				args = append(args, v)
				model[k] = append(model[k], v)
			}
			cmd(t, rw, args...)
			expect(t, rw, ":"+itoa(len(model[k])))
		case 2, 3: // LPUSH one or a few
			m := 1 + rng.Intn(3)
			args := []string{"LPUSH", k}
			for i := 0; i < m; i++ {
				seq++
				v := "v" + itoa(seq)
				args = append(args, v)
				model[k] = append([]string{v}, model[k]...)
			}
			cmd(t, rw, args...)
			expect(t, rw, ":"+itoa(len(model[k])))
		case 4: // RPOP
			cmd(t, rw, "RPOP", k)
			if n := len(model[k]); n == 0 {
				expect(t, rw, "$-1")
			} else {
				expect(t, rw, "$"+model[k][n-1])
				model[k] = model[k][:n-1]
			}
		case 5: // LPOP
			cmd(t, rw, "LPOP", k)
			if len(model[k]) == 0 {
				expect(t, rw, "$-1")
			} else {
				expect(t, rw, "$"+model[k][0])
				model[k] = model[k][1:]
			}
		case 6: // LSET at a valid index, or an out-of-range error when empty
			if len(model[k]) == 0 {
				cmd(t, rw, "LSET", k, "0", "x")
				expect(t, rw, "-ERR no such key")
				break
			}
			i := rng.Intn(len(model[k]))
			seq++
			v := "s" + itoa(seq)
			cmd(t, rw, "LSET", k, itoa(i), v)
			expect(t, rw, "+OK")
			model[k][i] = v
		case 7: // LTRIM to a random sub-window
			n := len(model[k])
			lo, hi := 0, 0
			if n > 0 {
				lo = rng.Intn(n)
				hi = rng.Intn(n)
			}
			cmd(t, rw, "LTRIM", k, itoa(lo), itoa(hi))
			expect(t, rw, "+OK")
			model[k] = trimModel(model[k], lo, hi)
		case 8: // DEL, which must evict the window and drop the key
			cmd(t, rw, "DEL", k)
			if len(model[k]) == 0 {
				expect(t, rw, ":0")
			} else {
				expect(t, rw, ":1")
			}
			delete(model, k)
		}

		// After every step the server's view of the touched key must equal the model exactly.
		if got, want := full(k), model[k]; !eqStrs(got, want) {
			t.Fatalf("step %d key %q: server %v != model %v", s, k, got, want)
		}
	}

	// A final sweep over every key, so a key left resident in a window is checked at quiescence too.
	for _, k := range keys {
		if got, want := full(k), model[k]; !eqStrs(got, want) {
			t.Fatalf("final key %q: server %v != model %v", k, got, want)
		}
	}
}

// TestListWindowCountPopDifferential drives the count forms of LPOP and RPOP against a hot key and
// checks each popped array and the surviving list against a reference deque. The count pop is the
// path popThroughWindow serves off the window when the count leaves the list non-empty, and it
// falls to the stripe path when the count would drain the list, so the sequence deliberately mixes
// counts that keep the list alive with counts that empty it, exercising both the fast pop and the
// bail. A divergence would show as a wrong popped array or a surviving list that does not match.
func TestListWindowCountPopDifferential(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	keys := []string{"a", "b", "c"}
	model := map[string][]string{}
	rng := rand.New(rand.NewSource(0x51ce55ed))
	full := func(k string) []string { return lrangeCall(t, rw, "LRANGE", k, "0", "-1") }

	const steps = 3000
	seq := 0
	for s := 0; s < steps; s++ {
		k := keys[rng.Intn(len(keys))]
		switch rng.Intn(6) {
		case 0, 1, 2: // push a small run so a hot key stays populated and its window admits
			atHead := rng.Intn(2) == 0
			verb := "RPUSH"
			if atHead {
				verb = "LPUSH"
			}
			m := 1 + rng.Intn(4)
			args := []string{verb, k}
			for i := 0; i < m; i++ {
				seq++
				v := "v" + itoa(seq)
				args = append(args, v)
				if atHead {
					model[k] = append([]string{v}, model[k]...)
				} else {
					model[k] = append(model[k], v)
				}
			}
			cmd(t, rw, args...)
			expect(t, rw, ":"+itoa(len(model[k])))
		case 3, 4: // LPOP k count: pop the first count elements, head outward
			cnt := rng.Intn(5)
			got := lrangeCall(t, rw, "LPOP", k, itoa(cnt))
			want := popHeadModel(&model, k, cnt)
			if !eqStrs(got, want) {
				t.Fatalf("step %d LPOP %s %d: server %v != model %v", s, k, cnt, got, want)
			}
		case 5: // RPOP k count: pop the last count elements, tail inward
			cnt := rng.Intn(5)
			got := lrangeCall(t, rw, "RPOP", k, itoa(cnt))
			want := popTailModel(&model, k, cnt)
			if !eqStrs(got, want) {
				t.Fatalf("step %d RPOP %s %d: server %v != model %v", s, k, cnt, got, want)
			}
		}
		if got, want := full(k), model[k]; !eqStrs(got, want) {
			t.Fatalf("step %d key %q: server %v != model %v", s, k, got, want)
		}
	}
}

// popHeadModel removes and returns the first cnt elements of key k from the reference deque, the
// LPOP-with-count result, clamping cnt to the list length and dropping an emptied key.
func popHeadModel(model *map[string][]string, k string, cnt int) []string {
	l := (*model)[k]
	if cnt > len(l) {
		cnt = len(l)
	}
	out := make([]string, cnt)
	copy(out, l[:cnt])
	rest := l[cnt:]
	if len(rest) == 0 {
		delete(*model, k)
	} else {
		(*model)[k] = append([]string(nil), rest...)
	}
	return out
}

// popTailModel removes the last cnt elements of key k and returns them tail inward, the
// RPOP-with-count result, so RPOP over [a b c d] with cnt 2 returns [d c].
func popTailModel(model *map[string][]string, k string, cnt int) []string {
	l := (*model)[k]
	if cnt > len(l) {
		cnt = len(l)
	}
	out := make([]string, cnt)
	for i := 0; i < cnt; i++ {
		out[i] = l[len(l)-1-i]
	}
	rest := l[:len(l)-cnt]
	if len(rest) == 0 {
		delete(*model, k)
	} else {
		(*model)[k] = append([]string(nil), rest...)
	}
	return out
}

// TestListWindowConcurrentPop seeds one hot key with a large list, then has many connections drain
// it with LPOP at once, each collecting the elements it pops until the list runs dry. The off-lock
// pop claims a disjoint run under the window's commit mutex, so the union of what every connection
// popped, plus whatever the final LRANGE still holds, must be the seeded set with each element
// exactly once. A torn claim would surface here as an element popped by two connections (a
// duplicate) or a slot claimed but never returned (a lost element and a short total).
func TestListWindowConcurrentPop(t *testing.T) {
	const conns = 8
	const total = 8000
	rws, cleanup := dialNGo(t, conns+1)
	defer cleanup()

	seed := rws[conns]
	// Seed in batches so the window admits (the second push sees the list exist) and every element
	// is a distinct value the drain can account for.
	for i := 0; i < total; i++ {
		cmd(t, seed, "RPUSH", "hot", "e"+itoa(i))
		if got := readReply(t, seed); len(got) == 0 || got[0] != ':' {
			t.Fatalf("seed push reply = %q, want an integer", got)
		}
	}

	var mu sync.Mutex
	seen := make(map[string]int, total)
	var wg sync.WaitGroup
	for g := 0; g < conns; g++ {
		wg.Add(1)
		go func(rw *bufio.ReadWriter) {
			defer wg.Done()
			for {
				cmd(t, rw, "LPOP", "hot")
				r := readReply(t, rw)
				if r == "$-1" { // list drained
					return
				}
				if len(r) == 0 || r[0] != '$' {
					t.Errorf("LPOP reply = %q, want a bulk string or nil", r)
					return
				}
				v := r[1:]
				mu.Lock()
				seen[v]++
				mu.Unlock()
			}
		}(rws[g])
	}
	wg.Wait()

	// Whatever survived a near-empty bail to the stripe path is still readable; fold it in.
	for _, v := range lrangeCall(t, seed, "LRANGE", "hot", "0", "-1") {
		seen[v]++
	}
	if len(seen) != total {
		t.Fatalf("distinct elements accounted = %d, want %d", len(seen), total)
	}
	for v, c := range seen {
		if c != 1 {
			t.Fatalf("element %q accounted %d times, want once (a torn or double claim)", v, c)
		}
	}
}

// writePop queues one no-count LPOP or RPOP into the writer without flushing, so a caller can pack
// several into one buffer and flush once, landing them in a single server drain where drainPop
// folds them into one window claim. This is the shape the test needs and cmd (which flushes every
// command) cannot express.
func writePop(t *testing.T, rw *bufio.ReadWriter, verb, key string) {
	t.Helper()
	rw.WriteString("*2\r\n$")
	rw.WriteString(itoa(len(verb)))
	rw.WriteString("\r\n")
	rw.WriteString(verb)
	rw.WriteString("\r\n$")
	rw.WriteString(itoa(len(key)))
	rw.WriteString("\r\n")
	rw.WriteString(key)
	rw.WriteString("\r\n")
}

// TestListWindowPipelinePopDifferential sends pipelines of no-count LPOP and RPOP against a hot key
// in one flush each, so the whole run lands in a single drain and takes the coalesced drainPop
// path, then checks every reply and the surviving list against a reference deque. The pipeline
// depth is chosen to sometimes exceed the live length, which drives the coalesced claim's
// near-empty bail into the per-command replay, and occasional refills keep the window admitted, so
// the run exercises both the fast fold on a populated list and the fallback as the list drains and
// past empty. A divergence would show as a wrong popped element, a missing or extra nil, or a
// surviving list that does not match.
func TestListWindowPipelinePopDifferential(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	const key = "p"
	var model []string
	rng := rand.New(rand.NewSource(0x900df00d))
	full := func() []string { return lrangeCall(t, rw, "LRANGE", key, "0", "-1") }

	seq := 0
	// Seed enough that the window admits (the second push sees the list exist) and the first
	// pipelines run on a populated list.
	for i := 0; i < 200; i++ {
		seq++
		v := "s" + itoa(seq)
		cmd(t, rw, "RPUSH", key, v)
		expect(t, rw, ":"+itoa(i+1))
		model = append(model, v)
	}

	const rounds = 2000
	for r := 0; r < rounds; r++ {
		switch rng.Intn(5) {
		case 0: // refill a small run so the list does not stay drained
			m := 1 + rng.Intn(6)
			args := []string{"RPUSH", key}
			for i := 0; i < m; i++ {
				seq++
				v := "r" + itoa(seq)
				args = append(args, v)
				model = append(model, v)
			}
			cmd(t, rw, args...)
			expect(t, rw, ":"+itoa(len(model)))
		default: // a pipeline of pops, all the same end, in one flush
			atHead := rng.Intn(2) == 0
			verb := "RPOP"
			if atHead {
				verb = "LPOP"
			}
			d := 1 + rng.Intn(20)
			for i := 0; i < d; i++ {
				writePop(t, rw, verb, key)
			}
			if err := rw.Flush(); err != nil {
				t.Fatalf("round %d flush: %v", r, err)
			}
			for i := 0; i < d; i++ {
				got := readReply(t, rw)
				if len(model) == 0 {
					if got != "$-1" {
						t.Fatalf("round %d pop %d on empty: got %q, want $-1", r, i, got)
					}
					continue
				}
				var want string
				if atHead {
					want = model[0]
					model = model[1:]
				} else {
					want = model[len(model)-1]
					model = model[:len(model)-1]
				}
				if got != "$"+want {
					t.Fatalf("round %d pop %d: got %q, want %q", r, i, got, "$"+want)
				}
			}
		}
		if got := full(); !eqStrs(got, model) {
			t.Fatalf("round %d: server %v != model %v", r, got, model)
		}
	}
}

// trimModel mirrors LTRIM on the reference deque: negative indexes count from the end, the range
// is clamped, and an inverted range empties the list. It matches Redis's LTRIM so the model tracks
// the server through the evicting path LTRIM takes.
func trimModel(l []string, start, stop int) []string {
	ln := len(l)
	if start < 0 {
		start += ln
	}
	if stop < 0 {
		stop += ln
	}
	if start < 0 {
		start = 0
	}
	if start >= ln || start > stop {
		return nil
	}
	if stop >= ln {
		stop = ln - 1
	}
	out := make([]string, stop-start+1)
	copy(out, l[start:stop+1])
	return out
}

// TestListWindowConcurrentAppend has many connections RPUSH distinct values to one hot key at
// once, then checks that the whole list is exactly the multiset pushed with no gaps and no
// duplicates. The window admits after the seeding pushes, so every concurrent push runs through
// the lock-free reserve/commit path; a torn commit would show up here as a missing element (a
// gap the reader would see as a short list) or an empty slot, and the length would not match.
func TestListWindowConcurrentAppend(t *testing.T) {
	const conns = 8
	const perConn = 400
	rws, cleanup := dialNGo(t, conns+1)
	defer cleanup()

	seed := rws[conns] // one extra connection to seed and to read at the end
	// Two seeding pushes admit the window: the first creates the list, the second sees it exist
	// and admits, so the concurrent pushes below all take the fast path.
	cmd(t, seed, "RPUSH", "hot", "seed0")
	expect(t, seed, ":1")
	cmd(t, seed, "RPUSH", "hot", "seed1")
	expect(t, seed, ":2")

	var wg sync.WaitGroup
	for g := 0; g < conns; g++ {
		wg.Add(1)
		go func(g int, rw *bufio.ReadWriter) {
			defer wg.Done()
			for r := 0; r < perConn; r++ {
				v := "g" + itoa(g) + "-" + itoa(r)
				cmd(t, rw, "RPUSH", "hot", v)
				// Read the integer reply so the connection stays in sync; its exact value is
				// undefined under concurrent appenders and is not asserted.
				if got := readReply(t, rw); len(got) == 0 || got[0] != ':' {
					t.Errorf("g%d push reply = %q, want an integer", g, got)
					return
				}
			}
		}(g, rws[g])
	}
	wg.Wait()

	// The full list must be the two seeds plus every connection's values, each exactly once.
	got := lrangeCall(t, seed, "LRANGE", "hot", "0", "-1")
	wantN := 2 + conns*perConn
	if len(got) != wantN {
		t.Fatalf("LRANGE length = %d, want %d (a gap or torn commit)", len(got), wantN)
	}
	seen := make(map[string]int, wantN)
	for _, v := range got {
		if v == "" {
			t.Fatalf("empty element in list, a reserved slot was published unfilled")
		}
		seen[v]++
	}
	want := []string{"seed0", "seed1"}
	for g := 0; g < conns; g++ {
		for r := 0; r < perConn; r++ {
			want = append(want, "g"+itoa(g)+"-"+itoa(r))
		}
	}
	sort.Strings(want)
	keys := make([]string, 0, len(seen))
	for v, c := range seen {
		if c != 1 {
			t.Fatalf("element %q appeared %d times, want once", v, c)
		}
		keys = append(keys, v)
	}
	sort.Strings(keys)
	if !eqStrs(keys, want) {
		t.Fatalf("multiset mismatch: %d distinct values, want %d", len(keys), len(want))
	}

	// LLEN reads through the window and must agree with the materialized length.
	cmd(t, seed, "LLEN", "hot")
	expect(t, seed, ":"+itoa(wantN))
}
