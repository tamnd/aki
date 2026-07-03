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
