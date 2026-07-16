package set

import (
	"fmt"
	"os"
	"sort"
	"testing"
)

// TestStoreAgainstRedis replays the STORE oracle cases against a live server when
// AKI_REDIS_ADDR is set, the same lever the encoding and algebra suites use. For
// each case it loads the operands, runs SINTERSTORE/SUNIONSTORE/SDIFFSTORE into a
// destination on the server, and checks the server's destination (its members,
// its cardinality reply, and its OBJECT ENCODING) matches the local STORE build,
// so a semantic drift shows up as a failure rather than silent skew. It also
// replays the aliasing shape (destination is a source) that the local suite
// proves needs no clone. Skipped by default.
func TestStoreAgainstRedis(t *testing.T) {
	addr := os.Getenv("AKI_REDIS_ADDR")
	if addr == "" {
		t.Skip("set AKI_REDIS_ADDR=host:port to check STORE forms against a live Redis")
	}
	c, err := dialRedis(addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	defer c.close()

	forms := []struct {
		verb  string
		build func([]*set) *set
	}{
		{"SINTERSTORE", storeInter},
		{"SUNIONSTORE", storeUnion},
		{"SDIFFSTORE", storeDiff},
	}

	for _, tc := range algebraCases {
		t.Run(tc.name, func(t *testing.T) {
			keys := make([]string, len(tc.ops))
			for i, op := range tc.ops {
				key := fmt.Sprintf("aki:store:%s:%d", tc.name, i)
				keys[i] = key
				c.cmd("DEL", key)
				if op == nil {
					continue
				}
				if _, err := c.cmd(append([]string{"SADD", key}, op...)...); err != nil {
					t.Fatalf("SADD %s: %v", key, err)
				}
			}
			dest := fmt.Sprintf("aki:store:%s:dest", tc.name)
			cleanup := append(append([]string{}, keys...), dest)
			defer func() {
				for _, k := range cleanup {
					c.cmd("DEL", k)
				}
			}()

			sets := setsFrom(tc.ops)
			for _, f := range forms {
				c.cmd("DEL", dest)
				want := f.build(sets)

				redisCard := c.cmdInt(t, append([]string{f.verb, dest}, keys...)...)
				if got := cardOf(want); redisCard != got {
					t.Fatalf("%s: redis card %d, local %d", f.verb, redisCard, got)
				}
				checkDest(t, c, f.verb, dest, want)
			}
		})
	}
}

// TestStoreAliasingAgainstRedis replays the destination-is-a-source shape on a
// live server: it must produce the same destination the local no-clone build
// does, proving the aliasing handling matches Redis end to end.
func TestStoreAliasingAgainstRedis(t *testing.T) {
	addr := os.Getenv("AKI_REDIS_ADDR")
	if addr == "" {
		t.Skip("set AKI_REDIS_ADDR=host:port to check STORE aliasing against a live Redis")
	}
	c, err := dialRedis(addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	defer c.close()

	a := "aki:storealias:a"
	b := "aki:storealias:b"
	defer func() { c.cmd("DEL", a); c.cmd("DEL", b) }()
	for _, k := range []string{a, b} {
		c.cmd("DEL", k)
	}
	if _, err := c.cmd("SADD", a, "1", "2", "3", "4"); err != nil {
		t.Fatalf("SADD %s: %v", a, err)
	}
	if _, err := c.cmd("SADD", b, "3", "4", "5", "6"); err != nil {
		t.Fatalf("SADD %s: %v", b, err)
	}

	// SUNIONSTORE a a b: the destination a is also the first source. Redis reads
	// both fully, then overwrites a with the union.
	redisCard := c.cmdInt(t, "SUNIONSTORE", a, a, b)
	local := storeUnion([]*set{
		setFrom([]string{"1", "2", "3", "4"}),
		setFrom([]string{"3", "4", "5", "6"}),
	})
	if got := cardOf(local); redisCard != got {
		t.Fatalf("SUNIONSTORE alias: redis card %d, local %d", redisCard, got)
	}
	checkDest(t, c, "SUNIONSTORE(alias)", a, local)
}

// cardOf is the built destination's cardinality, 0 when the result deleted it.
func cardOf(s *set) int {
	if s == nil {
		return 0
	}
	return s.card()
}

// checkDest compares the server's destination against the local build: the same
// members (sorted), and the same OBJECT ENCODING when the destination survives.
// An empty result must have deleted the destination on both sides.
func checkDest(t *testing.T, c *redisConn, verb, dest string, want *set) {
	t.Helper()
	got, err := c.cmdArray("SMEMBERS", dest)
	if err != nil {
		t.Fatalf("%s SMEMBERS %s: %v", verb, dest, err)
	}
	sort.Strings(got)
	w := storeMembers(want)
	if len(got) != len(w) {
		t.Fatalf("%s: redis %d members, local %d", verb, len(got), len(w))
	}
	for i := range w {
		if got[i] != w[i] {
			t.Fatalf("%s: at %d redis %q, local %q", verb, i, got[i], w[i])
		}
	}
	if want == nil {
		// An empty result: the destination must be gone (EXISTS 0).
		if ex := c.cmdInt(t, "EXISTS", dest); ex != 0 {
			t.Fatalf("%s: empty result but redis EXISTS %s = %d", verb, dest, ex)
		}
		return
	}
	enc, err := c.cmd("OBJECT", "ENCODING", dest)
	if err != nil {
		t.Fatalf("%s OBJECT ENCODING %s: %v", verb, dest, err)
	}
	if enc != want.enc.String() {
		t.Fatalf("%s: redis encoding %q, local %q", verb, enc, want.enc.String())
	}
}
