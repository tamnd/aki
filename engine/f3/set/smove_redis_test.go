package set

import (
	"os"
	"sort"
	"testing"
)

// TestSmoveAgainstRedis replays SMOVE against a live server when AKI_REDIS_ADDR is
// set, the same lever the encoding, algebra, and STORE suites use. For each shape
// it loads the two operands, runs SMOVE on the server, and checks the server's
// integer reply and both keys' resulting members and OBJECT ENCODING match the
// local smove core, so a semantic drift shows up as a failure rather than silent
// skew. Skipped by default.
func TestSmoveAgainstRedis(t *testing.T) {
	addr := os.Getenv("AKI_REDIS_ADDR")
	if addr == "" {
		t.Skip("set AKI_REDIS_ADDR=host:port to check SMOVE against a live Redis")
	}
	c, err := dialRedis(addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	defer c.close()

	cases := []struct {
		name     string
		src, dst []string
		member   string
	}{
		{"intset present", intGen(0, 6), intGen(100, 6), "3"},
		{"intset absent", intGen(0, 6), intGen(100, 6), "999"},
		{"into missing dst", intGen(0, 6), nil, "2"},
		{"listpack present", gen("w", 0, 6, 4), gen("w", 3, 6, 4), "w0"},
		{"listpack in both", gen("w", 0, 6, 4), gen("w", 3, 6, 4), "w4"},
		{"hashtable present", gen("m", 0, 300, 8), gen("m", 150, 300, 8), "m10"},
		{"hashtable in both", gen("m", 0, 300, 8), gen("m", 150, 300, 8), "m200"},
		{"cross-band intset to listpack", intGen(0, 6), gen("w", 0, 6, 4), "4"},
		{"last member deletes src", []string{"only"}, gen("w", 0, 3, 4), "only"},
		{"missing src", nil, gen("w", 0, 3, 4), "x"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			src, dst := "aki:smove:src", "aki:smove:dst"
			defer func() { c.cmd("DEL", src); c.cmd("DEL", dst) }()
			c.cmd("DEL", src)
			c.cmd("DEL", dst)
			if tc.src != nil {
				if _, err := c.cmd(append([]string{"SADD", src}, tc.src...)...); err != nil {
					t.Fatalf("SADD src: %v", err)
				}
			}
			if tc.dst != nil {
				if _, err := c.cmd(append([]string{"SADD", dst}, tc.dst...)...); err != nil {
					t.Fatalf("SADD dst: %v", err)
				}
			}

			// Local oracle on the same operands through the real registry path.
			cx, g := newCtx(t)
			if s := setFrom(tc.src); s != nil {
				g.m[src] = s
			}
			if d := setFrom(tc.dst); d != nil {
				g.m[dst] = d
			}
			moved, wrong := smove(g, cx, []byte(src), []byte(dst), []byte(tc.member))
			if wrong {
				t.Fatalf("local unexpected WRONGTYPE")
			}
			wantReply := 0
			if moved {
				wantReply = 1
			}

			redisReply := c.cmdInt(t, "SMOVE", src, dst, tc.member)
			if redisReply != wantReply {
				t.Fatalf("SMOVE reply: redis %d, local %d", redisReply, wantReply)
			}
			checkKey(t, c, "src", src, g.m[src])
			checkKey(t, c, "dst", dst, g.m[dst])
		})
	}
}

// checkKey compares the server's key against the local set: the same members
// (sorted) and, when the set survives, the same OBJECT ENCODING. A nil local set
// means the key must be gone on the server too (EXISTS 0).
func checkKey(t *testing.T, c *redisConn, label, key string, want *set) {
	t.Helper()
	got, err := c.cmdArray("SMEMBERS", key)
	if err != nil {
		t.Fatalf("%s SMEMBERS %s: %v", label, key, err)
	}
	sort.Strings(got)
	w := memberList(want)
	if len(got) != len(w) {
		t.Fatalf("%s: redis %d members, local %d", label, len(got), len(w))
	}
	for i := range w {
		if got[i] != w[i] {
			t.Fatalf("%s: at %d redis %q, local %q", label, i, got[i], w[i])
		}
	}
	if want == nil {
		if ex := c.cmdInt(t, "EXISTS", key); ex != 0 {
			t.Fatalf("%s: empty local set but redis EXISTS %s = %d", label, key, ex)
		}
		return
	}
	enc, err := c.cmd("OBJECT", "ENCODING", key)
	if err != nil {
		t.Fatalf("%s OBJECT ENCODING %s: %v", label, key, err)
	}
	if enc != want.enc.String() {
		t.Fatalf("%s: redis encoding %q, local %q", label, enc, want.enc.String())
	}
}
