package set

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestAlgebraAgainstRedis replays the algebra oracle cases against a live server
// when AKI_REDIS_ADDR is set (host:port), the same lever the encoding suite uses.
// It loads each operand with SADD, then checks aki's driver returns the exact set
// (sorted) that Redis does for SINTER, SUNION, SDIFF, and SINTERCARD, so a
// semantic drift (ordering aside) shows up as a failure rather than silent skew.
// Skipped by default.
func TestAlgebraAgainstRedis(t *testing.T) {
	addr := os.Getenv("AKI_REDIS_ADDR")
	if addr == "" {
		t.Skip("set AKI_REDIS_ADDR=host:port to check algebra against a live Redis")
	}
	c, err := dialRedis(addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	defer c.close()

	for _, tc := range algebraCases {
		t.Run(tc.name, func(t *testing.T) {
			keys := make([]string, len(tc.ops))
			for i, op := range tc.ops {
				key := fmt.Sprintf("aki:alg:%s:%d", tc.name, i)
				keys[i] = key
				c.cmd("DEL", key)
				if op == nil {
					continue // a missing key stays unset
				}
				args := append([]string{"SADD", key}, op...)
				if _, err := c.cmd(args...); err != nil {
					t.Fatalf("SADD %s: %v", key, err)
				}
			}
			defer func() {
				for _, k := range keys {
					c.cmd("DEL", k)
				}
			}()

			sets := setsFrom(tc.ops)

			checkCmd(t, c, "SINTER", keys, driveInter(sets))
			checkCmd(t, c, "SUNION", keys, driveUnion(sets))
			checkCmd(t, c, "SDIFF", keys, driveDiff(sets))

			// SINTERCARD numkeys key... against the local count, unlimited and with
			// a small limit.
			wantCard := len(oracleInter(tc.ops))
			redisCard := c.cmdInt(t, append([]string{"SINTERCARD", strconv.Itoa(len(keys))}, keys...)...)
			if redisCard != wantCard {
				t.Fatalf("SINTERCARD: redis %d, oracle %d", redisCard, wantCard)
			}
			if got := sintercard(sets, 0); got != redisCard {
				t.Fatalf("SINTERCARD driver %d, redis %d", got, redisCard)
			}
			limit := 5
			redisLim := c.cmdInt(t, append([]string{"SINTERCARD", strconv.Itoa(len(keys))}, append(append([]string{}, keys...), "LIMIT", strconv.Itoa(limit))...)...)
			if got := sintercard(sets, limit); got != redisLim {
				t.Fatalf("SINTERCARD LIMIT %d: driver %d, redis %d", limit, got, redisLim)
			}
		})
	}
}

// checkCmd runs verb over keys on the live server and checks the reply is the
// same set of members (sorted) as want.
func checkCmd(t *testing.T, c *redisConn, verb string, keys, want []string) {
	t.Helper()
	got, err := c.cmdArray(append([]string{verb}, keys...)...)
	if err != nil {
		t.Fatalf("%s: %v", verb, err)
	}
	sort.Strings(got)
	w := append([]string(nil), want...)
	sort.Strings(w)
	if len(got) != len(w) {
		t.Fatalf("%s: redis %d members, driver %d", verb, len(got), len(w))
	}
	for i := range w {
		if got[i] != w[i] {
			t.Fatalf("%s: at %d redis %q, driver %q", verb, i, got[i], w[i])
		}
	}
}

// cmdInt sends a command expecting an integer reply.
func (rc *redisConn) cmdInt(t *testing.T, args ...string) int {
	t.Helper()
	s, err := rc.cmd(args...)
	if err != nil {
		t.Fatalf("%v: %v", args, err)
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		t.Fatalf("%v: reply %q not an integer", args, s)
	}
	return n
}

// cmdArray sends a command and reads a flat multi-bulk array reply into its
// member strings. It handles the null array and null bulks, enough for the
// algebra replies.
func (rc *redisConn) cmdArray(args ...string) ([]string, error) {
	var b strings.Builder
	fmt.Fprintf(&b, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(a), a)
	}
	rc.c.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := rc.c.Write([]byte(b.String())); err != nil {
		return nil, err
	}
	line, err := rc.r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimRight(line, "\r\n")
	if len(line) == 0 || line[0] != '*' {
		return nil, fmt.Errorf("not an array reply: %q", line)
	}
	n, _ := strconv.Atoi(line[1:])
	if n < 0 {
		return nil, nil
	}
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		hdr, err := rc.r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		hdr = strings.TrimRight(hdr, "\r\n")
		if len(hdr) == 0 || hdr[0] != '$' {
			return nil, fmt.Errorf("element %d not a bulk: %q", i, hdr)
		}
		ln, _ := strconv.Atoi(hdr[1:])
		if ln < 0 {
			out = append(out, "")
			continue
		}
		buf := make([]byte, ln+2)
		if _, err := readFull(rc.r, buf); err != nil {
			return nil, err
		}
		out = append(out, string(buf[:ln]))
	}
	return out, nil
}
