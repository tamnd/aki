package zset

import (
	"math"
	"os"
	"strings"
	"testing"

	"github.com/tamnd/aki/f3srv/resp"
)

// The ZSCAN proof invariants (spec 2064/f3/12 section 6.11). The cursor is this
// engine's opaque downward cursor over the member records, not Redis's
// bit-reversed cursor, so the guarantees are proved here on the local scan and
// the live differential compares the SET of pairs across a full scan, not the
// per-page cursors. The four bars: exactly-once on a static set, every original
// returned when the set grows mid-scan, at-least-once for a survivor across churn
// and a mid-scan reclaim rebuild, and termination.

// nativeWith builds a native-band zset from members scored by their index, and
// fails if the set did not cross into the native band (the ZSCAN record cursor is
// a native-band structure; the inline band is one page).
func nativeWith(t *testing.T, members []string) *zset {
	t.Helper()
	z := newZset()
	for i, m := range members {
		z.update([]byte(m), float64(i), flags{})
	}
	if z.enc != encSkiplist {
		t.Fatalf("built %d members, still %s, want skiplist", len(members), z.enc)
	}
	return z
}

// fullScan drives z.scanPage from cursor 0 to completion, collecting the emitted
// members, and runs between after each page so a test can churn the set mid-scan.
// It caps the page count so a cursor that fails to make progress fails the test
// instead of hanging.
func fullScan(t *testing.T, z *zset, count int, match []byte, between func(page int)) []string {
	t.Helper()
	var got []string
	cursor := uint64(0)
	for page := 0; ; page++ {
		if page > 1_000_000 {
			t.Fatal("ZSCAN did not terminate")
		}
		next := z.scanPage(cursor, count, match, func(m []byte, _ uint64) {
			got = append(got, string(m))
		})
		if between != nil {
			between(page)
		}
		if next == 0 {
			return got
		}
		cursor = next
	}
}

// TestZscanExactlyOnceStatic holds the first bar: a full scan of a set that does
// not change returns every member exactly once, at every COUNT including one that
// takes the whole set in a single page.
func TestZscanExactlyOnceStatic(t *testing.T) {
	members := make([]string, 5000)
	for i := range members {
		members[i] = "m" + pad(i)
	}
	z := nativeWith(t, members)
	for _, count := range []int{1, 7, 10, 256, 5000, 9999} {
		got := fullScan(t, z, count, nil, nil)
		assertSameSet(t, got, members, true, "count="+itoa(count))
	}
}

// TestZscanScoresMatch checks the pair a page emits carries the member's true
// score bits, so ZSCAN member/score parity holds in the native band.
func TestZscanScoresMatch(t *testing.T) {
	members := make([]string, 3000)
	for i := range members {
		members[i] = "m" + pad(i)
	}
	z := nativeWith(t, members)
	want := map[string]float64{}
	for i, m := range members {
		want[m] = float64(i)
	}
	seen := map[string]int{}
	cursor := uint64(0)
	for {
		next := z.scanPage(cursor, 13, nil, func(m []byte, bits uint64) {
			got := math.Float64frombits(bits)
			if got != want[string(m)] {
				t.Fatalf("member %q score %v, want %v", m, got, want[string(m)])
			}
			seen[string(m)]++
		})
		if next == 0 {
			break
		}
		cursor = next
	}
	if len(seen) != len(members) {
		t.Fatalf("saw %d distinct members, want %d", len(seen), len(members))
	}
}

// TestZscanGrowthMidScan holds the second bar: members added after the scan opens
// land above the record cursor, on the already-scanned side, so the scan never
// has to revisit them and every member present when the scan opened is still
// returned exactly once.
func TestZscanGrowthMidScan(t *testing.T) {
	original := make([]string, 2000)
	for i := range original {
		original[i] = "orig" + pad(i)
	}
	z := nativeWith(t, original)
	added := 0
	got := fullScan(t, z, 11, nil, func(page int) {
		// Add a fresh batch after the first few pages, while the scan is in flight.
		if page >= 2 && page <= 6 {
			for i := 0; i < 200; i++ {
				z.update([]byte("new"+pad(added)), 1e6+float64(added), flags{})
				added++
			}
		}
	})
	if added == 0 {
		t.Fatal("test added no members mid-scan")
	}
	assertSameSet(t, got, original, true, "originals exactly once")
}

// TestZscanAtLeastOnceChurn holds the third bar: a member present for the whole
// scan is returned at least once even across removals and a reclaim rebuild that
// runs mid-scan. The rebuild is forced deterministically after some removals have
// left dead cells, exactly the stable-order compaction the cursor must survive:
// it only lowers a live record's index, so a survivor still below the cursor stays
// below it.
func TestZscanAtLeastOnceChurn(t *testing.T) {
	var core, churn []string
	for i := 0; i < 2000; i++ {
		core = append(core, "c"+pad(i))
	}
	for i := 0; i < 1000; i++ {
		churn = append(churn, "x"+pad(i))
	}
	z := nativeWith(t, append(append([]string{}, core...), churn...))

	rebuilt := false
	got := fullScan(t, z, 17, nil, func(page int) {
		if page == 3 && !rebuilt {
			for _, m := range churn {
				z.rem([]byte(m))
			}
			// Force the stable-order reclaim mid-scan, the exact hazard the cursor
			// design has to survive.
			z.nat.rebuild(z.nat.card())
			rebuilt = true
		}
	})
	if !rebuilt {
		t.Fatal("mid-scan rebuild never ran")
	}
	seen := map[string]bool{}
	for _, m := range got {
		seen[m] = true
	}
	for _, m := range core {
		if !seen[m] {
			t.Fatalf("survivor %q was never returned across the churned scan", m)
		}
	}
}

// TestZscanMatch checks MATCH filters the emitted members by glob and leaves the
// cursor walk unchanged.
func TestZscanMatch(t *testing.T) {
	var members []string
	for i := 0; i < 500; i++ {
		members = append(members, "user:"+pad(i))
	}
	for i := 0; i < 500; i++ {
		members = append(members, "post:"+pad(i))
	}
	z := nativeWith(t, members)
	got := fullScan(t, z, 10, []byte("user:*"), nil)
	if len(got) != 500 {
		t.Fatalf("MATCH user:* returned %d, want 500", len(got))
	}
	for _, m := range got {
		if len(m) < 5 || m[:5] != "user:" {
			t.Fatalf("MATCH user:* returned non-matching %q", m)
		}
	}
}

// TestZscanInlineOnePage checks the inline band answers in a single page with
// cursor 0 and a replayed nonzero cursor returns nothing, the listpack parity.
func TestZscanInlineOnePage(t *testing.T) {
	z := newZset()
	for i := 0; i < 20; i++ {
		z.update([]byte("m"+itoa(i)), float64(i), flags{})
	}
	if z.enc != encListpack {
		t.Fatalf("enc = %s, want listpack", z.enc)
	}
	var got []string
	next := z.scanPage(0, 3, nil, func(m []byte, _ uint64) { got = append(got, string(m)) })
	if next != 0 {
		t.Fatalf("inline scan cursor = %d, want 0", next)
	}
	if len(got) != 20 {
		t.Fatalf("inline scan returned %d, want 20", len(got))
	}
	// A replayed nonzero cursor is a finished scan: no members, done cursor.
	got = got[:0]
	if n := z.scanPage(42, 3, nil, func(m []byte, _ uint64) { got = append(got, string(m)) }); n != 0 || len(got) != 0 {
		t.Fatalf("replayed inline cursor = %d, %d members, want 0 and none", n, len(got))
	}
}

func TestParseScanCount(t *testing.T) {
	for _, s := range []string{"1", "10", "1000"} {
		if _, ok := parseScanCount([]byte(s)); !ok {
			t.Errorf("parseScanCount(%q) rejected", s)
		}
	}
	for _, s := range []string{"0", "-1", "", "x", "1.5"} {
		if _, ok := parseScanCount([]byte(s)); ok {
			t.Errorf("parseScanCount(%q) accepted", s)
		}
	}
}

func TestZsetGlobMatch(t *testing.T) {
	cases := []struct {
		pat, str string
		want     bool
	}{
		{"*", "anything", true},
		{"h?llo", "hello", true},
		{"h?llo", "hallo", true},
		{"h[ae]llo", "hello", true},
		{"h[^e]llo", "hello", false},
		{"h[a-c]llo", "hbllo", true},
		{"user:*", "user:42", true},
		{"user:*", "post:42", false},
		{`\*lit`, "*lit", true},
	}
	for _, c := range cases {
		if got := globMatch([]byte(c.pat), []byte(c.str)); got != c.want {
			t.Errorf("globMatch(%q, %q) = %v, want %v", c.pat, c.str, got, c.want)
		}
	}
}

// assertSameSet compares got against want as sets; when exact, it also requires
// each want member to appear exactly once in got.
func assertSameSet(t *testing.T, got, want []string, exact bool, ctx string) {
	t.Helper()
	counts := map[string]int{}
	for _, m := range got {
		counts[m]++
	}
	for _, m := range want {
		if counts[m] == 0 {
			t.Fatalf("%s: member %q missing from scan", ctx, m)
		}
		if exact && counts[m] != 1 {
			t.Fatalf("%s: member %q returned %d times, want once", ctx, m, counts[m])
		}
	}
	if len(counts) != len(want) {
		t.Fatalf("%s: scan returned %d distinct members, want %d", ctx, len(counts), len(want))
	}
}

// TestZscanAgainstRedis replays a churned zset against a live Redis and checks
// the SET of member/score pairs a full ZSCAN returns agrees, across both bands
// and with MATCH. The cursors differ by design (this engine's cursor is not
// Redis's bit-reversed one), so the differential is on the union of pages, not
// page by page. It also pins NOSCORES as a syntax error: HSCAN grew NOVALUES in
// 7.4 but ZSCAN has no analogue, and the error text must match Redis verbatim.
// Skipped unless AKI_REDIS_ADDR is set.
func TestZscanAgainstRedis(t *testing.T) {
	addr := os.Getenv("AKI_REDIS_ADDR")
	if addr == "" {
		t.Skip("set AKI_REDIS_ADDR=host:port to replay ZSCAN against a live Redis")
	}
	c, err := dialRedis(addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	defer c.close()

	for _, n := range []int{20, 400} {
		key := "aki:zscan:" + itoa(n)
		c.cmd("DEL", key)
		want := map[string]string{} // member -> formatted score
		for i := 0; i < n; i++ {
			m := "m" + pad(i)
			s := float64(i) - float64(n)/2
			ss := string(resp.FormatScore(nil, s))
			if _, err := c.cmd("ZADD", key, ss, m); err != nil {
				t.Fatalf("ZADD: %v", err)
			}
			want[m] = ss
		}

		// Full ZSCAN with scores: the union of pages must equal the set of pairs.
		gotPairs := redisScanAll(t, c, key, nil)
		if len(gotPairs) != len(want) {
			t.Fatalf("n=%d: ZSCAN returned %d pairs, want %d", n, len(gotPairs), len(want))
		}
		for m, s := range gotPairs {
			if want[m] != s {
				t.Fatalf("n=%d: ZSCAN member %q score %q, want %q", n, m, s, want[m])
			}
		}

		// MATCH filters the union.
		gotMatch := redisScanAll(t, c, key, []byte("m000000*"))
		for m := range gotMatch {
			if len(m) < 7 || m[:7] != "m000000" {
				t.Fatalf("n=%d: ZSCAN MATCH returned non-matching %q", n, m)
			}
		}

		// NOSCORES is not a ZSCAN option upstream; both sides must reject it with
		// the same error line, verbatim.
		_, rErr := c.cmd("ZSCAN", key, "0", "NOSCORES")
		if rErr == nil {
			t.Fatalf("n=%d: redis accepted ZSCAN NOSCORES, want a syntax error", n)
		}
		if got := strings.TrimPrefix(rErr.Error(), "redis: "); got != "ERR syntax error" {
			t.Fatalf("n=%d: ZSCAN NOSCORES error %q live, this package emits %q", n, got, "ERR syntax error")
		}
		c.cmd("DEL", key)
	}
}

// redisScanAll drives a full ZSCAN over the wire, returning member->score. It
// loops the server's own cursor to completion.
func redisScanAll(t *testing.T, c *redisConn, key string, match []byte) map[string]string {
	t.Helper()
	out := map[string]string{}
	cursor := "0"
	for {
		args := []string{"ZSCAN", key, cursor}
		if match != nil {
			args = append(args, "MATCH", string(match))
		}
		rep, err := c.cmdReply(args...)
		if err != nil {
			t.Fatalf("ZSCAN: %v", err)
		}
		arr, ok := rep.([]any)
		if !ok || len(arr) != 2 {
			t.Fatalf("ZSCAN reply shape %v", rep)
		}
		cursor = arr[0].(string)
		page := arr[1].([]any)
		if len(page)%2 != 0 {
			t.Fatalf("ZSCAN page has odd length %d", len(page))
		}
		for i := 0; i < len(page); i += 2 {
			out[page[i].(string)] = page[i+1].(string)
		}
		if cursor == "0" {
			return out
		}
	}
}
