package set

import "testing"

// drainScan pages a set to completion through scanPage and returns how many
// times each member came back plus the page count. It guards against a cursor
// that never lands on 0: the downward walk must terminate in about
// ceil(card/count) pages, so a run past that bound is a termination bug and the
// caller's page-count assertion catches it.
func drainScan(s *set, count int) (seen map[string]int, pages int) {
	seen = map[string]int{}
	var cur uint64
	guard := s.card()/count + 8
	for {
		pages++
		next := s.scanPage(cur, count, nil, func(m []byte) { seen[string(m)]++ })
		if next == 0 {
			return seen, pages
		}
		cur = next
		if pages > guard {
			return seen, pages // caller asserts pages <= guard
		}
	}
}

// TestScanPageFullPass is the base case of the carried proof: a static set
// paged with the downward cursor returns every member exactly once and
// terminates in the expected page count. No member is dropped and none repeats
// while nothing mutates.
func TestScanPageFullPass(t *testing.T) {
	all := members16(1000)
	s := buildHT(all[:1000])
	if s.enc != encHashtable {
		t.Fatalf("enc = %s, want hashtable", s.enc)
	}
	seen, pages := drainScan(s, 10)
	if pages != 100 {
		t.Fatalf("paged in %d pages, want 100 (1000 members / count 10)", pages)
	}
	if len(seen) != 1000 {
		t.Fatalf("saw %d distinct members, want 1000", len(seen))
	}
	for _, k := range all {
		if seen[string(k)] != 1 {
			t.Fatalf("member %x seen %d times, want exactly 1 on a static scan", k, seen[string(k)])
		}
	}
}

// TestScanPageGrowthMidScan grows the set across a scan and asserts the
// at-least-once guarantee for members present throughout. New members land at
// the top of the vector, the already-scanned side, so the walk never has to
// revisit them; the members that were there when the scan opened are all still
// returned. This is the proof's growth clause.
func TestScanPageGrowthMidScan(t *testing.T) {
	all := members16(500)
	s := buildHT(all[:300])
	orig := all[:300]

	seen := map[string]int{}
	var cur uint64
	grown := false
	for {
		next := s.scanPage(cur, 10, nil, func(m []byte) { seen[string(m)]++ })
		if !grown && next != 0 && next <= uint64(s.card()/2) {
			for _, k := range all[300:] {
				s.add(k)
			}
			grown = true
		}
		if next == 0 {
			break
		}
		cur = next
	}
	if !grown {
		t.Fatal("scan finished before the mid-scan growth fired")
	}
	for _, k := range orig {
		if seen[string(k)] == 0 {
			t.Fatalf("member %x present throughout the scan was never returned", k)
		}
	}
}

// TestScanPageRemoveMidScan churns the set with swap-remove during a scan and
// asserts every member that is never removed still comes back at least once,
// and the scan terminates. Swap-remove only slides the vector's last live
// ordinal into a vacated slot, so a stable member can move within the unscanned
// region or be revisited, but it cannot be skipped past the descending cursor.
// This is the proof's shrink clause, the tail-latency-critical path.
func TestScanPageRemoveMidScan(t *testing.T) {
	all := members16(400)
	s := buildHT(all)
	stable := all[:200]
	churn := all[200:]

	seen := map[string]int{}
	var cur uint64
	fired := false
	pages := 0
	for {
		pages++
		next := s.scanPage(cur, 10, nil, func(m []byte) { seen[string(m)]++ })
		if !fired && next != 0 && next <= uint64(s.card()) {
			for _, k := range churn {
				s.rem(k)
			}
			fired = true
		}
		if next == 0 {
			break
		}
		cur = next
		if pages > 100 {
			t.Fatalf("scan ran %d pages without terminating after churn", pages)
		}
	}
	if !fired {
		t.Fatal("scan finished before the mid-scan removal fired")
	}
	for _, k := range stable {
		if seen[string(k)] == 0 {
			t.Fatalf("stable member %x was skipped by the churned scan", k)
		}
	}
}

// TestScanPageInlineOnePage pins the inline-band contract: cursor 0 returns the
// whole set in one page and reports done, and a replayed nonzero cursor returns
// nothing. Both intset and listpack answer this way, the listpack-parity of
// Redis returning a small set in a single SCAN reply.
func TestScanPageInlineOnePage(t *testing.T) {
	for _, tc := range []struct {
		name    string
		members []string
	}{
		{"intset", []string{"1", "2", "3", "4", "5"}},
		{"listpack", []string{"a", "bb", "ccc", "dddd"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newSet([]byte(tc.members[0]))
			for _, m := range tc.members {
				s.add([]byte(m))
			}
			var got []string
			next := s.scanPage(0, 10, nil, func(m []byte) { got = append(got, string(m)) })
			if next != 0 {
				t.Fatalf("inline cursor after one page = %d, want 0", next)
			}
			if len(got) != len(tc.members) {
				t.Fatalf("one page returned %d members, want the whole set of %d", len(got), len(tc.members))
			}
			var none []string
			next = s.scanPage(7, 10, nil, func(m []byte) { none = append(none, string(m)) })
			if next != 0 || none != nil {
				t.Fatalf("replayed inline cursor returned %v with next %d, want empty and 0", none, next)
			}
		})
	}
}

// TestScanPageMatch filters a full scan by glob and asserts only the matching
// members come back, and all of them do. MATCH is applied to the member bytes
// after the record read, so it composes with the cursor without disturbing
// termination.
func TestScanPageMatch(t *testing.T) {
	s := newSet([]byte("seed-not-int"))
	want := map[string]bool{}
	for i := 0; i < 200; i++ {
		user := []byte("user:" + itoa(int64(i)))
		s.add(user)
		want[string(user)] = true
		s.add([]byte("admin:" + itoa(int64(i))))
	}
	if s.enc != encHashtable {
		t.Fatalf("enc = %s, want hashtable", s.enc)
	}
	seen := map[string]int{}
	var cur uint64
	for {
		next := s.scanPage(cur, 16, []byte("user:*"), func(m []byte) { seen[string(m)]++ })
		if next == 0 {
			break
		}
		cur = next
	}
	if len(seen) != len(want) {
		t.Fatalf("matched %d members, want %d user:* members", len(seen), len(want))
	}
	for k := range want {
		if seen[k] == 0 {
			t.Fatalf("user member %q did not survive the MATCH scan", k)
		}
	}
}

// TestScanPageCountBoundsPage checks COUNT bounds the slots a page examines: a
// single unfiltered page over a large set returns at most COUNT members. COUNT
// is a hint on work done, not a promise on members returned, but with no MATCH
// the two coincide, which is the tightest observable check.
func TestScanPageCountBoundsPage(t *testing.T) {
	s := buildHT(members16(1000))
	for _, count := range []int{1, 7, 10, 100, 250} {
		n := 0
		s.scanPage(0, count, nil, func(m []byte) { n++ })
		if n > count {
			t.Fatalf("count %d page returned %d members, want at most count", count, n)
		}
		if n != count {
			t.Fatalf("count %d unfiltered page returned %d, want exactly count on a set larger than count", count, n)
		}
	}
}

// itoa is defined in this package's tests via the shard helper shape; keep a
// local decimal formatter so the scan tests do not pull strconv into hot paths.
func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
