package set

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// The algebra-driver oracle suite (spec 2064/f3/11 section 6.4). Every case runs
// with SetAlgebraMaintain off and on: off proves the probe path is correct over
// every band, on proves the merge path returns exactly the same members, so the
// dispatcher's choice is invisible to the reply. The oracle is plain map
// arithmetic over the member strings.

// setFrom builds an operand from a member list, nil for a missing key. It applies
// members through the real SADD path so the band (intset, listpack, hashtable)
// and, under the flag, the algebra index engage exactly as they would live.
func setFrom(members []string) *set {
	if members == nil {
		return nil
	}
	s := newSet([]byte(members[0]))
	for _, m := range members {
		s.add([]byte(m))
	}
	return s
}

// indexedSet forces a native hashtable operand with its sorted arrays engaged,
// regardless of the flag, so the merge-choice unit tests can exercise the
// crossover directly.
func indexedSet(members []string) *set {
	s := &set{enc: encHashtable, ht: newHashtable(len(members))}
	for _, m := range members {
		s.ht.addRaw([]byte(m))
	}
	s.ht.engageAlgebra()
	return s
}

// gen builds n distinct members with a prefix; width pads each to at least w
// bytes so the large-member crossover bias can be exercised.
func gen(prefix string, lo, n, w int) []string {
	out := make([]string, n)
	for i := 0; i < n; i++ {
		m := fmt.Sprintf("%s%d", prefix, lo+i)
		if len(m) < w {
			m += strings.Repeat("x", w-len(m))
		}
		out[i] = m
	}
	return out
}

// intGen builds n distinct integer members from lo, the intset band.
func intGen(lo, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = strconv.Itoa(lo + i)
	}
	return out
}

func toSet(members []string) map[string]bool {
	if members == nil {
		return nil
	}
	m := make(map[string]bool, len(members))
	for _, s := range members {
		m[s] = true
	}
	return m
}

func oracleInter(ops [][]string) []string {
	for _, op := range ops {
		if len(op) == 0 {
			return nil
		}
	}
	base := toSet(ops[0])
	var out []string
	for m := range base {
		in := true
		for _, op := range ops[1:] {
			if !toSet(op)[m] {
				in = false
				break
			}
		}
		if in {
			out = append(out, m)
		}
	}
	sort.Strings(out)
	return out
}

func oracleUnion(ops [][]string) []string {
	u := map[string]bool{}
	for _, op := range ops {
		for _, m := range op {
			u[m] = true
		}
	}
	out := make([]string, 0, len(u))
	for m := range u {
		out = append(out, m)
	}
	sort.Strings(out)
	return out
}

func oracleDiff(ops [][]string) []string {
	if len(ops[0]) == 0 {
		return nil
	}
	var out []string
	for _, m := range ops[0] {
		excluded := false
		for _, op := range ops[1:] {
			if toSet(op)[m] {
				excluded = true
				break
			}
		}
		if !excluded {
			out = append(out, m)
		}
	}
	// ops[0] is distinct already, but dedup defensively for the oracle.
	sort.Strings(out)
	return out
}

func driveInter(sets []*set) []string {
	var got []string
	sinter(sets, func(m []byte) { got = append(got, string(m)) })
	sort.Strings(got)
	return got
}

func driveUnion(sets []*set) []string {
	var got []string
	sunion(sets, func(m []byte) { got = append(got, string(m)) })
	sort.Strings(got)
	return got
}

func driveDiff(sets []*set) []string {
	var got []string
	sdiff(sets, func(m []byte) { got = append(got, string(m)) })
	sort.Strings(got)
	return got
}

func setsFrom(ops [][]string) []*set {
	sets := make([]*set, len(ops))
	for i, op := range ops {
		sets[i] = setFrom(op)
	}
	return sets
}

// algebraCases spans the band combinations (intset x listpack x hashtable), the
// shapes doc 11 section 12.2 names (disjoint, equal, subset, partial, skewed
// 1:1000), sizes straddling the floor (128) and the crossover (k=7), and missing
// keys. A nil operand is a missing key.
var algebraCases = []struct {
	name string
	ops  [][]string
}{
	{"two intsets equal", [][]string{intGen(0, 50), intGen(0, 50)}},
	{"two intsets partial", [][]string{intGen(0, 60), intGen(30, 60)}},
	{"intset disjoint", [][]string{intGen(0, 40), intGen(1000, 40)}},
	{"listpack pair", [][]string{gen("w", 0, 20, 4), gen("w", 10, 20, 4)}},
	{"intset vs listpack", [][]string{intGen(0, 30), gen("w", 0, 30, 4)}},
	{"hashtable equal above floor", [][]string{gen("m", 0, 300, 8), gen("m", 0, 300, 8)}},
	{"hashtable partial above floor", [][]string{gen("m", 0, 300, 8), gen("m", 150, 300, 8)}},
	{"hashtable subset", [][]string{gen("m", 50, 150, 8), gen("m", 0, 600, 8)}},
	{"merge-eligible ratio 6", [][]string{gen("m", 0, 200, 8), gen("m", 0, 1200, 8)}},
	{"probe skew ratio 10", [][]string{gen("m", 0, 200, 8), gen("m", 0, 2000, 8)}},
	{"skewed 1 to 1000", [][]string{gen("s", 0, 10, 8), gen("s", 0, 10000, 8)}},
	{"large members ratio 10", [][]string{gen("L", 0, 200, 40), gen("L", 0, 2000, 40)}},
	{"three operands", [][]string{gen("m", 0, 300, 8), gen("m", 100, 300, 8), gen("m", 150, 300, 8)}},
	{"three intsets", [][]string{intGen(0, 80), intGen(20, 80), intGen(40, 80)}},
	{"missing first", [][]string{nil, gen("m", 0, 200, 8)}},
	{"missing middle", [][]string{gen("m", 0, 200, 8), nil, gen("m", 0, 200, 8)}},
	{"all missing", [][]string{nil, nil}},
	{"single operand hashtable", [][]string{gen("m", 0, 300, 8)}},
	{"single operand intset", [][]string{intGen(0, 40)}},
}

func TestAlgebraDriverOracle(t *testing.T) {
	for _, flag := range []bool{false, true} {
		for _, tc := range algebraCases {
			t.Run(fmt.Sprintf("%s/maintain=%v", tc.name, flag), func(t *testing.T) {
				defer SetAlgebraMaintain(SetAlgebraMaintain(flag))
				sets := setsFrom(tc.ops)

				eqStrings(t, "inter", driveInter(sets), oracleInter(tc.ops))
				eqStrings(t, "union", driveUnion(sets), oracleUnion(tc.ops))
				eqStrings(t, "diff", driveDiff(sets), oracleDiff(tc.ops))

				wantCard := len(oracleInter(tc.ops))
				if got := sintercard(sets, 0); got != wantCard {
					t.Fatalf("sintercard(limit 0) = %d, want %d", got, wantCard)
				}
			})
		}
	}
}

// TestSintercardLimit checks the LIMIT early-stop over both paths: a positive
// limit caps the count at min(limit, card), and limit 0 is unlimited (Redis).
func TestSintercardLimit(t *testing.T) {
	// Two shapes: one merge-eligible (both hashtable, ratio 1, flag on) and one
	// forced onto the probe path (intsets, no index), so the early exit is proven
	// on both branches.
	shapes := []struct {
		name string
		ops  [][]string
		card int
	}{
		{"hashtable equal", [][]string{gen("m", 0, 300, 8), gen("m", 0, 300, 8)}, 300},
		{"intset equal", [][]string{intGen(0, 300), intGen(0, 300)}, 300},
	}
	for _, flag := range []bool{false, true} {
		for _, sh := range shapes {
			t.Run(fmt.Sprintf("%s/maintain=%v", sh.name, flag), func(t *testing.T) {
				defer SetAlgebraMaintain(SetAlgebraMaintain(flag))
				sets := setsFrom(sh.ops)
				for _, limit := range []int{0, 1, 10, 100, sh.card - 1, sh.card, sh.card + 50} {
					want := limit
					if limit == 0 || limit > sh.card {
						want = sh.card
					}
					if got := sintercard(sets, limit); got != want {
						t.Fatalf("limit %d: got %d, want %d", limit, got, want)
					}
				}
			})
		}
	}
}

// TestChooseMergeCrossover pins the k=7 crossover and the large-member bias in
// the dispatcher (lab 03). Small operands (8-byte members) switch to probe at
// ratio 7; large operands (40-byte members) stay on merge well past it.
func TestChooseMergeCrossover(t *testing.T) {
	small := indexedSet(gen("m", 0, 200, 8)) // 200 members, avg < 32 bytes
	// ratio 6 (1200/200): below k=7, merge.
	if !chooseMergeIntersect(small, indexedSet(gen("m", 0, 1200, 8))) {
		t.Fatal("ratio 6 should choose merge")
	}
	// ratio 7 (1400/200): at k=7, probe (large.card < small.card*7 is false).
	if chooseMergeIntersect(small, indexedSet(gen("m", 0, 1400, 8))) {
		t.Fatal("ratio 7 should choose probe")
	}
	// ratio 10 (2000/200): well past k=7, probe.
	if chooseMergeIntersect(small, indexedSet(gen("m", 0, 2000, 8))) {
		t.Fatal("ratio 10 should choose probe")
	}

	// Large members: the bias raises the crossover to 64, so ratio 10 still merges.
	big := indexedSet(gen("L", 0, 200, 40)) // avg >= 32 bytes
	if big.avgMemberBytes() < largeMemberBytes {
		t.Fatalf("large-member operand avg %d, want >= %d", big.avgMemberBytes(), largeMemberBytes)
	}
	if !chooseMergeIntersect(big, indexedSet(gen("L", 0, 2000, 40))) {
		t.Fatal("large members at ratio 10 should still choose merge")
	}
}

// TestChooseMergeGuards checks the merge path stays off when it must: below the
// floor, an inline operand, or an unindexed operand (flag off) all fall to probe.
func TestChooseMergeGuards(t *testing.T) {
	// Below the floor: even indexed, the small operand is too small to merge.
	tiny := indexedSet(gen("m", 0, algebraFloor-1, 8))
	if chooseMergeIntersect(tiny, indexedSet(gen("m", 0, 300, 8))) {
		t.Fatal("below the floor must choose probe")
	}
	// An inline listpack operand is never mergeable.
	lp := setFrom(gen("w", 0, 20, 4))
	if lp.mergeable() {
		t.Fatal("listpack operand reported mergeable")
	}
	// A hashtable with no engaged index (flag off during build) is not mergeable.
	defer SetAlgebraMaintain(SetAlgebraMaintain(false))
	plain := setFrom(gen("m", 0, 300, 8))
	if plain.enc != encHashtable {
		t.Fatalf("enc = %s, want hashtable", plain.enc)
	}
	if plain.mergeable() {
		t.Fatal("unindexed hashtable reported mergeable")
	}
}
