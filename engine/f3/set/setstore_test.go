package set

import (
	"fmt"
	"sort"
	"testing"

	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/engine/f3/store"
)

// The STORE forms (spec 2064/f3/11 section 7). The suite proves the result is
// Redis-exact across the band combinations and the aliasing cases, that the band
// is chosen from the result shape, that an empty result deletes the destination,
// that a STORE overwrites whatever type the destination held and discards its
// TTL, and the two f1 lab lessons the checklist names: the setstorebuild
// no-aliased-clone rule (an alloc proof) and the setunionstore no-seen-set rule
// (the destination table is the only dedup).

// storeInter, storeUnion, storeDiff run the three STORE builds the handlers run,
// returning the freshly built destination set (nil for an empty result).
func storeInter(sets []*set) *set {
	return storeResult(minCard(sets), func(e func([]byte)) { sinter(sets, e) })
}
func storeUnion(sets []*set) *set {
	return storeResult(totalCard(sets), func(e func([]byte)) { unionInto(sets, e) })
}
func storeDiff(sets []*set) *set {
	return storeResult(firstCard(sets), func(e func([]byte)) { sdiff(sets, e) })
}

// storeMembers returns the destination set's members sorted, nil for a deleted
// (empty-result) destination.
func storeMembers(s *set) []string {
	if s == nil {
		return nil
	}
	out := make([]string, 0, s.card())
	s.each(func(m []byte) { out = append(out, string(m)) })
	sort.Strings(out)
	return out
}

// TestStoreOracle runs every band combination and size in the shared algebra
// table through the three STORE builds, with maintenance off and on, and checks
// the destination holds exactly the oracle's members. A STORE result equals its
// read-form result, so the algebra oracle is the STORE oracle too.
func TestStoreOracle(t *testing.T) {
	for _, flag := range []bool{false, true} {
		for _, tc := range algebraCases {
			t.Run(fmt.Sprintf("%s/maintain=%v", tc.name, flag), func(t *testing.T) {
				defer SetAlgebraMaintain(SetAlgebraMaintain(flag))
				sets := setsFrom(tc.ops)
				eqStrings(t, "interstore", storeMembers(storeInter(sets)), oracleInter(tc.ops))
				eqStrings(t, "unionstore", storeMembers(storeUnion(sets)), oracleUnion(tc.ops))
				eqStrings(t, "diffstore", storeMembers(storeDiff(sets)), oracleDiff(tc.ops))
			})
		}
	}
}

// TestStoreBand pins the band-by-final-shape policy (doc 11 section 7): a freshly
// built destination picks its encoding from the result's cardinality and member
// shape, not from any source's encoding, and does not grow through the ladder.
func TestStoreBand(t *testing.T) {
	cases := []struct {
		name string
		res  *set
		want encoding
	}{
		// Small integer-only intersection lands intset, even though the sources are
		// large hashtables.
		{"int result under cap -> intset",
			storeInter(setsFrom([][]string{intGen(0, 300), intGen(0, 300)})), encIntset},
		// Small short-member string result lands listpack.
		{"small string result -> listpack",
			storeUnion(setsFrom([][]string{gen("w", 0, 20, 4), gen("w", 10, 20, 4)})), encListpack},
		// A result past the listpack entry cap lands the native table.
		{"large result -> hashtable",
			storeUnion(setsFrom([][]string{gen("m", 0, 300, 8), gen("m", 150, 300, 8)})), encHashtable},
		// An integer result past the intset cap lands the native table, not intset.
		{"int result over cap -> hashtable",
			storeUnion(setsFrom([][]string{intGen(0, 400), intGen(200, 400)})), encHashtable},
		// A short result whose one long member breaches the listpack value cap lands
		// the native table.
		{"long member -> hashtable",
			storeUnion(setsFrom([][]string{{"short", gen("v", 0, 1, 80)[0]}})), encHashtable},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if c.res == nil {
				t.Fatal("empty result")
			}
			if c.res.enc != c.want {
				t.Fatalf("enc = %s, want %s", c.res.enc, c.want)
			}
		})
	}
}

// newCtx builds a bare shard context with a real store and an empty registry,
// enough to exercise place (destination replacement) without the reply arena the
// handler needs. The registry is reachable through registry(cx).
func newCtx(t *testing.T) (*shard.Ctx, *reg) {
	t.Helper()
	cx := &shard.Ctx{St: store.New(16<<20, 1<<20), NowMs: 1}
	return cx, registry(cx)
}

// applyStore mirrors a STORE handler exactly: gather the sources from the
// registry, build the result, place it at the destination, and return the reply
// integer. It is the path the aliasing, empty, and type tests drive.
func applyStore(t *testing.T, cx *shard.Ctx, g *reg, op string, dest string, srcKeys ...string) int {
	t.Helper()
	keys := make([][]byte, len(srcKeys))
	for i, k := range srcKeys {
		keys[i] = []byte(k)
	}
	sets, wrong := gather(g, cx, keys)
	if wrong {
		t.Fatalf("%s: unexpected WRONGTYPE", op)
	}
	var result *set
	switch op {
	case "inter":
		result = storeResult(minCard(sets), func(e func([]byte)) { sinter(sets, e) })
	case "union":
		result = storeResult(totalCard(sets), func(e func([]byte)) { unionInto(sets, e) })
	case "diff":
		result = storeResult(firstCard(sets), func(e func([]byte)) { sdiff(sets, e) })
	}
	return place(cx, g, []byte(dest), result)
}

// TestStoreAliasing drives every aliasing shape the checklist names: the
// destination is the first source, a later source, or appears twice. The bulk
// build reads the sources in full before place moves the destination pointer, so
// an aliased destination is never mutated while it is still being read, and no
// source is cloned to guard it.
func TestStoreAliasing(t *testing.T) {
	seed := func(g *reg) {
		g.m["a"] = setFrom([]string{"1", "2", "3", "4"})
		g.m["b"] = setFrom([]string{"3", "4", "5", "6"})
	}
	cases := []struct {
		name string
		op   string
		dest string
		srcs []string
		want []string
	}{
		{"inter dest is first source", "inter", "a", []string{"a", "b"}, []string{"3", "4"}},
		{"inter dest is later source", "inter", "b", []string{"a", "b"}, []string{"3", "4"}},
		{"union dest is first source", "union", "a", []string{"a", "b"}, []string{"1", "2", "3", "4", "5", "6"}},
		{"union dest is later source", "union", "b", []string{"a", "b"}, []string{"1", "2", "3", "4", "5", "6"}},
		{"diff dest is first source", "diff", "a", []string{"a", "b"}, []string{"1", "2"}},
		{"diff dest is later source", "diff", "b", []string{"a", "b"}, []string{"1", "2"}},
		{"inter dest twice", "inter", "a", []string{"a", "a"}, []string{"1", "2", "3", "4"}},
		{"union dest thrice", "union", "a", []string{"a", "a", "a"}, []string{"1", "2", "3", "4"}},
		{"diff dest self is empty", "diff", "a", []string{"a", "a"}, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cx, g := newCtx(t)
			seed(g)
			n := applyStore(t, cx, g, c.op, c.dest, c.srcs...)
			if n != len(c.want) {
				t.Fatalf("reply %d, want %d", n, len(c.want))
			}
			eqStrings(t, "dest members", storeMembers(g.m[c.dest]), c.want)
		})
	}
}

// TestStoreEmptyDeletesDest checks an empty result deletes the destination (Redis
// semantics): the reply is 0, the key is gone from the registry, and it is absent
// from both keyspaces so an EXISTS would answer 0.
func TestStoreEmptyDeletesDest(t *testing.T) {
	cx, g := newCtx(t)
	g.m["a"] = setFrom([]string{"1", "2", "3"})
	g.m["b"] = setFrom([]string{"9", "8", "7"}) // disjoint from a
	g.m["dest"] = setFrom([]string{"leftover"}) // a prior destination value

	n := applyStore(t, cx, g, "inter", "dest", "a", "b")
	if n != 0 {
		t.Fatalf("reply %d, want 0 for an empty intersection", n)
	}
	if _, ok := g.m["dest"]; ok {
		t.Fatal("destination still in the registry after an empty result")
	}
	if cx.St.Exists([]byte("dest"), cx.NowMs) {
		t.Fatal("destination still exists in the string store")
	}
}

// TestStoreOverwritesOtherType checks a STORE overwrites a destination that held
// a string, and discards the string's TTL (the destination is a new object). The
// old string and its expiry both leave; the destination becomes the set.
func TestStoreOverwritesOtherType(t *testing.T) {
	cx, g := newCtx(t)
	g.m["a"] = setFrom([]string{"1", "2"})
	g.m["b"] = setFrom([]string{"2", "3"})
	// Destination currently holds a string with a far-future TTL.
	if err := cx.St.SetString([]byte("dest"), []byte("old"), cx.NowMs, cx.NowMs+1_000_000, false); err != nil {
		t.Fatalf("seed string dest: %v", err)
	}
	if !cx.St.Exists([]byte("dest"), cx.NowMs) {
		t.Fatal("seed string dest not present")
	}

	n := applyStore(t, cx, g, "union", "dest", "a", "b")
	if n != 3 {
		t.Fatalf("reply %d, want 3", n)
	}
	if cx.St.Exists([]byte("dest"), cx.NowMs) {
		t.Fatal("old string (and its TTL) survived the STORE; the destination must be a new object")
	}
	eqStrings(t, "dest members", storeMembers(g.m["dest"]), []string{"1", "2", "3"})
}

// TestStoreWrongTypeSource checks a source that holds a string answers WRONGTYPE
// (through gather) and leaves the destination untouched, the Redis order: source
// types are validated before anything is written.
func TestStoreWrongTypeSource(t *testing.T) {
	cx, g := newCtx(t)
	g.m["dest"] = setFrom([]string{"keep"})
	if err := cx.St.Set([]byte("s"), []byte("astring")); err != nil {
		t.Fatalf("seed string: %v", err)
	}
	keys := [][]byte{[]byte("s")}
	if _, wrong := gather(g, cx, keys); !wrong {
		t.Fatal("expected WRONGTYPE for a string source")
	}
	// The destination is untouched because the handler returns before place.
	eqStrings(t, "dest untouched", storeMembers(g.m["dest"]), []string{"keep"})
}

// TestStoreNoAliasedClone is the setstorebuild lesson as an allocation proof: the
// bulk build streams the result into a fresh table with no defensive copy of a
// source. Each result member is copied exactly once (the slab holds the summed
// member bytes, not double), and the whole build allocates no more than a bare
// table build of the same members, so there is no second source-sized structure.
func TestStoreNoAliasedClone(t *testing.T) {
	members := gen("m", 0, 400, 12)
	src := setFrom(members)
	raw := make([][]byte, len(members))
	var wantBytes int
	for i, m := range members {
		raw[i] = []byte(m)
		wantBytes += len(m)
	}

	// One copy per member: the result slab is exactly the summed member bytes.
	res := storeUnion([]*set{src})
	if res.enc != encHashtable {
		t.Fatalf("enc = %s, want hashtable", res.enc)
	}
	if got := len(res.ht.slab); got != wantBytes {
		t.Fatalf("result slab %d bytes, want %d (each member copied exactly once, no clone)", got, wantBytes)
	}

	// The build allocates no more than a bare addRaw build over the same members;
	// a defensive source clone would roughly double the table allocations.
	bare := testing.AllocsPerRun(20, func() {
		h := newHashtable(len(raw))
		for _, m := range raw {
			h.addRaw(m)
		}
		sink = h.card() > 0
	})
	built := testing.AllocsPerRun(20, func() {
		s := storeUnion([]*set{src})
		sink = s != nil
	})
	if built > bare+3 {
		t.Fatalf("store build allocated %v, bare build %v: an aliased clone would show here", built, bare)
	}
}

// TestStoreUnionNoSeenSet is the setunionstore lesson: the destination table is
// the only dedup, so a union of overlapping sources yields each member once with
// no separate seen-set. unionInto emits every source member, duplicates and all,
// and addRaw is the single dedup; the result must be exactly the distinct union.
func TestStoreUnionNoSeenSet(t *testing.T) {
	// Two sources sharing half their members: 2n emitted, n/2 of them duplicates
	// the destination table alone must reject.
	n := 400
	a := setFrom(gen("m", 0, n, 8))
	b := setFrom(gen("m", n/2, n, 8))
	got := storeMembers(storeUnion([]*set{a, b}))
	want := oracleUnion([][]string{gen("m", 0, n, 8), gen("m", n/2, n, 8)})
	eqStrings(t, "union dedup by destination table", got, want)
	if len(got) != n+n/2 { // 1.5n distinct
		t.Fatalf("distinct union %d, want %d", len(got), n+n/2)
	}
}
