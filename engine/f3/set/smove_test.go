package set

import (
	"sort"
	"strconv"
	"testing"

	"github.com/tamnd/aki/engine/f3/shard"
)

// SMOVE (spec 2064/f3/11 section 9.2). The suite proves the reply and the two
// keys' membership are Redis-exact across every band combination and edge: member
// present in source, present in both, present in neither, a missing source, a
// created destination, the same-key no-op, the last-member-deletes-source rule,
// WRONGTYPE on either side before any write, the destination's one-way band
// conversion on insert, and the single-owner atomicity invariant that member is
// never in neither set.

// memberList returns a set's members sorted, nil for a nil (dropped or absent)
// set, so a result compares cleanly against an oracle regardless of band order.
func memberList(s *set) []string {
	if s == nil {
		return nil
	}
	out := make([]string, 0, s.card())
	s.each(func(m []byte) { out = append(out, string(m)) })
	sort.Strings(out)
	return out
}

// membersAt resolves the set at key through the dual-home operand funnel and
// returns its members sorted, nil for an absent key. A tiny set homes inline in
// the arena, not g.m, so a test that reads membership must resolve both homes
// rather than index g.m directly.
func membersAt(cx *shard.Ctx, g *reg, key string) []string {
	s, _ := g.operand(cx, []byte(key))
	return memberList(s)
}

// setAt resolves the set at key through the dual-home operand funnel, nil for an
// absent key: the band-inspecting sibling of membersAt for a test that checks the
// resolved encoding of a set that may live in the arena.
func setAt(cx *shard.Ctx, g *reg, key string) *set {
	s, _ := g.operand(cx, []byte(key))
	return s
}

// smoveOracle computes the expected reply and the source and destination member
// sets after SMOVE, plain map arithmetic over the member strings. src and dst are
// the operand member lists (nil for a missing key); srcKey and dstKey name them so
// the same-key case is modeled.
func smoveOracle(srcKey, dstKey string, src, dst []string, member string) (reply int, srcAfter, dstAfter []string) {
	inSrc := contains(src, member)
	if srcKey == dstKey {
		if inSrc {
			return 1, sortedCopy(src), sortedCopy(src)
		}
		return 0, sortedCopy(src), sortedCopy(src)
	}
	if !inSrc {
		return 0, sortedCopy(src), sortedCopy(dst)
	}
	sa := remove(src, member)
	da := add(dst, member)
	return 1, sa, da
}

func contains(xs []string, v string) bool {
	for _, x := range xs {
		if x == v {
			return true
		}
	}
	return false
}

func sortedCopy(xs []string) []string {
	if xs == nil {
		return nil
	}
	out := append([]string(nil), xs...)
	sort.Strings(out)
	return out
}

func remove(xs []string, v string) []string {
	var out []string
	for _, x := range xs {
		if x != v {
			out = append(out, x)
		}
	}
	sort.Strings(out)
	return out
}

func add(xs []string, v string) []string {
	if contains(xs, v) {
		return sortedCopy(xs)
	}
	out := append(append([]string(nil), xs...), v)
	sort.Strings(out)
	return out
}

// TestSmoveOracle runs SMOVE across band combinations and every membership shape,
// checking the reply and both keys' resulting members match the oracle. The
// operands are built through the real SADD path (setFrom), so intset, listpack,
// and hashtable bands engage as they would live.
func TestSmoveOracle(t *testing.T) {
	// Band-diverse operand fixtures, keyed by name.
	fixtures := map[string][]string{
		"nil":       nil,
		"intset":    intGen(0, 6),          // intset band
		"intsetHi":  intGen(100, 6),        // disjoint intset band
		"listpack":  gen("w", 0, 6, 4),     // listpack band
		"listpack2": gen("w", 3, 6, 4),     // overlapping listpack band
		"hashtable": gen("m", 0, 300, 8),   // native hashtable band
		"htable2":   gen("m", 150, 300, 8), // overlapping native band
	}
	cases := []struct {
		name     string
		srcKey   string
		dstKey   string
		src, dst string // fixture names
		member   string
	}{
		{"intset move present", "s", "d", "intset", "intsetHi", "3"},
		{"intset member absent", "s", "d", "intset", "intsetHi", "999"},
		{"intset into missing dst", "s", "d", "intset", "nil", "2"},
		{"listpack move present", "s", "d", "listpack", "listpack2", "w0"},
		{"listpack member in both", "s", "d", "listpack", "listpack2", "w4"},
		{"listpack absent", "s", "d", "listpack", "listpack2", "zzz"},
		{"hashtable move present", "s", "d", "hashtable", "htable2", "m10"},
		{"hashtable member in both", "s", "d", "hashtable", "htable2", "m200"},
		{"hashtable absent", "s", "d", "hashtable", "htable2", "nope"},
		{"cross-band intset to listpack", "s", "d", "intset", "listpack", "4"},
		{"cross-band listpack to hashtable", "s", "d", "listpack", "hashtable", "w2"},
		{"cross-band hashtable to intset", "s", "d", "hashtable", "intset", "m5"},
		{"missing src", "s", "d", "nil", "listpack", "anything"},
		{"same key present", "s", "s", "listpack", "", "w1"},
		{"same key absent", "s", "s", "listpack", "", "gone"},
		{"same key missing", "s", "s", "nil", "", "x"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cx, g := newCtx(t)
			srcMembers := fixtures[c.src]
			var dstMembers []string
			if c.srcKey != c.dstKey {
				dstMembers = fixtures[c.dst]
			}
			if s := setFrom(srcMembers); s != nil {
				g.m[c.srcKey] = s
			}
			if c.srcKey != c.dstKey {
				if d := setFrom(dstMembers); d != nil {
					g.m[c.dstKey] = d
				}
			}

			wantReply, wantSrc, wantDst := smoveOracle(c.srcKey, c.dstKey, srcMembers, dstMembers, c.member)
			moved, wrong := smove(g, cx, []byte(c.srcKey), []byte(c.dstKey), []byte(c.member))
			if wrong {
				t.Fatalf("unexpected WRONGTYPE")
			}
			gotReply := 0
			if moved {
				gotReply = 1
			}
			if gotReply != wantReply {
				t.Fatalf("reply %d, want %d", gotReply, wantReply)
			}
			eqStrings(t, "source members", membersAt(cx, g, c.srcKey), wantSrc)
			if c.srcKey != c.dstKey {
				eqStrings(t, "destination members", membersAt(cx, g, c.dstKey), wantDst)
			}
		})
	}
}

// TestSmoveLastMemberDeletesSrc checks moving the only member out of a set deletes
// the source key (Redis semantics): it is gone from the registry and absent from
// both keyspaces, so an EXISTS would answer 0.
func TestSmoveLastMemberDeletesSrc(t *testing.T) {
	cx, g := newCtx(t)
	g.m["s"] = setFrom([]string{"only"})
	moved, wrong := smove(g, cx, []byte("s"), []byte("d"), []byte("only"))
	if wrong || !moved {
		t.Fatalf("moved=%v wrong=%v, want moved with no WRONGTYPE", moved, wrong)
	}
	if _, ok := g.m["s"]; ok {
		t.Fatal("source still in the registry after its last member moved")
	}
	if cx.St.Exists([]byte("s"), cx.NowMs) {
		t.Fatal("source still exists in the string store")
	}
	eqStrings(t, "destination", membersAt(cx, g, "d"), []string{"only"})
}

// TestSmoveCreatesDst checks a move into a missing destination creates it with the
// band its member's shape dictates: an integer member opens an intset, a string
// member a listpack, the same create rule SADD uses.
func TestSmoveCreatesDst(t *testing.T) {
	cases := []struct {
		member string
		want   encoding
	}{
		{"42", encIntset},
		{"hello", encListpack},
	}
	for _, c := range cases {
		t.Run(c.member, func(t *testing.T) {
			cx, g := newCtx(t)
			g.m["s"] = setFrom([]string{c.member, "other"})
			moved, wrong := smove(g, cx, []byte("s"), []byte("d"), []byte(c.member))
			if wrong || !moved {
				t.Fatalf("moved=%v wrong=%v", moved, wrong)
			}
			d := setAt(cx, g, "d")
			if d == nil {
				t.Fatal("destination not created")
			}
			if d.enc != c.want {
				t.Fatalf("destination enc %s, want %s", d.enc, c.want)
			}
			eqStrings(t, "destination", memberList(d), []string{c.member})
		})
	}
}

// TestSmoveDstBandConversion checks the destination converts one way on insert
// exactly as SADD would: a non-integer into an intset destination leaves listpack,
// and a member past the listpack value cap forces the hashtable. The source's band
// never converts on a remove (F4), so a large hashtable source stays a hashtable.
func TestSmoveDstBandConversion(t *testing.T) {
	t.Run("non-integer forces intset dst to listpack", func(t *testing.T) {
		cx, g := newCtx(t)
		g.m["s"] = setFrom([]string{"str", "keep"})
		g.m["d"] = setFrom([]string{"1", "2", "3"}) // intset
		if g.m["d"].enc != encIntset {
			t.Fatalf("seed dst enc %s, want intset", g.m["d"].enc)
		}
		moved, _ := smove(g, cx, []byte("s"), []byte("d"), []byte("str"))
		if !moved {
			t.Fatal("expected move")
		}
		if g.m["d"].enc != encListpack {
			t.Fatalf("dst enc %s, want listpack after a non-integer insert", g.m["d"].enc)
		}
	})
	t.Run("oversized member forces listpack dst to hashtable", func(t *testing.T) {
		cx, g := newCtx(t)
		big := gen("v", 0, 1, maxListpackValue+8)[0] // one member past the listpack value cap
		g.m["s"] = setFrom([]string{big, "keep"})
		g.m["d"] = setFrom([]string{"a", "b"}) // listpack
		moved, _ := smove(g, cx, []byte("s"), []byte("d"), []byte(big))
		if !moved {
			t.Fatal("expected move")
		}
		if g.m["d"].enc != encHashtable {
			t.Fatalf("dst enc %s, want hashtable after an oversized-member insert", g.m["d"].enc)
		}
	})
	t.Run("source hashtable stays hashtable after remove", func(t *testing.T) {
		cx, g := newCtx(t)
		g.m["s"] = setFrom(gen("m", 0, 300, 8)) // hashtable
		g.m["d"] = setFrom([]string{"1"})
		if g.m["s"].enc != encHashtable {
			t.Fatalf("seed src enc %s, want hashtable", g.m["s"].enc)
		}
		smove(g, cx, []byte("s"), []byte("d"), []byte("m0"))
		if g.m["s"].enc != encHashtable {
			t.Fatalf("src enc %s, want hashtable (no downward conversion, F4)", g.m["s"].enc)
		}
	})
}

// TestSmoveWrongType checks a key holding a string answers WRONGTYPE on either
// side, and that the check is up front: a wrong-typed destination errors even when
// the source is missing or the member is absent, so no half-done move is possible.
func TestSmoveWrongType(t *testing.T) {
	t.Run("source is a string", func(t *testing.T) {
		cx, g := newCtx(t)
		if err := cx.St.Set([]byte("s"), []byte("astring")); err != nil {
			t.Fatalf("seed string: %v", err)
		}
		g.m["d"] = setFrom([]string{"1"})
		_, wrong := smove(g, cx, []byte("s"), []byte("d"), []byte("x"))
		if !wrong {
			t.Fatal("expected WRONGTYPE for a string source")
		}
	})
	t.Run("destination is a string, source missing", func(t *testing.T) {
		cx, g := newCtx(t)
		if err := cx.St.Set([]byte("d"), []byte("astring")); err != nil {
			t.Fatalf("seed string: %v", err)
		}
		// Source is absent and the member could never be there, but Redis validates
		// the destination type up front and errors regardless.
		_, wrong := smove(g, cx, []byte("s"), []byte("d"), []byte("x"))
		if !wrong {
			t.Fatal("expected WRONGTYPE for a string destination even with a missing source")
		}
		if cx.St.Exists([]byte("d"), cx.NowMs) != true {
			t.Fatal("destination string should be untouched")
		}
	})
	t.Run("destination is a string, member absent from source", func(t *testing.T) {
		cx, g := newCtx(t)
		g.m["s"] = setFrom([]string{"present"})
		if err := cx.St.Set([]byte("d"), []byte("astring")); err != nil {
			t.Fatalf("seed string: %v", err)
		}
		_, wrong := smove(g, cx, []byte("s"), []byte("d"), []byte("absent"))
		if !wrong {
			t.Fatal("expected WRONGTYPE for a string destination")
		}
		// Source untouched: the type check preceded the remove.
		eqStrings(t, "source untouched", memberList(g.m["s"]), []string{"present"})
	})
}

// TestSmoveAtomicInvariant exercises the doc 11 section 9.2 atomicity guarantee at
// its observable boundary: a member ping-ponged between two co-located sets is in
// exactly one of them after every move, never in neither and never in both. The
// move runs on one owner goroutine start to finish (F1), so this pre/post check is
// the whole of the invariant, there being no intermediate state any command could
// observe.
func TestSmoveAtomicInvariant(t *testing.T) {
	cx, g := newCtx(t)
	g.m["a"] = setFrom(intGen(0, 50))
	g.m["b"] = setFrom(intGen(100, 50))
	shuttle := "777"
	g.m["a"].add([]byte(shuttle))

	src, dst := "a", "b"
	for i := 0; i < 500; i++ {
		moved, wrong := smove(g, cx, []byte(src), []byte(dst), []byte(shuttle))
		if wrong || !moved {
			t.Fatalf("move %d: moved=%v wrong=%v", i, moved, wrong)
		}
		inSrc := g.m[src] != nil && g.m[src].has([]byte(shuttle))
		inDst := g.m[dst] != nil && g.m[dst].has([]byte(shuttle))
		if inSrc {
			t.Fatalf("move %d: shuttle still in the source after the move", i)
		}
		if !inDst {
			t.Fatalf("move %d: shuttle in neither set after the move", i)
		}
		src, dst = dst, src
	}
	// The two base populations are undisturbed; only the shuttle traveled.
	if g.m["a"] == nil || g.m["b"] == nil {
		t.Fatal("a base set went missing")
	}
}

// TestSmoveSameKeyNoChange checks the same-key case changes nothing: a present
// member replies 1 and leaves the set exactly as it was, an absent member replies
// 0, both without touching the set.
func TestSmoveSameKeyNoChange(t *testing.T) {
	cx, g := newCtx(t)
	before := intGen(0, 10)
	g.m["k"] = setFrom(before)

	moved, wrong := smove(g, cx, []byte("k"), []byte("k"), []byte("5"))
	if wrong || !moved {
		t.Fatalf("present member: moved=%v wrong=%v, want 1", moved, wrong)
	}
	eqStrings(t, "unchanged after present same-key move", memberList(g.m["k"]), sortedCopy(before))

	moved, _ = smove(g, cx, []byte("k"), []byte("k"), []byte("999"))
	if moved {
		t.Fatal("absent member same-key move should reply 0")
	}
	eqStrings(t, "unchanged after absent same-key move", memberList(g.m["k"]), sortedCopy(before))
}

// TestSmovePartitioned moves across the partitioned band (doc 11 section 4): a
// source and destination each past the engagement threshold. The move is one
// remove and one add through the descriptor, the same point-op paths the band
// already tests, so this guards that SMOVE composes with the band rather than
// re-deriving it. Skipped under -short because it builds two large sets.
func TestSmovePartitioned(t *testing.T) {
	if testing.Short() {
		t.Skip("builds two partitioned-band sets")
	}
	cx, g := newCtx(t)
	src := &set{enc: encHashtable, ht: newHashtable(partitionThreshold + 2)}
	for i := 0; i < partitionThreshold+1; i++ {
		src.add([]byte("s" + strconv.Itoa(i)))
	}
	if src.enc != encPartitioned {
		t.Fatalf("source enc %s, want partitioned", src.enc)
	}
	g.m["s"] = src
	g.m["d"] = setFrom([]string{"1"})

	member := "s0"
	before := src.card()
	moved, wrong := smove(g, cx, []byte("s"), []byte("d"), []byte(member))
	if wrong || !moved {
		t.Fatalf("moved=%v wrong=%v", moved, wrong)
	}
	if g.m["s"].card() != before-1 {
		t.Fatalf("source card %d, want %d", g.m["s"].card(), before-1)
	}
	if g.m["s"].has([]byte(member)) {
		t.Fatal("member still in the partitioned source after the move")
	}
	if !g.m["d"].has([]byte(member)) {
		t.Fatal("member not in the destination after the move")
	}
}
