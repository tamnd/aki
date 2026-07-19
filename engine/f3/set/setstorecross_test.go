package set

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/tamnd/aki/engine/f3/shard"
)

// Cross-shard set STORE forms (setstorecross.go): the differential suite holds
// the intent-path store byte-identical in effect to the co-located point
// handler across the full operand matrix. The co-located arm IS the oracle, the
// same shape TestGatherCrossDifferential uses for the read algebra: the point
// handlers' own Redis-exactness is setstore_test.go's job, so here the only
// question is whether the cross path stores exactly what the co-located path
// stores. The destination's own iteration order is band-dependent and not part
// of the contract, so the two arms are compared on the integer reply and on the
// sorted destination membership, not on raw SMEMBERS bytes.

// sortedMembers parses a SMEMBERS/SPOP-style flat multi-bulk reply into its
// members sorted, so two stores of the same set compare equal regardless of the
// band-dependent iteration order each destination happens to hold.
func sortedMembers(t *testing.T, rep []byte) []string {
	t.Helper()
	s := string(rep)
	if len(s) == 0 || (s[0] != '*' && s[0] != '~') {
		t.Fatalf("not an array reply: %q", rep)
	}
	nl := strings.IndexByte(s, '\n')
	n, err := strconv.Atoi(strings.TrimSpace(s[1:nl]))
	if err != nil {
		t.Fatalf("bad array header %q: %v", s[:nl], err)
	}
	rest := s[nl+1:]
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		// $len\r\n
		hn := strings.IndexByte(rest, '\n')
		ln, err := strconv.Atoi(strings.TrimSpace(rest[1:hn]))
		if err != nil {
			t.Fatalf("bad bulk header: %v", err)
		}
		rest = rest[hn+1:]
		out = append(out, rest[:ln])
		rest = rest[ln+2:] // skip member and its trailing CRLF
	}
	slices.Sort(out)
	return out
}

// TestStoreCrossDifferential replays SINTERSTORE, SUNIONSTORE, and SDIFFSTORE on
// both paths: the sources co-located with the destination on one shard through
// the point handler, and the same sources spread across shards through the Cross
// store under DoTxn. The integer reply and the sorted stored membership must
// agree across every operand shape, including a destination that is also one of
// the sources (the aliasing case the bulk build handles without a clone).
func TestStoreCrossDifferential(t *testing.T) {
	rt := gatherRuntime(t, 4)
	c := rt.NewConn()

	type operand struct {
		members []string
		str     bool
		missing bool
	}
	set := func(m ...string) operand { return operand{members: m} }
	str := operand{str: true}
	missing := operand{missing: true}

	big := func(lo, hi, step int) operand {
		var m []string
		for i := lo; i < hi; i += step {
			m = append(m, "m"+strconv.Itoa(i))
		}
		return operand{members: m}
	}

	cases := []struct {
		name     string
		operands []operand
	}{
		{"two overlap", []operand{set("a", "b", "c"), set("b", "c", "d")}},
		{"two disjoint", []operand{set("a", "b"), set("x", "y")}},
		{"three chain", []operand{set("a", "b", "c", "d"), set("b", "c", "d"), set("c", "d")}},
		{"first missing", []operand{missing, set("a", "b")}},
		{"middle missing", []operand{set("a", "b", "c"), missing, set("b", "c")}},
		{"last missing", []operand{set("a", "b"), set("a"), missing}},
		{"all missing", []operand{missing, missing}},
		{"intset operands", []operand{set("1", "2", "3", "4"), set("2", "4", "6")}},
		{"intset mixed member", []operand{set("1", "2", "hello"), set("2", "hello", "3")}},
		{"single operand", []operand{set("a", "b", "c")}},
		{"single missing", []operand{missing}},
		{"wrongtype first", []operand{str, set("a")}},
		{"wrongtype middle", []operand{set("a"), str, set("b")}},
		{"wrongtype last", []operand{set("a"), set("b"), str}},
		{"big overlap", []operand{big(0, 400, 1), big(200, 600, 1)}},
		{"big vs small probe", []operand{big(0, 400, 1), set("m5", "m399", "zz")}},
		{"big three", []operand{big(0, 300, 1), big(0, 300, 2), big(0, 300, 3)}},
		{"empty result deletes", []operand{set("a", "b"), set("x", "y")}},
	}

	forms := []struct {
		op   byte
		name string
	}{
		{gcSinterstore, "SINTERSTORE"},
		{gcSunionstore, "SUNIONSTORE"},
		{gcSdiffstore, "SDIFFSTORE"},
	}

	crossFor := func(op byte, dest string, srcs []string) func(tx *shard.Txn) []byte {
		args := bytesKeys(append([]string{dest}, srcs...))
		switch op {
		case gcSinterstore:
			return func(tx *shard.Txn) []byte { return SinterstoreCross(tx, args) }
		case gcSunionstore:
			return func(tx *shard.Txn) []byte { return SunionstoreCross(tx, args) }
		default:
			return func(tx *shard.Txn) []byte { return SdiffstoreCross(tx, args) }
		}
	}

	for ci, tc := range cases {
		for _, form := range forms {
			t.Run(tc.name+"/"+form.name, func(t *testing.T) {
				n := len(tc.operands)
				co := make([]string, n)
				cross := make([]string, n)
				for i, op := range tc.operands {
					p := fmt.Sprintf("s%d_%d_%s", ci, i, form.name)
					co[i] = keyOn(t, rt, 2, "co"+p)
					cross[i] = keyOn(t, rt, i%4, "x"+p)
					seed := func(key string, op operand) {
						switch {
						case op.missing:
						case op.str:
							do(t, c, gcStrSet, 0, key, "notaset")
						default:
							for lo := 0; lo < len(op.members); {
								hi := min(lo+128, len(op.members))
								do(t, c, gcSadd, 0, append([]string{key}, op.members[lo:hi]...)...)
								lo = hi
							}
						}
					}
					seed(co[i], op)
					seed(cross[i], op)
				}

				// The co-located destination shares shard 2 with the co sources; the
				// cross destination lands on shard 3, off the round-robin sources, so
				// the store really does span shards.
				coDest := keyOn(t, rt, 2, fmt.Sprintf("codst%d_%s", ci, form.name))
				crossDest := keyOn(t, rt, 3, fmt.Sprintf("xdst%d_%s", ci, form.name))

				coRep := do(t, c, form.op, 0, append([]string{coDest}, co...)...)
				xRep := crossAlgebra(t, c, append([]string{crossDest}, cross...),
					crossFor(form.op, crossDest, cross))
				if string(coRep) != string(xRep) {
					t.Fatalf("%s reply drift: co-located %q, cross-shard %q", form.name, coRep, xRep)
				}

				coMembers := sortedMembers(t, do(t, c, gcSmembers, 0, coDest))
				xMembers := sortedMembers(t, do(t, c, gcSmembers, 0, crossDest))
				if !slices.Equal(coMembers, xMembers) {
					t.Fatalf("%s stored membership drift: co-located %v, cross-shard %v",
						form.name, coMembers, xMembers)
				}
			})
		}
	}
}

// TestStoreCrossAliasing pins the aliasing STORE across shards: the destination
// is also one of the sources. The bulk build reads every source in full before
// place moves the destination pointer, so the aliased source is never mutated
// while it is still being read. The destination lands on the same shard as the
// aliased source (they are the same key), the other source on a different shard,
// so the store still spans shards through the sources.
func TestStoreCrossAliasing(t *testing.T) {
	rt := gatherRuntime(t, 4)
	c := rt.NewConn()

	dest := keyOn(t, rt, 1, "aliasdst")
	other := keyOn(t, rt, 2, "aliassrc")
	do(t, c, gcSadd, 0, dest, "a", "b", "c", "d")
	do(t, c, gcSadd, 0, other, "b", "c", "e")

	// SINTERSTORE dest dest other: intersect the destination with other, storing
	// back into the destination. Expected result {b, c}.
	xRep := crossAlgebra(t, c, []string{dest, dest, other}, func(tx *shard.Txn) []byte {
		return SinterstoreCross(tx, bytesKeys([]string{dest, dest, other}))
	})
	if string(xRep) != ":2\r\n" {
		t.Fatalf("aliased SINTERSTORE reply = %q, want :2", xRep)
	}
	got := sortedMembers(t, do(t, c, gcSmembers, 0, dest))
	if !slices.Equal(got, []string{"b", "c"}) {
		t.Fatalf("aliased SINTERSTORE stored %v, want [b c]", got)
	}
}
