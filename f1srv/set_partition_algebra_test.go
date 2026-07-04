package f1srv

import (
	"fmt"
	"sort"
	"strings"
	"testing"
)

// Slice 5b routes the set algebra read forms (SINTER/SUNION/SDIFF, SINTERCARD) and their STORE
// forms (SINTERSTORE/SUNIONSTORE/SDIFFSTORE) over the P partitions of every source and destination
// (spec 2064/f1_rewrite_ltm/19 section 6.9). A partitioned set stores its members as P per-partition
// runs that sort by member only within a partition, so the merge-based algebra needs a cursor that
// merges the P runs back into one member-ordered stream, and the probe-based algebra needs its
// membership probes routed to the member's partition. The STORE forms additionally write the result
// into a partitioned destination under routed keys. These tests pin that every form returns and
// stores exactly what the unpartitioned path does across P=1, 2, 4, 8, in both the sorted-merge and
// the smallest-set-probe strategy, and that the type and aliasing guards survive routing.

// setArgs builds a SADD argument list for key with the given members.
func setArgs(key string, members ...string) []string {
	return append([]string{"SADD", key}, members...)
}

// loadSet adds every member to key through the routed SADD path.
func loadSet(t *testing.T, c *connState, key string, members []string) {
	t.Helper()
	call(c, func(c *connState, a [][]byte) { c.cmdSAdd(a) }, setArgs(key, members...)...)
}

// sortedArray parses a flat RESP array-of-bulks reply and returns its elements sorted, so a read
// form's result can be compared order-independently (the algebra order across partitions is
// unspecified, section 6.9). It fails the test on a non-array reply so a shape bug surfaces loudly.
func sortedFlatReply(t *testing.T, reply string) []string {
	t.Helper()
	if !strings.HasPrefix(reply, "*") {
		t.Fatalf("expected an array reply, got %q", reply[:min(len(reply), 40)])
	}
	out := parseArrayBulks(t, reply)
	sort.Strings(out)
	return out
}

// algebraRefSets seeds three overlapping sets on c and returns their key names. The overlap is
// arranged so every read form has a non-trivial non-empty result: the intersection is the shared
// core, the union is everything, and A minus B minus C is A's private members. Members are spread
// across many distinct byte values so they scatter over every partition of P=8.
func algebraRefSets(t *testing.T, c *connState) (a, b, cc string) {
	core := make([]string, 25)
	for i := range core {
		core[i] = fmt.Sprintf("core:%04d", i)
	}
	amem := append([]string{}, core...)
	bmem := append([]string{}, core...)
	cmem := append([]string{}, core[:15]...) // C shares only part of the core
	for i := 0; i < 30; i++ {
		amem = append(amem, fmt.Sprintf("aonly:%04d", i))
		bmem = append(bmem, fmt.Sprintf("bonly:%04d", i))
		cmem = append(cmem, fmt.Sprintf("conly:%04d", i))
	}
	loadSet(t, c, "A", amem)
	loadSet(t, c, "B", bmem)
	loadSet(t, c, "C", cmem)
	return "A", "B", "C"
}

// TestSetAlgebraReadPartitionIdentical runs SINTER, SUNION, SDIFF, and SINTERCARD over three
// overlapping sets at P=1, 2, 4, 8 and asserts each reply matches the P=1 reply. Matching the
// unpartitioned path proves the partition-merging cursor recovers pure member order (so the k-way
// merge is exact) and the routed membership probe finds every member under its partition key.
func TestSetAlgebraReadPartitionIdentical(t *testing.T) {
	type form struct {
		name string
		fn   func(*connState, [][]byte)
		args []string
		flat bool // an array reply compared order-independently; false means an integer reply
	}
	forms := []form{
		{"SINTER", func(c *connState, a [][]byte) { c.cmdSInter(a) }, []string{"SINTER", "A", "B", "C"}, true},
		{"SUNION", func(c *connState, a [][]byte) { c.cmdSUnion(a) }, []string{"SUNION", "A", "B", "C"}, true},
		{"SDIFF", func(c *connState, a [][]byte) { c.cmdSDiff(a) }, []string{"SDIFF", "A", "B", "C"}, true},
		{"SINTERCARD", func(c *connState, a [][]byte) { c.cmdSInterCard(a) }, []string{"SINTERCARD", "3", "A", "B", "C"}, false},
	}

	run := func(p int) map[string]string {
		srv := newPartServer(t, p)
		defer srv.Close()
		c := bareConn(srv)
		algebraRefSets(t, c)
		got := map[string]string{}
		for _, f := range forms {
			reply := call(c, f.fn, f.args...)
			if f.flat {
				got[f.name] = strings.Join(sortedFlatReply(t, reply), "\x00")
			} else {
				got[f.name] = reply
			}
		}
		return got
	}

	ref := run(1)
	// SINTER of A,B,C is the part of the core C also holds: 15 members.
	if want := 15; strings.Count(ref["SINTER"], "\x00")+1 != want {
		t.Fatalf("P=1 SINTER has %d members, want %d", strings.Count(ref["SINTER"], "\x00")+1, want)
	}
	for _, p := range []int{2, 4, 8} {
		got := run(p)
		for _, f := range forms {
			if got[f.name] != ref[f.name] {
				t.Fatalf("P=%d %s = %q, want %q (P=1)", p, f.name, got[f.name], ref[f.name])
			}
		}
	}
}

// TestSetAlgebraProbePartitionIdentical forces the smallest-set-probe strategy (a tiny driver set
// against two large sets) so the routed membership probe path is exercised, not just the merge, and
// asserts SINTER and SINTERCARD match across P. The driver is small enough and the others large
// enough that sinterEach picks sinterProbeEach, which point-probes each large source per driver
// member: without routing those probes to the member's partition they would miss every hit and the
// intersection would come back empty at P>1.
func TestSetAlgebraProbePartitionIdentical(t *testing.T) {
	load := func(c *connState) {
		small := make([]string, 10)
		for i := range small {
			small[i] = fmt.Sprintf("core:%04d", i)
		}
		loadSet(t, c, "small", small)
		for _, k := range []string{"big1", "big2"} {
			big := append([]string{}, small...)
			for i := 0; i < 1000; i++ {
				big = append(big, fmt.Sprintf("%s:%05d", k, i))
			}
			loadSet(t, c, k, big)
		}
	}

	run := func(p int) (inter string, card string) {
		srv := newPartServer(t, p)
		defer srv.Close()
		c := bareConn(srv)
		load(c)
		inter = strings.Join(sortedFlatReply(t, call(c, func(c *connState, a [][]byte) { c.cmdSInter(a) },
			"SINTER", "small", "big1", "big2")), "\x00")
		card = call(c, func(c *connState, a [][]byte) { c.cmdSInterCard(a) },
			"SINTERCARD", "3", "small", "big1", "big2")
		return inter, card
	}

	refInter, refCard := run(1)
	if refCard != ":10\r\n" {
		t.Fatalf("P=1 SINTERCARD = %q, want :10", refCard)
	}
	for _, p := range []int{2, 4, 8} {
		inter, card := run(p)
		if inter != refInter {
			t.Fatalf("P=%d SINTER (probe path) differs from P=1", p)
		}
		if card != refCard {
			t.Fatalf("P=%d SINTERCARD (probe path) = %q, want %q", p, card, refCard)
		}
	}
}

// TestSetAlgebraStorePartitionIdentical runs the three STORE forms at every P and asserts both the
// returned cardinality and the destination's stored members (read back through the routed SMEMBERS)
// match the unpartitioned run. This proves the routed destination write lands each result member
// under its partition key, so the same reader that framed the sources reads the result back exactly,
// and the header carries the right count. The destination is read back at the same P it was written.
func TestSetAlgebraStorePartitionIdentical(t *testing.T) {
	type form struct {
		name string
		fn   func(*connState, [][]byte)
		args []string
	}
	forms := []form{
		{"SINTERSTORE", func(c *connState, a [][]byte) { c.cmdSInterStore(a) }, []string{"SINTERSTORE", "dst", "A", "B", "C"}},
		{"SUNIONSTORE", func(c *connState, a [][]byte) { c.cmdSUnionStore(a) }, []string{"SUNIONSTORE", "dst", "A", "B", "C"}},
		{"SDIFFSTORE", func(c *connState, a [][]byte) { c.cmdSDiffStore(a) }, []string{"SDIFFSTORE", "dst", "A", "B", "C"}},
	}

	for _, f := range forms {
		run := func(p int) (card, members, scard string) {
			srv := newPartServer(t, p)
			defer srv.Close()
			c := bareConn(srv)
			algebraRefSets(t, c)
			card = call(c, f.fn, f.args...)
			members = strings.Join(smembersSorted(t, c, "dst"), "\x00")
			scard = call(c, func(c *connState, a [][]byte) { c.cmdSCard(a) }, "SCARD", "dst")
			return card, members, scard
		}
		refCard, refMembers, refScard := run(1)
		// The stored cardinality the command returns must equal SCARD of the destination.
		if refCard != strings.Replace(refScard, ":", ":", 1) {
			t.Fatalf("%s P=1 return %q disagrees with dst SCARD %q", f.name, refCard, refScard)
		}
		for _, p := range []int{2, 4, 8} {
			card, members, scard := run(p)
			if card != refCard {
				t.Fatalf("%s P=%d returned card %q, want %q (P=1)", f.name, p, card, refCard)
			}
			if members != refMembers {
				t.Fatalf("%s P=%d stored a different member set than P=1", f.name, p)
			}
			if scard != refScard {
				t.Fatalf("%s P=%d dst SCARD %q, want %q (P=1)", f.name, p, scard, refScard)
			}
		}
	}
}

// TestSetAlgebraStoreAliasedPartition stores into a destination that is also a source (SINTERSTORE A
// A B), the aliasing case that buffers the arena-stable result before clearing the destination. It
// must survive routing: the buffered members are cleared from A's partition rows and rewritten, and
// the result read back at each P must match the unpartitioned run.
func TestSetAlgebraStoreAliasedPartition(t *testing.T) {
	run := func(p int) (card, members string) {
		srv := newPartServer(t, p)
		defer srv.Close()
		c := bareConn(srv)
		algebraRefSets(t, c)
		// SINTERSTORE A A B: dst A is a source, result is A intersect B = the full core.
		card = call(c, func(c *connState, a [][]byte) { c.cmdSInterStore(a) }, "SINTERSTORE", "A", "A", "B")
		members = strings.Join(smembersSorted(t, c, "A"), "\x00")
		return card, members
	}
	refCard, refMembers := run(1)
	if refCard != ":25\r\n" {
		t.Fatalf("aliased SINTERSTORE P=1 returned %q, want :25 (the shared core)", refCard)
	}
	for _, p := range []int{2, 4, 8} {
		card, members := run(p)
		if card != refCard {
			t.Fatalf("aliased SINTERSTORE P=%d returned %q, want %q (P=1)", p, card, refCard)
		}
		if members != refMembers {
			t.Fatalf("aliased SINTERSTORE P=%d stored a different member set than P=1", p)
		}
	}
}

// TestSetAlgebraSInterCardLimitPartition checks SINTERCARD's LIMIT early-stop is honored under
// routing: a positive limit below the true intersection size returns exactly the limit at every P,
// and a limit above it returns the full count. A routed probe that miscounted would drift the stop.
func TestSetAlgebraSInterCardLimitPartition(t *testing.T) {
	for _, p := range []int{1, 2, 4, 8} {
		srv := newPartServer(t, p)
		c := bareConn(srv)
		algebraRefSets(t, c) // SINTER A B C has 15 members
		if got := call(c, func(c *connState, a [][]byte) { c.cmdSInterCard(a) },
			"SINTERCARD", "3", "A", "B", "C", "LIMIT", "5"); got != ":5\r\n" {
			t.Fatalf("P=%d SINTERCARD LIMIT 5 = %q, want :5", p, got)
		}
		if got := call(c, func(c *connState, a [][]byte) { c.cmdSInterCard(a) },
			"SINTERCARD", "3", "A", "B", "C", "LIMIT", "100"); got != ":15\r\n" {
			t.Fatalf("P=%d SINTERCARD LIMIT 100 = %q, want :15", p, got)
		}
		srv.Close()
	}
}

// TestSetAlgebraStoreWrongTypePartition confirms the routing preserves the type guards: a source
// held by a string makes the whole STORE a WRONGTYPE and leaves the destination untouched, while a
// string sitting at the destination is overwritten, not rejected, since the STORE replaces whatever
// the destination held.
func TestSetAlgebraStoreWrongTypePartition(t *testing.T) {
	srv := newPartServer(t, 8)
	defer srv.Close()
	c := bareConn(srv)
	algebraRefSets(t, c)
	call(c, func(c *connState, a [][]byte) { c.cmdSet(a) }, "SET", "str", "v")

	// A string source is a WRONGTYPE for the whole command.
	if got := call(c, func(c *connState, a [][]byte) { c.cmdSInterStore(a) },
		"SINTERSTORE", "dst", "A", "str"); !strings.HasPrefix(got, "-WRONGTYPE") {
		t.Fatalf("SINTERSTORE with a string source = %q, want WRONGTYPE", got)
	}
	// A string destination is overwritten by the result, not rejected.
	got := call(c, func(c *connState, a [][]byte) { c.cmdSUnionStore(a) }, "SUNIONSTORE", "str", "A", "B")
	if !strings.HasPrefix(got, ":") {
		t.Fatalf("SUNIONSTORE over a string destination = %q, want an integer count", got)
	}
	if tp := call(c, func(c *connState, a [][]byte) { c.cmdType(a) }, "TYPE", "str"); tp != "+set\r\n" {
		t.Fatalf("destination TYPE after SUNIONSTORE = %q, want +set", tp)
	}
}
