package f1srv

import (
	"bufio"
	"sort"
	"strconv"
	"testing"
)

// sortedArray reads an array reply and returns its members sorted, so a test can assert a
// set-valued result without depending on the emission order (the algebra emits in member
// order, but the assertions stay order-independent to document the set semantics).
func sortedArray(t *testing.T, rw *bufio.ReadWriter) []string {
	t.Helper()
	out := readArray(t, rw)
	sort.Strings(out)
	return out
}

func eqStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// SINTER returns the members present in every source set, and the empty set when the
// sources share nothing or any source is missing.
func TestSInter(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SADD", "a", "x", "y", "z", "w")
	expect(t, rw, ":4")
	cmd(t, rw, "SADD", "b", "y", "z", "q")
	expect(t, rw, ":3")
	cmd(t, rw, "SADD", "c", "z", "y", "m")
	expect(t, rw, ":3")

	// Intersection of all three is {y, z}.
	cmd(t, rw, "SINTER", "a", "b", "c")
	if got := sortedArray(t, rw); !eqStrings(got, []string{"y", "z"}) {
		t.Fatalf("SINTER a b c = %v, want [y z]", got)
	}

	// A single source intersects to itself.
	cmd(t, rw, "SINTER", "b")
	if got := sortedArray(t, rw); !eqStrings(got, []string{"q", "y", "z"}) {
		t.Fatalf("SINTER b = %v, want [q y z]", got)
	}

	// A missing source empties the intersection.
	cmd(t, rw, "SINTER", "a", "b", "missing")
	if got := readArray(t, rw); len(got) != 0 {
		t.Fatalf("SINTER with a missing key = %v, want empty", got)
	}

	// Disjoint sets intersect to empty.
	cmd(t, rw, "SADD", "d", "1", "2")
	expect(t, rw, ":2")
	cmd(t, rw, "SINTER", "a", "d")
	if got := readArray(t, rw); len(got) != 0 {
		t.Fatalf("SINTER of disjoint sets = %v, want empty", got)
	}
}

// SUNION returns every member in any source exactly once, deduplicating members shared by
// several sources.
func TestSUnion(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SADD", "a", "x", "y")
	expect(t, rw, ":2")
	cmd(t, rw, "SADD", "b", "y", "z")
	expect(t, rw, ":2")

	cmd(t, rw, "SUNION", "a", "b")
	if got := sortedArray(t, rw); !eqStrings(got, []string{"x", "y", "z"}) {
		t.Fatalf("SUNION a b = %v, want [x y z]", got)
	}

	// Union with a missing key is just the present set.
	cmd(t, rw, "SUNION", "a", "gone")
	if got := sortedArray(t, rw); !eqStrings(got, []string{"x", "y"}) {
		t.Fatalf("SUNION a gone = %v, want [x y]", got)
	}

	// Union of all-missing keys is empty.
	cmd(t, rw, "SUNION", "gone1", "gone2")
	if got := readArray(t, rw); len(got) != 0 {
		t.Fatalf("SUNION of missing keys = %v, want empty", got)
	}
}

// SDIFF returns the members of the first set that none of the later sets hold, and is not
// commutative: the first key is always the base.
func TestSDiff(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SADD", "a", "x", "y", "z", "w")
	expect(t, rw, ":4")
	cmd(t, rw, "SADD", "b", "y")
	expect(t, rw, ":1")
	cmd(t, rw, "SADD", "c", "z")
	expect(t, rw, ":1")

	// a minus b minus c is {x, w}.
	cmd(t, rw, "SDIFF", "a", "b", "c")
	if got := sortedArray(t, rw); !eqStrings(got, []string{"w", "x"}) {
		t.Fatalf("SDIFF a b c = %v, want [w x]", got)
	}

	// The base being empty (missing) yields empty regardless of the others.
	cmd(t, rw, "SDIFF", "missing", "a")
	if got := readArray(t, rw); len(got) != 0 {
		t.Fatalf("SDIFF missing a = %v, want empty", got)
	}

	// Subtracting a missing set changes nothing.
	cmd(t, rw, "SDIFF", "a", "gone")
	if got := sortedArray(t, rw); !eqStrings(got, []string{"w", "x", "y", "z"}) {
		t.Fatalf("SDIFF a gone = %v, want [w x y z]", got)
	}
}

// TestSInterBranches drives both SINTER strategies with the same known-overlap data so the
// merge path (chosen for similar-sized sources) and the probe path (chosen when one source is
// far smaller) each return the exact intersection. sinterEach picks by cardinality, so equal
// sets take the sorted merge and a tiny-versus-large pair takes the probe, and both must agree.
func TestSInterBranches(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	addRange := func(key string, lo, hi int) {
		args := []string{"SADD", key}
		for i := lo; i < hi; i++ {
			args = append(args, "m"+strconv.Itoa(i))
		}
		cmd(t, rw, args...)
		expect(t, rw, ":"+strconv.Itoa(hi-lo))
	}
	want := func(lo, hi int) []string {
		out := make([]string, 0, hi-lo)
		for i := lo; i < hi; i++ {
			out = append(out, "m"+strconv.Itoa(i))
		}
		sort.Strings(out)
		return out
	}

	// Equal 300-member sets overlapping on m150..m299: sinterEach takes the merge branch.
	addRange("eqa", 0, 300)
	addRange("eqb", 150, 450)
	cmd(t, rw, "SINTER", "eqa", "eqb")
	if got := sortedArray(t, rw); !eqStrings(got, want(150, 300)) {
		t.Fatalf("SINTER equal sets (merge) = %d members, want 150", len(got))
	}

	// A 10-member source against a 400-member source (400 > 3*10) takes the probe branch;
	// the small set sits inside the big one, so the intersection is the whole small set.
	addRange("tiny", 20, 30)
	addRange("big", 0, 400)
	cmd(t, rw, "SINTER", "tiny", "big")
	if got := sortedArray(t, rw); !eqStrings(got, want(20, 30)) {
		t.Fatalf("SINTER asymmetric sets (probe) = %v, want m20..m29", got)
	}

	// Same asymmetric pair through SINTERCARD, which shares sinterEach: full count then a
	// LIMIT that stops the merge/probe early.
	cmd(t, rw, "SINTERCARD", "2", "tiny", "big")
	expect(t, rw, ":10")
	cmd(t, rw, "SINTERCARD", "2", "eqa", "eqb", "LIMIT", "40")
	expect(t, rw, ":40")
}

// SINTERCARD returns the size of the intersection, and stops counting at a positive LIMIT.
func TestSInterCard(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SADD", "a", "x", "y", "z", "w")
	expect(t, rw, ":4")
	cmd(t, rw, "SADD", "b", "y", "z", "w", "q")
	expect(t, rw, ":4")

	// The intersection {y, z, w} has cardinality 3.
	cmd(t, rw, "SINTERCARD", "2", "a", "b")
	expect(t, rw, ":3")

	// LIMIT 0 means no limit, so it counts the whole intersection.
	cmd(t, rw, "SINTERCARD", "2", "a", "b", "LIMIT", "0")
	expect(t, rw, ":3")

	// A positive LIMIT caps the reported count.
	cmd(t, rw, "SINTERCARD", "2", "a", "b", "LIMIT", "2")
	expect(t, rw, ":2")

	// A LIMIT above the intersection size reports the true size.
	cmd(t, rw, "SINTERCARD", "2", "a", "b", "LIMIT", "10")
	expect(t, rw, ":3")

	// A missing source makes the intersection empty.
	cmd(t, rw, "SINTERCARD", "2", "a", "missing")
	expect(t, rw, ":0")
}

// SINTERCARD rejects a bad numkeys, an argument overrun, a malformed LIMIT clause, and a
// negative LIMIT, matching Redis's error text.
func TestSInterCardErrors(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SADD", "a", "x")
	expect(t, rw, ":1")

	cmd(t, rw, "SINTERCARD", "0", "a")
	expect(t, rw, "-ERR numkeys should be greater than 0")

	cmd(t, rw, "SINTERCARD", "3", "a")
	expect(t, rw, "-ERR Number of keys can't be greater than number of args")

	cmd(t, rw, "SINTERCARD", "1", "a", "BADWORD", "1")
	expect(t, rw, "-ERR syntax error")

	cmd(t, rw, "SINTERCARD", "1", "a", "LIMIT", "-1")
	expect(t, rw, "-ERR LIMIT can't be negative")
}

// A set-algebra command against a key holding a plain string is WRONGTYPE, whether the
// string is the first source or a later one.
func TestSetAlgebraWrongType(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SET", "str", "v")
	expect(t, rw, "+OK")
	cmd(t, rw, "SADD", "s", "a")
	expect(t, rw, ":1")

	for _, op := range []string{"SINTER", "SUNION", "SDIFF"} {
		cmd(t, rw, op, "str", "s")
		expect(t, rw, "-"+wrongType)
		cmd(t, rw, op, "s", "str")
		expect(t, rw, "-"+wrongType)
	}

	cmd(t, rw, "SINTERCARD", "2", "s", "str")
	expect(t, rw, "-"+wrongType)
}

// The read-form algebra commands reject the wrong argument count.
func TestSetAlgebraArity(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SINTER")
	expect(t, rw, "-ERR wrong number of arguments for 'sinter' command")
	cmd(t, rw, "SUNION")
	expect(t, rw, "-ERR wrong number of arguments for 'sunion' command")
	cmd(t, rw, "SDIFF")
	expect(t, rw, "-ERR wrong number of arguments for 'sdiff' command")
	cmd(t, rw, "SINTERCARD", "2")
	expect(t, rw, "-ERR wrong number of arguments for 'sintercard' command")
}

// The intersection emits its members in member-byte order, the ordered-index property the
// k-way merge rides, so a client that wants sorted output gets it without asking.
func TestSInterEmitsSorted(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SADD", "a", "d", "b", "a", "c", "e")
	expect(t, rw, ":5")
	cmd(t, rw, "SADD", "b", "e", "c", "a", "b", "d")
	expect(t, rw, ":5")

	cmd(t, rw, "SINTER", "a", "b")
	got := readArray(t, rw)
	want := []string{"a", "b", "c", "d", "e"}
	if !eqStrings(got, want) {
		t.Fatalf("SINTER emitted %v, want sorted %v", got, want)
	}
}
