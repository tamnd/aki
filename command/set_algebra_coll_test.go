package command

import (
	"fmt"
	"sort"
	"testing"

	"github.com/tamnd/aki/networking"
)

// TestSetAlgebraCollIsBounded guards the read-form set algebra against the loadSets
// materialize trap on coll-form sources. The old path cloned every member of every
// source onto the heap before computing, so SINTER or SDIFF of a small set against a
// set far larger than RAM dragged the huge set through memory in full, an OOM under a
// tight cap even though the result can be no larger than the small set.
//
// The witness keeps the driver small (a few dozen members) and the other source huge,
// the exact OOM shape: the huge source is the one that must not be cloned. The bounded
// path point-probes it instead, so the allocation count tracks the small driver, not
// the huge probe target. A materialize would clone the huge source every run.
func TestSetAlgebraCollIsBounded(t *testing.T) {
	skipAllocWitnessUnderRace(t)
	d := newFuzzDispatcher(t)
	conn := networking.NewOfflineConn()

	const big = 4000
	const small = 40
	pad := make([]byte, 240)
	for i := range pad {
		pad[i] = 'x'
	}
	member := func(i int) []byte {
		return []byte(fmt.Sprintf("m:%08d", i) + string(pad))
	}
	// big holds members 0..big-1; small holds the first `small` of them, so the
	// intersection is `small` and the difference small\big is empty.
	for i := range big {
		conn.ResetOut()
		d.Handle(conn, [][]byte{[]byte("SADD"), []byte("big"), member(i)})
		if i < small {
			conn.ResetOut()
			d.Handle(conn, [][]byte{[]byte("SADD"), []byte("small"), member(i)})
		}
	}
	conn.ResetOut()
	d.Handle(conn, [][]byte{[]byte("OBJECT"), []byte("ENCODING"), []byte("big")})
	if got := string(conn.OutBytes()); got != "$9\r\nhashtable\r\n" {
		t.Fatalf("big not in coll form: OBJECT ENCODING = %q", got)
	}

	// SINTER drives the smaller source (small) and point-probes big. SDIFF small big
	// drives small (the first arg) and point-probes big. Both must stay near the small
	// driver size, never touch big's member total.
	for _, cmd := range [][][]byte{
		{[]byte("SINTER"), []byte("small"), []byte("big")},
		{[]byte("SDIFF"), []byte("small"), []byte("big")},
	} {
		c := cmd
		allocs := testing.AllocsPerRun(10, func() {
			conn.ResetOut()
			d.Handle(conn, c)
		})
		if allocs > 500 {
			t.Fatalf("%s over a %d-member coll source allocated %.0f objects per run; "+
				"the bounded path should track the small driver, not clone the huge source", c[0], big, allocs)
		}
	}
}

// TestSetAlgebraCollMatchesNaive checks the streamed read-form results match a naive
// in-memory computation over coll-form sources, across intersection, union and
// difference, including the order-independent set semantics and the empty cases.
func TestSetAlgebraCollMatchesNaive(t *testing.T) {
	r, c := startData(t)

	// Three coll-form sets (well past the listpack threshold) with deliberate overlap:
	// a = 0..599, b = 300..899, c = 500..1099.
	build := func(name string, lo, hi int) map[string]bool {
		set := map[string]bool{}
		for i := lo; i < hi; i++ {
			m := fmt.Sprintf("m:%06d", i)
			set[m] = true
			_ = sendLine(t, r, c, fmt.Sprintf("SADD %s %s", name, m))
		}
		if enc := bulk(t, r, c, "OBJECT ENCODING "+name); enc != "hashtable" {
			t.Fatalf("set %s encoding = %q want hashtable", name, enc)
		}
		return set
	}
	sa := build("a", 0, 600)
	sb := build("b", 300, 900)
	sc := build("c", 500, 1100)

	sortedKeys := func(m map[string]bool) []string {
		out := make([]string, 0, len(m))
		for k := range m {
			out = append(out, k)
		}
		sort.Strings(out)
		return out
	}
	check := func(cmd string, want map[string]bool) {
		got := readArray(t, r, c, cmd)
		sort.Strings(got)
		w := sortedKeys(want)
		if len(got) != len(w) {
			t.Fatalf("%s returned %d members want %d", cmd, len(got), len(w))
		}
		for i := range w {
			if got[i] != w[i] {
				t.Fatalf("%s member %d = %q want %q", cmd, i, got[i], w[i])
			}
		}
	}

	inter := map[string]bool{}
	for m := range sa {
		if sb[m] && sc[m] {
			inter[m] = true
		}
	}
	check("SINTER a b c", inter)

	union := map[string]bool{}
	for _, s := range []map[string]bool{sa, sb, sc} {
		for m := range s {
			union[m] = true
		}
	}
	check("SUNION a b c", union)

	diff := map[string]bool{}
	for m := range sa {
		if !sb[m] && !sc[m] {
			diff[m] = true
		}
	}
	check("SDIFF a b c", diff)

	// Missing source: empty intersection, no change to union or difference.
	check("SINTER a nokey", map[string]bool{})
	check("SDIFF a nokey", sa)

	// Wrong-type source still reports WRONGTYPE.
	_ = sendLine(t, r, c, "SET str hello")
	for _, cmd := range []string{"SINTER a str", "SUNION a str", "SDIFF a str"} {
		if reply := sendLine(t, r, c, cmd); reply[:1] != "-" {
			t.Fatalf("%s = %q want WRONGTYPE error", cmd, reply)
		}
	}
}

// TestSetAlgebraCollStreamsLargeReply drives a union whose reply is well past the
// mid-reply flush threshold over a real connection, so StreamFlush spills the buffer
// several times during the one command. It also exercises the huge-driver difference
// (SDIFF big small), whose driver is the large source walked in two passes, to check
// the streamed framing stays clean when the result itself is large. The client must
// reassemble the exact member sets across the chunk boundaries.
func TestSetAlgebraCollStreamsLargeReply(t *testing.T) {
	r, c := startData(t)
	const n = 1000
	pad := make([]byte, 240)
	for i := range pad {
		pad[i] = 'x'
	}
	// big = 0..n-1, small = first 100, both padded so the reply crosses the flush
	// threshold several times.
	bigSet := map[string]bool{}
	for i := range n {
		m := fmt.Sprintf("m:%06d:%s", i, pad)
		bigSet[m] = true
		_ = sendLine(t, r, c, fmt.Sprintf("SADD big %s", m))
		if i < 100 {
			_ = sendLine(t, r, c, fmt.Sprintf("SADD small %s", m))
		}
	}
	if enc := bulk(t, r, c, "OBJECT ENCODING big"); enc != "hashtable" {
		t.Fatalf("big encoding = %q want hashtable", enc)
	}

	// SUNION big small = big (small is a subset).
	got := readArray(t, r, c, "SUNION big small")
	if len(got) != n {
		t.Fatalf("SUNION big small returned %d members want %d", len(got), n)
	}
	seen := map[string]bool{}
	for _, m := range got {
		if !bigSet[m] {
			t.Fatalf("SUNION returned %q, not a member", m)
		}
		seen[m] = true
	}
	if len(seen) != n {
		t.Fatalf("SUNION dropped or duplicated members across the streamed reply: %d distinct", len(seen))
	}

	// SDIFF big small = big minus the first 100; the driver is the large source.
	got = readArray(t, r, c, "SDIFF big small")
	if len(got) != n-100 {
		t.Fatalf("SDIFF big small returned %d members want %d", len(got), n-100)
	}
	for _, m := range got {
		if !bigSet[m] {
			t.Fatalf("SDIFF returned %q, not a member of big", m)
		}
	}
}
