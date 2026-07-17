package sqlo1

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"testing"
)

func (r *listRig) insert(key string, before bool, pivot, elem string) int64 {
	r.t.Helper()
	n, err := r.l.Insert(context.Background(), []byte(key), before, []byte(pivot), []byte(elem))
	if err != nil {
		r.t.Fatalf("Insert(%q, before=%v, %q): %v", key, before, pivot, err)
	}
	return n
}

func (r *listRig) rem(key string, count int64, elem string) int64 {
	r.t.Helper()
	n, err := r.l.Rem(context.Background(), []byte(key), count, []byte(elem))
	if err != nil {
		r.t.Fatalf("Rem(%q, %d, %q): %v", key, count, elem, err)
	}
	return n
}

func (r *listRig) pos(l *List, key, elem string, rank, num, maxlen int64) []int64 {
	r.t.Helper()
	out := []int64{}
	err := l.Pos(context.Background(), []byte(key), []byte(elem), rank, num, maxlen, func(i int64) {
		out = append(out, i)
	})
	if err != nil {
		r.t.Fatalf("Pos(%q, %q, rank=%d): %v", key, elem, rank, err)
	}
	return out
}

// refPos mirrors Pos against a plain slice.
func refPos(ref []string, elem string, rank, num, maxlen int64) []int64 {
	if maxlen == 0 {
		maxlen = math.MaxInt64
	}
	reverse := rank < 0
	skip := rank - 1
	if reverse {
		skip = -rank - 1
	}
	out := []int64{}
	compared := int64(0)
	for i := range ref {
		j := i
		if reverse {
			j = len(ref) - 1 - i
		}
		if compared >= maxlen {
			break
		}
		compared++
		if ref[j] != elem {
			continue
		}
		if skip > 0 {
			skip--
			continue
		}
		out = append(out, int64(j))
		if int64(len(out)) >= num {
			break
		}
	}
	return out
}

// refInsert mirrors Insert against a plain slice.
func refInsert(ref []string, before bool, pivot, elem string) ([]string, int64) {
	at := slices.Index(ref, pivot)
	if at < 0 {
		return ref, -1
	}
	if !before {
		at++
	}
	out := append(append(append([]string{}, ref[:at]...), elem), ref[at:]...)
	return out, int64(len(out))
}

// refRem mirrors Rem against a plain slice.
func refRem(ref []string, count int64, elem string) ([]string, int64) {
	m := int64(0)
	for _, e := range ref {
		if e == elem {
			m++
		}
	}
	budget := int64(math.MaxInt64)
	reverse := false
	if count > 0 {
		budget = count
	} else if count < 0 {
		budget, reverse = -count, true
	}
	k := min(budget, m)
	lo, hi := int64(0), k
	if reverse {
		lo, hi = m-k, m
	}
	out := []string{}
	ord := int64(0)
	for _, e := range ref {
		if e == elem {
			o := ord
			ord++
			if o >= lo && o < hi {
				continue
			}
		}
		out = append(out, e)
	}
	return out, k
}

// TestListScanOracle drives Pos, Insert, and Rem against a reference
// slice on both tiers: dup-heavy values, both scan directions, rank
// skips, maxlen caps, budgeted and remove-all Rem, and the doors.
func TestListScanOracle(t *testing.T) {
	rig := newListRig(t)
	ctx := context.Background()

	for _, tier := range []struct {
		name string
		n    int
	}{
		{"inline", 60},
		{"noded", 3000},
	} {
		key := "k-" + tier.name
		ref := make([]string, tier.n)
		for i := range ref {
			ref[i] = fmt.Sprintf("v%02d", i%17)
		}
		rig.push(key, false, ref...)

		check := func(l *List) {
			t.Helper()
			got := rig.rng(l, key, 0, -1)
			if len(got) != len(ref) {
				t.Fatalf("%s: %d elements, want %d", tier.name, len(got), len(ref))
			}
			for i := range got {
				if got[i] != ref[i] {
					t.Fatalf("%s: [%d] = %q, want %q", tier.name, i, got[i], ref[i])
				}
			}
		}

		// Pos probes, both directions, rank skips, maxlen caps.
		for _, p := range []struct {
			elem              string
			rank, num, maxlen int64
		}{
			{"v05", 1, 1, 0},
			{"v05", 3, 2, 0},
			{"v05", 1, math.MaxInt64, 0},
			{"v05", -1, 1, 0},
			{"v05", -2, math.MaxInt64, 0},
			{"v05", 1, math.MaxInt64, 40},
			{"v05", -1, math.MaxInt64, 40},
			{"nope", 1, 1, 0},
			{"nope", -1, math.MaxInt64, 0},
		} {
			got := rig.pos(rig.l, key, p.elem, p.rank, p.num, p.maxlen)
			want := refPos(ref, p.elem, p.rank, p.num, p.maxlen)
			if !slices.Equal(got, want) {
				t.Fatalf("%s: Pos(%q, rank=%d, num=%d, maxlen=%d) = %v, want %v",
					tier.name, p.elem, p.rank, p.num, p.maxlen, got, want)
			}
		}

		// Inserts: before, after, and the missing pivot.
		var want int64
		ref, want = refInsert(ref, true, "v03", "ins-a")
		if got := rig.insert(key, true, "v03", "ins-a"); got != want {
			t.Fatalf("%s: Insert before = %d, want %d", tier.name, got, want)
		}
		check(rig.l)
		ref, want = refInsert(ref, false, "v09", "ins-b")
		if got := rig.insert(key, false, "v09", "ins-b"); got != want {
			t.Fatalf("%s: Insert after = %d, want %d", tier.name, got, want)
		}
		check(rig.l)
		if got := rig.insert(key, true, "nope", "x"); got != -1 {
			t.Fatalf("%s: Insert missing pivot = %d, want -1", tier.name, got)
		}

		// Rems: budgeted both ways, then remove-all.
		for _, w := range []struct {
			count int64
			elem  string
		}{
			{2, "v04"},
			{-3, "v07"},
			{0, "v11"},
			{5, "nope"},
		} {
			var wantN int64
			ref, wantN = refRem(ref, w.count, w.elem)
			if got := rig.rem(key, w.count, w.elem); got != wantN {
				t.Fatalf("%s: Rem(%d, %q) = %d, want %d", tier.name, w.count, w.elem, got, wantN)
			}
			check(rig.l)
		}

		// The cold view agrees after a drain.
		if err := rig.tr.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		check(rig.reopen())
	}

	// Removing every element deletes the key.
	rig.push("gone", false, "x", "x", "x", "x", "x")
	if got := rig.rem("gone", 0, "x"); got != 5 {
		t.Fatalf("Rem all = %d, want 5", got)
	}
	if n, err := rig.l.Len(ctx, []byte("gone")); err != nil || n != 0 {
		t.Fatalf("Len after Rem all = %d, %v", n, err)
	}

	// The doors: missing keys are quiet, the wrong type refuses.
	if got := rig.insert("missing", true, "p", "e"); got != 0 {
		t.Fatalf("Insert on missing key = %d, want 0", got)
	}
	if got := rig.rem("missing", 0, "e"); got != 0 {
		t.Fatalf("Rem on missing key = %d, want 0", got)
	}
	if got := rig.pos(rig.l, "missing", "e", 1, 1, 0); len(got) != 0 {
		t.Fatalf("Pos on missing key = %v, want none", got)
	}
	if err := rig.s.Set(ctx, []byte("str"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if _, err := rig.l.Insert(ctx, []byte("str"), true, []byte("p"), []byte("e")); !errors.Is(err, ErrWrongType) {
		t.Fatalf("Insert on a string = %v", err)
	}
	if _, err := rig.l.Rem(ctx, []byte("str"), 0, []byte("e")); !errors.Is(err, ErrWrongType) {
		t.Fatalf("Rem on a string = %v", err)
	}
	if err := rig.l.Pos(ctx, []byte("str"), []byte("e"), 1, 1, 0, func(int64) {}); !errors.Is(err, ErrWrongType) {
		t.Fatalf("Pos on a string = %v", err)
	}
}

// TestListInsertGrowth covers the two growth doors: an inline root
// pushed past its byte cap by an insert upgrades to nodes, and a noded
// node grown past the cut threshold splits at the half-byte element
// boundary, growing the fence by one.
func TestListInsertGrowth(t *testing.T) {
	rig := newListRig(t)
	ctx := context.Background()

	// Inline upgrade: ~68 B elements near the byte cap, then a 2 KiB
	// insert.
	ref := make([]string, 20)
	for i := range ref {
		ref[i] = fmt.Sprintf("e%02d-%s", i, strings.Repeat("x", 60))
	}
	rig.push("up", false, ref...)
	if enc, _, _ := rig.l.Encoding(ctx, []byte("up")); enc != "listpack" {
		t.Fatalf("encoding before insert = %q", enc)
	}
	big := strings.Repeat("B", 2048)
	pivot := ref[5]
	var want int64
	ref, want = refInsert(ref, false, pivot, big)
	if got := rig.insert("up", false, pivot, big); got != want {
		t.Fatalf("upgrading insert = %d, want %d", got, want)
	}
	if enc, _, _ := rig.l.Encoding(ctx, []byte("up")); enc != "quicklist" {
		t.Fatalf("encoding after insert = %q", enc)
	}
	if got := rig.rng(rig.l, "up", 0, -1); !slices.Equal(got, ref) {
		t.Fatalf("upgraded order diverged: %d elements", len(got))
	}

	// Noded split: 500 B elements pack 7 to a node; one more into a
	// full node overflows its bytes and splits it.
	ref = ref[:0]
	for i := range 21 {
		ref = append(ref, fmt.Sprintf("n%02d-%s", i, strings.Repeat("y", 500)))
	}
	rig.push("sp", false, ref...)
	before := len(rig.nodedRoot("sp").fence)
	pivot = ref[9]
	ref, want = refInsert(ref, false, pivot, strings.Repeat("z", 500))
	if got := rig.insert("sp", false, pivot, strings.Repeat("z", 500)); got != want {
		t.Fatalf("splitting insert = %d, want %d", got, want)
	}
	after := len(rig.nodedRoot("sp").fence)
	if after != before+1 {
		t.Fatalf("fence went %d -> %d nodes, want a one-node split", before, after)
	}
	if got := rig.rng(rig.l, "sp", 0, -1); !slices.Equal(got, ref) {
		t.Fatalf("split order diverged: %d elements", len(got))
	}
	if err := rig.tr.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if got := rig.rng(rig.reopen(), "sp", 0, -1); !slices.Equal(got, ref) {
		t.Fatalf("cold split order diverged: %d elements", len(got))
	}
}

// TestListRemMerge is the lmid counterweight test: a decimation LREM
// that halves every node coalesces the walk's survivors pairwise, the
// merged-away records leave as deletes, and the fence shrinks instead
// of accumulating half-empty nodes.
func TestListRemMerge(t *testing.T) {
	rig := newListRig(t)
	ctx := context.Background()

	const n = 3000
	elems := make([]string, n)
	kept := []string{}
	for i := range elems {
		if i%2 == 0 {
			elems[i] = fmt.Sprintf("k%04d", i)
			kept = append(kept, elems[i])
		} else {
			elems[i] = "drop"
		}
	}
	rig.push("k", false, elems...)
	if err := rig.tr.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	before := len(rig.nodedRoot("k").fence)
	if before < 20 {
		t.Fatalf("only %d nodes; the test wants a long fence", before)
	}

	mark := len(rig.rs.batches)
	if got := rig.rem("k", 0, "drop"); got != n/2 {
		t.Fatalf("Rem all drops = %d, want %d", got, n/2)
	}
	if err := rig.tr.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	puts, dels := 0, 0
	for _, b := range rig.rs.batches[mark:] {
		for _, op := range b.Ops {
			if op.Del {
				dels++
			} else {
				puts++
			}
		}
	}
	after := len(rig.nodedRoot("k").fence)
	if after > before/2+1 {
		t.Fatalf("fence went %d -> %d nodes; the counterweight should pair them up", before, after)
	}
	if dels < before/2-1 {
		t.Fatalf("decimation billed %d dels across %d merged-away nodes", dels, before-after)
	}
	if puts > after+1 {
		t.Fatalf("decimation billed %d puts for %d surviving nodes and one root", puts, after)
	}

	got := rig.rng(rig.l, "k", 0, -1)
	if !slices.Equal(got, kept) {
		t.Fatalf("post-merge content diverged: %d elements, want %d", len(got), len(kept))
	}
	if err := rig.tr.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if cold := rig.rng(rig.reopen(), "k", 0, -1); !slices.Equal(cold, kept) {
		t.Fatalf("cold post-merge content diverged: %d elements", len(cold))
	}
}

// TestListScanEdgeIO pins the directional early exit: on a cold list a
// match near the scanned end touches the root and a couple of nodes,
// never the whole list, in both directions, and a budgeted tail Rem
// bills one node rewrite plus the root.
func TestListScanEdgeIO(t *testing.T) {
	rig := newListRig(t)
	ctx := context.Background()

	const n = 3000
	elems := make([]string, n)
	for i := range elems {
		elems[i] = fmt.Sprintf("e%04d", i)
	}
	rig.push("k", false, elems...)
	if err := rig.tr.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	// Cold reads through a fresh runtime so every node read is a store
	// round.
	l2 := rig.reopen()
	mark := rig.rs.readRounds
	if got := rig.pos(l2, "k", "e0004", 1, 1, 0); !slices.Equal(got, []int64{4}) {
		t.Fatalf("head Pos = %v", got)
	}
	if r := rig.rs.readRounds - mark; r > 3 {
		t.Fatalf("head Pos took %d read rounds on a %d-node list", r, len(rig.nodedRoot("k").fence))
	}
	mark = rig.rs.readRounds
	if got := rig.pos(l2, "k", "e2995", -1, 1, 0); !slices.Equal(got, []int64{2995}) {
		t.Fatalf("tail Pos = %v", got)
	}
	if r := rig.rs.readRounds - mark; r > 3 {
		t.Fatalf("tail Pos took %d read rounds on a %d-node list", r, len(rig.nodedRoot("k").fence))
	}

	// A tail-directed budgeted Rem: one node rewrite plus the root.
	batch := len(rig.rs.batches)
	if got := rig.rem("k", -1, "e2999"); got != 1 {
		t.Fatalf("tail Rem = %d, want 1", got)
	}
	if err := rig.tr.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	puts, dels := 0, 0
	for _, b := range rig.rs.batches[batch:] {
		for _, op := range b.Ops {
			if op.Del {
				dels++
			} else {
				puts++
			}
		}
	}
	if puts != 2 || dels != 0 {
		t.Fatalf("tail Rem billed %d puts, %d dels, want 2 and 0", puts, dels)
	}
}
