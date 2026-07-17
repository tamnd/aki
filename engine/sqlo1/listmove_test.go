package sqlo1

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

// move wraps List.Move.
func (r *listRig) move(src, dst string, srcLeft, dstLeft bool) (string, bool) {
	r.t.Helper()
	e, ok, err := r.l.Move(context.Background(), []byte(src), []byte(dst), srcLeft, dstLeft)
	if err != nil {
		r.t.Fatalf("Move(%q, %q, %v, %v): %v", src, dst, srcLeft, dstLeft, err)
	}
	return string(e), ok
}

// refMove mirrors Move on reference slices.
func refMove(src, dst []string, srcLeft, dstLeft bool) ([]string, []string, string) {
	var e string
	if srcLeft {
		e, src = src[0], src[1:]
	} else {
		e, src = src[len(src)-1], src[:len(src)-1]
	}
	if dstLeft {
		dst = append([]string{e}, dst...)
	} else {
		dst = append(dst, e)
	}
	return src, dst, e
}

// TestListMoveOracle drives Move against reference slices across the
// tier combinations and all four direction pairs, then checks the
// missing-source, source-death, same-key, and type doors.
func TestListMoveOracle(t *testing.T) {
	rig := newListRig(t)
	ctx := context.Background()

	for _, tier := range []struct {
		name string
		n    int
	}{
		{"inline", 20},
		{"noded", 300},
	} {
		src, dst := "src-"+tier.name, "dst-"+tier.name
		srcRef := make([]string, tier.n)
		elems := make([]string, tier.n)
		for i := range srcRef {
			srcRef[i] = fmt.Sprintf("e%04d", i)
			elems[i] = srcRef[i]
		}
		rig.push(src, false, elems...)
		var dstRef []string

		check := func(l *List, key string, ref []string) {
			t.Helper()
			got := rig.rng(l, key, 0, -1)
			if len(got) != len(ref) {
				t.Fatalf("%s: %q has %d elements, want %d", tier.name, key, len(got), len(ref))
			}
			for i := range got {
				if got[i] != ref[i] {
					t.Fatalf("%s: %q[%d] = %q, want %q", tier.name, key, i, got[i], ref[i])
				}
			}
			if n, err := l.Len(ctx, []byte(key)); err != nil || n != int64(len(ref)) {
				t.Fatalf("%s: Len(%q) = %d, %v, want %d", tier.name, key, n, err, len(ref))
			}
		}

		// All four direction pairs, dst growing from missing through
		// inline (and past it on the noded arm's fat schedule).
		for i, dirs := range [][2]bool{
			{false, true}, // RPOPLPUSH's shape
			{true, false},
			{true, true},
			{false, false},
			{false, true},
			{true, false},
		} {
			var want string
			srcRef, dstRef, want = refMove(srcRef, dstRef, dirs[0], dirs[1])
			got, ok := rig.move(src, dst, dirs[0], dirs[1])
			if !ok || got != want {
				t.Fatalf("%s: move %d = (%q, %v), want (%q, true)", tier.name, i, got, ok, want)
			}
			check(rig.l, src, srcRef)
			check(rig.l, dst, dstRef)
		}

		// The cold view agrees after a drain.
		if err := rig.tr.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		l2 := rig.reopen()
		check(l2, src, srcRef)
		check(l2, dst, dstRef)
	}

	// Same-key rotation on both tiers: opposite ends rotate, the same
	// end answers the element without writing.
	for _, tier := range []struct {
		name string
		n    int
	}{
		{"inline", 6},
		{"noded", 200},
	} {
		key := "rot-" + tier.name
		ref := make([]string, tier.n)
		elems := make([]string, tier.n)
		for i := range ref {
			ref[i] = fmt.Sprintf("r%03d", i)
			elems[i] = ref[i]
		}
		rig.push(key, false, elems...)

		ref, ref2, want := refMove(ref, nil, true, false)
		ref = append(ref, ref2...)
		if got, ok := rig.move(key, key, true, false); !ok || got != want {
			t.Fatalf("%s: rotate head-to-tail = (%q, %v), want (%q, true)", tier.name, got, ok, want)
		}
		refHead := ref[len(ref)-1]
		ref = append([]string{refHead}, ref[:len(ref)-1]...)
		if got, ok := rig.move(key, key, false, true); !ok || got != refHead {
			t.Fatalf("%s: rotate tail-to-head = (%q, %v), want (%q, true)", tier.name, got, ok, refHead)
		}
		if err := rig.tr.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		if got, ok := rig.move(key, key, true, true); !ok || got != ref[0] {
			t.Fatalf("%s: same-end move = (%q, %v), want (%q, true)", tier.name, got, ok, ref[0])
		}
		if rig.tr.ht.dirtyKey([]byte(key)) {
			t.Fatalf("%s: same-end move dirtied the key", tier.name)
		}
		got := rig.rng(rig.l, key, 0, -1)
		for i := range got {
			if got[i] != ref[i] {
				t.Fatalf("%s: after rotations [%d] = %q, want %q", tier.name, i, got[i], ref[i])
			}
		}
	}

	// Source death: moving the last element deletes the key, and a
	// missing source answers not-ok without touching dst.
	rig.push("last", false, "only")
	if got, ok := rig.move("last", "sink", true, true); !ok || got != "only" {
		t.Fatalf("last move = (%q, %v), want (only, true)", got, ok)
	}
	if enc, ok, _ := rig.l.Encoding(ctx, []byte("last")); ok {
		t.Fatalf("source survived its last move as %q", enc)
	}
	if _, ok, err := rig.l.Move(ctx, []byte("last"), []byte("sink"), true, true); err != nil || ok {
		t.Fatalf("missing-source move = (ok=%v, err=%v), want (false, nil)", ok, err)
	}
	if got := rig.rng(rig.l, "sink", 0, -1); len(got) != 1 || got[0] != "only" {
		t.Fatalf("sink after missing-source move = %v, want [only]", got)
	}

	// The type doors: a wrong-typed source or destination errors before
	// any write, so the untouched side keeps its image.
	if err := rig.s.Set(ctx, []byte("str"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := rig.l.Move(ctx, []byte("str"), []byte("sink"), true, true); !errors.Is(err, ErrWrongType) {
		t.Fatalf("wrong-typed source: %v, want ErrWrongType", err)
	}
	if _, _, err := rig.l.Move(ctx, []byte("sink"), []byte("str"), true, true); !errors.Is(err, ErrWrongType) {
		t.Fatalf("wrong-typed destination: %v, want ErrWrongType", err)
	}
	if got := rig.rng(rig.l, "sink", 0, -1); len(got) != 1 || got[0] != "only" {
		t.Fatalf("sink after wrong-typed destination = %v, want [only]", got)
	}
}

// TestListMoveEdgeIO bills the noded-to-noded move: an edge-amending
// move is exactly four puts (source edge node and root, destination
// edge node and root), and a move that drains a one-element edge node
// trades a put for the node's delete.
func TestListMoveEdgeIO(t *testing.T) {
	rig := newListRig(t)
	ctx := context.Background()

	bill := func(f func()) (puts, dels int) {
		t.Helper()
		if err := rig.tr.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		mark := len(rig.rs.batches)
		f()
		if err := rig.tr.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		for _, b := range rig.rs.batches[mark:] {
			for _, op := range b.Ops {
				if op.Del {
					dels++
				} else {
					puts++
				}
			}
		}
		return puts, dels
	}

	// src: nodes of 128, 128, 44. dst: head node popped down to 125 so
	// a left push amends instead of cutting a fresh node.
	elems := func(prefix string, n int) []string {
		out := make([]string, n)
		for i := range out {
			out[i] = fmt.Sprintf("%s%04d", prefix, i)
		}
		return out
	}
	rig.push("src", false, elems("e", 300)...)
	rig.push("dst", false, elems("d", 300)...)
	rig.pop("dst", true, 3)

	puts, dels := bill(func() {
		if got, ok := rig.move("src", "dst", false, true); !ok || got != "e0299" {
			t.Fatalf("move = (%q, %v), want (e0299, true)", got, ok)
		}
	})
	if puts != 4 || dels != 0 {
		t.Fatalf("edge-amending move billed %d puts %d dels, want 4 and 0", puts, dels)
	}

	// 257 elements cut as 128, 128, 1: the move drains the tail node.
	rig.push("thin", false, elems("t", 257)...)
	puts, dels = bill(func() {
		if got, ok := rig.move("thin", "dst", false, true); !ok || got != "t0256" {
			t.Fatalf("move = (%q, %v), want (t0256, true)", got, ok)
		}
	})
	if puts != 3 || dels != 1 {
		t.Fatalf("node-draining move billed %d puts %d dels, want 3 and 1", puts, dels)
	}
}
