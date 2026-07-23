package obs1_test

import (
	"errors"
	"testing"

	"github.com/tamnd/aki/engine/obs1"
)

// The set algebra merges on hand-built streams (spec 2064/obs1 doc 08
// section 4): discriminators here are chosen by hand, which is the only
// practical way to force the collision ties the identity rule exists for.

func setElems(pairs ...any) []obs1.Elem {
	var out []obs1.Elem
	for i := 0; i < len(pairs); i += 2 {
		out = append(out, obs1.Elem{Disc: uint64(pairs[i].(int)), Data: []byte(pairs[i+1].(string))})
	}
	return out
}

func drain(t *testing.T, run func(yield func(m []byte) error) error) []string {
	t.Helper()
	var got []string
	if err := run(func(m []byte) error {
		got = append(got, string(m))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return got
}

func wantSeq(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("yielded %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("yielded %v, want %v", got, want)
		}
	}
}

func TestSetAlgebraMerges(t *testing.T) {
	// Disc 5 is a three-way collision: alpha in both operands, omega only in
	// the first, kappa only in the second.
	a := func() obs1.ElemIter {
		return obs1.SliceIter(setElems(1, "one", 5, "alpha", 5, "omega", 9, "nine"))
	}
	b := func() obs1.ElemIter {
		return obs1.SliceIter(setElems(2, "two", 5, "alpha", 5, "kappa", 9, "nine"))
	}

	wantSeq(t, drain(t, func(y func([]byte) error) error {
		return obs1.SetUnion([]obs1.ElemIter{a(), b()}, y)
	}), []string{"one", "two", "alpha", "omega", "kappa", "nine"})

	wantSeq(t, drain(t, func(y func([]byte) error) error {
		return obs1.SetInter([]obs1.ElemIter{a(), b()}, y)
	}), []string{"alpha", "nine"})

	wantSeq(t, drain(t, func(y func([]byte) error) error {
		return obs1.SetDiff([]obs1.ElemIter{a(), b()}, y)
	}), []string{"one", "omega"})

	// Diff is asymmetric.
	wantSeq(t, drain(t, func(y func([]byte) error) error {
		return obs1.SetDiff([]obs1.ElemIter{b(), a()}, y)
	}), []string{"two", "kappa"})

	// A single operand drains through union unchanged, the SMEMBERS shape.
	wantSeq(t, drain(t, func(y func([]byte) error) error {
		return obs1.SetUnion([]obs1.ElemIter{a()}, y)
	}), []string{"one", "alpha", "omega", "nine"})
}

func TestSetAlgebraBackwardStreamRefused(t *testing.T) {
	bad := obs1.SliceIter(setElems(9, "nine", 1, "one"))
	good := obs1.SliceIter(setElems(2, "two"))
	err := obs1.SetUnion([]obs1.ElemIter{good, bad}, func([]byte) error { return nil })
	if !errors.Is(err, obs1.ErrDiscOrder) {
		t.Fatalf("backward stream merged with err %v, want ErrDiscOrder", err)
	}
}

// TestSetMergeShadowSame holds the overlay rule with the set identity: a
// dead overlay claim (an SREM) suppresses exactly its member inside a
// collision, and a re-add passes through once.
func TestSetMergeShadowSame(t *testing.T) {
	cold := setElems(3, "three", 5, "alpha", 5, "omega")
	overlay := setElems(5, "omega", 7, "seven")
	overlay[0].Dead = true // SREM omega while cold

	var got []string
	if err := obs1.MergeShadow(obs1.SliceIter(cold), obs1.SliceIter(overlay), obs1.SetSame, func(e obs1.Elem) error {
		got = append(got, string(e.Data))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	wantSeq(t, got, []string{"three", "alpha", "seven"})
}
