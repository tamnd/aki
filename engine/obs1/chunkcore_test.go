package obs1_test

import (
	"hash/fnv"
	"strings"
	"testing"

	obs1 "github.com/tamnd/aki/engine/obs1"
)

func TestClampChunkTarget(t *testing.T) {
	cases := []struct{ in, want int }{
		{0, obs1.ChunkTargetDefault},
		{-1, obs1.ChunkTargetDefault},
		{1 << 10, obs1.ChunkTargetMin},
		{obs1.ChunkTargetMin, obs1.ChunkTargetMin},
		{20 << 10, 20 << 10},
		{obs1.ChunkTargetMax, obs1.ChunkTargetMax},
		{1 << 20, obs1.ChunkTargetMax},
	}
	for _, c := range cases {
		if got := obs1.ClampChunkTarget(c.in); got != c.want {
			t.Errorf("ClampChunkTarget(%d) = %d, want %d", c.in, got, c.want)
		}
	}
}

// TestDiscCoordinate pins Disc to the FNV-1a 64 coordinate the keymap,
// bloom, and fold sort share (#1266 as-built): a drift here would split
// the one-hash-per-name agreement across structures.
func TestDiscCoordinate(t *testing.T) {
	for _, name := range []string{"", "k", "field:name", strings.Repeat("m", 300)} {
		h := fnv.New64a()
		h.Write([]byte(name))
		if got, want := obs1.Disc([]byte(name)), h.Sum64(); got != want {
			t.Fatalf("Disc(%q) = %#x, want the FNV-1a coordinate %#x", name, got, want)
		}
	}
}

// mergeAll runs MergeShadow with name-prefix identity (payloads are
// "name=value" strings) and collects the yielded payloads.
func mergeAll(t *testing.T, cold, overlay []obs1.Elem) []string {
	t.Helper()
	same := func(c, o []byte) bool {
		return string(nameOf(c)) == string(nameOf(o))
	}
	var out []string
	err := obs1.MergeShadow(obs1.SliceIter(cold), obs1.SliceIter(overlay), same, func(e obs1.Elem) error {
		out = append(out, string(e.Data))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func nameOf(b []byte) []byte {
	for i, c := range b {
		if c == '=' {
			return b[:i]
		}
	}
	return b
}

func el(disc uint64, data string) obs1.Elem { return obs1.Elem{Disc: disc, Data: []byte(data)} }
func tomb(disc uint64, data string) obs1.Elem {
	return obs1.Elem{Disc: disc, Data: []byte(data), Dead: true}
}

func TestMergeShadowInterleave(t *testing.T) {
	got := mergeAll(t,
		[]obs1.Elem{el(10, "a=1"), el(30, "c=1"), el(50, "e=1")},
		[]obs1.Elem{el(20, "b=2"), el(40, "d=2"), el(60, "f=2")})
	want := []string{"a=1", "b=2", "c=1", "d=2", "e=1", "f=2"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("merged %v, want %v", got, want)
	}
}

func TestMergeShadowOverlayWins(t *testing.T) {
	got := mergeAll(t,
		[]obs1.Elem{el(10, "a=old"), el(20, "b=cold")},
		[]obs1.Elem{el(10, "a=new")})
	want := []string{"a=new", "b=cold"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("merged %v, want %v", got, want)
	}
}

func TestMergeShadowTombstoneSuppresses(t *testing.T) {
	got := mergeAll(t,
		[]obs1.Elem{el(10, "a=1"), el(20, "b=1"), el(30, "c=1")},
		[]obs1.Elem{tomb(20, "b=")})
	want := []string{"a=1", "c=1"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("merged %v, want %v", got, want)
	}
}

// TestMergeShadowCollisionTie puts two distinct identities on one
// discriminator: the overlay claims one of them and the other must
// survive, which disc comparison alone cannot decide.
func TestMergeShadowCollisionTie(t *testing.T) {
	got := mergeAll(t,
		[]obs1.Elem{el(10, "a=cold"), el(10, "z=cold")},
		[]obs1.Elem{el(10, "a=hot"), tomb(10, "q=")})
	want := []string{"z=cold", "a=hot"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("merged %v, want %v", got, want)
	}
}

func TestMergeShadowTails(t *testing.T) {
	got := mergeAll(t,
		[]obs1.Elem{el(10, "a=1")},
		[]obs1.Elem{el(20, "b=2"), el(30, "c=2"), tomb(40, "d=")})
	want := []string{"a=1", "b=2", "c=2"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("merged %v, want %v", got, want)
	}
	got = mergeAll(t,
		[]obs1.Elem{el(20, "b=1"), el(30, "c=1")},
		nil)
	want = []string{"b=1", "c=1"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("merged %v, want %v", got, want)
	}
}

func TestMergeShadowOrderGuard(t *testing.T) {
	same := func(c, o []byte) bool { return false }
	err := obs1.MergeShadow(
		obs1.SliceIter([]obs1.Elem{el(20, "b=1"), el(10, "a=1")}),
		obs1.SliceIter(nil), same,
		func(obs1.Elem) error { return nil })
	if err != obs1.ErrDiscOrder {
		t.Fatalf("backward cold stream returned %v, want ErrDiscOrder", err)
	}
	err = obs1.MergeShadow(
		obs1.SliceIter(nil),
		obs1.SliceIter([]obs1.Elem{el(20, "b=1"), el(10, "a=1")}), same,
		func(obs1.Elem) error { return nil })
	if err != obs1.ErrDiscOrder {
		t.Fatalf("backward overlay stream returned %v, want ErrDiscOrder", err)
	}
}

func TestMergeShadowYieldError(t *testing.T) {
	same := func(c, o []byte) bool { return false }
	boom := &yieldErr{}
	err := obs1.MergeShadow(
		obs1.SliceIter([]obs1.Elem{el(10, "a=1"), el(20, "b=1")}),
		obs1.SliceIter(nil), same,
		func(obs1.Elem) error { return boom })
	if err != boom {
		t.Fatalf("yield error came back as %v", err)
	}
}

type yieldErr struct{}

func (*yieldErr) Error() string { return "stop" }
