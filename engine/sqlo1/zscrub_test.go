package sqlo1

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"testing"
)

// zscrubSeed fills key with n members under scores drawn from rng,
// duplicate score bands included so equal-score member ordering is
// exercised.
func zscrubSeed(t *testing.T, z *ZSet, key []byte, n int, rng *rand.Rand) {
	t.Helper()
	ctx := context.Background()
	for i := 0; i < n; i++ {
		sc := float64(rng.Intn(n / 4))
		if i%7 == 0 {
			sc += rng.Float64()
		}
		m := []byte(fmt.Sprintf("m%05d", i))
		if _, _, _, _, err := z.ZAdd(ctx, key, m, sc, ZAddFlags{}); err != nil {
			t.Fatalf("ZAdd(%q): %v", m, err)
		}
	}
}

// TestZVerifyHealthy drives the cross-check over the trivially
// consistent tiers and a segmented zset, hot and again cold after a
// Flush and reopen.
func TestZVerifyHealthy(t *testing.T) {
	r := newZsetRig(t)
	ctx := context.Background()
	rng := rand.New(rand.NewSource(31))

	if err := r.z.ZVerify(ctx, []byte("absent")); err != nil {
		t.Fatalf("ZVerify(absent) = %v", err)
	}
	for i := 0; i < 5; i++ {
		if _, _, _, _, err := r.z.ZAdd(ctx, []byte("small"), []byte(fmt.Sprintf("i%d", i)), float64(i), ZAddFlags{}); err != nil {
			t.Fatalf("ZAdd: %v", err)
		}
	}
	zscrubSeed(t, r.z, []byte("big"), 1200, rng)
	if enc, _, err := r.z.Encoding(ctx, []byte("big")); err != nil || enc != "skiplist" {
		t.Fatalf("big encoding = (%q, %v), want segmented", enc, err)
	}
	for _, k := range []string{"small", "big"} {
		if err := r.z.ZVerify(ctx, []byte(k)); err != nil {
			t.Fatalf("ZVerify(%s) hot = %v", k, err)
		}
	}

	if err := r.tr.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	z2 := r.reopen()
	for _, k := range []string{"absent", "small", "big"} {
		if err := z2.ZVerify(ctx, []byte(k)); err != nil {
			t.Fatalf("ZVerify(%s) cold = %v", k, err)
		}
	}
}

// TestZVerifyPaged runs the cross-check over a paged score fence
// under the shrunken fanouts, so the score-side stream crosses upper
// and leaf loads.
func TestZVerifyPaged(t *testing.T) {
	defer SetZFenceCapsForTest(3, 5, 3, 4)()
	r := newZsetRig(t)
	ctx := context.Background()
	key := []byte("board")
	rng := rand.New(rand.NewSource(43))
	for _, i := range rng.Perm(70) {
		if _, _, _, _, err := r.z.ZAdd(ctx, key, fatMember("p", i), float64(i)+0.25, ZAddFlags{}); err != nil {
			t.Fatalf("ZAdd: %v", err)
		}
	}
	if _, err := r.z.zscoreState(ctx, key); err != nil {
		t.Fatalf("zscoreState: %v", err)
	}
	if !r.z.zpaged {
		t.Fatal("fence still flat under the 3-run cap, the paged case is not being tested")
	}
	if err := r.z.ZVerify(ctx, key); err != nil {
		t.Fatalf("ZVerify paged hot = %v", err)
	}
	if err := r.tr.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := r.reopen().ZVerify(ctx, key); err != nil {
		t.Fatalf("ZVerify paged cold = %v", err)
	}
}

// TestZVerifyCatchesDivergence injects each single-side disagreement
// through the score-side test scaffolding and pins that the check
// names it: a member entry with no run entry, a run entry with no
// member entry, and the two sides disagreeing on a live member's
// score.
func TestZVerifyCatchesDivergence(t *testing.T) {
	ctx := context.Background()
	rng := rand.New(rand.NewSource(59))

	fail := func(t *testing.T, z *ZSet, key []byte, want string) {
		t.Helper()
		err := z.ZVerify(ctx, key)
		if err == nil {
			t.Fatalf("ZVerify(%q) passed a diverged zset", key)
		}
		if !strings.Contains(err.Error(), "zset scrub") || !strings.Contains(err.Error(), want) {
			t.Fatalf("ZVerify(%q) = %q, want a scrub error mentioning %q", key, err, want)
		}
	}

	t.Run("member orphan", func(t *testing.T) {
		r := newZsetRig(t)
		key := []byte("z")
		zscrubSeed(t, r.z, key, 300, rng)
		if _, err := r.z.memSet(ctx, key, []byte("ghost"), 5); err != nil {
			t.Fatalf("memSet: %v", err)
		}
		fail(t, r.z, key, "member side")
		if err := r.tr.Flush(ctx); err != nil {
			t.Fatalf("Flush: %v", err)
		}
		fail(t, r.reopen(), key, "member side")
	})

	t.Run("run orphan", func(t *testing.T) {
		r := newZsetRig(t)
		key := []byte("z")
		zscrubSeed(t, r.z, key, 300, rng)
		if _, err := r.z.zrunAdd(ctx, key, 7.5, []byte("phantom")); err != nil {
			t.Fatalf("zrunAdd: %v", err)
		}
		fail(t, r.z, key, "score side")
	})

	t.Run("score mismatch", func(t *testing.T) {
		r := newZsetRig(t)
		key := []byte("z")
		zscrubSeed(t, r.z, key, 300, rng)
		if _, err := r.z.memSet(ctx, key, []byte("m00042"), 1e9); err != nil {
			t.Fatalf("memSet: %v", err)
		}
		fail(t, r.z, key, "diverges at rank")
	})
}

// TestZVerifySample pins the deterministic draw: the same seed walks
// the same keys, a corrupt key inside the sample surfaces, and a
// too-large n clamps to the list.
func TestZVerifySample(t *testing.T) {
	r := newZsetRig(t)
	ctx := context.Background()
	rng := rand.New(rand.NewSource(61))

	var keys [][]byte
	for i := 0; i < 5; i++ {
		k := []byte(fmt.Sprintf("k%d", i))
		zscrubSeed(t, r.z, k, 200, rng)
		keys = append(keys, k)
	}
	if err := r.z.ZVerifySample(ctx, keys, 0, 1); err != nil {
		t.Fatalf("n=0 sample = %v", err)
	}
	if err := r.z.ZVerifySample(ctx, keys, 100, 1); err != nil {
		t.Fatalf("healthy sample = %v", err)
	}
	if _, err := r.z.memSet(ctx, keys[3], []byte("ghost"), 1); err != nil {
		t.Fatalf("memSet: %v", err)
	}
	err1 := r.z.ZVerifySample(ctx, keys, 100, 7)
	if err1 == nil {
		t.Fatal("full sample missed the corrupt key")
	}
	err2 := r.z.ZVerifySample(ctx, keys, 100, 7)
	if err2 == nil || err1.Error() != err2.Error() {
		t.Fatalf("same seed diverged: %v vs %v", err1, err2)
	}
}
