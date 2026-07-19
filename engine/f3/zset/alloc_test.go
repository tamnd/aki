package zset

import (
	"strconv"
	"testing"
)

// The inline read path must not allocate (F7, spec 2064/f3/12 section 4):
// ZSCORE scans the packed blob in place and ZCARD reads a counter.
// testing.AllocsPerRun rounds to whole allocations, so anything above zero is a
// real escape, not noise.

func TestZeroAllocInline(t *testing.T) {
	z := newZset()
	for i := 0; i < 64; i++ {
		z.update([]byte("m"+strconv.Itoa(i)), float64(i), flags{})
	}
	if z.enc != encListpack {
		t.Fatalf("enc = %s, want listpack", z.enc)
	}
	hit := []byte("m40")
	miss := []byte("absent")

	checkZero(t, "inline score hit", func() { _, sinkBool = z.score(hit) })
	checkZero(t, "inline score miss", func() { _, sinkBool = z.score(miss) })
	checkZero(t, "inline card", func() { sinkInt = z.card() })
}

// The native band holds the same bar: ZSCORE is one member-hash probe reading
// the record's raw bits, ZCARD reads the table's counter, neither allocates.
func TestZeroAllocNative(t *testing.T) {
	z := newZset()
	for i := 0; i < maxListpackEntries+64; i++ {
		z.update([]byte("m"+strconv.Itoa(i)), float64(i), flags{})
	}
	if z.enc != encSkiplist {
		t.Fatalf("enc = %s, want skiplist", z.enc)
	}
	hit := []byte("m140")
	miss := []byte("absent")

	checkZero(t, "native score hit", func() { _, sinkBool = z.score(hit) })
	checkZero(t, "native score miss", func() { _, sinkBool = z.score(miss) })
	checkZero(t, "native card", func() { sinkInt = z.card() })
}

// The native rank path is one member-hash probe plus a counted descent (section
// 6.3), no second lookup and no walk, so it holds the zero-allocation bar.
func TestZeroAllocRankNative(t *testing.T) {
	z := buildNative(20_000)
	hit := []byte("member:" + pad(10_000))
	miss := []byte("absent")
	checkZero(t, "native rank hit", func() { sinkInt, _, sinkBool = z.rank(hit) })
	checkZero(t, "native rank miss", func() { sinkInt, _, sinkBool = z.rank(miss) })
}

// ZRANGE by index over the native band seeks with a counted select and streams
// the window straight into a reply buffer (section 6.4): once the scratch is
// warm it grows for none of the elements, so the walk allocates nothing per
// element in any direction, with or without scores.
func TestZeroAllocRangeNative(t *testing.T) {
	z := buildNative(20_000)
	buf := make([]byte, 0, 1<<20) // pre-grown reply scratch, reused each run
	run := func(name string, rev, ws bool) {
		checkZero(t, name, func() {
			sinkBytes = z.rangeByIndex(buf[:0], 5_000, 5_099, rev, ws, false)
		})
	}
	run("native range fwd", false, false)
	run("native range fwd withscores", false, true)
	run("native range rev", true, false)
	run("native range rev withscores", true, true)
}

// ZRANGEBYSCORE and ZRANGEBYLEX seek to a rank window with counted descents and
// stream the window straight into a reply buffer (sections 6.4, 6.5), so once
// the scratch is warm the walk grows the buffer for none of its elements. The
// window resolution itself (scoreWindow, lexWindow) is two descents and no
// allocation either.
func TestZeroAllocRangeByScoreNative(t *testing.T) {
	z := buildNative(20_000)
	buf := make([]byte, 0, 1<<20)
	min := scoreBound{value: 5_000}
	max := scoreBound{value: 5_099}
	run := func(name string, rev, ws bool) {
		checkZero(t, name, func() {
			lo, hi := z.scoreWindow(min, max)
			a, b, _ := applyLimit(lo, hi, rev, false, 0, 0)
			sinkBytes = z.rangeByRankWindow(buf[:0], a, b, rev, ws, false)
		})
	}
	run("byscore fwd", false, false)
	run("byscore fwd withscores", false, true)
	run("byscore rev", true, false)
	checkZero(t, "scoreWindow", func() {
		sinkInt, sinkInt2 = z.scoreWindow(min, max)
	})
}

func TestZeroAllocRangeByLexNative(t *testing.T) {
	z := buildTiedNative(20_000)
	buf := make([]byte, 0, 1<<20)
	min := lexBound{value: []byte("k" + pad(5_000))}
	max := lexBound{value: []byte("k" + pad(5_099))}
	run := func(name string, rev bool) {
		checkZero(t, name, func() {
			lo, hi := z.lexWindow(min, max)
			a, b, _ := applyLimit(lo, hi, rev, false, 0, 0)
			sinkBytes = z.rangeByRankWindow(buf[:0], a, b, rev, false, false)
		})
	}
	run("bylex fwd", false)
	run("bylex rev", true)
	checkZero(t, "lexWindow", func() {
		sinkInt, sinkInt2 = z.lexWindow(min, max)
	})
}

// The native single pop is one fused tree descent plus the member-hash delete,
// no state restore and no per-pop rebuild below the reclaim thresholds (section
// 6.7), so it holds the zero-allocation bar. The emit sink captures nothing, so
// it stays a static function and adds no closure allocation of its own. The set
// is large enough that 200 pops never trip a wholesale rebuild.
func TestZeroAllocPopNative(t *testing.T) {
	zmin := buildNative(20_000)
	checkZero(t, "native pop min single", func() {
		zmin.nat.pop(true, 1, func(m []byte, s float64) { sinkBytes, sinkFloat = m, s })
	})
	zmax := buildNative(20_000)
	checkZero(t, "native pop max single", func() {
		zmax.nat.pop(false, 1, func(m []byte, s float64) { sinkBytes, sinkFloat = m, s })
	})
}

// ZREM over the native band is one member-hash probe plus a fused tree delete,
// with the dead counters bumped in place: no down-conversion, no state restore,
// no allocation (section 6.6). The members are pre-built so the loop counter
// alone selects a distinct present member each run, keeping the removal on the
// real delete path without allocating the key.
func TestZeroAllocRemNative(t *testing.T) {
	z := buildNative(20_000)
	members := make([][]byte, 400)
	for j := range members {
		members[j] = []byte("member:" + pad(j))
	}
	i := 0
	checkZero(t, "native rem", func() {
		sinkBool = z.rem(members[i])
		i++
	})
}

// ZREMRANGEBY* over the native band deletes its rank window as a bounded run of
// counted tree deletes plus member-hash deletes, bumping the dead counters in
// place (section 6.9). The window here is small against a large set, so the
// amortized reclaim rebuild never trips across the 200 measured runs and the hot
// loop stands alone: it must not allocate. The removal shrinks the set each run,
// so the window sits at the midpoint where a present band is always found.
func TestZeroAllocRemoveRange(t *testing.T) {
	z := buildNative(300_000)
	const win = 8
	checkZero(t, "native remove range", func() {
		lo := z.card() / 2
		sinkInt = z.removeRange(lo, lo+win)
	})
}

var (
	sinkBool  bool
	sinkInt   int
	sinkInt2  int
	sinkFloat float64
)

func checkZero(t *testing.T, name string, fn func()) {
	t.Helper()
	if n := testing.AllocsPerRun(200, fn); n != 0 {
		t.Errorf("%s allocated %v times per run, want 0", name, n)
	}
}
