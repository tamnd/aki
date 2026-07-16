package zset

import (
	"math"
	"math/rand/v2"
	"sort"
	"testing"

	"github.com/tamnd/aki/obs1srv/resp"
)

// The inline-band microbenchmarks (spec 2064/f3/12 section 4). ZSCORE and ZCARD
// are the zero-allocation reads the small-cardinality gate rows lean on, and
// ZADD is the insert path amortized over a full inline build (the memmove plus
// the ordered splice). These are Go microbenchmarks, not the GamingPC gate; they
// order the mechanism and quote a floor, and the gate rows key on the server
// numbers.

func buildInline(n int) *zset {
	z := newZset()
	for i := 0; i < n; i++ {
		z.update([]byte("m"+itoa(i)), float64(i), flags{})
	}
	return z
}

func BenchmarkZScoreInlineHit(b *testing.B) {
	z := buildInline(64)
	m := []byte("m40")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, sinkBool = z.score(m)
	}
}

func BenchmarkZScoreInlineMiss(b *testing.B) {
	z := buildInline(64)
	m := []byte("absent")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, sinkBool = z.score(m)
	}
}

func BenchmarkZCardInline(b *testing.B) {
	z := buildInline(64)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sinkInt = z.card()
	}
}

// ZADD over a churning member: re-add an existing member at a new score, the
// rescore memmove path that dominates a live inline leaderboard.
func BenchmarkZAddInlineRescore(b *testing.B) {
	z := buildInline(64)
	m := []byte("m32")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		z.update(m, float64(i%128), flags{})
	}
}

// ZSCORE formatting: the reply path over an inline hit, the shape ZSCORE ships.
func BenchmarkZScoreFormat(b *testing.B) {
	z := buildInline(64)
	m := []byte("m40")
	var buf [40]byte
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s, _ := z.score(m)
		sinkBytes = resp.FormatScore(buf[:0], s)
	}
}

var sinkBytes []byte

// The native-band microbenchmarks at 10k and 1M members, the same sizes the
// tree lab quoted, so the dual structure's cost over the bare tree is visible:
// ZSCORE is a hash probe, ZADD on an existing member is the rescore path (tree
// delete plus reinsert plus the probe), ZINCRBY is the same plus the read.

func buildNative(n int) *zset {
	z := newZset()
	z.nat = newNativeStore(n)
	z.enc = encSkiplist
	for i := 0; i < n; i++ {
		z.nat.appendSorted([]byte("member:"+pad(i)), float64(i))
	}
	z.nat.seal()
	return z
}

func benchNativeSizes(b *testing.B, fn func(b *testing.B, z *zset, n int)) {
	for _, n := range []int{10_000, 1_000_000} {
		b.Run(itoa(n), func(b *testing.B) {
			z := buildNative(n)
			b.ReportAllocs()
			b.ResetTimer()
			fn(b, z, n)
		})
	}
}

func BenchmarkZScoreNative(b *testing.B) {
	benchNativeSizes(b, func(b *testing.B, z *zset, n int) {
		m := []byte("member:" + pad(n/2))
		for i := 0; i < b.N; i++ {
			_, sinkBool = z.score(m)
		}
	})
}

// ZADD rescore: one member churning scores, the leaderboard steady state.
func BenchmarkZAddNativeRescore(b *testing.B) {
	benchNativeSizes(b, func(b *testing.B, z *zset, n int) {
		m := []byte("member:" + pad(n/2))
		for i := 0; i < b.N; i++ {
			z.update(m, float64(i%n), flags{})
		}
	})
}

func BenchmarkZIncrByNative(b *testing.B) {
	benchNativeSizes(b, func(b *testing.B, z *zset, n int) {
		m := []byte("member:" + pad(n/2))
		for i := 0; i < b.N; i++ {
			z.update(m, 1.5, flags{incr: true})
		}
	})
}

// The ZRANK rows the M2 headline cell keys on (PRED-F3-M2-ZRANKZIPF): one member
// hash probe plus one counted descent, at 10k and 1M members, under a uniform
// draw and a zipfian draw that concentrates on the hot low ranks. buildNative
// seats member i at rank i, so a drawn rank names its member directly. The bar
// is the tree lab's 307.7ns counted descent at 1M plus the hash probe; the
// zipfian row is the one that stuck at 1.86x/1.83x in v1, so it earns its own
// benchmark against the uniform baseline.
const drawCount = 8192

func BenchmarkZRankNative(b *testing.B) {
	for _, n := range []int{10_000, 1_000_000} {
		z := buildNative(n)
		for _, zipf := range []bool{false, true} {
			name := itoa(n) + "/uniform"
			if zipf {
				name = itoa(n) + "/zipfian"
			}
			b.Run(name, func(b *testing.B) {
				ranks := drawRanks(n, zipf, uint64(n))
				members := make([][]byte, drawCount)
				for i, r := range ranks {
					members[i] = []byte("member:" + pad(r))
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					_, _, sinkBool = z.rank(members[i&(drawCount-1)])
				}
			})
		}
	}
}

// ZRANGE by index at 1M over windows of 10, 100 and 10k, both directions
// (PRED-F3-M2-ZRANGE): a counted select seek then a bounded leaf-chain walk,
// streamed into a reused buffer. Divide ns/op by the window to read the
// per-element cost the streaming reply is meant to flatten.
func BenchmarkZRangeNative(b *testing.B) {
	const n = 1_000_000
	z := buildNative(n)
	buf := make([]byte, 0, 1<<22)
	for _, win := range []int{10, 100, 10_000} {
		for _, rev := range []bool{false, true} {
			dir := "fwd"
			if rev {
				dir = "rev"
			}
			b.Run(itoa(win)+"/"+dir, func(b *testing.B) {
				lo, hi, _ := clampRange(n/2, n/2+win-1, z.card())
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					sinkBytes = z.rangeByIndex(buf[:0], lo, hi, rev, false)
				}
			})
		}
	}
}

// ZRANGEBYSCORE at 1M over score windows of 10, 100 and 10k, both directions
// (PRED-F3-M2-ZRANGE, the headline cell). buildNative seats member i at score i,
// so a window of w scores is a band of w members, resolved by two counted
// descents then streamed. Divide ns/op by the window to read the per-element
// cost the streaming reply flattens, the number to compare against #615's index
// range.
func BenchmarkZRangeByScoreNative(b *testing.B) {
	const n = 1_000_000
	z := buildNative(n)
	buf := make([]byte, 0, 1<<22)
	for _, win := range []int{10, 100, 10_000} {
		for _, rev := range []bool{false, true} {
			dir := "fwd"
			if rev {
				dir = "rev"
			}
			b.Run(itoa(win)+"/"+dir, func(b *testing.B) {
				min := scoreBound{value: float64(n / 2)}
				max := scoreBound{value: float64(n/2 + win - 1)}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					lo, hi := z.scoreWindow(min, max)
					a, c, _ := applyLimit(lo, hi, rev, false, 0, 0)
					sinkBytes = z.rangeByRankWindow(buf[:0], a, c, rev, false)
				}
			})
		}
	}
}

// ZCOUNT at 1M: two counted descents, no walk, so it is flat in the window
// width (spec 2064/f3/12 section 6.4). The window here spans a tenth of the set
// to show the count is independent of how many members it covers.
func BenchmarkZCountNative(b *testing.B) {
	const n = 1_000_000
	z := buildNative(n)
	min := scoreBound{value: float64(n / 2)}
	max := scoreBound{value: float64(n/2 + n/10)}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lo, hi := z.scoreWindow(min, max)
		sinkInt = hi - lo
	}
}

// ZRANGEBYLEX in a 1M tied band (PRED gate row, section 3.2): every member at
// one score, so the tree is keyed by member bytes and the seek routes through
// the interior spill-slot tie-breaks. Windows of 10, 100 and 10k members.
func BenchmarkZRangeByLexNative(b *testing.B) {
	const n = 1_000_000
	z := buildTiedNative(n)
	buf := make([]byte, 0, 1<<22)
	for _, win := range []int{10, 100, 10_000} {
		b.Run(itoa(win), func(b *testing.B) {
			min := lexBound{value: []byte("k" + pad(n/2))}
			max := lexBound{value: []byte("k" + pad(n/2+win-1))}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				lo, hi := z.lexWindow(min, max)
				a, c, _ := applyLimit(lo, hi, false, false, 0, 0)
				sinkBytes = z.rangeByRankWindow(buf[:0], a, c, false, false)
			}
		})
	}
}

// BenchmarkZMPopNative measures the ZMPOP drain per element at the lab's
// saturating batch of 31 (one leaf), over the native band at 10k and 1M
// members. Each step is one fused tree pop plus the member-hash delete; the set
// is rebuilt off the timer when it drains, so the steady figure is the pop pair
// looped over a cache-resident spine. ns/op reads per element, the number that
// sits against the tree lab's fused pop.
func BenchmarkZMPopNative(b *testing.B) {
	const batch = 31
	for _, n := range []int{10_000, 1_000_000} {
		b.Run(itoa(n), func(b *testing.B) {
			z := buildNative(n)
			b.ReportAllocs()
			b.ResetTimer()
			done := 0
			for i := 0; i < b.N; i += batch {
				if done+batch > n {
					b.StopTimer()
					z = buildNative(n)
					done = 0
					b.StartTimer()
				}
				z.pop(true, batch, func([]byte, float64) {})
				done += batch
			}
		})
	}
}

// BenchmarkZRandMemberNative measures the single ZRANDMEMBER draw at 10k and 1M
// members: one owner-local PCG draw (Lemire, exactly uniform) resolved to a
// member by one counted select on the tree, no removal. The draw allocates
// nothing per call, so this is the read-path cost the F15 kernel adds over a
// bare counted select.
func BenchmarkZRandMemberNative(b *testing.B) {
	benchNativeSizes(b, func(b *testing.B, z *zset, n int) {
		g := newReg(uint64(n))
		for i := 0; i < b.N; i++ {
			sinkBytes, _ = z.at(g.next(z.card()))
		}
	})
}

// BenchmarkZRemNative measures the ZREM hot pair at 10k and 1M members: remove
// one hot member then re-add it, so the set size holds steady and the timed cost
// is the member-hash delete plus the fused tree delete, plus the reinsert that
// keeps the member present. It is the churn a leaderboard's ZREM/ZADD pair pays.
func BenchmarkZRemNative(b *testing.B) {
	benchNativeSizes(b, func(b *testing.B, z *zset, n int) {
		m := []byte("member:" + pad(n/2))
		s := float64(n / 2)
		for i := 0; i < b.N; i++ {
			z.rem(m)
			z.update(m, s, flags{})
		}
	})
}

// drawRanks precomputes drawCount rank samples over [0,n): uniform, or a zipfian
// draw at exponent 0.99 (the PRED-F3-M2-ZRANKZIPF shape) that piles onto the low
// ranks so the descents hammer one hot region of the tree.
func drawRanks(n int, zipf bool, seed uint64) []int {
	r := rand.New(rand.NewPCG(seed, 0x9e3779b9))
	out := make([]int, drawCount)
	if !zipf {
		for i := range out {
			out[i] = r.IntN(n)
		}
		return out
	}
	cdf := make([]float64, n)
	sum := 0.0
	for k := 0; k < n; k++ {
		sum += 1.0 / math.Pow(float64(k+1), 0.99)
		cdf[k] = sum
	}
	for i := range out {
		x := r.Float64() * sum
		k := sort.SearchFloat64s(cdf, x)
		if k >= n {
			k = n - 1
		}
		out[i] = k
	}
	return out
}

// BenchmarkZscanNative measures the ZSCAN per-member cost at 10k and 1M members:
// each page rides COUNT records down the member array, probes the hash to skip
// dead cells, and emits the live member with its stored score bits. The cursor
// wraps to the top when it reaches the floor so the steady loop keeps paging a
// cache-resident array. A page emits pageCount members, so ns/op divided by
// pageCount reads the per-member cost, the figure the ZSCAN row quotes.
func BenchmarkZscanNative(b *testing.B) {
	const pageCount = 1000
	benchNativeSizes(b, func(b *testing.B, z *zset, n int) {
		cursor := uint64(0)
		for i := 0; i < b.N; i++ {
			cursor = z.nat.scanPage(cursor, pageCount, nil, func(m []byte, bits uint64) {
				sinkBytes = m
				sinkInt2 = int(bits)
			})
			if cursor == 0 {
				cursor = uint64(n)
			}
		}
	})
}

// BenchmarkRemoveRangeNative measures ZREMRANGEBYRANK per element at the 10k
// window on a 1M native set, the shape the removal spec prices (lab 04 tied v1's
// ZREM p99 shoulder to deferred teardown, so this removal is inline: the window
// is deleted as a bounded high-to-low run of counted tree deletes plus member
// hash deletes, one amortized reclaim rebuild at the end). The set is rebuilt off
// the timer before each removal, so the timed figure is the window delete alone.
// ns/op divided by the window reads the per-element cost.
func BenchmarkRemoveRangeNative(b *testing.B) {
	const n, win = 1_000_000, 10_000
	lo := n/2 - win/2
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		z := buildNative(n)
		b.StartTimer()
		sinkInt = z.removeRange(lo, lo+win)
	}
}
