// Lab: timer-wheel level width and churn strategy for the sqlo1 reaper
// (spec 2064/sqlo1 doc 11 section 3.1, milestone S1 lab 02).
//
// Doc 11 fixes the resident-key wheel at 3 levels of 256 slots on a
// 1-second tick and 8 bytes per volatile resident key. This lab prices
// the constants before the wheel slice bakes them, under the workload the
// doc names as the stress: heavy EXPIRE rewrite traffic (GT/LT flag
// churn) at millions of volatile keys.
//
// The structural choice the doc leaves open is what a rewrite does with
// the old bucket entry. Scanning the old bucket to remove it is not
// priced here because it is analytically dead: upper-level buckets hold
// O(keys/width) entries (megabytes of them at 10M keys), so scan-eager
// EXPIRE is O(bucket) and the churn phase would not terminate. The two
// viable designs are lazy (leave the stale entry behind, filter it at
// reap time by comparing the entry's filed expiry against the
// authoritative header expiry; costs memory bloat proportional to churn)
// and eager with a per-key backpointer (O(1) swap-delete, but 8 more
// bytes per volatile key and a backpointer write on every entry a
// cascade moves, making cascades dearer).
//
// Method: in-process, no server, no engine import. Entries are 8 bytes
// (key id plus filed expiry tick); the authoritative expiry lives in a
// flat array standing in for the hot header's expireLo. Load N volatile
// keys with expiries uniform over a fixed common horizon of 2^18 ticks
// (all widths file the same TTL population, so cascade traffic is
// comparable), churn with EXPIRE rewrites (half GT extends, half LT
// shortens, uniform key choice), then drain ticks until every key has
// reaped. Tick size never appears: the wheel operates in ticks, so a
// smaller tick trades horizon for precision at identical per-op cost;
// width is what actually moves cascade traffic and bucket sizes.
package main

import (
	"flag"
	"fmt"
	"math/rand"
	"time"
)

// ttlMax is the common expiry horizon in ticks (about 3 days at a
// 1-second tick), inside every width's 3-level reach.
const ttlMax = 1 << 18

// entry is one wheel slot member: the key and the expiry tick it was
// filed under, 8 bytes per the doc 11 budget. exp doubles as the lazy
// staleness check against the authoritative expiry.
type entry struct {
	key int32
	exp uint32
}

// backptr locates a key's live entry for eager O(1) removal: level,
// slot, and index in the bucket. The padded struct is 8 bytes, which is
// exactly the extra per-key cost the eager design pays.
type backptr struct {
	l uint8
	s uint16
	i uint32
}

// wheel is a 3-level hierarchical timer wheel with power-of-two widths.
type wheel struct {
	width  uint32
	shift  uint
	lvl    [3][][]entry
	now    uint32
	expiry []uint32
	eager  bool
	back   []backptr

	entries     int
	stale       int
	reaped      int
	cascadeMove int
	maxBatch    int
}

func newWheel(width uint32, shift uint, keys int, eager bool) *wheel {
	w := &wheel{width: width, shift: shift, eager: eager, expiry: make([]uint32, keys)}
	if eager {
		w.back = make([]backptr, keys)
	}
	for l := range w.lvl {
		w.lvl[l] = make([][]entry, width)
	}
	return w
}

// levelFor picks the level whose granularity covers the remaining time:
// level 0 within width ticks, level 1 within width squared, else level 2.
func (w *wheel) levelFor(exp uint32) int {
	delta := exp - w.now
	if delta < w.width {
		return 0
	}
	if delta < w.width<<w.shift {
		return 1
	}
	return 2
}

func (w *wheel) slotFor(level int, exp uint32) uint32 {
	return (exp >> (w.shift * uint(level))) & (w.width - 1)
}

func (w *wheel) file(key int32, exp uint32) {
	l := w.levelFor(exp)
	s := w.slotFor(l, exp)
	w.lvl[l][s] = append(w.lvl[l][s], entry{key, exp})
	if w.eager {
		w.back[key] = backptr{uint8(l), uint16(s), uint32(len(w.lvl[l][s]) - 1)}
	}
	w.entries++
}

// unfile is the eager O(1) removal: swap-delete via the backpointer,
// patching the moved entry's own backpointer.
func (w *wheel) unfile(key int32) {
	bp := w.back[key]
	b := w.lvl[bp.l][bp.s]
	last := len(b) - 1
	if int(bp.i) != last {
		b[bp.i] = b[last]
		w.back[b[bp.i].key] = bp
	}
	w.lvl[bp.l][bp.s] = b[:last]
	w.entries--
}

// rewrite is EXPIRE on an already-volatile key.
func (w *wheel) rewrite(key int32, exp uint32) {
	if w.eager {
		w.unfile(key)
	}
	w.expiry[key] = exp
	w.file(key, exp)
}

// advance moves time forward one tick: cascade upper levels on their
// boundaries, then reap the due level-0 bucket. Entries whose filed
// expiry mismatches the authoritative one are lazy-churn leftovers and
// drop on the floor.
func (w *wheel) advance() {
	w.now++
	if w.now&(w.width-1) == 0 {
		w.cascade(1)
		if (w.now>>w.shift)&(w.width-1) == 0 {
			w.cascade(2)
		}
	}
	s := w.now & (w.width - 1)
	b := w.lvl[0][s]
	batch := 0
	for _, e := range b {
		w.entries--
		if w.expiry[e.key] != e.exp {
			w.stale++
			continue
		}
		if e.exp != w.now {
			// A different lap of this slot; re-file. The cascade already
			// re-files whole-revolution-away expiries, but the check
			// keeps the wheel correct at small widths where laps are
			// short.
			w.entries++
			w.file(e.key, e.exp)
			continue
		}
		w.reaped++
		w.expiry[e.key] = 0
		batch++
	}
	w.lvl[0][s] = b[:0]
	if batch > w.maxBatch {
		w.maxBatch = batch
	}
}

// cascade re-files one upper-level bucket into the levels below.
func (w *wheel) cascade(level int) {
	s := w.slotFor(level, w.now)
	b := w.lvl[level][s]
	for _, e := range b {
		w.entries--
		if w.expiry[e.key] != e.exp {
			w.stale++
			continue
		}
		if e.exp <= w.now {
			// Due exactly on the boundary tick; reap rather than re-file
			// into the past.
			w.reaped++
			w.expiry[e.key] = 0
			continue
		}
		w.cascadeMove++
		w.file(e.key, e.exp)
	}
	w.lvl[level][s] = b[:0]
}

type result struct {
	churnNsPerOp float64
	entriesAfter int
	staleFrac    float64
	drainMs      float64
	cascadeMove  int
	maxBatch     int
	reaped       int
	ticks        uint32
}

func runConfig(keys, churn int, width uint32, shift uint, eager bool, seed int64) result {
	w := newWheel(width, shift, keys, eager)
	rng := rand.New(rand.NewSource(seed))

	for k := range keys {
		exp := uint32(rng.Int63n(ttlMax-2)) + 1
		w.expiry[k] = exp
		w.file(int32(k), exp)
	}

	// Churn at now=0: half GT extends toward the horizon, half LT
	// shortens toward now; the wheel cost is the refiling either way,
	// both directions run so old entries sit in both near and far
	// buckets.
	start := time.Now()
	for range churn {
		k := int32(rng.Intn(keys))
		old := int64(w.expiry[k])
		var exp int64
		if rng.Intn(2) == 0 {
			exp = old + rng.Int63n(int64(ttlMax)-old) // GT, capped under the horizon
		} else {
			exp = 1 + rng.Int63n(old) // LT
		}
		if exp == old {
			continue
		}
		w.rewrite(k, uint32(exp))
	}
	churnDur := time.Since(start)

	res := result{
		churnNsPerOp: float64(churnDur.Nanoseconds()) / float64(churn),
		entriesAfter: w.entries,
	}

	start = time.Now()
	for w.reaped < keys {
		w.advance()
		if w.now > 4*ttlMax {
			panic("wheel leaked keys: drain passed 4x the horizon")
		}
	}
	res.drainMs = float64(time.Since(start).Microseconds()) / 1000
	res.staleFrac = float64(w.stale) / float64(res.entriesAfter)
	res.cascadeMove = w.cascadeMove
	res.maxBatch = w.maxBatch
	res.reaped = w.reaped
	res.ticks = w.now
	return res
}

func main() {
	quick := flag.Bool("quick", false, "shrink the sweep for the shared runner")
	flag.Parse()

	keys, churn := 10_000_000, 4_000_000
	if *quick {
		keys, churn = 1_000_000, 400_000
	}

	fmt.Printf("wheel lab: %d volatile keys, %d EXPIRE rewrites (half GT, half LT), horizon %d ticks\n\n",
		keys, churn, ttlMax)

	fmt.Println("sweep A: churn strategy (width 256, 3 levels)")
	fmt.Println("| strategy | churn ns/op | entries after churn | stale frac | drain ms | cascade moves | max batch |")
	fmt.Println("|---|---|---|---|---|---|---|")
	for _, eager := range []bool{false, true} {
		name := "lazy"
		if eager {
			name = "eager-backptr"
		}
		r := runConfig(keys, churn, 256, 8, eager, 3)
		fmt.Printf("| %s | %.0f | %d | %.3f | %.0f | %d | %d |\n",
			name, r.churnNsPerOp, r.entriesAfter, r.staleFrac, r.drainMs, r.cascadeMove, r.maxBatch)
	}

	fmt.Println("\nsweep B: level width (lazy, 3 levels, same key and TTL population)")
	fmt.Println("| width | churn ns/op | drain ms | cascade moves | max batch |")
	fmt.Println("|---|---|---|---|---|")
	for _, cfg := range []struct {
		width uint32
		shift uint
	}{{64, 6}, {128, 7}, {256, 8}, {512, 9}} {
		r := runConfig(keys, churn, cfg.width, cfg.shift, false, 3)
		fmt.Printf("| %d | %.0f | %.0f | %d | %d |\n",
			cfg.width, r.churnNsPerOp, r.drainMs, r.cascadeMove, r.maxBatch)
	}
}
