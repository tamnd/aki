// Lab: APPEND growth multiplier (spec 2064/f3/09 section 2, M0 lab 7).
//
// The question: when an APPEND outgrows a record's reserved capacity, how
// much should the republish over-reserve? Doc 09 doubles the embedded band
// toward str_inline_max and grows separated runs at 1.5x; SDS grows at 2x
// then 1MB steps. The multiplier trades republish frequency (each one is an
// arena alloc plus a full copy, and the old bytes become dead-byte charge)
// against slack (F14 charges every reserved byte forever). This lab sweeps
// growth factors 1.25, 1.5, 2.0 and a fixed 4KiB step.
//
// Method: appends run against the real engine/f3/store. Each growth
// republishes for real: Set with the value padded to the new reserved
// capacity, which is the engine's actual republish (fresh record, full copy,
// entry repoint, old record charged dead). Appends that fit the reservation
// are the engine's in-place tail write, emulated as the tail memcpy into the
// value buffer (identical cost for every policy, so it moves no comparison;
// the store's public Set would recopy the whole value and drown the signal).
// The engine is not modified.
//
// Two workloads. One-key build: 16B appends growing one key from empty to
// 64KiB (the store's value cap), 400 rounds. Mixed: 10k keys, appends of
// 1 to 256B, each key growing to its own target between 256B and 8KiB, ops
// interleaved across keys in random order, identical op sequence for every
// policy. Reported per policy: amortized ns per append, republishes, write
// amplification (physical bytes copied per logical byte appended), peak
// overallocation (reserved capacity over logical bytes, the F14 exposure),
// and for the mixed workload the arena churn (bytes the arena handed out,
// live plus dead, per logical byte).
//
// See README.md for the numbers and the verdict.
package main

import (
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/tamnd/aki/engine/f3/store"
)

func align8(n int) int { return (n + 7) &^ 7 }

type policy struct {
	name string
	grow func(cap, need int) int
}

func factor(num, den int) func(cap, need int) int {
	return func(c, need int) int {
		n := c * num / den
		if n < need {
			n = need
		}
		return align8(n)
	}
}

var policies = []policy{
	{"x1.25", factor(5, 4)},
	{"x1.5", factor(3, 2)},
	{"x2.0", factor(2, 1)},
	{"+4KiB", func(c, need int) int {
		n := c + 4096
		if n < need {
			n = need + 4096
		}
		return align8(n)
	}},
}

const (
	oneKeyTarget = 63 << 10 // just under the store's 64KiB value cap
	oneKeyDelta  = 16
	oneKeyRounds = 400

	// maxReserved keeps a growth step inside the store's value width; the
	// real engine would hand values past this to the separated band.
	maxReserved = 0xffff &^ 7

	mixedKeys      = 10000
	mixedMinTarget = 256
	mixedMaxTarget = 8 << 10
)

func keyBytes(dst []byte, i uint64) []byte {
	x := i + 0x9e3779b97f4a7c15
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	dst = dst[:0]
	for j := 0; j < 16; j++ {
		dst = append(dst, "0123456789abcdef"[(x>>(j*4))&15])
	}
	return dst
}

func runOneKey(p policy) {
	s := store.New(256<<20, 0)
	buf := make([]byte, oneKeyTarget+8<<10)
	src := make([]byte, oneKeyDelta)
	for i := range src {
		src[i] = byte('a' + i%26)
	}
	key := make([]byte, 0, 16)

	var el time.Duration
	var appends, republishes, physical, logical int
	peakOver := 1.0
	for r := 0; r < oneKeyRounds; r++ {
		s.Reset()
		vlen, reserved := 0, 0
		t0 := time.Now()
		for vlen < oneKeyTarget {
			copy(buf[vlen:], src)
			vlen += oneKeyDelta
			logical += oneKeyDelta
			physical += oneKeyDelta
			appends++
			if vlen > reserved {
				if reserved == 0 {
					reserved = align8(vlen)
				} else {
					reserved = p.grow(reserved, vlen)
				}
				if reserved > maxReserved {
					reserved = maxReserved
				}
				if err := s.Set(keyBytes(key, 1), buf[:reserved]); err != nil {
					panic(err)
				}
				republishes++
				physical += reserved
				if over := float64(reserved) / float64(vlen); over > peakOver {
					peakOver = over
				}
			}
		}
		el += time.Since(t0)
	}
	fmt.Printf("| %s | %.1f | %.1f | %.2f | %.2fx |\n",
		p.name,
		float64(el.Nanoseconds())/float64(appends),
		float64(republishes)/oneKeyRounds,
		float64(physical)/float64(logical),
		peakOver)
	_ = os.Stdout.Sync()
}

type mixedOp struct {
	key   uint32
	delta uint16
}

// mixedPlan builds one op sequence shared by every policy: per-key targets,
// then appends in random key order until every key reaches its target.
func mixedPlan() ([]int, []mixedOp) {
	rng := rand.New(rand.NewSource(7))
	targets := make([]int, mixedKeys)
	remaining := make([]int, mixedKeys)
	alive := make([]int, mixedKeys)
	for i := range targets {
		targets[i] = mixedMinTarget + rng.Intn(mixedMaxTarget-mixedMinTarget)
		remaining[i] = targets[i]
		alive[i] = i
	}
	var ops []mixedOp
	for len(alive) > 0 {
		ai := rng.Intn(len(alive))
		k := alive[ai]
		d := 1 + rng.Intn(256)
		if d >= remaining[k] {
			d = remaining[k]
			alive[ai] = alive[len(alive)-1]
			alive = alive[:len(alive)-1]
		}
		remaining[k] -= d
		ops = append(ops, mixedOp{uint32(k), uint16(d)})
	}
	return targets, ops
}

func runMixed(p policy, targets []int, ops []mixedOp) {
	s := store.New(1<<30, 0)
	bufs := make([][]byte, mixedKeys)
	vlens := make([]int, mixedKeys)
	caps := make([]int, mixedKeys)
	for i := range bufs {
		bufs[i] = make([]byte, align8(targets[i])+8<<10)
	}
	src := make([]byte, 256)
	for i := range src {
		src[i] = byte('a' + i%26)
	}
	key := make([]byte, 0, 16)

	var republishes, physical, logical int
	var totLen, totReserved int
	peakSlack := 1.0
	t0 := time.Now()
	for i, op := range ops {
		k := int(op.key)
		d := int(op.delta)
		copy(bufs[k][vlens[k]:], src[:d])
		vlens[k] += d
		totLen += d
		logical += d
		physical += d
		if vlens[k] > caps[k] {
			var next int
			if caps[k] == 0 {
				next = align8(vlens[k])
			} else {
				next = p.grow(caps[k], vlens[k])
			}
			totReserved += next - caps[k]
			caps[k] = next
			if err := s.Set(keyBytes(key, uint64(k)), bufs[k][:next]); err != nil {
				panic(err)
			}
			republishes++
			physical += next
		}
		if i > len(ops)/10 {
			if slack := float64(totReserved) / float64(totLen); slack > peakSlack {
				peakSlack = slack
			}
		}
	}
	el := time.Since(t0)
	used, _ := s.ArenaBytes()
	fmt.Printf("| %s | %.1f | %.2f | %.2f | %.2fx | %.2fx |\n",
		p.name,
		float64(el.Nanoseconds())/float64(len(ops)),
		float64(republishes)/mixedKeys,
		float64(physical)/float64(logical),
		peakSlack,
		float64(used)/float64(logical))
	_ = os.Stdout.Sync()
}

func main() {
	fmt.Printf("one-key: %dB appends to %dKiB, %d rounds\n\n",
		oneKeyDelta, oneKeyTarget>>10, oneKeyRounds)
	fmt.Println("| policy | ns/append | republishes/round | write amp | peak cap/len |")
	fmt.Println("|---|---|---|---|---|")
	for _, p := range policies {
		runOneKey(p)
	}

	targets, ops := mixedPlan()
	var logical int
	for _, t := range targets {
		logical += t
	}
	fmt.Printf("\nmixed: %d keys, targets %dB-%dKiB (%.0fMiB logical), %d appends of 1-256B\n\n",
		mixedKeys, mixedMinTarget, mixedMaxTarget>>10, float64(logical)/(1<<20), len(ops))
	fmt.Println("| policy | ns/append | republishes/key | write amp | peak slack | arena churn |")
	fmt.Println("|---|---|---|---|---|---|")
	for _, p := range policies {
		runMixed(p, targets, ops)
	}
}
