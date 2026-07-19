// Lab 09: the drained-cursor read short-circuit (doc 14 sections 6.4 and 7.5, M5
// lab 09).
//
// A polling XREAD from "$" (the last ID) and a caught-up XREADGROUP consumer (its
// group cursor at the stream's last-delivered ID) both read "give me entries newer
// than X" where X is already the newest entry. That read finds nothing, but the
// original path still did work to discover that: readAfter resolved to a forward
// range walk over (X, +inf], which seeks the block X lands in (the tail block) and
// walks every entry in it, testing each against the exclusive lower bound, all of
// them failing because none is newer than X, before it runs off the end of the
// blocks with an empty result. On a card-10k stream the tail block holds hundreds
// of entries, so every empty poll walked hundreds of entries and comparisons to
// return nil. Redis makes an O(1) not-newer check first (the group cursor vs the
// stream last-id) and returns nil without walking, which is why a box XREADGROUP
// poll that drained its group read redis at 2.27M nil returns/s against aki's 279K,
// an 8x gap on the group-empty fast path.
//
// The fix is the same O(1) check: readAfter (and the XREAD immediate loop) return
// nil the moment the after-ID is at or above the stream's last-added ID, before any
// seek or walk. This lab prices the walk it removes: a block of n entries whose IDs
// are all at or below the cursor, read the old way (walk-and-compare) versus the new
// way (one cmp against lastID).
//
// Two arms over the same drained tail block:
//
//	walk       seek the tail block, walk every entry testing the lower bound, return nil
//	shortcut   one cmp: after >= lastID, return nil
//
// Read: ns/op per empty poll across a tail-block-size sweep. The walk arm scales
// with the block's entry count; the shortcut arm is flat.
package main

import (
	"fmt"
	"time"
)

// id models a streamID: a (ms, seq) pair with the same compare order the engine
// uses.
type id struct{ ms, seq uint64 }

func (a id) cmp(b id) int {
	switch {
	case a.ms != b.ms:
		if a.ms < b.ms {
			return -1
		}
		return 1
	case a.seq != b.seq:
		if a.seq < b.seq {
			return -1
		}
		return 1
	}
	return 0
}

// tailBlock models the stream's last block: n live entry IDs in ascending order,
// the run a drained poll walks. lastID is the greatest, the stream's last-added ID.
type tailBlock struct {
	ids    []id
	lastID id
}

func makeTail(n int) tailBlock {
	ids := make([]id, n)
	for i := 0; i < n; i++ {
		ids[i] = id{ms: uint64(i + 1), seq: 0}
	}
	return tailBlock{ids: ids, lastID: ids[n-1]}
}

// walkArm models the pre-fix read: the after-ID is the cursor at lastID, and the
// forward walk tests every entry in the block against the exclusive lower bound
// (id > after), all failing, returning a count of matches (zero). The loop is the
// work the fix removes.
func walkArm(tb tailBlock, after id) int {
	matched := 0
	for _, e := range tb.ids {
		if e.cmp(after) > 0 { // aboveLo with an exclusive bound
			matched++
		}
	}
	return matched
}

// shortcutArm models the fix: one cmp of the after-ID against the stream's last-added
// ID. At or above it, nothing is newer, so return nil without touching the block.
func shortcutArm(tb tailBlock, after id) bool {
	return after.cmp(tb.lastID) >= 0
}

func main() {
	sweep := []int{16, 64, 256, 1024, 4096}
	fmt.Println("tail-entries | walk-ns  shortcut-ns  speedup")
	for _, n := range sweep {
		tb := makeTail(n)
		after := tb.lastID // the drained cursor: exactly the last-added ID
		tw := timeIt(func() { sinkInt += walkArm(tb, after) })
		ts := timeIt(func() {
			if shortcutArm(tb, after) {
				sinkInt++
			}
		})
		fmt.Printf("%12d | %6d %11d  %.1fx\n", n, tw, ts, float64(tw)/float64(ts))
	}
}

var sinkInt int

func timeIt(fn func()) int64 {
	const iters = 200000
	for i := 0; i < 1000; i++ {
		fn()
	}
	start := time.Now()
	for i := 0; i < iters; i++ {
		fn()
	}
	return time.Since(start).Nanoseconds() / iters
}
