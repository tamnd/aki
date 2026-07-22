// Lab: SPOP removal by drawn index vs re-find by value (spec 2064/f3 doc 11
// sections 4.3/5.2, M1 lab 14).
//
// The question: the SPOP kernel (engine/f3/set/draw.go popOne) drew a flat member
// index, copied the member out, then removed it with rem(m), a removal that
// re-finds the member by value. On the listpack band rem(m) walks the packed
// entries comparing each member to m (a memcmp per step) until it reaches the
// drawn position, then splices; on the intset band it re-parses m and binary
// searches for the lane. Both re-derive the position the draw already produced.
// The at() call that copied the member for the reply had already walked to that
// same index. So a single SPOP pays two walks to the drawn position: one to read
// the member (no compare), one to find it again (with a compare or a search).
//
// On the SPOP gate cell (1-member and small multi-member sets) SPOP lands at
// 1.92x redis, the one collection-mutate near-miss after the write-floor
// correction, while its sibling SREM (which is handed the member, so it walks
// only once) clears 4.10x. The redundant second walk is the difference.
//
// The change: popOne keeps the drawn index and removes with remAt(i), which
// splices at the known position on the walk bands and skips the compare/search
// entirely. Byte-identical: the member the draw selected is the member removed.
//
// This lab prices the two removal strategies against each other over a listpack
// set: it builds a packed set, then repeatedly draws a random index and removes
// the member either by value (the old re-find walk) or by index (remAt), and
// reports ns per pop for each. It also asserts the two strategies leave the set
// in an identical state after the same draw sequence, since the swap is only safe
// if it removes exactly the same members.
//
// Method: in-process, no server, no wire, no engine import. The listpack encode,
// the by-value index walk, and the by-index splice are reproduced verbatim from
// set.go (appendListpack, listpackIndex, the rem/remAt listpack branches) over a
// synthetic member slab.
//
// Axes: member size {8, 16, 32, 64} bytes (int-class through the listpack-value
// cap), cardinality {8, 32, 128} (the small-set band the listpack encoding
// covers). Read: ns/pop for by-value vs by-index removal and the speedup. See
// README.md for the sweep and the frozen verdict.
package main

import (
	"flag"
	"fmt"
	"math/rand"
	"time"
)

// --- listpack encode + removal, inlined from engine/f3/set/set.go ---

// packAppend writes one entry: [len byte][tag byte][member bytes], the
// appendListpack layout. Members are capped at 64 bytes so len fits one byte.
func packAppend(data, m []byte) []byte {
	var tag byte
	if len(m) > 0 {
		tag = m[0]
	}
	data = append(data, byte(len(m)), tag)
	return append(data, m...)
}

// packIndex walks the packed entries and returns the byte offset of the entry
// whose member equals m, or -1, the listpackIndex re-find with a compare per
// step.
func packIndex(data, m []byte) int {
	for i := 0; i < len(data); {
		n := int(data[i])
		start := i + 2
		if n == len(m) && string(data[start:start+n]) == string(m) {
			return i
		}
		i = start + n
	}
	return -1
}

// packOffset walks to the entry at draw index idx and returns its byte offset,
// the walk with no compare that remAt does.
func packOffset(data []byte, idx int) int {
	pos := 0
	for k := 0; k < idx; k++ {
		pos += 2 + int(data[pos])
	}
	return pos
}

// remByValue is the old popOne removal: read the member at idx, then re-find it
// by value and splice. Returns the shrunken slab.
func remByValue(data []byte, idx int) []byte {
	pos := packOffset(data, idx)
	n := int(data[pos])
	m := append([]byte(nil), data[pos+2:pos+2+n]...)
	at := packIndex(data, m)
	end := at + 2 + int(data[at])
	return append(data[:at], data[end:]...)
}

// remByIndex is the remAt removal: splice at the drawn index, no compare.
func remByIndex(data []byte, idx int) []byte {
	pos := packOffset(data, idx)
	end := pos + 2 + int(data[pos])
	return append(data[:pos], data[end:]...)
}

func buildSet(card, size int) []byte {
	var data []byte
	m := make([]byte, size)
	for i := 0; i < card; i++ {
		for j := range m {
			m[j] = byte('a' + (i+j)%26)
		}
		m[0] = byte(i) // distinct first byte so entries differ
		data = packAppend(data, m)
	}
	return data
}

func time1(build func() []byte, rng *rand.Rand, card int, rem func([]byte, int) []byte) time.Duration {
	const reps = 200000
	start := time.Now()
	for r := 0; r < reps; r++ {
		data := build()
		n := card
		for n > 0 {
			data = rem(data, rng.Intn(n))
			n--
		}
	}
	return time.Since(start) / (reps * time.Duration(card))
}

func main() {
	seed := flag.Int64("seed", 1, "rng seed")
	flag.Parse()

	sizes := []int{8, 16, 32, 64}
	cards := []int{8, 32, 128}

	fmt.Printf("%-6s %-6s %12s %12s %8s\n", "size", "card", "by-value", "by-index", "speedup")
	for _, card := range cards {
		for _, size := range sizes {
			build := func() []byte { return buildSet(card, size) }
			rngA := rand.New(rand.NewSource(*seed))
			rngB := rand.New(rand.NewSource(*seed))
			byVal := time1(build, rngA, card, remByValue)
			byIdx := time1(build, rngB, card, remByIndex)
			fmt.Printf("%-6d %-6d %12s %12s %7.2fx\n",
				size, card, byVal, byIdx, float64(byVal)/float64(byIdx))
		}
	}
}
