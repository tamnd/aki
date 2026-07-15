// Lab: alloczero, the zero-allocation gate for sqlo1 hot paths
// (spec 2064/sqlo1 doc 04 section 16, milestone S1 lab 03).
//
// Doc 04's rule is that steady-state allocation is a bug: arenas,
// headers, buffers, continuation slots, and mailbox rings are
// preallocated, and every hot-path PR is gated on allocs/op == 0.
// This lab is that gate. Unlike labs 01 and 02 it is not a model
// sweep: it imports engine/sqlo1 (the boundary allowlist permits
// sqlo1 packages depending on each other) and measures the real code
// with testing.AllocsPerRun, and its test fails CI the moment any
// gated path allocates.
//
// The gate grows with the engine. Today it covers the wire path,
// which is complete and performance-relevant: command parsing with a
// reused argument slice and reply building into a presized buffer,
// separately and as GET- and SET-shaped round trips. Hot-tier point
// ops register here when the header-table and eviction slices land;
// the S0 placeholder server dispatch is deliberately not gated (it
// clones keys and builds one-op batches by design, and doc 04 calls
// it no performance statement).
package main

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/tamnd/aki/engine/sqlo1"
)

// probe is one gated operation. Each closure owns preallocated state
// (buffers, argument slices) captured outside the measured function,
// the way the real connection loop owns its buffers.
type probe struct {
	name string
	f    func()
}

func mustParse(buf []byte, args [][]byte) [][]byte {
	out, n, err := sqlo1.ParseCommand(buf, args)
	if err != nil || n != len(buf) {
		panic(fmt.Sprintf("parse %q: n %d err %v", buf, n, err))
	}
	return out
}

// wireProbes builds the gated wire-path operations.
func wireProbes() []probe {
	getCmd := []byte("*2\r\n$3\r\nGET\r\n$8\r\nk0000001\r\n")
	setCmd := []byte("*3\r\n$3\r\nSET\r\n$8\r\nk0000001\r\n$16\r\nvvvvvvvvvvvvvvvv\r\n")
	bigVal := bytes.Repeat([]byte("v"), 4096)
	setBig := fmt.Appendf(nil, "*3\r\n$3\r\nSET\r\n$8\r\nk0000001\r\n$%d\r\n%s\r\n", len(bigVal), bigVal)
	pipe := bytes.Repeat(getCmd, 16)

	args := make([][]byte, 0, 8)
	reply := make([]byte, 0, 8<<10)
	val := bytes.Repeat([]byte("v"), 16)

	return []probe{
		{"parse GET", func() {
			args = mustParse(getCmd, args[:0])
		}},
		{"parse SET 16B", func() {
			args = mustParse(setCmd, args[:0])
		}},
		{"parse SET 4KiB", func() {
			args = mustParse(setBig, args[:0])
		}},
		{"parse 16-deep GET pipeline", func() {
			consumed := 0
			for consumed < len(pipe) {
				var n int
				var err error
				args, n, err = sqlo1.ParseCommand(pipe[consumed:], args[:0])
				if err != nil {
					panic(err)
				}
				consumed += n
			}
		}},
		{"append simple OK", func() {
			reply = sqlo1.AppendSimple(reply[:0], "OK")
		}},
		{"append integer", func() {
			reply = sqlo1.AppendInt(reply[:0], 1234567)
		}},
		{"append bulk 16B", func() {
			reply = sqlo1.AppendBulk(reply[:0], val)
		}},
		{"append bulk 4KiB", func() {
			reply = sqlo1.AppendBulk(reply[:0], bigVal)
		}},
		{"append null bulk", func() {
			reply = sqlo1.AppendNullBulk(reply[:0])
		}},
		{"GET round trip (parse + bulk reply)", func() {
			args = mustParse(getCmd, args[:0])
			reply = sqlo1.AppendBulk(reply[:0], val)
		}},
		{"SET round trip (parse + OK reply)", func() {
			args = mustParse(setCmd, args[:0])
			reply = sqlo1.AppendSimple(reply[:0], "OK")
		}},
	}
}

// hotProbes builds the gated hot-tier point ops: hits, in-place and
// cross-class overwrites, and delete-reinsert cycles over a warm table,
// where every structure (header slots, arena classes, map cells) is
// recycled rather than reallocated.
func hotProbes() []probe {
	ht := sqlo1.NewHotTable(1024)
	val16 := bytes.Repeat([]byte("v"), 16)
	val150 := bytes.Repeat([]byte("w"), 150)
	keys := make([][]byte, 512)
	for i := range keys {
		keys[i] = fmt.Appendf(nil, "hot-key-%04d", i)
		if !ht.Put(keys[i], val16, sqlo1.TagString) {
			panic("warm put refused")
		}
	}
	grow := false

	return []probe{
		{"hot GET hit", func() {
			v, ok := ht.Get(keys[7])
			if !ok || len(v) != 16 {
				panic("hot get missed")
			}
		}},
		{"hot SET overwrite in place", func() {
			if !ht.Put(keys[8], val16, sqlo1.TagString) {
				panic("overwrite refused")
			}
		}},
		{"hot SET realloc across classes", func() {
			v := val16
			if grow {
				v = val150
			}
			grow = !grow
			if !ht.Put(keys[9], v, sqlo1.TagString) {
				panic("realloc put refused")
			}
		}},
		{"hot DEL + reinsert", func() {
			if !ht.Del(keys[10]) {
				panic("del missed")
			}
			if !ht.Put(keys[10], val16, sqlo1.TagString) {
				panic("reinsert refused")
			}
		}},
	}
}

func main() {
	fmt.Println("alloczero: allocs/op on the gated hot paths (the gate is 0 on every row)")
	fmt.Println("| op | allocs/op |")
	fmt.Println("|---|---|")
	for _, p := range append(wireProbes(), hotProbes()...) {
		fmt.Printf("| %s | %.0f |\n", p.name, testing.AllocsPerRun(2000, p.f))
	}
}
