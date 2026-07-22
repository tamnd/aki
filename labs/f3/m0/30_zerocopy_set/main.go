// Lab m0/30: the SET value-copy ceiling for the zero-copy net->store arc.
//
// The reactor writes a SET value twice: the loop goroutine copies the value out
// of the socket read buffer into the batch span table (b.data) so it can free
// the socket buffer immediately (copy 1, engine/f3/shard/batch.go), then the
// shard worker copies b.data into the arena run the record owns (copy 2,
// engine/f3/store/str.go / bands.go). redis writes the value once, out of its
// per-connection query buffer into the value's sds. Copy 1 is the decouple tax:
// it exists only so the reader does not wait on the worker.
//
// The zero-copy design gives each in-flight batch its own read buffer (per-conn
// buffer rotation), so a value span references the handed-off socket buffer and
// the worker copies once, net->arena. This lab isolates the memcpy ceiling that
// removes: how large is copy 1 next to the rest of the per-op value handling,
// as a function of value size, and does the per-conn buffer handoff cost stay
// small next to the saved copy. It does NOT model the reactor's cross-batch
// pipelining, which only a box A/B can settle; it bounds the upside so the
// engine refactor is only attempted where the ceiling is real.
package main

import (
	"fmt"
	"os"
	"text/tabwriter"
)

// The embedded band cap in the engine (engine/f3/store/bands.go strInlineMax).
// Values at or below it land in the arena inline; the gate's 1 KiB cell sits on
// this boundary.
const inlineMax = 1024

// twoCopy models the reactor path: stage the whole command (key, flags, value)
// into a reused batch buffer, then copy the value out of that staging into the
// destination arena. Both copies run; staging is reused across ops like b.data.
type twoCopy struct {
	stage []byte // reused batch span table (b.data)
	arena []byte // reused destination run (s.arena.buf)
}

func (t *twoCopy) set(src []byte, keyLen, valOff, valLen int) {
	// copy 1: the loop goroutine stages the command bytes so it can release the
	// socket buffer. b.data appends every argument; here we stage key + value.
	t.stage = t.stage[:0]
	t.stage = append(t.stage, src[:keyLen]...)
	t.stage = append(t.stage, src[valOff:valOff+valLen]...)
	// copy 2: the worker copies the staged value into the arena run.
	sv := t.stage[keyLen : keyLen+valLen]
	if cap(t.arena) < valLen {
		t.arena = make([]byte, valLen)
	}
	copy(t.arena[:valLen], sv)
}

// oneCopy models the zero-copy path: the key (small, control) is still staged,
// but the value is referenced in the handed-off read buffer and copied once,
// straight into the arena. A per-conn buffer handoff (take/put a buffer from a
// small pool) replaces copy 1 for the value.
type oneCopy struct {
	stage []byte   // reused batch buffer, key/control args only
	arena []byte   // reused destination run
	pool  [][]byte // per-conn read-buffer pool the handoff draws from
}

func (o *oneCopy) set(src []byte, keyLen, valOff, valLen int) {
	// The control args (key, flags) are small and still staged; the value is
	// not. This is the copy the arc removes.
	o.stage = o.stage[:0]
	o.stage = append(o.stage, src[:keyLen]...)
	// Buffer handoff: the filled read buffer moves to the batch and a fresh one
	// is taken for the next read. Modelled as a pool put/take; no value copy.
	o.handoff(src)
	// copy (the only one): the worker copies the referenced value into the arena.
	if cap(o.arena) < valLen {
		o.arena = make([]byte, valLen)
	}
	copy(o.arena[:valLen], src[valOff:valOff+valLen])
}

func (o *oneCopy) handoff(buf []byte) {
	// take a fresh buffer for the next read, return the handed-off one. The pool
	// is tiny (bounded in-flight batches per conn); this is the whole added cost.
	if len(o.pool) > 0 {
		o.pool = o.pool[:len(o.pool)-1]
	}
	o.pool = append(o.pool, buf)
	if len(o.pool) > 4 {
		o.pool = o.pool[:4]
	}
}

// synthCmd builds a SET command image in a socket-buffer-like slice: a 16-byte
// key then a value of valLen filled with a non-integer pattern (so ParseInt
// bails on byte 0 in the real path, matching redis-benchmark's fill).
func synthCmd(valLen int) (src []byte, keyLen, valOff int) {
	keyLen = 16
	src = make([]byte, keyLen+valLen)
	for i := range keyLen {
		src[i] = byte('k')
	}
	for i := range valLen {
		src[keyLen+i] = byte('x')
	}
	return src, keyLen, keyLen
}

func main() {
	sizes := []int{64, 256, 512, 1024, 4096, 16384}
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "value\tband\tcopy1 bytes/op saved")
	for _, n := range sizes {
		band := "embedded"
		if n > inlineMax {
			band = "separated"
		}
		fmt.Fprintf(w, "%d\t%s\t%d\n", n, band, n)
	}
	w.Flush()
	fmt.Println("\nRun `go test -bench . -benchmem` for the per-op ns and alloc delta.")
}
