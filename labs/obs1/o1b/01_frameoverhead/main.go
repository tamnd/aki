// Lab: frame overhead, the K1 number (spec 2064/obs1 doc 04 section 12,
// doc 11 milestone O1b lab 01).
//
// The question: what does encoding the op frame into the group WAL
// buffer cost on the owner's hot path, against the doc 04 budget of
// under 100ns for small ops, and what tax does that project onto the
// hot gate the 2x inheritance rides on?
//
// Method: in-process, no server, no wire. The substrate is the ported
// store, byte-identical to f3's by the O1a port contract, so the
// apply-only arm IS the f3 baseline on shared bytes and the index
// tier-tag branch (also byte-identical) taxes zero by construction;
// the one genuinely added cost is the frame append, and that is what
// the frame arm adds. Both arms run the same SetString loop over a
// uniform keyspace; the frame arm additionally encodes a strset frame
// (doc 03 section 4 layout byte for byte: flen, kind, flags, slot,
// seq, klen, key, then the doc 04 strset payload of value bytes,
// expiry, flags) into a preallocated per-node buffer that resets at
// the 8 MiB flush-size default, the flusher's swap made cheap. The
// slot rides in from dispatch for free in the real path, so the arm
// charges a mask, not a CRC16.
//
// Arms alternate per batch so box drift cancels, and the statistic is
// the median of per-batch ns/op, robust to the loaded-box outliers the
// paritysmoke lab documented. One CSV row per value size.
package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"sort"
	"time"

	"github.com/tamnd/aki/engine/obs1/store"
)

const (
	keys      = 1 << 17 // 128k keys, comfortably resident
	batchOps  = 4096
	flushSize = 8 << 20 // the doc 04 default the buffer resets at
)

// appendFrame is the hot-path candidate slice 1 bakes if the number
// holds: one op frame in the doc 03 section 4 byte layout, payload
// already built into the same append. Returns the buffer so growth
// stays amortized across the run.
func appendFrame(b []byte, kind, flags uint8, slot uint16, seq uint64, key, val []byte, expiry int64) []byte {
	// strset payload: value bytes, expiry ms, ladder flags (doc 04 s2).
	flen := 4 + 1 + 1 + 2 + 8 + 2 + len(key) + len(val) + 8 + 1
	b = binary.LittleEndian.AppendUint32(b, uint32(flen))
	b = append(b, kind, flags)
	b = binary.LittleEndian.AppendUint16(b, slot)
	b = binary.LittleEndian.AppendUint64(b, seq)
	b = binary.LittleEndian.AppendUint16(b, uint16(len(key)))
	b = append(b, key...)
	b = append(b, val...)
	b = binary.LittleEndian.AppendUint64(b, uint64(expiry))
	b = append(b, 0)
	return b
}

type xorshift uint64

func (x *xorshift) next() uint64 {
	v := *x
	v ^= v << 13
	v ^= v >> 7
	v ^= v << 17
	*x = v
	return uint64(v)
}

// arm runs batches of SetString, framed or not, and returns per-batch
// ns/op samples.
func arm(st *store.Store, val []byte, batches int, framed bool) []float64 {
	rng := xorshift(0x9E3779B97F4A7C15)
	key := make([]byte, 12)
	copy(key, "k:")
	buf := make([]byte, 0, flushSize+1<<16)
	var seq uint64
	samples := make([]float64, 0, batches)
	for b := 0; b < batches; b++ {
		start := time.Now()
		for i := 0; i < batchOps; i++ {
			n := rng.next() & (keys - 1)
			binary.LittleEndian.PutUint64(key[2:10], n)
			if err := st.Set(key, val); err != nil {
				panic(err)
			}
			if framed {
				seq++
				buf = appendFrame(buf, 0x01, 0, uint16(n&0x3FFF), seq, key, val, 0)
				if len(buf) >= flushSize {
					buf = buf[:0]
				}
			}
		}
		el := time.Since(start)
		samples = append(samples, float64(el.Nanoseconds())/batchOps)
	}
	return samples
}

func median(xs []float64) float64 {
	s := append([]float64(nil), xs...)
	sort.Float64s(s)
	if len(s)%2 == 1 {
		return s[len(s)/2]
	}
	return (s[len(s)/2-1] + s[len(s)/2]) / 2
}

func main() {
	batches := flag.Int("batches", 256, "measured batches per arm per size")
	warm := flag.Int("warm", 32, "warmup batches per arm per size, unmeasured")
	quick := flag.Bool("quick", false, "tiny counts")
	flag.Parse()
	if *quick {
		*batches, *warm = 8, 2
	}

	fmt.Println("val_bytes,base_ns_op,frame_ns_op,delta_ns,delta_pct")
	for _, vs := range []int{16, 64, 256, 1024} {
		val := make([]byte, vs)
		for i := range val {
			val[i] = byte('a' + i%26)
		}
		// One store per size, big enough that demotion never engages
		// and both arms see the identical overwrite-in-place regime.
		st := store.New(1<<30, 8<<20)
		arm(st, val, *warm, false)
		arm(st, val, *warm, true)
		var base, framed []float64
		// Alternate per round, and alternate which arm leads the round,
		// so neither drift nor a warmer-store second slot favors an arm.
		for r, round := 0, 0; r < *batches; r, round = r+16, round+1 {
			n := 16
			if *batches-r < n {
				n = *batches - r
			}
			if round%2 == 0 {
				base = append(base, arm(st, val, n, false)...)
				framed = append(framed, arm(st, val, n, true)...)
			} else {
				framed = append(framed, arm(st, val, n, true)...)
				base = append(base, arm(st, val, n, false)...)
			}
		}
		bm, fm := median(base), median(framed)
		fmt.Printf("%d,%.1f,%.1f,%.1f,%.2f%%\n", vs, bm, fm, fm-bm, (fm-bm)/bm*100)
	}
}
