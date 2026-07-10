package store

import (
	"fmt"
	"math/rand/v2"
	"path/filepath"
	"testing"
)

// walkLedger is the ledger recomputed from first principles: a full walk over
// every live index entry, charging each record, run, and chunk directory to
// its arena segment, with the log figures and the band census rebuilt on the
// side. The counters in the store are incremental deltas at every alloc,
// kill, move, and resize site; this walk is the ground truth they must sum
// to, and the invariant test replays a hostile op mix and checks the two
// after every step.
type walkLedger struct {
	keys    uint64
	segLive []int64
	chunked uint64
	logLive uint64
	logRuns uint64
	bands   [4]uint64
}

func (s *Store) recomputeLedger() walkLedger {
	w := walkLedger{segLive: make([]int64, len(s.arena.segs))}
	charge := func(off, n uint64) {
		si, ok := s.arena.segOf(off)
		if !ok {
			panic(fmt.Sprintf("walk: offset %d outside the arena tiling", off))
		}
		w.segLive[si] += int64(n)
	}
	seen := make([]bool, len(s.idx.segs))
	visit := func(b *bucket) {
		for i := 0; i < slotsPerBucket; i++ {
			word := b.slots[i]
			if word == 0 {
				continue
			}
			addr := word & addrMask
			w.keys++
			charge(addr, s.recBytes(addr))
			f := s.recFlags(addr)
			w.bands[bandIdx(f)]++
			switch {
			case f&flagChunked != 0:
				w.chunked += s.vlen(addr)
				dw, n, dcap := s.readPtr(s.valueStart(addr))
				dirOff := dw & runAddrMask
				charge(dirOff, uint64(dcap)*ptrSize)
				for k := uint32(0); k < n; k++ {
					cw, clen, cc := s.readPtr(dirOff + uint64(k)*ptrSize)
					if cw&inLogBit != 0 {
						w.logRuns++
						w.logLive += uint64(clen)
						continue
					}
					charge(cw&runAddrMask, uint64(cc))
				}
			case f&flagSep != 0:
				rw, vlen, vcap := s.readPtr(s.valueStart(addr))
				if rw&inLogBit != 0 {
					w.logRuns++
					w.logLive += uint64(vlen)
					continue
				}
				charge(rw&runAddrMask, uint64(vcap))
			}
		}
	}
	for _, ord := range s.idx.dir {
		if seen[ord] {
			continue
		}
		seen[ord] = true
		seg := s.idx.segs[ord]
		for bi := range seg.buckets {
			visit(&seg.buckets[bi])
		}
		for bi := range seg.overflow {
			visit(&seg.overflow[bi])
		}
	}
	return w
}

// checkLedger asserts the store's incremental counters equal the walk.
func checkLedger(t *testing.T, s *Store, step string) {
	t.Helper()
	w := s.recomputeLedger()
	if uint64(s.count) != w.keys {
		t.Fatalf("%s: count = %d, walk found %d live entries", step, s.count, w.keys)
	}
	for si := range s.arena.segs {
		if got := s.arena.segs[si].live; got != w.segLive[si] {
			t.Fatalf("%s: segment %d live = %d, walk charged %d", step, si, got, w.segLive[si])
		}
	}
	if s.chunkBytes != w.chunked {
		t.Fatalf("%s: chunkBytes = %d, walk charged %d", step, s.chunkBytes, w.chunked)
	}
	if s.logRuns != w.logRuns {
		t.Fatalf("%s: logRuns = %d, walk found %d", step, s.logRuns, w.logRuns)
	}
	if s.vlog != nil {
		if live := s.vlog.tail - s.vlog.dead; live != w.logLive {
			t.Fatalf("%s: log live = %d (tail %d dead %d), walk found %d",
				step, live, s.vlog.tail, s.vlog.dead, w.logLive)
		}
	}
	for i, got := range s.bands {
		if got != w.bands[i] {
			t.Fatalf("%s: band %d census = %d, walk found %d", step, i, got, w.bands[i])
		}
	}
}

// TestLedgerInvariantOpMix replays a seeded random op mix over every band and
// every mutation path (SET new, overwrite in place, overwrite grow and
// shrink, DEL, APPEND, SETRANGE, INCR, TTL reaps, FLUSHALL, arena and log
// compaction) and after each step recomputes the ledger from a full walk. Any
// alloc, kill, move, or resize site that misses its ledger delta fails here
// with the step that broke the balance.
func TestLedgerInvariantOpMix(t *testing.T) {
	s, err := Open(Options{
		ArenaBytes:       64 << 20,
		SegBytes:         1 << 20,
		VlogPath:         filepath.Join(t.TempDir(), "vlog"),
		ResidentCapBytes: 24 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	rng := rand.New(rand.NewPCG(42, 2064))
	const nKeys = 256
	key := func(i int) []byte { return fmt.Appendf(nil, "key:%03d", i) }
	// Value sizes spanning the bands: int cell, embedded, separated, chunked.
	sizes := []int{0, 5, 60, 900, 2 << 10, 6 << 10, 70 << 10, 200 << 10}
	val := func(n int) []byte {
		if n == 0 {
			return fmt.Appendf(nil, "%d", rng.Int64N(1<<40))
		}
		b := make([]byte, n)
		for i := range b {
			b[i] = 'a' + byte(rng.IntN(26))
		}
		return b
	}

	now := int64(1_000_000)
	const steps = 4000
	for i := 0; i < steps; i++ {
		k := key(rng.IntN(nKeys))
		var step string
		switch op := rng.IntN(100); {
		case op < 45:
			v := val(sizes[rng.IntN(len(sizes))])
			at := int64(0)
			if rng.IntN(4) == 0 {
				at = now + int64(rng.IntN(20)) // some land already expired next touch
			}
			step = fmt.Sprintf("step %d: SET %s (%d bytes, at %d)", i, k, len(v), at)
			if err := s.SetString(k, v, now, at, false); err != nil {
				t.Fatalf("%s: %v", step, err)
			}
		case op < 60:
			step = fmt.Sprintf("step %d: DEL %s", i, k)
			s.Del(k, now)
		case op < 75:
			v := val(sizes[rng.IntN(len(sizes)-1)+1])
			step = fmt.Sprintf("step %d: APPEND %s (%d bytes)", i, k, len(v))
			if _, err := s.Append(k, v, now); err != nil {
				t.Fatalf("%s: %v", step, err)
			}
		case op < 85:
			v := val(sizes[rng.IntN(4)+1])
			off := rng.IntN(96 << 10)
			step = fmt.Sprintf("step %d: SETRANGE %s @%d (%d bytes)", i, k, off, len(v))
			if _, err := s.SetRange(k, off, v, now); err != nil {
				t.Fatalf("%s: %v", step, err)
			}
		case op < 92:
			step = fmt.Sprintf("step %d: INCR %s", i, k)
			if _, err := s.IncrBy(k, int64(rng.IntN(1000)), now); err != nil && err != ErrNotInt {
				t.Fatalf("%s: %v", step, err)
			}
		case op < 96:
			step = fmt.Sprintf("step %d: CompactArena", i)
			s.CompactArena()
		case op < 99:
			step = fmt.Sprintf("step %d: CompactLog", i)
			if err := s.CompactLog(); err != nil {
				t.Fatalf("%s: %v", step, err)
			}
		default:
			step = fmt.Sprintf("step %d: FLUSHALL", i)
			s.Reset()
		}
		now += int64(rng.IntN(10))
		checkLedger(t, s, step)
	}
}

// TestLedgerInvariantTightArena replays the same balance check against a
// store one segment from full, so the mid-command reclaim backstop, the
// tightness-widened compactor, and every ErrFull unwind path get exercised.
// A missed credit on an error unwind shows up here as a segment whose live
// counter disagrees with the walk.
func TestLedgerInvariantTightArena(t *testing.T) {
	s := New(4<<20, 256<<10)
	rng := rand.New(rand.NewPCG(9, 2064))
	key := func(i int) []byte { return fmt.Appendf(nil, "k%02d", i) }
	sizes := []int{0, 40, 700, 3 << 10, 70 << 10}
	val := func(n int) []byte {
		if n == 0 {
			return fmt.Appendf(nil, "%d", rng.Int64N(1<<40))
		}
		b := make([]byte, n)
		for i := range b {
			b[i] = 'a' + byte(rng.IntN(26))
		}
		return b
	}
	now := int64(1_000_000)
	for i := 0; i < 3000; i++ {
		k := key(rng.IntN(48))
		var step string
		var err error
		switch op := rng.IntN(100); {
		case op < 55:
			v := val(sizes[rng.IntN(len(sizes))])
			step = fmt.Sprintf("step %d: SET %s (%d bytes)", i, k, len(v))
			err = s.SetString(k, v, now, 0, false)
		case op < 70:
			step = fmt.Sprintf("step %d: DEL %s", i, k)
			s.Del(k, now)
		case op < 85:
			v := val(sizes[rng.IntN(len(sizes)-1)+1])
			step = fmt.Sprintf("step %d: APPEND %s (%d bytes)", i, k, len(v))
			_, err = s.Append(k, v, now)
		case op < 95:
			v := val(sizes[rng.IntN(3)+1])
			off := rng.IntN(80 << 10)
			step = fmt.Sprintf("step %d: SETRANGE %s @%d (%d bytes)", i, k, off, len(v))
			_, err = s.SetRange(k, off, v, now)
		default:
			step = fmt.Sprintf("step %d: CompactArena", i)
			s.CompactArena()
		}
		// ErrFull is this configuration's point: the unwind must balance too.
		if err != nil && err != ErrFull {
			t.Fatalf("%s: %v", step, err)
		}
		checkLedger(t, s, step)
	}
}
