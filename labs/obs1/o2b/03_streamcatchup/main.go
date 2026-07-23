// streamcatchup prices the doc 08 section 7 stream model before the
// stream slice lands: consumer catch-up from ID 0 over a cold stream of
// dense ID-range runs, the XREAD batch planning knee (readahead
// coalescing vs per-batch block fetches), the XRANGE window bill, and
// the million-entry PEL fold and reclaim.
//
// The chunk frames are the real store codec on the counting sim; the
// entry packing, directory, and range planning are lab-local models
// disclosed in the README, the same stance as the zsetdual lab. The
// stream slice replaces the models with the landed planes and the O2b
// ledger prediction re-measures these cells there.
package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"os"

	"github.com/tamnd/aki/engine/obs1"
	"github.com/tamnd/aki/engine/obs1/sim"
	"github.com/tamnd/aki/engine/obs1/store"
)

const (
	kindStream    = 0x05 // engine/obs1/stream as-built
	blockBytes    = 128 << 10
	coalesceBytes = 16 << 20
	chunkHdr      = 16
	flagsPEL      = 0x01 // lab-local sub-kind flag, the hexpire pattern
	baseMS        = 1_700_000_000_000
)

// idDisc packs a stream ID into the 16-byte big-endian discriminator the
// doc 08 section 7 layout sorts by: ms then seq, lexicographic order.
func idDisc(ms, seq uint64) [16]byte {
	var d [16]byte
	binary.BigEndian.PutUint64(d[0:], ms)
	binary.BigEndian.PutUint64(d[8:], seq)
	return d
}

// entryID derives entry i's ID: four entries per millisecond, append order.
func entryID(i int) (ms, seq uint64) {
	return baseMS + uint64(i)/4, uint64(i) % 4
}

func entryVal(i int) []byte {
	return fmt.Appendf(nil, "sv-%09d-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", i)
}

type chunkEnt struct {
	off      int64
	ln       int
	count    int
	cumStart int // corpus index of the chunk's first entry
}

type projection struct {
	obj     []byte
	dir     []chunkEnt
	payload int // corpus payload bytes before framing
}

// build packs n items into block-aligned run chunks in append (ID) order:
// item i contributes its 16-byte disc and a variable payload piece, the
// dense immutable runs of doc 08 section 7.
func build(n int, flags byte, piece func(i int) []byte) projection {
	var p projection
	var payload []byte
	first := 0
	count := 0
	colKey := []byte("x")
	flush := func() {
		if count == 0 {
			return
		}
		ms, seq := entryID(first)
		d := idDisc(ms, seq)
		frameLen := chunkHdr + len(colKey) + 16 + len(payload)
		if rem := blockBytes - int(int64(len(p.obj))%blockBytes); frameLen > rem {
			p.obj = append(p.obj, make([]byte, rem)...)
		}
		off := int64(len(p.obj))
		p.obj = store.AppendRunChunk(p.obj, kindStream|store.ChunkKindBit, flags, uint16(count), colKey, d[:], payload)
		p.dir = append(p.dir, chunkEnt{off: off, ln: int(int64(len(p.obj)) - off), count: count, cumStart: first})
		payload, count = payload[:0], 0
	}
	for i := range n {
		if count == 0 {
			first = i
		}
		ms, seq := entryID(i)
		var h [18]byte
		binary.LittleEndian.PutUint64(h[0:], ms)
		binary.LittleEndian.PutUint64(h[8:], seq)
		pc := piece(i)
		binary.LittleEndian.PutUint16(h[16:], uint16(len(pc)))
		payload = append(payload, h[:]...)
		payload = append(payload, pc...)
		p.payload += 18 + len(pc)
		count++
		if len(payload) >= obs1.ChunkTargetDefault {
			flush()
		}
	}
	flush()
	return p
}

type entry struct {
	ms, seq uint64
	piece   []byte
}

// decodeLocal decodes chunk ci from the already-transferred object bytes,
// request-free, standing in for the buffer the client just paid for.
func decodeLocal(p projection, ci int) []entry {
	ce := p.dir[ci]
	f := p.obj[ce.off:]
	total := int(binary.LittleEndian.Uint32(f[0:]))
	klen := int(binary.LittleEndian.Uint16(f[6:]))
	payload := f[chunkHdr+klen+16 : total]
	out := make([]entry, 0, ce.count)
	for len(payload) >= 18 {
		ms := binary.LittleEndian.Uint64(payload[0:])
		seq := binary.LittleEndian.Uint64(payload[8:])
		pl := int(binary.LittleEndian.Uint16(payload[16:]))
		out = append(out, entry{ms: ms, seq: seq, piece: payload[18 : 18+pl]})
		payload = payload[18+pl:]
	}
	return out
}

// chunkOf finds the chunk holding corpus index i over the resident
// per-run counts, the directory math XREAD plans from.
func chunkOf(p projection, i int) int {
	lo, hi := 0, len(p.dir)-1
	for lo < hi {
		mid := (lo + hi + 1) / 2
		if p.dir[mid].cumStart <= i {
			lo = mid
		} else {
			hi = mid - 1
		}
	}
	return lo
}

// fetchSpan transfers [start, end) of the object in coalesced range GETs.
func fetchSpan(ctx context.Context, s *sim.Sim, key string, start, end int64) {
	for off := start; off < end; off += coalesceBytes {
		nb := int64(coalesceBytes)
		if off+nb > end {
			nb = end - off
		}
		if _, _, err := s.GetRange(ctx, key, off, nb); err != nil {
			die("get range: %v", err)
		}
	}
}

// verifyRange checks entries [from, to) against the corpus: exact IDs in
// order and byte-exact values.
func verifyRange(p projection, from, to int) {
	ci := chunkOf(p, from)
	ents := decodeLocal(p, ci)
	for i := from; i < to; i++ {
		if i >= p.dir[ci].cumStart+p.dir[ci].count {
			ci++
			ents = decodeLocal(p, ci)
		}
		e := ents[i-p.dir[ci].cumStart]
		ms, seq := entryID(i)
		if e.ms != ms || e.seq != seq || string(e.piece) != string(entryVal(i)) {
			die("entry %d decoded (%d-%d, %q)", i, e.ms, e.seq, e.piece)
		}
	}
}

// catchupReadahead is the doc 05 plan: XREAD from 0 walks the dense cold
// span in coalesced 16 MiB GETs regardless of the COUNT batching, the
// bandwidth-bound arm.
func catchupReadahead(ctx context.Context, s *sim.Sim, key string, p projection, n int) {
	fetchSpan(ctx, s, key, 0, int64(len(p.obj)))
	verifyRange(p, 0, n)
}

// catchupPerBlock is the counter-arm: no readahead and no cross-batch
// cache (the block cache is a deferred doc 05 follow-up), so every XREAD
// COUNT batch fetches exactly the block-aligned span covering its own
// entries and adjacent batches re-pay shared boundary blocks.
func catchupPerBlock(ctx context.Context, s *sim.Sim, key string, p projection, count, limit int) {
	for ei := 0; ei < limit; {
		be := min(ei+count, limit)
		ci, cj := chunkOf(p, ei), chunkOf(p, be-1)
		start := p.dir[ci].off / blockBytes * blockBytes
		endOff := p.dir[cj].off + int64(p.dir[cj].ln)
		end := min((endOff+blockBytes-1)/blockBytes*blockBytes, int64(len(p.obj)))
		fetchSpan(ctx, s, key, start, end)
		verifyRange(p, ei, be)
		ei = be
	}
}

// xrange plans a k-entry window from corpus index `from`: the covering
// chunk span transfers in coalesced GETs, the ledger's ceil row.
func xrange(ctx context.Context, s *sim.Sim, key string, p projection, from, k int) {
	ci, cj := chunkOf(p, from), chunkOf(p, from+k-1)
	fetchSpan(ctx, s, key, p.dir[ci].off, p.dir[cj].off+int64(p.dir[cj].ln))
	verifyRange(p, from, from+k)
}

type labErr struct{ err error }

func die(format string, args ...any) { panic(labErr{fmt.Errorf(format, args...)}) }

type cell struct {
	name  string
	gets  int64
	mib   float64
	extra string
}

type results struct {
	n     int
	amp   float64
	cells []cell
}

func run(n, kneeSpan int) (res results, rerr error) {
	defer func() {
		if r := recover(); r != nil {
			le, ok := r.(labErr)
			if !ok {
				panic(r)
			}
			rerr = le.err
		}
	}()
	ctx := context.Background()
	s := sim.New(sim.Config{})
	p := build(n, 0, entryVal)
	if _, err := s.Put(ctx, "seg/st", p.obj); err != nil {
		die("put: %v", err)
	}
	res.n = n
	res.amp = float64(len(p.obj)) / float64(p.payload)

	score := func(name string, extra string, f func()) {
		before := s.Usage()
		f()
		after := s.Usage()
		res.cells = append(res.cells, cell{
			name:  name,
			gets:  after.GetRequests - before.GetRequests,
			mib:   float64(after.BytesDown-before.BytesDown) / (1 << 20),
			extra: extra,
		})
	}

	want := int64((int64(len(p.obj)) + coalesceBytes - 1) / coalesceBytes)
	score("catchup_readahead", fmt.Sprintf("expect %d", want), func() {
		catchupReadahead(ctx, s, "seg/st", p, n)
	})
	span := min(kneeSpan, n)
	for _, c := range []int{10, 100, 1000, 8000} {
		score(fmt.Sprintf("perblock_c%d", c), fmt.Sprintf("span %d", span), func() {
			catchupPerBlock(ctx, s, "seg/st", p, c, span)
		})
	}
	for _, k := range []int{100, 10_000} {
		if k > n {
			continue
		}
		score(fmt.Sprintf("xrange_k%d", k), "", func() {
			xrange(ctx, s, "seg/st", p, n/3, k)
		})
	}
	return res, nil
}

// pelRun folds a PEL of n entries as its own chunk kind under the stream
// key (disc = entry ID, payload = consumer, delivery count, last delivery
// ms), then reclaims it: every entry acked, so the fold drops every chunk
// whole by manifest, the doc 06 free case, and nothing survives.
func pelRun(n, streamBytes int) (bPerEntry, ratio float64, dropped, residual int, rerr error) {
	defer func() {
		if r := recover(); r != nil {
			le, ok := r.(labErr)
			if !ok {
				panic(r)
			}
			rerr = le.err
		}
	}()
	pel := build(n, flagsPEL, func(i int) []byte {
		var b [14]byte
		binary.LittleEndian.PutUint16(b[0:], uint16(i%64)) // consumer
		binary.LittleEndian.PutUint32(b[2:], 1)            // deliveries
		binary.LittleEndian.PutUint64(b[6:], baseMS+uint64(i)/4)
		return b[:]
	})
	bPerEntry = float64(len(pel.obj)) / float64(n)
	ratio = float64(len(pel.obj)) / float64(streamBytes)
	// Reclaim: all n acked. A chunk whose whole ID range is acked drops by
	// manifest without a read; entries in surviving chunks would rewrite.
	acked := n
	for _, ce := range pel.dir {
		if ce.cumStart+ce.count <= acked {
			dropped++
		} else {
			residual += ce.count - max(0, acked-ce.cumStart)
		}
	}
	return bPerEntry, ratio, dropped, residual, nil
}

func main() {
	quick := flag.Bool("quick", false, "smoke sizes")
	flag.Parse()
	decades := []int{100_000, 1_000_000, 10_000_000}
	kneeSpan, pelN := 100_000, 1_000_000
	if *quick {
		decades, kneeSpan, pelN = []int{50_000}, 20_000, 50_000
	}
	fmt.Println("n,cell,gets,mib_down,extra")
	streamBytes := 0
	for _, n := range decades {
		res, err := run(n, kneeSpan)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		for _, c := range res.cells {
			fmt.Printf("%d,%s,%d,%.2f,%s\n", n, c.name, c.gets, c.mib, c.extra)
		}
		fmt.Printf("%d,amp_ratio,,%.4f,\n", n, res.amp)
		streamBytes = int(float64(n) * 70 * res.amp)
	}
	bpe, ratio, dropped, residual, err := pelRun(pelN, streamBytes)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("%d,pel_b_per_entry,,%.2f,\n", pelN, bpe)
	fmt.Printf("%d,pel_vs_stream_ratio,,%.4f,\n", pelN, ratio)
	fmt.Printf("%d,pel_reclaim,%d,,residual %d\n", pelN, dropped, residual)
}
