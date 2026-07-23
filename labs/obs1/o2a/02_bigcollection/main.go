// bigcollection sweeps hash and set cardinality across five decades
// and the chunk target across the doc 08 4-32 KiB band, and checks the
// claims that make big collections safe to fold: cold field access is
// flat (one GET of one block at any cardinality), the directory's
// resident share per element depends on chunk size and never on
// cardinality, scans stay on the ceil(bytes / 16 MiB) identity, and
// set algebra streams at one block per operand of peak extra RAM.
// Scores PRED-OBS1-O2A-BIGCOLL before the chunk-codec slice bakes the
// chunk target.
//
// The chunk frames are the real store codec on the counting sim; the
// element packing, directory, and keymap are the same disclosed lab
// models as the typepoint lab. The hash sweep tops out at 10^7
// elements locally for RAM; the 10^8 decade rides the set corpus,
// which doc 08 defines as a valueless hash with identical planning.
package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"hash/fnv"
	"os"
	"sort"

	"github.com/tamnd/aki/engine/obs1/sim"
	"github.com/tamnd/aki/engine/obs1/store"
)

const (
	blockBytes    = 128 << 10 // #1085
	coalesceBytes = 16 << 20  // doc 05 section 3
	dirEntBytes   = 24        // #1269 as-built
	chunkHdr      = 16
)

func fp(b []byte) uint64 {
	h := fnv.New64a()
	h.Write(b)
	return h.Sum64()
}

type chunkEnt struct {
	first uint64
	count int
	off   int64
	ln    int
}

// corpus builds one collection object chunk by chunk without holding
// per-element blobs: elements are generated from a sorted (disc, index)
// table, so a 10^8 set costs the table plus the object, nothing else.
type corpus struct {
	obj  []byte
	dir  []chunkEnt
	n    int
	elem func(i int) []byte // element bytes by original index
}

type discIdx struct {
	disc uint64
	idx  int
}

// pack appends one element's payload bytes for the lab-local layouts:
// hash (disc, flen u16, vlen u16, field, value), set (disc, mlen u16,
// member).
func pack(dst []byte, disc uint64, field, value []byte, hasValue bool) []byte {
	var h [12]byte
	binary.LittleEndian.PutUint64(h[0:], disc)
	binary.LittleEndian.PutUint16(h[8:], uint16(len(field)))
	if hasValue {
		binary.LittleEndian.PutUint16(h[10:], uint16(len(value)))
		dst = append(dst, h[:12]...)
	} else {
		dst = append(dst, h[:10]...)
	}
	dst = append(dst, field...)
	if hasValue {
		dst = append(dst, value...)
	}
	return dst
}

func buildCorpus(n int, value []byte, hasValue bool, chunkTarget int, kind byte) corpus {
	name := func(i int) []byte { return fmt.Appendf(nil, "m:%09d", i) }
	tab := make([]discIdx, n)
	for i := range n {
		tab[i] = discIdx{disc: fp(name(i)), idx: i}
	}
	sort.Slice(tab, func(i, j int) bool { return tab[i].disc < tab[j].disc })
	var obj []byte
	var dir []chunkEnt
	var payload []byte
	var first uint64
	count := 0
	colKey := []byte("c")
	flush := func() {
		if count == 0 {
			return
		}
		var disc [8]byte
		binary.LittleEndian.PutUint64(disc[:], first)
		frameLen := chunkHdr + len(colKey) + 8 + len(payload)
		if rem := blockBytes - int(int64(len(obj))%blockBytes); frameLen > rem {
			obj = append(obj, make([]byte, rem)...)
		}
		off := int64(len(obj))
		obj = store.AppendRunChunk(obj, kind|store.ChunkKindBit, 0, uint16(count), colKey, disc[:], payload)
		dir = append(dir, chunkEnt{first: first, count: count, off: off, ln: int(int64(len(obj)) - off)})
		payload, count = payload[:0], 0
	}
	for _, e := range tab {
		if count == 0 {
			first = e.disc
		}
		payload = pack(payload, e.disc, name(e.idx), value, hasValue)
		count++
		if len(payload) >= chunkTarget {
			flush()
		}
	}
	flush()
	return corpus{obj: obj, dir: dir, n: n, elem: name}
}

func findChunk(dir []chunkEnt, disc uint64) chunkEnt {
	i := sort.Search(len(dir), func(i int) bool { return dir[i].first > disc })
	if i == 0 {
		return dir[0]
	}
	return dir[i-1]
}

// pointGet fetches the one block covering the element's chunk and
// confirms the element is inside it.
func pointGet(ctx context.Context, s *sim.Sim, key string, c corpus, member []byte, hasValue bool) (bool, error) {
	d := fp(member)
	ce := findChunk(c.dir, d)
	blk := ce.off / blockBytes * blockBytes
	nb := int64(blockBytes)
	if blk+nb > int64(len(c.obj)) {
		nb = int64(len(c.obj)) - blk
	}
	b, _, err := s.GetRange(ctx, key, blk, nb)
	if err != nil {
		return false, err
	}
	f := b[ce.off-blk:]
	total := int(binary.LittleEndian.Uint32(f[0:]))
	klen := int(binary.LittleEndian.Uint16(f[6:]))
	dlen := int(binary.LittleEndian.Uint16(f[14:]))
	payload := f[chunkHdr+klen+dlen : total]
	hdr := 10
	if hasValue {
		hdr = 12
	}
	for len(payload) >= hdr {
		pd := binary.LittleEndian.Uint64(payload[0:])
		ml := int(binary.LittleEndian.Uint16(payload[8:]))
		vl := 0
		if hasValue {
			vl = int(binary.LittleEndian.Uint16(payload[10:]))
		}
		if pd == d && string(payload[hdr:hdr+ml]) == string(member) {
			return true, nil
		}
		payload = payload[hdr+ml+vl:]
	}
	return false, nil
}

// intersect streams two set objects through a disc-ordered merge. The
// transport unit is the doc 05 coalesced range (that is what the GET
// bill counts) but the merge decodes and holds ONE BLOCK per operand
// at a time, which is the doc 08 working-set claim; a streaming client
// never holds the whole range. It reports matches, total requests, the
// merge window peak (decoded discs plus the two raw blocks), and the
// transport buffer peak the sim forces (two whole ranges).
func intersect(ctx context.Context, s *sim.Sim, keys [2]string, sizes [2]int64) (matches, reqs int, winPeak, transPeak int64, err error) {
	type cursor struct {
		key    string
		size   int64
		off    int64 // next range offset in the object
		buf    []byte
		bufPos int
		pos    int
		discs  []uint64
	}
	cur := [2]*cursor{{key: keys[0], size: sizes[0]}, {key: keys[1], size: sizes[1]}}
	// nextBlock decodes the discs of the next 128 KiB block, fetching
	// the next coalesced range when the current one is spent. Returns
	// false at end of object.
	nextBlock := func(c *cursor) (bool, error) {
		c.discs = c.discs[:0]
		c.pos = 0
		for len(c.discs) == 0 {
			if c.bufPos >= len(c.buf) {
				if c.off >= c.size {
					return false, nil
				}
				nb := int64(coalesceBytes)
				if c.off+nb > c.size {
					nb = c.size - c.off
				}
				b, _, err := s.GetRange(ctx, c.key, c.off, nb)
				if err != nil {
					return false, err
				}
				reqs++
				c.buf, c.bufPos = b, 0
				c.off += nb
			}
			end := c.bufPos + blockBytes
			if end > len(c.buf) {
				end = len(c.buf)
			}
			blk := c.buf[c.bufPos:end]
			c.bufPos = end
			for f := blk; len(f) >= chunkHdr; {
				total := int(binary.LittleEndian.Uint32(f[0:]))
				if total < chunkHdr || total > len(f) {
					break // zero padding to the block end
				}
				klen := int(binary.LittleEndian.Uint16(f[6:]))
				dlen := int(binary.LittleEndian.Uint16(f[14:]))
				payload := f[chunkHdr+klen+dlen : total]
				for len(payload) >= 10 {
					c.discs = append(c.discs, binary.LittleEndian.Uint64(payload[0:]))
					ml := int(binary.LittleEndian.Uint16(payload[8:]))
					payload = payload[10+ml:]
				}
				f = f[total:]
			}
		}
		return true, nil
	}
	note := func() {
		if w := int64(len(cur[0].discs)+len(cur[1].discs))*8 + 2*blockBytes; w > winPeak {
			winPeak = w
		}
		if t := int64(len(cur[0].buf)) + int64(len(cur[1].buf)); t > transPeak {
			transPeak = t
		}
	}
	for _, c := range cur {
		if _, err := nextBlock(c); err != nil {
			return 0, 0, 0, 0, err
		}
	}
	note()
	for {
		a, b := cur[0], cur[1]
		if a.pos >= len(a.discs) {
			more, err := nextBlock(a)
			if err != nil {
				return 0, 0, 0, 0, err
			}
			if !more {
				break
			}
			note()
			continue
		}
		if b.pos >= len(b.discs) {
			more, err := nextBlock(b)
			if err != nil {
				return 0, 0, 0, 0, err
			}
			if !more {
				break
			}
			note()
			continue
		}
		switch {
		case a.discs[a.pos] < b.discs[b.pos]:
			a.pos++
		case a.discs[a.pos] > b.discs[b.pos]:
			b.pos++
		default:
			matches++
			a.pos++
			b.pos++
		}
	}
	return matches, reqs, winPeak, transPeak, nil
}

func main() {
	ops := flag.Int("ops", 10000, "point ops per cell")
	maxHash := flag.Int("maxhash", 10_000_000, "largest hash cardinality")
	maxSet := flag.Int("maxset", 100_000_000, "largest set cardinality")
	quick := flag.Bool("quick", false, "smoke sizes")
	flag.Parse()
	if *quick {
		*ops, *maxHash, *maxSet = 1000, 100_000, 1_000_000
	}
	ctx := context.Background()
	value := make([]byte, 64)
	for i := range value {
		value[i] = byte(i*37 + 11)
	}

	fmt.Println("kind,n,chunk_kib,obj_mib,chunks,dir_b_per_elem,gets_per_op,kib_per_op,found_pct,scan_reqs,scan_ceil")
	run := func(kindName string, n int, hasValue bool, chunkKiB int) {
		s := sim.New(sim.Config{})
		var val []byte
		if hasValue {
			val = value
		}
		c := buildCorpus(n, val, hasValue, chunkKiB<<10, 0x0B)
		if _, err := s.Put(ctx, "seg/c", c.obj); err != nil {
			die(err)
		}
		before := s.Usage()
		found := 0
		for i := range *ops {
			ok, err := pointGet(ctx, s, "seg/c", c, c.elem((i*7919)%n), hasValue)
			if err != nil {
				die(err)
			}
			if ok {
				found++
			}
		}
		mid := s.Usage()
		scan := 0
		for off := int64(0); off < int64(len(c.obj)); off += coalesceBytes {
			nb := int64(coalesceBytes)
			if off+nb > int64(len(c.obj)) {
				nb = int64(len(c.obj)) - off
			}
			if _, _, err := s.GetRange(ctx, "seg/c", off, nb); err != nil {
				die(err)
			}
			scan++
		}
		ceil := (int64(len(c.obj)) + coalesceBytes - 1) / coalesceBytes
		fmt.Printf("%s,%d,%d,%.1f,%d,%.3f,%.4f,%.1f,%.2f,%d,%d\n",
			kindName, n, chunkKiB, float64(len(c.obj))/(1<<20), len(c.dir),
			float64(dirEntBytes*len(c.dir))/float64(n),
			float64(mid.GetRequests-before.GetRequests)/float64(*ops),
			float64(mid.BytesDown-before.BytesDown)/float64(*ops)/1024,
			100*float64(found)/float64(*ops), scan, ceil)
	}

	// Cardinality sweep at the 16 KiB chunk default: hashes to maxhash,
	// the set carrying the top decade.
	for n := 1000; n <= *maxHash; n *= 10 {
		run("hash", n, true, 16)
	}
	run("set", *maxSet, false, 16)

	// Chunk-size sweep at fixed cardinality.
	nSweep := 1_000_000
	if *quick {
		nSweep = 100_000
	}
	for _, kib := range []int{4, 8, 16, 32} {
		run("hash", nSweep, true, kib)
	}

	// Set algebra: two overlapping sets streamed through a disc-ordered
	// merge, one coalesced range buffer per operand.
	nAlg := 1_000_000
	if *quick {
		nAlg = 100_000
	}
	s := sim.New(sim.Config{})
	a := buildCorpus(nAlg, nil, false, 16<<10, 0x0B)
	b := buildCorpus(nAlg*2, nil, false, 16<<10, 0x0B)
	if _, err := s.Put(ctx, "seg/a", a.obj); err != nil {
		die(err)
	}
	if _, err := s.Put(ctx, "seg/b", b.obj); err != nil {
		die(err)
	}
	matches, reqs, winPeak, transPeak, err := intersect(ctx, s, [2]string{"seg/a", "seg/b"}, [2]int64{int64(len(a.obj)), int64(len(b.obj))})
	if err != nil {
		die(err)
	}
	wantReqs := (int64(len(a.obj))+coalesceBytes-1)/coalesceBytes + (int64(len(b.obj))+coalesceBytes-1)/coalesceBytes
	fmt.Printf("sinter,n_a=%d,n_b=%d,matches=%d,reqs=%d,want_reqs=%d,window_kib=%d,transport_mib=%.1f\n",
		nAlg, nAlg*2, matches, reqs, wantReqs, winPeak/1024, float64(transPeak)/(1<<20))
}

func die(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
