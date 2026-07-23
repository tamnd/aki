// fieldttl prices the inline hash field-TTL column (doc 08 section 3)
// before the hashes slice bakes its chunk encoding. Three candidate
// encodings carry the HEXPIRE expiry inline: a whole 8 B column added
// to every element of any chunk that holds at least one bearer, a
// 1 B flag byte on every element with 8 B on bearers, and a presence
// bitmap at the head of contaminated chunks with 8 B on bearers. The
// sweep moves the fraction of fields carrying a TTL from zero to all
// and measures bytes per element over the plain packing, chunk
// contamination, and the point-read bill, which doc 08 says never
// changes. The burden arm expires a fraction of an all-TTL hash and
// measures the lazy rule's price: dead bytes ride until rewrite, live
// and expired probes both cost exactly one GET, and a rewrite reclaims
// the dead share.
//
// Chunk frames are the real store codec on the counting sim; element
// packing, directory, and TTL encodings are lab-local models,
// disclosed in the README.
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
	chunkHdr      = 16
	nowMs         = int64(1_700_000_000_000)
	pastMs        = nowMs - 1_000
	futureMs      = nowMs + 1_000_000_000

	// Lab-local chunk flag bits for the TTL encodings; the real codec's
	// run bit (store.ChunkFlagRun, 0x80) stays clear of them.
	flagColumn = 0x01
	flagBitmap = 0x02
)

type encoding int

const (
	encColumn encoding = iota // 8 B on every element of a contaminated chunk
	encFlag                   // 1 B on every element, 8 B more on bearers
	encBitmap                 // ceil(count/8) B per contaminated chunk, 8 B on bearers
)

var encName = map[encoding]string{encColumn: "column", encFlag: "flag", encBitmap: "bitmap"}

func fp(b []byte) uint64 {
	h := fnv.New64a()
	h.Write(b)
	return h.Sum64()
}

// salted derives a deterministic per-element draw in [0, 1000) so TTL
// assignment and expiry are reproducible without a random source.
func salted(name []byte, salt byte) int {
	h := fnv.New64a()
	h.Write(name)
	h.Write([]byte{salt})
	return int(h.Sum64() % 1000)
}

type chunkEnt struct {
	first uint64
	off   int64
	ln    int
}

type corpus struct {
	obj     []byte
	dir     []chunkEnt
	n       int
	enc     encoding
	contam  int // chunks carrying the TTL column or bitmap
	bearers int
	elem    func(i int) []byte
}

type discIdx struct {
	disc uint64
	idx  int
}

// buildTTL packs a hash whose fields at draw(name, saltBearer) <
// fPermille carry an inline expiry, expired ones at draw(name,
// saltExpired) < ePermille. keep filters fields out entirely, which is
// how the rewrite arm rebuilds the live set. fPermille 0 must produce
// bytes identical to the plain packing for the column and bitmap
// encodings; that identity is the pay-only-if-used check.
func buildTTL(n int, value []byte, chunkTarget int, enc encoding, fPermille, ePermille int, keep func(i int) bool) corpus {
	name := func(i int) []byte { return fmt.Appendf(nil, "m:%09d", i) }
	tab := make([]discIdx, 0, n)
	for i := range n {
		if keep != nil && !keep(i) {
			continue
		}
		tab = append(tab, discIdx{disc: fp(name(i)), idx: i})
	}
	sort.Slice(tab, func(i, j int) bool { return tab[i].disc < tab[j].disc })
	c := corpus{n: n, enc: enc, elem: name}
	colKey := []byte("c")
	var elems []byte
	var bits []byte
	var first uint64
	count, chunkBearers := 0, 0
	flush := func() {
		if count == 0 {
			return
		}
		var disc [8]byte
		binary.LittleEndian.PutUint64(disc[:], first)
		var flags byte
		payload := elems
		if chunkBearers > 0 {
			switch enc {
			case encColumn:
				flags = flagColumn
			case encBitmap:
				flags = flagBitmap
				payload = append(bits[:(count+7)/8:(count+7)/8], elems...)
			}
		}
		if flags != 0 {
			c.contam++
		}
		frameLen := chunkHdr + len(colKey) + 8 + len(payload)
		if rem := blockBytes - int(int64(len(c.obj))%blockBytes); frameLen > rem {
			c.obj = append(c.obj, make([]byte, rem)...)
		}
		off := int64(len(c.obj))
		c.obj = store.AppendRunChunk(c.obj, 0x0B|store.ChunkKindBit, flags, uint16(count), colKey, disc[:], payload)
		c.dir = append(c.dir, chunkEnt{first: first, off: off, ln: int(int64(len(c.obj)) - off)})
		elems, bits = elems[:0], bits[:0]
		count, chunkBearers = 0, 0
	}
	// pack appends one element for the chunk being built. The column
	// encoding cannot know at element time whether the chunk ends up
	// contaminated, so it always writes the expiry slot and flush
	// strips nothing: instead the builder packs the chunk plain and
	// repacks with the column when the first bearer arrives.
	pack := func(field []byte, exp int64, bearer bool) {
		if enc == encColumn && bearer && chunkBearers == 0 && count > 0 {
			elems = repackWithColumn(elems, count)
		}
		var h [12]byte
		binary.LittleEndian.PutUint64(h[0:], fp(field))
		binary.LittleEndian.PutUint16(h[8:], uint16(len(field)))
		binary.LittleEndian.PutUint16(h[10:], uint16(len(value)))
		elems = append(elems, h[:]...)
		switch {
		case enc == encColumn && (bearer || chunkBearers > 0):
			var e [8]byte
			binary.LittleEndian.PutUint64(e[0:], uint64(exp))
			elems = append(elems, e[:]...)
		case enc == encFlag:
			if bearer {
				elems = append(elems, 1)
				var e [8]byte
				binary.LittleEndian.PutUint64(e[0:], uint64(exp))
				elems = append(elems, e[:]...)
			} else {
				elems = append(elems, 0)
			}
		case enc == encBitmap && bearer:
			var e [8]byte
			binary.LittleEndian.PutUint64(e[0:], uint64(exp))
			elems = append(elems, e[:]...)
		}
		elems = append(elems, field...)
		elems = append(elems, value...)
		if bearer {
			chunkBearers++
			c.bearers++
		}
	}
	for _, e := range tab {
		if count == 0 {
			first = e.disc
		}
		field := name(e.idx)
		bearer := fPermille > 0 && salted(field, 0xB1) < fPermille
		exp := int64(0)
		if bearer {
			exp = futureMs
			if ePermille > 0 && salted(field, 0xE2) < ePermille {
				exp = pastMs
			}
		}
		if enc == encBitmap {
			for len(bits) <= count/8 {
				bits = append(bits, 0)
			}
			if bearer {
				bits[count/8] |= 1 << (count % 8)
			}
		}
		pack(field, exp, bearer)
		count++
		if len(elems) >= chunkTarget {
			flush()
		}
	}
	flush()
	return c
}

// repackWithColumn rewrites a plain-packed element run with the 8 B
// expiry slot (zero, meaning none) inserted, for the column encoding's
// first-bearer-in-chunk transition.
func repackWithColumn(elems []byte, count int) []byte {
	out := make([]byte, 0, len(elems)+8*count)
	for range count {
		fl := int(binary.LittleEndian.Uint16(elems[8:]))
		vl := int(binary.LittleEndian.Uint16(elems[10:]))
		out = append(out, elems[:12]...)
		out = append(out, make([]byte, 8)...)
		out = append(out, elems[12:12+fl+vl]...)
		elems = elems[12+fl+vl:]
	}
	return out
}

func findChunk(dir []chunkEnt, disc uint64) chunkEnt {
	i := sort.Search(len(dir), func(i int) bool { return dir[i].first > disc })
	if i == 0 {
		return dir[0]
	}
	return dir[i-1]
}

// pointGet fetches the one block covering the field's chunk and walks
// it under the encoding's layout, applying the doc 05 lazy rule: an
// inline expiry at or below now answers absent, at the price of the
// same single GET a live hit pays.
func pointGet(ctx context.Context, s *sim.Sim, key string, c corpus, field []byte) (bool, error) {
	d := fp(field)
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
	flags := f[5]
	klen := int(binary.LittleEndian.Uint16(f[6:]))
	count := int(binary.LittleEndian.Uint16(f[12:]))
	dlen := int(binary.LittleEndian.Uint16(f[14:]))
	payload := f[chunkHdr+klen+dlen : total]
	var bits []byte
	if flags&flagBitmap != 0 {
		bits = payload[:(count+7)/8]
		payload = payload[(count+7)/8:]
	}
	for i := range count {
		pd := binary.LittleEndian.Uint64(payload[0:])
		fl := int(binary.LittleEndian.Uint16(payload[8:]))
		vl := int(binary.LittleEndian.Uint16(payload[10:]))
		payload = payload[12:]
		exp := int64(0)
		switch {
		case flags&flagColumn != 0:
			exp = int64(binary.LittleEndian.Uint64(payload))
			payload = payload[8:]
		case c.enc == encFlag:
			if payload[0] != 0 {
				exp = int64(binary.LittleEndian.Uint64(payload[1:]))
				payload = payload[9:]
			} else {
				payload = payload[1:]
			}
		case bits != nil && bits[i/8]&(1<<(i%8)) != 0:
			exp = int64(binary.LittleEndian.Uint64(payload))
			payload = payload[8:]
		}
		if pd == d && string(payload[:fl]) == string(field) {
			if exp != 0 && exp <= nowMs {
				return false, nil
			}
			return true, nil
		}
		payload = payload[fl+vl:]
	}
	return false, nil
}

func scan(ctx context.Context, s *sim.Sim, key string, size int64) (int, error) {
	reqs := 0
	for off := int64(0); off < size; off += coalesceBytes {
		nb := int64(coalesceBytes)
		if off+nb > size {
			nb = size - off
		}
		if _, _, err := s.GetRange(ctx, key, off, nb); err != nil {
			return 0, err
		}
		reqs++
	}
	return reqs, nil
}

func probe(ctx context.Context, s *sim.Sim, key string, c corpus, sample []int) (gets float64, foundPct float64, err error) {
	before := s.Usage()
	found := 0
	for _, i := range sample {
		ok, err := pointGet(ctx, s, key, c, c.elem(i))
		if err != nil {
			return 0, 0, err
		}
		if ok {
			found++
		}
	}
	after := s.Usage()
	return float64(after.GetRequests-before.GetRequests) / float64(len(sample)),
		100 * float64(found) / float64(len(sample)), nil
}

func main() {
	n := flag.Int("n", 1_000_000, "hash cardinality")
	ops := flag.Int("ops", 10000, "point ops per cell")
	quick := flag.Bool("quick", false, "smoke sizes")
	flag.Parse()
	if *quick {
		*n, *ops = 100_000, 1000
	}
	ctx := context.Background()
	value := make([]byte, 64)
	for i := range value {
		value[i] = byte(i*37 + 11)
	}
	const chunkTarget = 16 << 10

	base := buildTTL(*n, value, chunkTarget, encBitmap, 0, 0, nil)
	baseLen := len(base.obj)

	fmt.Println("row,enc,f_permille,obj_mib,ttl_b_per_elem,contam_pct,bearer_pct,gets_per_op,found_pct")
	sample := make([]int, *ops)
	for i := range sample {
		sample[i] = (i * 7919) % *n
	}
	for _, enc := range []encoding{encColumn, encFlag, encBitmap} {
		for _, f := range []int{0, 1, 10, 100, 500, 1000} {
			s := sim.New(sim.Config{})
			c := buildTTL(*n, value, chunkTarget, enc, f, 0, nil)
			if _, err := s.Put(ctx, "seg/h", c.obj); err != nil {
				die(err)
			}
			gets, foundPct, err := probe(ctx, s, "seg/h", c, sample)
			if err != nil {
				die(err)
			}
			fmt.Printf("enc,%s,%d,%.1f,%.3f,%.1f,%.1f,%.4f,%.2f\n",
				encName[enc], f, float64(len(c.obj))/(1<<20),
				float64(len(c.obj)-baseLen)/float64(*n),
				100*float64(c.contam)/float64(len(c.dir)),
				100*float64(c.bearers)/float64(*n), gets, foundPct)
		}
	}

	// Burden arm: all fields carry a TTL (bitmap encoding, the sweep's
	// winner shape), a fraction expire, and the corpus rides unrewritten
	// while live and expired probes pay the same single GET. The rewrite
	// rebuilds only live fields and reclaims the dead share.
	fmt.Println("row,e_permille,obj_mib,dead_pct,live_gets,live_found,exp_gets,exp_found,rw_obj_mib,reclaimed_pct,scan_before,scan_after,scan_ceil_after")
	for _, e := range []int{250, 500, 900} {
		s := sim.New(sim.Config{})
		c := buildTTL(*n, value, chunkTarget, encBitmap, 1000, e, nil)
		if _, err := s.Put(ctx, "seg/h", c.obj); err != nil {
			die(err)
		}
		expired := func(i int) bool { return salted(c.elem(i), 0xE2) < e }
		var live, dead []int
		expiredCount := 0
		for i := range *n {
			if expired(i) {
				expiredCount++
				if len(dead) < *ops {
					dead = append(dead, i)
				}
			} else if len(live) < *ops {
				live = append(live, i)
			}
		}
		liveGets, liveFound, err := probe(ctx, s, "seg/h", c, live)
		if err != nil {
			die(err)
		}
		expGets, expFound, err := probe(ctx, s, "seg/h", c, dead)
		if err != nil {
			die(err)
		}
		scanBefore, err := scan(ctx, s, "seg/h", int64(len(c.obj)))
		if err != nil {
			die(err)
		}
		rw := buildTTL(*n, value, chunkTarget, encBitmap, 1000, e, func(i int) bool { return !expired(i) })
		if _, err := s.Put(ctx, "seg/h2", rw.obj); err != nil {
			die(err)
		}
		scanAfter, err := scan(ctx, s, "seg/h2", int64(len(rw.obj)))
		if err != nil {
			die(err)
		}
		ceilAfter := (int64(len(rw.obj)) + coalesceBytes - 1) / coalesceBytes
		deadPct := 100 * float64(expiredCount) / float64(*n)
		reclaimedPct := 100 * (1 - float64(len(rw.obj))/float64(len(c.obj)))
		fmt.Printf("burden,%d,%.1f,%.1f,%.4f,%.2f,%.4f,%.2f,%.1f,%.1f,%d,%d,%d\n",
			e, float64(len(c.obj))/(1<<20), deadPct, liveGets, liveFound,
			expGets, expFound, float64(len(rw.obj))/(1<<20), reclaimedPct,
			scanBefore, scanAfter, ceilAfter)
	}
}

func die(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
