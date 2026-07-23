// typepoint scores the doc 08 per-type ledger cells for the O2a types:
// cold GET, HGET, SISMEMBER at one GET of one block each, HGETALL as a
// coalesced scan, the definitive miss at zero requests, and the
// directory's resident share per element across the 4 to 32 KiB chunk
// band. Scores PRED-OBS1-O2A-TYPEPOINT before the chunk-codec slice
// bakes any of it.
//
// The chunk and record frames are the real store codec
// (AppendRunChunk, AppendRecordFrame) and every read is a real ranged
// GET against the counting sim, so the request and byte columns are
// measured, not derived. The element packing inside a collection
// chunk's payload is lab-local (the hash and set slices bake the real
// one), and the keymap and directory are RAM maps carrying the
// as-built entry sizes, 16 B and 24 B.
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
	coldHdr       = 12
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

// elem is one packed element: disc plus the lab-local payload bytes.
type elem struct {
	disc uint64
	blob []byte
}

// packHash lays out (disc u64, flen u16, vlen u16, field, value).
func packHash(field, value []byte) []byte {
	b := make([]byte, 12, 12+len(field)+len(value))
	binary.LittleEndian.PutUint64(b[0:], fp(field))
	binary.LittleEndian.PutUint16(b[8:], uint16(len(field)))
	binary.LittleEndian.PutUint16(b[10:], uint16(len(value)))
	b = append(b, field...)
	return append(b, value...)
}

// packSet lays out (disc u64, mlen u16, member): a valueless hash.
func packSet(member []byte) []byte {
	b := make([]byte, 10, 10+len(member))
	binary.LittleEndian.PutUint64(b[0:], fp(member))
	binary.LittleEndian.PutUint16(b[8:], uint16(len(member)))
	return append(b, member...)
}

// build packs sorted elements into chunk frames under one collection
// key, cutting at chunkTarget payload bytes and padding so no chunk
// spans a block, and returns the object plus its directory.
func build(colKey []byte, kind byte, elems []elem, chunkTarget int) ([]byte, []chunkEnt) {
	sort.Slice(elems, func(i, j int) bool { return elems[i].disc < elems[j].disc })
	var obj []byte
	var dir []chunkEnt
	var payload []byte
	var first uint64
	count := 0
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
	for _, e := range elems {
		if count == 0 {
			first = e.disc
		}
		payload = append(payload, e.blob...)
		count++
		if len(payload) >= chunkTarget {
			flush()
		}
	}
	flush()
	return obj, dir
}

// findChunk binary-searches the directory for the chunk that could
// hold disc, the doc 03 first-discriminator rule.
func findChunk(dir []chunkEnt, disc uint64) chunkEnt {
	i := sort.Search(len(dir), func(i int) bool { return dir[i].first > disc })
	if i == 0 {
		return dir[0]
	}
	return dir[i-1]
}

// fetchBlock is the point-read fetch unit: the whole 128 KiB block
// holding the chunk, clipped at the object end.
func fetchBlock(ctx context.Context, s *sim.Sim, key string, size int64, ce chunkEnt) ([]byte, int64, error) {
	blk := ce.off / blockBytes * blockBytes
	n := int64(blockBytes)
	if blk+n > size {
		n = size - blk
	}
	b, _, err := s.GetRange(ctx, key, blk, n)
	return b, ce.off - blk, err
}

// chunkPayload parses the real chunk frame at off inside a fetched
// block and returns its packed payload.
func chunkPayload(blk []byte, off int64) ([]byte, error) {
	f := blk[off:]
	if len(f) < chunkHdr {
		return nil, fmt.Errorf("torn chunk header")
	}
	total := int(binary.LittleEndian.Uint32(f[0:]))
	klen := int(binary.LittleEndian.Uint16(f[6:]))
	plen := int(binary.LittleEndian.Uint32(f[8:]))
	dlen := int(binary.LittleEndian.Uint16(f[14:]))
	if total > len(f) || chunkHdr+klen+dlen+plen != total {
		return nil, fmt.Errorf("chunk frame arithmetic off")
	}
	return f[chunkHdr+klen+dlen : total], nil
}

func hashLookup(payload []byte, disc uint64, field []byte) bool {
	for len(payload) >= 12 {
		d := binary.LittleEndian.Uint64(payload[0:])
		fl := int(binary.LittleEndian.Uint16(payload[8:]))
		vl := int(binary.LittleEndian.Uint16(payload[10:]))
		if d == disc && string(payload[12:12+fl]) == string(field) {
			return true
		}
		payload = payload[12+fl+vl:]
	}
	return false
}

func setLookup(payload []byte, disc uint64, member []byte) bool {
	for len(payload) >= 10 {
		d := binary.LittleEndian.Uint64(payload[0:])
		ml := int(binary.LittleEndian.Uint16(payload[8:]))
		if d == disc && string(payload[10:10+ml]) == string(member) {
			return true
		}
		payload = payload[10+ml:]
	}
	return false
}

// strLookup walks the run chunk's concatenated record frames.
func strLookup(payload []byte, key []byte) bool {
	for len(payload) >= coldHdr {
		total := int(binary.LittleEndian.Uint32(payload[0:]))
		klen := int(binary.LittleEndian.Uint16(payload[6:]))
		if string(payload[coldHdr:coldHdr+klen]) == string(key) {
			return true
		}
		payload = payload[total:]
	}
	return false
}

type cell struct {
	name     string
	ops      int
	getsPer  float64
	kibPer   float64
	foundPct float64
}

func measure(name string, s *sim.Sim, ops int, op func(i int) (bool, error)) (cell, error) {
	before := s.Usage()
	found := 0
	for i := range ops {
		ok, err := op(i)
		if err != nil {
			return cell{}, fmt.Errorf("%s op %d: %w", name, i, err)
		}
		if ok {
			found++
		}
	}
	after := s.Usage()
	return cell{
		name: name, ops: ops,
		getsPer:  float64(after.GetRequests-before.GetRequests) / float64(ops),
		kibPer:   float64(after.BytesDown-before.BytesDown) / float64(ops) / 1024,
		foundPct: 100 * float64(found) / float64(ops),
	}, nil
}

func main() {
	n := flag.Int("n", 1<<20, "elements per collection and string keys")
	ops := flag.Int("ops", 20000, "point ops per cell")
	chunkKiB := flag.Int("chunk", 16, "chunk target KiB for the point cells")
	quick := flag.Bool("quick", false, "smoke sizes")
	flag.Parse()
	if *quick {
		*n, *ops = 1<<16, 2000
	}
	ctx := context.Background()
	s := sim.New(sim.Config{})
	ct := *chunkKiB << 10

	value := make([]byte, 64)
	for i := range value {
		value[i] = byte(i*37 + 11)
	}

	// Hash: N fields under one collection key, field-hash discriminator.
	fields := make([][]byte, *n)
	helems := make([]elem, *n)
	selems := make([]elem, *n)
	strs := make([][]byte, *n)
	strElems := make([]elem, *n)
	for i := range *n {
		f := fmt.Appendf(nil, "field:%09d", i)
		fields[i] = f
		helems[i] = elem{disc: fp(f), blob: packHash(f, value)}
		selems[i] = elem{disc: fp(f), blob: packSet(f)}
		k := fmt.Appendf(nil, "str:%09d", i)
		strs[i] = k
		strElems[i] = elem{disc: fp(k), blob: store.AppendRecordFrame(nil, 0x01, 0, uint32(len(value)), k, value)}
	}
	hobj, hdir := build([]byte("h"), 0x0B, helems, ct)
	sobj, sdir := build([]byte("s"), 0x0C, selems, ct)
	tobj, tdir := build(nil, 0x01, strElems, ct)
	for k, o := range map[string][]byte{"seg/h": hobj, "seg/s": sobj, "seg/t": tobj} {
		if _, err := s.Put(ctx, k, o); err != nil {
			die(err)
		}
	}
	// The string keymap: fingerprint to chunk, the regime A locate.
	skm := make(map[uint64]chunkEnt, *n)
	for _, ce := range tdir {
		skm[ce.first] = ce
	}
	for i, ce := range tdir {
		last := len(strElems)
		if i+1 < len(tdir) {
			last = firstIndex(strElems, tdir[i+1].first)
		}
		for _, e := range strElems[firstIndex(strElems, ce.first):last] {
			skm[e.disc] = ce
		}
	}

	fmt.Println("cell,ops,gets_per_op,kib_per_op,found_pct")
	cells := []struct {
		name string
		op   func(i int) (bool, error)
	}{
		{"string_get", func(i int) (bool, error) {
			key := strs[(i*7919)%*n]
			blk, off, err := fetchBlock(ctx, s, "seg/t", int64(len(tobj)), skm[fp(key)])
			if err != nil {
				return false, err
			}
			p, err := chunkPayload(blk, off)
			if err != nil {
				return false, err
			}
			return strLookup(p, key), nil
		}},
		{"hash_hget", func(i int) (bool, error) {
			f := fields[(i*7919)%*n]
			blk, off, err := fetchBlock(ctx, s, "seg/h", int64(len(hobj)), findChunk(hdir, fp(f)))
			if err != nil {
				return false, err
			}
			p, err := chunkPayload(blk, off)
			if err != nil {
				return false, err
			}
			return hashLookup(p, fp(f), f), nil
		}},
		{"set_sismember", func(i int) (bool, error) {
			m := fields[(i*7919)%*n]
			blk, off, err := fetchBlock(ctx, s, "seg/s", int64(len(sobj)), findChunk(sdir, fp(m)))
			if err != nil {
				return false, err
			}
			p, err := chunkPayload(blk, off)
			if err != nil {
				return false, err
			}
			return setLookup(p, fp(m), m), nil
		}},
		{"definitive_miss", func(i int) (bool, error) {
			// Regime A: the keymap answers absent with zero requests.
			_, hit := skm[fp(fmt.Appendf(nil, "absent:%09d", i))]
			return !hit, nil
		}},
	}
	for _, c := range cells {
		r, err := measure(c.name, s, *ops, c.op)
		if err != nil {
			die(err)
		}
		fmt.Printf("%s,%d,%.4f,%.1f,%.2f\n", r.name, r.ops, r.getsPer, r.kibPer, r.foundPct)
	}

	// HGETALL: the whole chunk sequence as one coalesced scan.
	before := s.Usage()
	var walked int
	for off := int64(0); off < int64(len(hobj)); off += coalesceBytes {
		nb := int64(coalesceBytes)
		if off+nb > int64(len(hobj)) {
			nb = int64(len(hobj)) - off
		}
		if _, _, err := s.GetRange(ctx, "seg/h", off, nb); err != nil {
			die(err)
		}
	}
	for _, ce := range hdir {
		walked += ce.count
	}
	after := s.Usage()
	reqs := after.GetRequests - before.GetRequests
	want := (int64(len(hobj)) + coalesceBytes - 1) / coalesceBytes
	if walked != *n {
		die(fmt.Errorf("hgetall walked %d of %d elements", walked, *n))
	}
	fmt.Printf("hash_hgetall_elems=%d,reqs=%d,want_ceil=%d,mib_down=%.1f\n",
		walked, reqs, want, float64(after.BytesDown-before.BytesDown)/(1<<20))

	// Directory resident share across the chunk band, hash elements.
	fmt.Println("dirshare_chunk_kib,chunks,dir_b_per_elem")
	for _, kib := range []int{4, 8, 16, 32} {
		_, d := build([]byte("h"), 0x0B, helems, kib<<10)
		fmt.Printf("%d,%d,%.3f\n", kib, len(d), float64(dirEntBytes*len(d))/float64(*n))
	}
}

func firstIndex(elems []elem, disc uint64) int {
	return sort.Search(len(elems), func(i int) bool { return elems[i].disc >= disc })
}

func die(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
