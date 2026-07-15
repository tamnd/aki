package sqlo1b

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"testing"
)

type groupKey struct {
	ext uint64
	grp uint16
}

// memGroups is the counting group store the IO-count assertions
// lean on: every ReadGroup is one simulated 4 KiB read.
type memGroups struct {
	m     map[groupKey][]byte
	reads int
}

func newMemGroups() *memGroups { return &memGroups{m: map[groupKey][]byte{}} }

func (g *memGroups) put(ext uint64, grp uint16, img []byte) { g.m[groupKey{ext, grp}] = img }

func (g *memGroups) ReadGroup(ext uint64, grp uint16) ([]byte, error) {
	g.reads++
	img, ok := g.m[groupKey{ext, grp}]
	if !ok {
		return nil, fmt.Errorf("no group at extent %d group %d", ext, grp)
	}
	return img, nil
}

func encodeStringRecord(t *testing.T, key, value []byte) []byte {
	t.Helper()
	b, err := (&Record{RType: RecString, Key: key, Value: value}).Encode()
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// placeChunk writes a chunk image into a fresh group at the given
// slot and returns its position.
func placeChunk(t *testing.T, mem *memGroups, ext uint64, grp, slot uint16, c *Chunk) Pos {
	t.Helper()
	img, ok := mem.m[groupKey{ext, grp}]
	if !ok {
		img = make([]byte, GroupSize)
		mem.put(ext, grp, img)
	}
	copy(img[int(slot)*ChunkSize:], c.Bytes())
	pos, err := NewPos(ext, grp, slot)
	if err != nil {
		t.Fatal(err)
	}
	return pos
}

// buildSingle lays out one key behind one chunk and returns the
// pieces: the counting store, the resident directory, the paged
// root, and the record position.
func buildSingle(t *testing.T, key, value []byte) (*memGroups, *Directory, *PagedDir, Pos) {
	t.Helper()
	mem := newMemGroups()

	gb := NewGroupBuilder(GroupSize)
	slot, err := gb.Append(encodeStringRecord(t, key, value))
	if err != nil {
		t.Fatal(err)
	}
	mem.put(100, 3, gb.Close())
	vptr, err := NewPos(100, 3, slot)
	if err != nil {
		t.Fatal(err)
	}

	h := KeyHash(key)
	c := newChunk(t, 0, 0)
	if err := c.InsertEntry(Fingerprint(h), metaFor(t, h, 0), uint64(vptr)); err != nil {
		t.Fatal(err)
	}
	chunkPos := placeChunk(t, mem, 1, 5, 2, c)
	dir := NewDirectory(MakeFullPtr(chunkPos, c.Bytes()))

	pages := dir.Pages()
	pagePos, err := NewPos(2, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	mem.put(2, 0, pages[0])
	paged := &PagedDir{Root: []FullPtr{MakeFullPtr(pagePos, pages[0])}, DirLen: 1, Groups: mem}
	return mem, dir, paged, vptr
}

// otherFingerprint finds a key whose fingerprint differs from ref,
// for a guaranteed clean miss.
func otherFingerprint(ref []byte) []byte {
	fp := Fingerprint(KeyHash(ref))
	for i := 0; ; i++ {
		k := fmt.Appendf(nil, "miss-%d", i)
		if Fingerprint(KeyHash(k)) != fp {
			return k
		}
	}
}

// TestReadPathIOCount pins the doc 04 read shape: exactly three
// group reads fully cold (directory page, chunk group, record
// group), exactly two with the directory resident, exactly two for
// a clean cold miss.
func TestReadPathIOCount(t *testing.T) {
	key, value := []byte("alpha"), []byte("v1")
	mem, dir, paged, vptr := buildSingle(t, key, value)
	epoch := PackHashEpoch(0, 0)

	cold := &IndexReader{Dir: paged, Groups: mem}
	mem.reads = 0
	rec, pos, err := cold.Lookup(key, epoch)
	if err != nil || rec == nil {
		t.Fatalf("cold lookup: %v, %v", rec, err)
	}
	if string(rec.Value) != string(value) || pos != vptr {
		t.Fatalf("cold lookup returned %q at %v", rec.Value, pos)
	}
	if mem.reads != 3 {
		t.Fatalf("cold unchained hit cost %d group reads, the doc 04 shape is exactly 3", mem.reads)
	}
	if cold.Stats.ChunkReads != 1 || cold.Stats.RecordReads != 1 || cold.Stats.FalseHits != 0 || cold.Stats.ChainFollows != 0 {
		t.Fatalf("cold stats %+v", cold.Stats)
	}

	warm := &IndexReader{Dir: dir, Groups: mem}
	mem.reads = 0
	if rec, _, err := warm.Lookup(key, epoch); err != nil || rec == nil {
		t.Fatalf("warm lookup: %v, %v", rec, err)
	}
	if mem.reads != 2 {
		t.Fatalf("resident-directory hit cost %d group reads, want exactly 2", mem.reads)
	}

	miss := otherFingerprint(key)
	mem.reads = 0
	rec, _, err = cold.Lookup(miss, epoch)
	if err != nil || rec != nil {
		t.Fatalf("miss lookup: %v, %v", rec, err)
	}
	if mem.reads != 2 {
		t.Fatalf("cold clean miss cost %d group reads, want exactly 2 (no record read)", mem.reads)
	}
	if cold.Stats.RecordReads != 1 {
		t.Fatalf("clean miss resolved a record: %+v", cold.Stats)
	}
}

// TestReadPathIOCountChained forces a chain: the base chunk holds 41
// non-matching entries plus the chain pointer, the overflow chunk
// holds the key. Exactly four group reads cold.
func TestReadPathIOCountChained(t *testing.T) {
	key, value := []byte("omega"), []byte("v2")
	mem := newMemGroups()

	gb := NewGroupBuilder(GroupSize)
	slot, err := gb.Append(encodeStringRecord(t, key, value))
	if err != nil {
		t.Fatal(err)
	}
	mem.put(100, 3, gb.Close())
	vptr, err := NewPos(100, 3, slot)
	if err != nil {
		t.Fatal(err)
	}

	h := KeyHash(key)
	fp := Fingerprint(h)
	base := newChunk(t, 0, 0)
	for i := range 41 {
		decoy := fp + 1 + uint16(i) // never equal to fp
		if err := base.InsertEntry(decoy, metaFor(t, uint64(decoy)<<48, 0), uint64(9000+i)); err != nil {
			t.Fatal(err)
		}
	}
	over := newChunk(t, 0, 0)
	if err := over.InsertEntry(fp, metaFor(t, h, 0), uint64(vptr)); err != nil {
		t.Fatal(err)
	}
	overPos := placeChunk(t, mem, 1, 6, 0, over)
	if err := base.SetChain(overPos, ChunkCheck32(over.Bytes())); err != nil {
		t.Fatal(err)
	}
	basePos := placeChunk(t, mem, 1, 7, 0, base)
	dir := NewDirectory(MakeFullPtr(basePos, base.Bytes()))

	pages := dir.Pages()
	pagePos, err := NewPos(2, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	mem.put(2, 0, pages[0])
	paged := &PagedDir{Root: []FullPtr{MakeFullPtr(pagePos, pages[0])}, DirLen: 1, Groups: mem}

	r := &IndexReader{Dir: paged, Groups: mem}
	mem.reads = 0
	rec, pos, err := r.Lookup(key, PackHashEpoch(0, 0))
	if err != nil || rec == nil {
		t.Fatalf("chained lookup: %v, %v", rec, err)
	}
	if string(rec.Value) != string(value) || pos != vptr {
		t.Fatalf("chained lookup returned %q at %v", rec.Value, pos)
	}
	if mem.reads != 4 {
		t.Fatalf("forced-chain cold hit cost %d group reads, want exactly 4", mem.reads)
	}
	if r.Stats.ChainFollows != 1 || r.Stats.ChunkReads != 2 || r.Stats.FalseHits != 0 {
		t.Fatalf("chained stats %+v", r.Stats)
	}
}

// TestReadPathFalseHit pins slot-order candidate resolution: a decoy
// entry sharing the fingerprint sits in an earlier slot, costs one
// wasted record read, and the probe still lands.
func TestReadPathFalseHit(t *testing.T) {
	key, value := []byte("real"), []byte("v3")
	mem := newMemGroups()

	gb := NewGroupBuilder(GroupSize)
	decoySlot, err := gb.Append(encodeStringRecord(t, []byte("decoy"), []byte("x")))
	if err != nil {
		t.Fatal(err)
	}
	realSlot, err := gb.Append(encodeStringRecord(t, key, value))
	if err != nil {
		t.Fatal(err)
	}
	mem.put(100, 3, gb.Close())
	decoyPtr, _ := NewPos(100, 3, decoySlot)
	realPtr, _ := NewPos(100, 3, realSlot)

	h := KeyHash(key)
	fp := Fingerprint(h)
	c := newChunk(t, 0, 0)
	if err := c.InsertEntry(fp, metaFor(t, h, 0), uint64(decoyPtr)); err != nil {
		t.Fatal(err)
	}
	if err := c.InsertEntry(fp, metaFor(t, h, 0), uint64(realPtr)); err != nil {
		t.Fatal(err)
	}
	chunkPos := placeChunk(t, mem, 1, 5, 0, c)
	dir := NewDirectory(MakeFullPtr(chunkPos, c.Bytes()))

	r := &IndexReader{Dir: dir, Groups: mem}
	rec, pos, err := r.Lookup(key, PackHashEpoch(0, 0))
	if err != nil || rec == nil {
		t.Fatalf("lookup: %v, %v", rec, err)
	}
	if pos != realPtr || string(rec.Value) != string(value) {
		t.Fatalf("lookup landed on %v %q", pos, rec.Value)
	}
	if r.Stats.FalseHits != 1 || r.Stats.RecordReads != 2 {
		t.Fatalf("stats %+v, want 1 false hit over 2 record reads", r.Stats)
	}
}

func TestReadPathCorruptionRejects(t *testing.T) {
	key, value := []byte("alpha"), []byte("v1")
	epoch := PackHashEpoch(0, 0)

	// A flipped byte inside the chunk image fails the directory
	// pointer's checksum.
	mem, dir, _, _ := buildSingle(t, key, value)
	mem.m[groupKey{1, 5}][2*ChunkSize+9] ^= 1
	r := &IndexReader{Dir: dir, Groups: mem}
	if _, _, err := r.Lookup(key, epoch); err == nil {
		t.Error("flipped chunk byte passed the full pointer")
	}

	// A flipped byte in the directory page fails the root pointer.
	mem, _, paged, _ := buildSingle(t, key, value)
	mem.m[groupKey{2, 0}][7] ^= 1
	r = &IndexReader{Dir: paged, Groups: mem}
	if _, _, err := r.Lookup(key, epoch); err == nil {
		t.Error("flipped directory page passed the root pointer")
	}

	// A neighbor chunk's corruption stays outside the check: the
	// pointer covers 512 bytes, not the group.
	mem, dir, _, _ = buildSingle(t, key, value)
	mem.m[groupKey{1, 5}][0] ^= 1 // slot 0, ours is slot 2
	r = &IndexReader{Dir: dir, Groups: mem}
	if _, _, err := r.Lookup(key, epoch); err != nil {
		t.Errorf("neighbor chunk corruption leaked into the check: %v", err)
	}

	// A corrupted overflow chunk fails the chain check32 before
	// parse.
	memC := newMemGroups()
	h := KeyHash(key)
	base := newChunk(t, 0, 0)
	over := newChunk(t, 0, 0)
	if err := over.InsertEntry(Fingerprint(h), metaFor(t, h, 0), 42); err != nil {
		t.Fatal(err)
	}
	overPos := placeChunk(t, memC, 1, 6, 0, over)
	if err := base.SetChain(overPos, ChunkCheck32(over.Bytes())); err != nil {
		t.Fatal(err)
	}
	memC.m[groupKey{1, 6}][20] ^= 1
	basePos := placeChunk(t, memC, 1, 7, 0, base)
	dirC := NewDirectory(MakeFullPtr(basePos, base.Bytes()))
	r = &IndexReader{Dir: dirC, Groups: memC}
	if _, _, err := r.Lookup(key, epoch); err == nil {
		t.Error("corrupted overflow chunk passed the chain check32")
	}
}

// TestReadPathFalseHitRate is the store-level confirmation of the
// chunkindex verdict's sim number (0.066% false hits on present-key
// probes at lf85): a real table built through SplitBucket, real
// records in slotted groups, every key probed once.
func TestReadPathFalseHitRate(t *testing.T) {
	const n = 100_000
	rng := rand.New(rand.NewSource(1))
	m := newMiniTable(t)
	mem := newMemGroups()

	// Records first: n distinct 8-byte keys packed into slotted
	// groups in extent 100.
	type placed struct {
		key  []byte
		vptr Pos
	}
	var all []placed
	gb := NewGroupBuilder(GroupSize)
	grp := uint16(0)
	seen := map[string]bool{}
	for len(all) < n {
		key := make([]byte, 8)
		binary.LittleEndian.PutUint64(key, rng.Uint64())
		if seen[string(key)] {
			continue
		}
		seen[string(key)] = true
		rec := encodeStringRecord(t, key, []byte("v"))
		if !gb.Fits(len(rec)) {
			mem.put(100, grp, gb.Close())
			grp++
			gb = NewGroupBuilder(GroupSize)
		}
		slot, err := gb.Append(rec)
		if err != nil {
			t.Fatal(err)
		}
		vptr, err := NewPos(100, grp, slot)
		if err != nil {
			t.Fatal(err)
		}
		all = append(all, placed{key, vptr})
	}
	mem.put(100, grp, gb.Close())

	for _, p := range all {
		m.insert(p.key, uint64(p.vptr))
	}
	m.verify()

	// Serialize the table: chunks packed eight to a group in extent
	// 1, chains linked back to front so every image is final when
	// its pointer is minted.
	dir := (*Directory)(nil)
	chunkIdx := 0
	images := map[groupKey][]byte{}
	for b, chain := range m.buckets {
		// A bucket that grew past a packBucket-full final chunk leaves
		// a 42-entry chunk mid-chain with no room for the chain
		// pointer. The store's flush rebalances by shifting an entry
		// into the next chunk before linking; do the same here.
		for j := 0; j < len(chain)-1; j++ {
			for chain[j].Count() > ChunkChainCap {
				last := chain[j].Count() - 1
				fp, meta, vptr := chain[j].EntryAt(last)
				if err := chain[j].RemoveEntry(last); err != nil {
					t.Fatal(err)
				}
				if err := chain[j+1].InsertEntry(fp, meta, vptr); err != nil {
					t.Fatal(err)
				}
			}
		}
		positions := make([]Pos, len(chain))
		for i := range chain {
			g, s := uint16(chunkIdx/chunksPerGroup), uint16(chunkIdx%chunksPerGroup)
			pos, err := NewPos(1, g, s)
			if err != nil {
				t.Fatal(err)
			}
			positions[i] = pos
			chunkIdx++
		}
		for i := len(chain) - 1; i > 0; i-- {
			if err := chain[i-1].SetChain(positions[i], ChunkCheck32(chain[i].Bytes())); err != nil {
				t.Fatal(err)
			}
		}
		for i, c := range chain {
			gk := groupKey{1, positions[i].Group()}
			img, ok := images[gk]
			if !ok {
				img = make([]byte, GroupSize)
				images[gk] = img
			}
			copy(img[int(positions[i].Slot())*ChunkSize:], c.Bytes())
		}
		ptr := MakeFullPtr(positions[0], chain[0].Bytes())
		if b == 0 {
			dir = NewDirectory(ptr)
		} else {
			dir.Append(ptr)
		}
	}
	for gk, img := range images {
		mem.put(gk.ext, gk.grp, img)
	}
	if dir.Len() != NumBuckets(m.level, m.split) {
		t.Fatalf("directory holds %d buckets, table has %d", dir.Len(), NumBuckets(m.level, m.split))
	}

	r := &IndexReader{Dir: dir, Groups: mem}
	epoch := PackHashEpoch(m.split, m.level)
	for _, p := range all {
		rec, pos, err := r.Lookup(p.key, epoch)
		if err != nil {
			t.Fatalf("key %x: %v", p.key, err)
		}
		if rec == nil || pos != p.vptr {
			t.Fatalf("key %x resolved to %v, want %v", p.key, pos, p.vptr)
		}
	}
	rate := 100 * float64(r.Stats.FalseHits) / float64(n)
	// The sim said 0.066% at 1e7 lf85; the bound leaves room for
	// scale and fill-phase variance but catches an order-of-magnitude
	// regression. Zero would mean the counter is dead.
	if r.Stats.FalseHits == 0 {
		t.Fatal("no false hits over 100k probes, the counter or the fingerprints are broken")
	}
	if rate > 0.2 {
		t.Fatalf("false-hit rate %.4f%% over %d probes, sim promised about 0.066%%", rate, n)
	}
	t.Logf("present-key probes %d, false hits %d (%.4f%%), chain follows %d, record reads %d",
		n, r.Stats.FalseHits, rate, r.Stats.ChainFollows, r.Stats.RecordReads)
}
