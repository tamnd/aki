package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"strconv"
	"strings"

	"github.com/cespare/xxhash/v2"
	"github.com/tamnd/aki/engine/sqlo1"
	"github.com/tamnd/aki/engine/sqlo1b"
)

// The T2 exit-gate crash rig (spec 2064/sqlo1, milestone T2): the real
// hash ladder over Tiered over sqlo1b, SIGKILLed mid-stream, recovered,
// and held against the count-exactness oracle. The keyset spans all
// three representation rungs at once: inline roots, segmented hashes a
// few fences wide, and paged hashes past the 128-segment boundary, so
// every kill window cuts multi-frame drain batches (root, segments,
// fence pages) in whatever order the write path produced them.
//
// What the suite claims: any recovered image must be a self-consistent
// hash state. HLEN, the iterator's begin(count), and the reachable
// field set must agree exactly on every key (H-I2, rule W3), every
// reachable value must be byte-exact f(key, field, version) for a
// version the seeded stream really assigned (rules W1/W2: no torn or
// invented segment images), and recovery must be repeatable. Losing a
// suffix of undrained writes is legal at this seam; disagreeing with
// itself never is. The clean-shutdown control arm demands the stream's
// exact final state back. Memory-pressure kills (eviction, compaction,
// shed) are the tiered matrix's business, not this one's: budgets here
// are generous so every engine error is a hard failure.

const (
	hashDataFile = "h.aki"
	// hashKeys spans the rungs: 4 inline, 2 segmented, 2 paged.
	hashKeys = 8
	// Band widths, fields per key. Inline stays under both inline
	// caps (128 fields, 2 KiB), segmented crosses the byte cap into
	// a handful of segments, paged crosses the 128-segment fence
	// boundary into rtype 5 pages (~176 segments at these sizes).
	hashInlineFields = 32
	hashSegFields    = 288
	hashPagedFields  = 704
	// Value-size bands. The layout needs 28 bytes minimum; the paged
	// band's fat values are what push segment counts past the page
	// boundary with a keyset a local run can walk.
	hashValInlineMin  = 28
	hashValInlineSpan = 12
	hashValSegMin     = 96
	hashValSegSpan    = 64
	hashValPagedMin   = 824
	hashValPagedSpan  = 64
	// Hot-tier budget, deliberately roomy: no sheds, no evictions.
	hashHotEntries = 8192
	hashHotArenas  = 64 << 20
	// Cadences. Flush is the durability ratchet the kill arm's
	// recovered image may trail but the READY high-water bounds;
	// Tick keeps the composite's background loops honest; kills
	// landing mid-Flush are the interesting cuts.
	hashTickEvery     = 64
	hashFlushEvery    = 512
	hashProgressEvery = 256
	hashBoundSlack    = 1 << 16
	hashValMagic      = 0x31766873 // "shv1"
)

func hashKeyName(idx int) []byte {
	return fmt.Appendf(nil, "hk%d", idx)
}

// hashBandFields is the field universe width of key idx.
func hashBandFields(idx int) int {
	switch {
	case idx < 4:
		return hashInlineFields
	case idx < 6:
		return hashSegFields
	default:
		return hashPagedFields
	}
}

func hashFieldName(i int) []byte {
	return fmt.Appendf(nil, "f%04d", i)
}

func parseHashField(f []byte, band int) (int, bool) {
	s := string(f)
	if !strings.HasPrefix(s, "f") {
		return 0, false
	}
	i, err := strconv.Atoi(s[1:])
	if err != nil || i < 0 || i >= band {
		return 0, false
	}
	if !bytes.Equal(f, hashFieldName(i)) {
		return 0, false
	}
	return i, true
}

// hashValue is the self-describing field value: magic, key index,
// field index, version, and total length up front, seeded filler in
// the middle, an xxhash64 of everything before it at the tail. The
// whole value is a deterministic function of (seed, key, field,
// version), so the parent classifies any recovered value with no
// journal, exactly as the tiered rig does.
func hashValue(seed uint64, key, field, ver uint32) []byte {
	rng := rand.New(rand.NewPCG(seed^0x6873767631, uint64(key)<<48|uint64(field)<<32|uint64(ver)))
	var n int
	switch {
	case key < 4:
		n = hashValInlineMin + rng.IntN(hashValInlineSpan)
	case key < 6:
		n = hashValSegMin + rng.IntN(hashValSegSpan)
	default:
		n = hashValPagedMin + rng.IntN(hashValPagedSpan)
	}
	b := make([]byte, n)
	binary.LittleEndian.PutUint32(b[0:], hashValMagic)
	binary.LittleEndian.PutUint32(b[4:], key)
	binary.LittleEndian.PutUint32(b[8:], field)
	binary.LittleEndian.PutUint32(b[12:], ver)
	binary.LittleEndian.PutUint32(b[16:], uint32(n))
	for i := 20; i < n-8; i++ {
		b[i] = byte(rng.UintN(256))
	}
	binary.LittleEndian.PutUint64(b[n-8:], xxhash.Sum64(b[:n-8]))
	return b
}

// parseHashValue verifies a recovered value against the key and field
// it sits under and returns the version it embeds. The regeneration
// compare at the end is the authority.
func parseHashValue(seed uint64, key, field uint32, val []byte) (uint32, error) {
	if len(val) < hashValInlineMin {
		return 0, fmt.Errorf("value is %d bytes, below the generator minimum", len(val))
	}
	if got := binary.LittleEndian.Uint32(val[0:]); got != hashValMagic {
		return 0, fmt.Errorf("value magic %#x", got)
	}
	if got := binary.LittleEndian.Uint32(val[4:]); got != key {
		return 0, fmt.Errorf("value claims key %d but is stored under key %d", got, key)
	}
	if got := binary.LittleEndian.Uint32(val[8:]); got != field {
		return 0, fmt.Errorf("value claims field %d but is stored under field %d", got, field)
	}
	ver := binary.LittleEndian.Uint32(val[12:])
	if ver == 0 {
		return 0, fmt.Errorf("value claims version 0, the stream starts at 1")
	}
	if got := binary.LittleEndian.Uint32(val[16:]); got != uint32(len(val)) {
		return 0, fmt.Errorf("value claims %d bytes, holds %d", got, len(val))
	}
	tail := len(val) - 8
	if got, want := xxhash.Sum64(val[:tail]), binary.LittleEndian.Uint64(val[tail:]); got != want {
		return 0, fmt.Errorf("value checksum %#x, tail holds %#x", got, want)
	}
	if !bytes.Equal(val, hashValue(seed, key, field, ver)) {
		return 0, fmt.Errorf("value does not regenerate from (key %d, field %d, version %d)", key, field, ver)
	}
	return ver, nil
}

// The op stream. A fixed population prefix writes every field of every
// key once at version 1, which is what forces the paged keys onto the
// rtype 5 rung before any kill window opens; after that, 70 percent
// HSET, 20 percent HDEL, 10 percent HGET over the whole universe.
// Everything is drawn from one seeded PCG so the parent replays it
// bit-exactly, GETs included since they advance the generator.
const (
	hashOpSet = iota
	hashOpDel
	hashOpGet
)

type hashOp struct {
	kind  int
	key   int
	field int
	ver   uint32
}

type hashStream struct {
	rng *rand.Rand
	ver [][]uint32
	pop int
}

func hashPopOps() int {
	n := 0
	for k := range hashKeys {
		n += hashBandFields(k)
	}
	return n
}

func newHashStream(seed uint64) *hashStream {
	s := &hashStream{rng: rand.New(rand.NewPCG(seed, 0x543268617368))}
	s.ver = make([][]uint32, hashKeys)
	for k := range s.ver {
		s.ver[k] = make([]uint32, hashBandFields(k))
	}
	return s
}

func (s *hashStream) next() hashOp {
	if s.pop < hashPopOps() {
		i := s.pop
		s.pop++
		for k := range hashKeys {
			if i < hashBandFields(k) {
				s.ver[k][i] = 1
				return hashOp{kind: hashOpSet, key: k, field: i, ver: 1}
			}
			i -= hashBandFields(k)
		}
	}
	k := s.rng.IntN(hashKeys)
	f := s.rng.IntN(hashBandFields(k))
	switch p := s.rng.IntN(100); {
	case p < 70:
		s.ver[k][f]++
		return hashOp{kind: hashOpSet, key: k, field: f, ver: s.ver[k][f]}
	case p < 90:
		return hashOp{kind: hashOpDel, key: k, field: f}
	default:
		return hashOp{kind: hashOpGet, key: k, field: f}
	}
}

// simulateHash replays the stream for n ops with no store: landed is
// the final version per field (0 means absent) and maxVer the highest
// version the stream ever assigned. The kill arm uses maxVer as its
// one-sided bound, the clean arm uses landed as the exact state owed.
func simulateHash(seed uint64, n int) (landed, maxVer [][]uint32) {
	st := newHashStream(seed)
	landed = make([][]uint32, hashKeys)
	for k := range landed {
		landed[k] = make([]uint32, hashBandFields(k))
	}
	for range n {
		op := st.next()
		switch op.kind {
		case hashOpSet:
			landed[op.key][op.field] = op.ver
		case hashOpDel:
			landed[op.key][op.field] = 0
		}
	}
	return landed, st.ver
}

// hashCrashRig is the composite under test plus the in-process shadow.
// The shadow is what the composite must serve while the process lives;
// durability is the parent's business.
type hashCrashRig struct {
	seed   uint64
	db     *sqlo1b.Store
	tr     *sqlo1.Tiered
	h      *sqlo1.Hash
	st     *hashStream
	landed [][]uint32
	ops    int
}

func newHashCrashRig(dir string, seed uint64) (*hashCrashRig, error) {
	db, err := sqlo1b.CreateStore(dir+"/"+hashDataFile, tieredSegSize)
	if err != nil {
		return nil, err
	}
	db.SetCheckpointPolicy(sqlo1b.CheckpointPolicy{Bytes: tieredCkptBytes})
	tr := sqlo1.NewTiered(db, sqlo1.TieredConfig{
		Budget: sqlo1.Budget{Entries: hashHotEntries, Arenas: hashHotArenas},
		Seed:   seed,
	})
	h, err := sqlo1.NewHash(tr, sqlo1.HashConfig{})
	if err != nil {
		db.Close()
		return nil, err
	}
	r := &hashCrashRig{seed: seed, db: db, tr: tr, h: h, st: newHashStream(seed)}
	r.landed = make([][]uint32, hashKeys)
	for k := range r.landed {
		r.landed[k] = make([]uint32, hashBandFields(k))
	}
	return r, nil
}

// step runs one stream op through the composite and self-checks it
// against the shadow live. Any engine error is fatal: budgets are
// sized so nothing sheds, so a refusal here is a rig bug, not load.
func (r *hashCrashRig) step(ctx context.Context) error {
	op := r.st.next()
	key := hashKeyName(op.key)
	field := hashFieldName(op.field)
	switch op.kind {
	case hashOpSet:
		created, err := r.h.HSet(ctx, key, field, hashValue(r.seed, uint32(op.key), uint32(op.field), op.ver))
		if err != nil {
			return fmt.Errorf("op %d: HSet(%s, %s): %w", r.ops, key, field, err)
		}
		if created != (r.landed[op.key][op.field] == 0) {
			return fmt.Errorf("op %d: HSet(%s, %s) created=%v, shadow holds version %d",
				r.ops, key, field, created, r.landed[op.key][op.field])
		}
		r.landed[op.key][op.field] = op.ver
	case hashOpDel:
		gone, err := r.h.HDel(ctx, key, field)
		if err != nil {
			return fmt.Errorf("op %d: HDel(%s, %s): %w", r.ops, key, field, err)
		}
		if gone != (r.landed[op.key][op.field] != 0) {
			return fmt.Errorf("op %d: HDel(%s, %s) = %v, shadow holds version %d",
				r.ops, key, field, gone, r.landed[op.key][op.field])
		}
		r.landed[op.key][op.field] = 0
	case hashOpGet:
		v, ok, err := r.h.HGet(ctx, key, field)
		if err != nil {
			return fmt.Errorf("op %d: HGet(%s, %s): %w", r.ops, key, field, err)
		}
		want := r.landed[op.key][op.field]
		if ok != (want != 0) {
			return fmt.Errorf("op %d: HGet(%s, %s) hit %v, shadow holds version %d", r.ops, key, field, ok, want)
		}
		if ok && !bytes.Equal(v, hashValue(r.seed, uint32(op.key), uint32(op.field), want)) {
			return fmt.Errorf("op %d: HGet(%s, %s) bytes do not match version %d", r.ops, key, field, want)
		}
	}
	r.ops++
	if r.ops%hashTickEvery == 0 {
		if err := r.tr.Tick(ctx); err != nil {
			return fmt.Errorf("op %d: Tick: %w", r.ops, err)
		}
	}
	return nil
}

// selfCheckCounts holds HLEN against the live shadow on every key, the
// in-process half of the count oracle.
func (r *hashCrashRig) selfCheckCounts(ctx context.Context) error {
	for k := range hashKeys {
		want := int64(0)
		for _, v := range r.landed[k] {
			if v != 0 {
				want++
			}
		}
		n, err := r.h.HLen(ctx, hashKeyName(k))
		if err != nil {
			return fmt.Errorf("HLen(%s): %w", hashKeyName(k), err)
		}
		if n != want {
			return fmt.Errorf("HLen(%s) = %d, shadow holds %d live fields", hashKeyName(k), n, want)
		}
	}
	return nil
}

// hashRecovered summarizes a verified image for the iteration log.
type hashRecovered struct {
	Fields    int
	HighWater int64
}

// verifyHashRecovered holds a killed or cleanly closed image against
// the seed. The kill arm passes maxVer, the one-sided bound; the clean
// arm passes exact and demands the final state to the byte. Both arms
// enforce the count oracle on every key: begin(count), the walked
// field set, HLEN, and per-field point reads must all agree, which is
// the H-I2 / rule W3 evidence, then a second open must land on the
// same state.
func verifyHashRecovered(dataPath string, seed uint64, maxVer, exact [][]uint32, minHW int64) (hashRecovered, error) {
	var out hashRecovered
	if err := scrubTieredImage(dataPath, sqlo1.WALPath(dataPath)); err != nil {
		return out, fmt.Errorf("scrub pass: %w", err)
	}
	ctx := context.Background()

	open := func() (*sqlo1b.Store, *sqlo1.Hash, error) {
		db, err := sqlo1b.OpenStore(dataPath, tieredSegSize)
		if err != nil {
			return nil, nil, fmt.Errorf("reopen: %w", err)
		}
		tr := sqlo1.NewTiered(db, sqlo1.TieredConfig{
			Budget: sqlo1.Budget{Entries: hashHotEntries, Arenas: hashHotArenas},
			Seed:   seed + 1,
		})
		h, err := sqlo1.NewHash(tr, sqlo1.HashConfig{})
		if err != nil {
			db.Close()
			return nil, nil, err
		}
		return db, h, nil
	}

	// checkKey walks one key and returns its reachable field versions.
	checkKey := func(h *sqlo1.Hash, k int) (map[int]uint32, error) {
		key := hashKeyName(k)
		band := hashBandFields(k)
		walked := map[int]uint32{}
		beginCount := -1
		var walkErr error
		err := h.HIterate(ctx, key, func(count int) { beginCount = count }, func(field, val []byte) {
			if walkErr != nil {
				return
			}
			fi, ok := parseHashField(field, band)
			if !ok {
				walkErr = fmt.Errorf("%s: walk delivered foreign field %q", key, field)
				return
			}
			if _, dup := walked[fi]; dup {
				walkErr = fmt.Errorf("%s: walk delivered field %s twice", key, field)
				return
			}
			ver, err := parseHashValue(seed, uint32(k), uint32(fi), val)
			if err != nil {
				walkErr = fmt.Errorf("%s field %s: %w", key, field, err)
				return
			}
			walked[fi] = ver
		})
		if err != nil {
			return nil, fmt.Errorf("%s: HIterate: %w", key, err)
		}
		if walkErr != nil {
			return nil, walkErr
		}
		if beginCount < 0 {
			if len(walked) != 0 {
				return nil, fmt.Errorf("%s: walk emitted %d fields with no begin call", key, len(walked))
			}
		} else if beginCount != len(walked) {
			return nil, fmt.Errorf("%s: begin(%d) but %d fields walked (W3 count drift)", key, beginCount, len(walked))
		}
		hlen, err := h.HLen(ctx, key)
		if err != nil {
			return nil, fmt.Errorf("%s: HLen: %w", key, err)
		}
		if int(hlen) != len(walked) {
			return nil, fmt.Errorf("%s: HLEN %d but %d fields reachable (W3 count drift)", key, hlen, len(walked))
		}
		if _, _, err := h.Encoding(ctx, key); err != nil {
			return nil, fmt.Errorf("%s: Encoding: %w", key, err)
		}
		// Point reads must agree with the walk field by field.
		for fi := range band {
			v, ok, err := h.HGet(ctx, key, hashFieldName(fi))
			if err != nil {
				return nil, fmt.Errorf("%s: HGet f%04d: %w", key, fi, err)
			}
			ver, inWalk := walked[fi]
			if ok != inWalk {
				return nil, fmt.Errorf("%s field f%04d: point read hit=%v, walk hit=%v", key, fi, ok, inWalk)
			}
			if ok && !bytes.Equal(v, hashValue(seed, uint32(k), uint32(fi), ver)) {
				return nil, fmt.Errorf("%s field f%04d: point read disagrees with walked version %d", key, fi, ver)
			}
		}
		return walked, nil
	}

	db, h, err := open()
	if err != nil {
		return out, err
	}
	st1 := db.Stats()
	out.HighWater = st1.HighWater
	first := make([]map[int]uint32, hashKeys)
	for k := range hashKeys {
		walked, err := checkKey(h, k)
		if err != nil {
			db.Close()
			return out, err
		}
		first[k] = walked
		out.Fields += len(walked)
		for fi, ver := range walked {
			if exact == nil {
				if ver > maxVer[k][fi] {
					db.Close()
					return out, fmt.Errorf("hk%d field f%04d recovered at version %d, the stream never went past %d",
						k, fi, ver, maxVer[k][fi])
				}
			} else if exact[k][fi] != ver {
				db.Close()
				return out, fmt.Errorf("hk%d field f%04d recovered at version %d, clean shutdown owed %d",
					k, fi, ver, exact[k][fi])
			}
		}
		if exact != nil {
			for fi, ver := range exact[k] {
				if ver != 0 {
					if _, ok := walked[fi]; !ok {
						db.Close()
						return out, fmt.Errorf("hk%d field f%04d version %d was acked and flushed but is gone", k, fi, ver)
					}
				}
			}
		}
	}
	if err := db.Close(); err != nil {
		return out, fmt.Errorf("close after verify: %w", err)
	}
	if st1.HighWater < minHW {
		return out, fmt.Errorf("recovered high-water %d below the worker's durable %d", st1.HighWater, minHW)
	}

	// Recovery must be repeatable: a second open of the same dir has
	// to land on the same hash states, not merely succeed.
	db2, h2, err := open()
	if err != nil {
		return out, fmt.Errorf("second reopen: %w", err)
	}
	defer db2.Close()
	if hw2 := db2.Stats().HighWater; hw2 != st1.HighWater {
		return out, fmt.Errorf("second open drifted: high-water %d -> %d", st1.HighWater, hw2)
	}
	for k := range hashKeys {
		walked, err := checkKey(h2, k)
		if err != nil {
			return out, fmt.Errorf("second open: %w", err)
		}
		if len(walked) != len(first[k]) {
			return out, fmt.Errorf("second open walked %d fields on hk%d, first %d", len(walked), k, len(first[k]))
		}
		for fi, ver := range first[k] {
			if walked[fi] != ver {
				return out, fmt.Errorf("second open holds hk%d f%04d at version %d, first at %d", k, fi, walked[fi], ver)
			}
		}
	}
	return out, nil
}
