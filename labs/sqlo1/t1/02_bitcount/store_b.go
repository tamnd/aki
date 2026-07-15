package main

// The Track B arm: the same count paths over sqlo1b records. A chunk
// is a segment record under a 16-byte subkey (one rooth minted for
// the lab's bitmap key, kind SubkindSeg, cid as segid) with Gen
// pinned to 1, so placement clusters the bitmap's chunks by rooth
// exactly as production collections will. The popcount cache is not a
// column here: doc 05 section 3.2 gives it kind 2 subkey segments,
// segment j covering chunks [j*1024, (j+1)*1024) with one
// little-endian u32 per chunk, written in the same DrainBatch as the
// chunks they cover. Cache mode therefore reads the covering cache
// segments plus the two edge chunk records, scan mode reads every
// chunk record, and the cold reps reopen the store after the load
// checkpoint so reads come from state rebuilt off the settled file,
// not the writer's dirty RAM.

import (
	"context"
	"encoding/binary"
	"errors"
	"slices"

	"github.com/tamnd/aki/engine/sqlo1"
	"github.com/tamnd/aki/engine/sqlo1b"
)

// bWalSeg is the production WAL segment size; bMaxBatch caps one
// ApplyBatch payload well under it, so a preload flush that outgrows
// the cap splits into several batches with increasing Seq.
const bWalSeg = 64 << 20
const bMaxBatch = 16 << 20

// pcKind is the popcount cache subkey kind and pcWindow the chunks
// one cache segment covers (doc 05 section 3.2).
const pcKind uint8 = 2
const pcWindow = 1024

type bdb struct {
	db    *sqlo1b.Store
	ctx   context.Context
	rooth uint64
}

func openB(path string, create bool) (*bdb, error) {
	open := sqlo1b.OpenStore
	if create {
		open = sqlo1b.CreateStore
	}
	db, err := open(path, bWalSeg)
	if err != nil {
		return nil, err
	}
	rooth, err := sqlo1b.MintRooth(0, 0)
	if err != nil {
		db.Close()
		return nil, err
	}
	return &bdb{db: db, ctx: context.Background(), rooth: rooth}, nil
}

func (b *bdb) subkey(kind uint8, segid uint64) ([]byte, error) {
	sk, err := sqlo1b.NewSubkey(b.rooth, kind, segid)
	if err != nil {
		return nil, err
	}
	return sk.Encode(), nil
}

// getVal reads one record's value, nil for a miss; the clone makes
// the bytes safe to keep across later store calls.
func (b *bdb) getVal(key []byte) ([]byte, error) {
	rec, err := b.db.Get(b.ctx, key)
	if errors.Is(err, sqlo1.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return slices.Clone(rec.Value), nil
}

// rawVal skips the clone for reads consumed before the next store
// call; scan mode and the cache-segment sum never keep the bytes, and
// a clone there would tax the scan arm by the very bytes it reads.
func (b *bdb) rawVal(key []byte) ([]byte, error) {
	rec, err := b.db.Get(b.ctx, key)
	if errors.Is(err, sqlo1.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return rec.Value, nil
}

// chunkGet returns the stored chunk bytes, or nil for a missing cid.
func (b *bdb) chunkGet(cid int64) ([]byte, error) {
	key, err := b.subkey(sqlo1b.SubkindSeg, uint64(cid))
	if err != nil {
		return nil, err
	}
	return b.getVal(key)
}

// sumPC sums cache entries for chunks [c0, c1]: one cache segment
// record per pcWindow chunks, a missing segment or an entry past the
// segment's tail counting as zero, matching the a arm's absent rows.
func (b *bdb) sumPC(c0, c1 int64) (int64, error) {
	var total int64
	for segid := c0 / pcWindow; segid <= c1/pcWindow; segid++ {
		key, err := b.subkey(pcKind, uint64(segid))
		if err != nil {
			return 0, err
		}
		img, err := b.rawVal(key)
		if err != nil {
			return 0, err
		}
		if img == nil {
			continue
		}
		lo := max(c0, segid*pcWindow)
		hi := min(c1, (segid+1)*pcWindow-1)
		for cid := lo; cid <= hi; cid++ {
			off := int(cid%pcWindow) * 4
			if off+4 > len(img) {
				break
			}
			total += int64(binary.LittleEndian.Uint32(img[off:]))
		}
	}
	return total, nil
}

// scanChunks visits every stored chunk in [c0, c1] in cid order,
// skipping missing cids; img aliases store memory and is only valid
// during the visit.
func (b *bdb) scanChunks(c0, c1 int64, visit func(cid int64, img []byte) error) error {
	for cid := c0; cid <= c1; cid++ {
		key, err := b.subkey(sqlo1b.SubkindSeg, uint64(cid))
		if err != nil {
			return err
		}
		img, err := b.rawVal(key)
		if err != nil {
			return err
		}
		if img == nil {
			continue
		}
		if err := visit(cid, img); err != nil {
			return err
		}
	}
	return nil
}

// flush lands one preload write set: chunk records and their cache
// segment updates in one DrainBatch, the cache segments read, widened
// to the touched chunk, and rewritten whole, the same read-modify the
// owner pays at drain time. A payload past bMaxBatch splits into
// several batches; the model advances fs.seq by the chunk count per
// flush, so the split seqs fs.seq-(groups-1)..fs.seq stay strictly
// increasing across every ApplyBatch.
func (b *bdb) flush(fs *flushSet) error {
	var groups [][]chunkRow
	start, bytes := 0, 0
	for i, c := range fs.chunks {
		if i > start && bytes+len(c.row) > bMaxBatch {
			groups = append(groups, fs.chunks[start:i])
			start, bytes = i, 0
		}
		bytes += len(c.row)
	}
	groups = append(groups, fs.chunks[start:])
	for gi, g := range groups {
		ops := make([]sqlo1.Op, 0, len(g)+1)
		pcSegs := map[uint64][]byte{}
		for _, c := range g {
			key, err := b.subkey(sqlo1b.SubkindSeg, uint64(c.cid))
			if err != nil {
				return err
			}
			ops = append(ops, sqlo1.Op{Rec: sqlo1.Record{Key: key, Value: c.row, Gen: 1}})
			segid := uint64(c.cid) / pcWindow
			img, ok := pcSegs[segid]
			if !ok {
				key, err := b.subkey(pcKind, segid)
				if err != nil {
					return err
				}
				if img, err = b.getVal(key); err != nil {
					return err
				}
			}
			off := (int(c.cid) % pcWindow) * 4
			if off+4 > len(img) {
				img = append(img, make([]byte, off+4-len(img))...)
			}
			binary.LittleEndian.PutUint32(img[off:], uint32(c.pc))
			pcSegs[segid] = img
		}
		for segid, img := range pcSegs {
			key, err := b.subkey(pcKind, segid)
			if err != nil {
				return err
			}
			ops = append(ops, sqlo1.Op{Rec: sqlo1.Record{Key: key, Value: img, Gen: 1}})
		}
		seq := fs.seq - int64(len(groups)-1-gi)
		if err := b.db.ApplyBatch(b.ctx, &sqlo1.DrainBatch{Seq: seq, Ops: ops}); err != nil {
			return err
		}
	}
	return nil
}

func (b *bdb) checkpoint() error { return b.db.Checkpoint() }

func (b *bdb) close() error { return b.db.Close() }
