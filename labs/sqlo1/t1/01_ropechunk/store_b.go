package main

// The Track B arm: the same rope model over sqlo1b records. A chunk
// is a segment record under a 16-byte subkey (rooth minted per rope
// key, kind SubkindSeg, cid as segid) with Gen pinned to 1, so
// placement clusters a rope's chunks by rooth exactly as production
// collections will; the root length is a plain record under the user
// key. The bitmap mix maintains the doc 05 section 3.2 popcount
// cache: kind 2 subkey segments holding one u32 per chunk, read,
// updated for the dirty chunks, and drained in the same batch, which
// is the extra write the pc column costs on this track. A flush is
// one DrainBatch with the rope's sequence as the high-water mark, and
// the checkpoint cadence calls the store's own checkpoint, so the
// flush row prices the WAL append plus RAM apply a production drain
// cycle pays.

import (
	"context"
	"encoding/binary"
	"errors"
	"slices"

	"github.com/tamnd/aki/engine/sqlo1"
	"github.com/tamnd/aki/engine/sqlo1b"
)

// bWalSeg is the production WAL segment size; one flush of dirty
// chunks at the 8 MiB drain threshold must fit one segment.
const bWalSeg = 64 << 20

// pcKind is the popcount cache subkey kind and pcWindow the chunks
// one cache segment covers (doc 05 section 3.2).
const pcKind uint8 = 2
const pcWindow = 1024

type bdb struct {
	db      *sqlo1b.Store
	ctx     context.Context
	path    string
	keys    [][]byte
	rooths  []uint64
	countPC bool
}

func openB(path string, keys [][]byte, countPC bool) (*bdb, error) {
	db, err := sqlo1b.CreateStore(path, bWalSeg)
	if err != nil {
		return nil, err
	}
	b := &bdb{db: db, ctx: context.Background(), path: path, keys: keys, countPC: countPC}
	b.rooths = make([]uint64, len(keys))
	for i := range keys {
		if b.rooths[i], err = sqlo1b.MintRooth(0, uint64(i)); err != nil {
			db.Close()
			return nil, err
		}
	}
	return b, nil
}

func (b *bdb) subkey(ki int, kind uint8, segid uint64) ([]byte, error) {
	sk, err := sqlo1b.NewSubkey(b.rooths[ki], kind, segid)
	if err != nil {
		return nil, err
	}
	return sk.Encode(), nil
}

// getVal reads one record's value, nil for a miss.
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

// chunkGet returns the stored chunk bytes, or nil for a lazy gap.
func (b *bdb) chunkGet(ki int, cid int64) ([]byte, error) {
	key, err := b.subkey(ki, sqlo1b.SubkindSeg, uint64(cid))
	if err != nil {
		return nil, err
	}
	return b.getVal(key)
}

// chunkProbe reads the stored row and looks its popcount up in the
// covering cache segment for the oracle readback.
func (b *bdb) chunkProbe(ki int, cid int64) ([]byte, int64, error) {
	row, err := b.chunkGet(ki, cid)
	if err != nil || row == nil || !b.countPC {
		return row, 0, err
	}
	key, err := b.subkey(ki, pcKind, uint64(cid)/pcWindow)
	if err != nil {
		return nil, 0, err
	}
	seg, err := b.getVal(key)
	if err != nil {
		return nil, 0, err
	}
	off := (int(cid) % pcWindow) * 4
	if off+4 > len(seg) {
		return row, 0, nil
	}
	return row, int64(binary.LittleEndian.Uint32(seg[off:])), nil
}

// flush is one DrainBatch: chunk records, updated popcount cache
// segments for the bitmap mix, root length records, and the
// high-water sequence landing atomically. Cache segments are read,
// widened to the touched chunk, and rewritten whole, the same
// read-modify the owner pays at drain time.
func (b *bdb) flush(fs *flushSet) error {
	ops := make([]sqlo1.Op, 0, len(fs.chunks)+len(fs.roots))
	pcSegs := map[uint64][]byte{}
	for _, c := range fs.chunks {
		key, err := b.subkey(c.ki, sqlo1b.SubkindSeg, uint64(c.cid))
		if err != nil {
			return err
		}
		ops = append(ops, sqlo1.Op{Rec: sqlo1.Record{Key: key, Value: c.row, Gen: 1}})
		if !b.countPC {
			continue
		}
		segid := uint64(c.cid) / pcWindow
		sk := uint64(c.ki)<<32 | segid
		img, ok := pcSegs[sk]
		if !ok {
			key, err := b.subkey(c.ki, pcKind, segid)
			if err != nil {
				return err
			}
			if img, err = b.getVal(key); err != nil {
				return err
			}
			pcSegs[sk] = img
		}
		off := (int(c.cid) % pcWindow) * 4
		if off+4 > len(img) {
			img = append(img, make([]byte, off+4-len(img))...)
		}
		binary.LittleEndian.PutUint32(img[off:], uint32(c.pc))
		pcSegs[sk] = img
	}
	for sk, img := range pcSegs {
		key, err := b.subkey(int(sk>>32), pcKind, sk&0xffffffff)
		if err != nil {
			return err
		}
		ops = append(ops, sqlo1.Op{Rec: sqlo1.Record{Key: key, Value: img, Gen: 1}})
	}
	var rootBuf [8]byte
	for _, rt := range fs.roots {
		binary.LittleEndian.PutUint64(rootBuf[:], uint64(rt.len))
		ops = append(ops, sqlo1.Op{Rec: sqlo1.Record{Key: b.keys[rt.ki], Value: slices.Clone(rootBuf[:])}})
	}
	return b.db.ApplyBatch(b.ctx, &sqlo1.DrainBatch{Seq: fs.seq, Ops: ops})
}

func (b *bdb) checkpoint() error { return b.db.Checkpoint() }

func (b *bdb) dataMB() float64 { return fileMB(b.path) }
func (b *bdb) walMB() float64  { return fileMB(sqlo1.WALPath(b.path)) }

func (b *bdb) close() error { return b.db.Close() }
