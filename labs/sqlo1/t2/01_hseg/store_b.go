package main

// The Track B arm: the same segmented-hash model over sqlo1b records.
// A segment is a record under a 16-byte subkey (rooth minted per hash
// key, kind SubkindSeg, segid) with Gen pinned to 1, so placement
// clusters a hash's segments by rooth exactly as production
// collections will; the root payload is a plain record under the user
// key. A flush is one DrainBatch with the model's sequence as the
// high-water mark, and the checkpoint cadence calls the store's own
// checkpoint, so the flush row prices the WAL append plus RAM apply a
// production drain cycle pays. The W2/W4 WAL column never reads this
// arm; it stays modeled arithmetic in the shared model.

import (
	"context"
	"errors"
	"slices"

	"github.com/tamnd/aki/engine/sqlo1"
	"github.com/tamnd/aki/engine/sqlo1b"
)

// bWalSeg is the production WAL segment size; one flush of encoded
// segments at the 8 MiB drain threshold must fit one segment.
const bWalSeg = 64 << 20

type bdb struct {
	db     *sqlo1b.Store
	ctx    context.Context
	path   string
	keys   [][]byte
	rooths []uint64
}

func openB(path string, keys [][]byte) (*bdb, error) {
	db, err := sqlo1b.CreateStore(path, bWalSeg)
	if err != nil {
		return nil, err
	}
	b := &bdb{db: db, ctx: context.Background(), path: path, keys: keys}
	b.rooths = make([]uint64, len(keys))
	for i := range keys {
		if b.rooths[i], err = sqlo1b.MintRooth(0, uint64(i)); err != nil {
			db.Close()
			return nil, err
		}
	}
	return b, nil
}

func (b *bdb) segkey(ki int, segid uint64) ([]byte, error) {
	sk, err := sqlo1b.NewSubkey(b.rooths[ki], sqlo1b.SubkindSeg, segid)
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

// segGet returns the stored segment blob, nil for a missing record.
func (b *bdb) segGet(ki int, segid uint64) ([]byte, error) {
	key, err := b.segkey(ki, segid)
	if err != nil {
		return nil, err
	}
	return b.getVal(key)
}

// rootGet returns the stored root payload, nil for a missing record.
func (b *bdb) rootGet(ki int) ([]byte, error) {
	return b.getVal(b.keys[ki])
}

// flush is one DrainBatch: segment records under their subkeys, root
// records under the user keys, and the high-water sequence landing
// atomically with them.
func (b *bdb) flush(fs *flushSet) error {
	ops := make([]sqlo1.Op, 0, len(fs.segs)+len(fs.roots))
	for _, s := range fs.segs {
		key, err := b.segkey(s.ki, s.segid)
		if err != nil {
			return err
		}
		ops = append(ops, sqlo1.Op{Rec: sqlo1.Record{Key: key, Value: s.row, Gen: 1}})
	}
	for _, rt := range fs.roots {
		ops = append(ops, sqlo1.Op{Rec: sqlo1.Record{Key: b.keys[rt.ki], Value: rt.row}})
	}
	return b.db.ApplyBatch(b.ctx, &sqlo1.DrainBatch{Seq: fs.seq, Ops: ops})
}

func (b *bdb) checkpoint() error { return b.db.Checkpoint() }

func (b *bdb) dataMB() float64 { return fileMB(b.path) }
func (b *bdb) walMB() float64  { return fileMB(sqlo1.WALPath(b.path)) }

func (b *bdb) close() error { return b.db.Close() }
