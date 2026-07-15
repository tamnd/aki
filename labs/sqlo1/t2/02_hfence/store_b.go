package main

// The Track B arm: the same fence ladder over sqlo1b records. The
// root is a plain record under the user key (Gen 0); segments are
// subkey records (rooth minted for the lab key, kind SubkindSeg,
// segid) and fence pages subkey records (kind SubkindFence, pageid),
// both with Gen pinned to 1, so placement clusters the hash's records
// by rooth exactly as production collections will and a cold lookup
// is the same three Gets the a arm pays as row probes. A preload
// write set is one DrainBatch with the model's strictly increasing
// sequence as the high-water mark, already capped by flushCap well
// under the 64 MiB WAL segment, and the checkpoint cadence calls the
// store's own checkpoint.
//
// Cold on this arm: the a arm gets a fresh connection per lookup, so
// its cold rows price uncached B-tree descents. The closest honest
// analog here is one checkpoint, close, and reopen of the store
// before the cold phase: whatever the preload left warm in process
// state is gone and lookups run against what a fresh open loads from
// disk. There is no per-connection page cache to drop, so a
// per-lookup reopen would only re-time the open itself; reps after
// the first run on the reopened store and the cold row prices the
// post-open read path.

import (
	"context"
	"errors"
	"slices"

	"github.com/tamnd/aki/engine/sqlo1"
	"github.com/tamnd/aki/engine/sqlo1b"
)

// bWalSeg is the production WAL segment size; the model's flushCap
// keeps every DrainBatch well under it.
const bWalSeg = 64 << 20

type bdb struct {
	db    *sqlo1b.Store
	ctx   context.Context
	path  string
	key   []byte
	rooth uint64
	cold  bool
}

func openB(path string, key []byte) (*bdb, error) {
	db, err := sqlo1b.CreateStore(path, bWalSeg)
	if err != nil {
		return nil, err
	}
	b := &bdb{db: db, ctx: context.Background(), path: path, key: key}
	if b.rooth, err = sqlo1b.MintRooth(0, 0); err != nil {
		db.Close()
		return nil, err
	}
	return b, nil
}

func (b *bdb) subkey(kind uint8, segid uint64) ([]byte, error) {
	sk, err := sqlo1b.NewSubkey(b.rooth, kind, segid)
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

func (b *bdb) rootGet() ([]byte, error) { return b.getVal(b.key) }

func (b *bdb) pageGet(page int64) ([]byte, error) {
	key, err := b.subkey(sqlo1b.SubkindFence, uint64(page))
	if err != nil {
		return nil, err
	}
	return b.getVal(key)
}

func (b *bdb) segGet(segid int64) ([]byte, error) {
	key, err := b.subkey(sqlo1b.SubkindSeg, uint64(segid))
	if err != nil {
		return nil, err
	}
	return b.getVal(key)
}

// flush is one DrainBatch: segment and fence page subkey records plus
// the root when the set carries it, the model's sequence landing
// atomically as the high-water mark.
func (b *bdb) flush(fs *flushSet) error {
	ops := make([]sqlo1.Op, 0, len(fs.segs)+len(fs.pages)+1)
	for _, s := range fs.segs {
		key, err := b.subkey(sqlo1b.SubkindSeg, uint64(s.id))
		if err != nil {
			return err
		}
		ops = append(ops, sqlo1.Op{Rec: sqlo1.Record{Key: key, Value: s.row, Gen: 1}})
	}
	for _, p := range fs.pages {
		key, err := b.subkey(sqlo1b.SubkindFence, uint64(p.id))
		if err != nil {
			return err
		}
		ops = append(ops, sqlo1.Op{Rec: sqlo1.Record{Key: key, Value: p.row, Gen: 1}})
	}
	if fs.root != nil {
		ops = append(ops, sqlo1.Op{Rec: sqlo1.Record{Key: b.key, Value: fs.root}})
	}
	return b.db.ApplyBatch(b.ctx, &sqlo1.DrainBatch{Seq: fs.seq, Ops: ops})
}

func (b *bdb) checkpoint() error { return b.db.Checkpoint() }

// reopen is the cold boundary described in the header comment: the
// first call checkpoints, closes, and reopens the store, later calls
// are no-ops.
func (b *bdb) reopen() error {
	if b.cold {
		return nil
	}
	if err := b.db.Checkpoint(); err != nil {
		return err
	}
	if err := b.db.Close(); err != nil {
		return err
	}
	db, err := sqlo1b.OpenStore(b.path, bWalSeg)
	if err != nil {
		return err
	}
	b.db = db
	b.cold = true
	return nil
}

func (b *bdb) dataMB() float64 { return fileMB(b.path) }

func (b *bdb) close() error { return b.db.Close() }
