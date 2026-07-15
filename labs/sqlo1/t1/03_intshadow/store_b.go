package main

// The Track B arm: the same counters model over sqlo1b records. A
// counter is a plain record under its user key with the canonical
// decimal string as the value and Gen zero; doc 05 section 2 keeps
// int-shaped values flat, so there is no subkey structure here. A
// flush is one DrainBatch with the model's sequence as the high-water
// mark, and the checkpoint cadence calls the store's own checkpoint,
// so the cold miss and flush rows price the WAL append plus RAM apply
// a production drain cycle pays.

import (
	"context"
	"errors"
	"slices"

	"github.com/tamnd/aki/engine/sqlo1"
	"github.com/tamnd/aki/engine/sqlo1b"
)

// bWalSeg is the production WAL segment size; one flush at the 8192
// dirty-counter threshold is well under one segment.
const bWalSeg = 64 << 20

type bdb struct {
	db   *sqlo1b.Store
	ctx  context.Context
	path string
	keys [][]byte
}

func openB(path string, keys [][]byte) (*bdb, error) {
	db, err := sqlo1b.CreateStore(path, bWalSeg)
	if err != nil {
		return nil, err
	}
	return &bdb{db: db, ctx: context.Background(), path: path, keys: keys}, nil
}

// get returns the stored decimal bytes, false for a missing key. The
// clone is deliberate: the record's value aliases store memory.
func (b *bdb) get(ki int) ([]byte, bool, error) {
	rec, err := b.db.Get(b.ctx, b.keys[ki])
	if errors.Is(err, sqlo1.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return slices.Clone(rec.Value), true, nil
}

// flush is one DrainBatch: every dirty counter as a plain record and
// the high-water sequence landing atomically with them.
func (b *bdb) flush(fs *flushSet) error {
	ops := make([]sqlo1.Op, 0, len(fs.vals))
	for _, v := range fs.vals {
		ops = append(ops, sqlo1.Op{Rec: sqlo1.Record{Key: b.keys[v.ki], Value: v.val}})
	}
	return b.db.ApplyBatch(b.ctx, &sqlo1.DrainBatch{Seq: fs.seq, Ops: ops})
}

func (b *bdb) checkpoint() error { return b.db.Checkpoint() }

func (b *bdb) dataMB() float64 { return fileMB(b.path) }

func (b *bdb) close() error { return b.db.Close() }
