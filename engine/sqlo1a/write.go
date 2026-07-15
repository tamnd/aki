package sqlo1a

import (
	"context"
	"fmt"
	"os"

	"github.com/ncruces/go-sqlite3"
	"github.com/tamnd/aki/engine/sqlo1"
)

// The full seam lands with this file: reads in read.go, writes and
// accounting here.
var _ sqlo1.Store = (*DB)(nil)

// ApplyBatch applies one drain batch as one SQL transaction (doc 02
// section 1: the DrainBatch is the atomicity unit) with the high-water
// mark moved inside it, so the ops and the mark land or vanish together.
// A batch at or below the current mark is the exactly-once no-op the seam
// contract demands: recovery replays the aki WAL from the mark, and the
// store must shrug off everything it already holds.
//
// SQLite binds copy, so no op memory is retained after return; the drain
// scheduler is free to reuse its arenas the moment this comes back.
func (d *DB) ApplyBatch(ctx context.Context, b *sqlo1.DrainBatch) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	hw, err := d.highWaterLocked()
	if err != nil {
		return err
	}
	if b.Seq <= hw {
		return nil
	}
	txn, err := d.conn.BeginImmediate()
	if err != nil {
		return err
	}
	for i := range b.Ops {
		if err := d.applyOpLocked(&b.Ops[i]); err != nil {
			txn.Rollback()
			return err
		}
	}
	if err := bindExec(d.st.metaSetHW, func(s *sqlite3.Stmt) error {
		return s.BindInt64(1, b.Seq)
	}); err != nil {
		txn.Rollback()
		return fmt.Errorf("sqlo1a: move high-water to %d: %w", b.Seq, err)
	}
	return txn.Commit()
}

// applyOpLocked runs one op inside the batch transaction. A delete clears
// the kv row and every elem table's rows under the same root key: that is
// the gen-sweep work riding the drain transaction, and at the flat seam,
// where nothing writes elem rows yet, each of those deletes is one index
// seek on an empty tree. A put stores the record under recordTag with the
// crc the read path will verify.
func (d *DB) applyOpLocked(op *sqlo1.Op) error {
	if op.Del {
		if err := bindExec(d.st.kvDel, func(s *sqlite3.Stmt) error {
			return s.BindBlob(1, op.Rec.Key)
		}); err != nil {
			return fmt.Errorf("sqlo1a: delete key %x: %w", op.Rec.Key, err)
		}
		for _, del := range d.st.elemDelRoot {
			if err := bindExec(del, func(s *sqlite3.Stmt) error {
				return s.BindBlob(1, op.Rec.Key)
			}); err != nil {
				return fmt.Errorf("sqlo1a: sweep root %x: %w", op.Rec.Key, err)
			}
		}
		return nil
	}
	rec := &op.Rec
	crc := rowCRC(rec.Key, recordTag, rec.ExpireMs, int64(rec.Gen), rec.Value)
	if err := bindExec(d.st.kvPut, func(s *sqlite3.Stmt) error {
		for _, err := range []error{
			s.BindBlob(1, rec.Key),
			s.BindInt64(2, recordTag),
			s.BindInt64(3, rec.ExpireMs),
			s.BindInt64(4, int64(rec.Gen)),
			s.BindBlob(5, rec.Value),
			s.BindInt64(6, int64(crc)),
		} {
			if err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return fmt.Errorf("sqlo1a: put key %x: %w", rec.Key, err)
	}
	return nil
}

// Stats is best-effort accounting for INFO and the bench budget
// reconciliation; a field that cannot be read right now reports zero
// rather than failing the poll.
func (d *DB) Stats() sqlo1.StoreStats {
	d.mu.Lock()
	defer d.mu.Unlock()
	var st sqlo1.StoreStats
	if n, err := stmtInt64(d.st.kvCount); err == nil {
		st.Keys = n
	}
	if hw, err := d.highWaterLocked(); err == nil {
		st.HighWater = hw
	}
	if fi, err := os.Stat(d.path); err == nil {
		st.DiskBytes = fi.Size()
	}
	if fi, err := os.Stat(d.path + "-wal"); err == nil {
		st.DiskBytes += fi.Size()
	}
	return st
}

func (d *DB) highWaterLocked() (int64, error) {
	hw, err := stmtInt64(d.st.metaHW)
	if err != nil {
		return 0, fmt.Errorf("sqlo1a: read high-water: %w", err)
	}
	return hw, nil
}

// bindExec runs one execution of a prepared write statement: bind, step to
// completion, reset. The reset always runs so a failed op cannot leave the
// statement holding the transaction's locks past rollback.
func bindExec(s *sqlite3.Stmt, bind func(*sqlite3.Stmt) error) error {
	err := bind(s)
	if err == nil {
		s.Step()
		err = s.Err()
	}
	if rerr := s.Reset(); err == nil {
		err = rerr
	}
	return err
}

// stmtInt64 runs a single-row single-column query statement and resets it.
func stmtInt64(s *sqlite3.Stmt) (n int64, err error) {
	defer func() {
		if rerr := s.Reset(); rerr != nil && err == nil {
			err = rerr
		}
	}()
	if !s.Step() {
		if err := s.Err(); err != nil {
			return 0, err
		}
		return 0, fmt.Errorf("sqlo1a: statement returned no row")
	}
	return s.ColumnInt64(0), nil
}
