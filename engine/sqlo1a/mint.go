package sqlo1a

import (
	"context"
	"fmt"

	"github.com/ncruces/go-sqlite3"
	"github.com/tamnd/aki/engine/sqlo1"
)

var _ sqlo1.Minter = (*DB)(nil)

// MintLease durably reserves the next n rooth counters and returns
// the first (the seam Minter contract). The mark rides the meta row
// beside the high-water mark in its own immediate transaction, so the
// reservation is committed, durable at the store's synchronous level
// exactly like a drain batch, before the caller sees the range.
// Counters a crash strands in a lease are abandoned; the mint is a
// bijection, so holes waste only address space.
func (d *DB) MintLease(ctx context.Context, n uint64) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	start, err := d.leaseLocked()
	if err != nil {
		return 0, err
	}
	mark, err := sqlo1.LeaseEnd(start, n)
	if err != nil {
		return 0, err
	}
	txn, err := d.conn.BeginImmediate()
	if err != nil {
		return 0, err
	}
	if err := bindExec(d.st.metaSetLease, func(s *sqlite3.Stmt) error {
		return s.BindInt64(1, int64(mark))
	}); err != nil {
		txn.Rollback()
		return 0, fmt.Errorf("sqlo1a: move mint lease to %d: %w", mark, err)
	}
	if err := txn.Commit(); err != nil {
		return 0, err
	}
	return start, nil
}

func (d *DB) leaseLocked() (uint64, error) {
	n, err := stmtInt64(d.st.metaLease)
	if err != nil {
		return 0, fmt.Errorf("sqlo1a: read mint lease: %w", err)
	}
	return uint64(n), nil
}
