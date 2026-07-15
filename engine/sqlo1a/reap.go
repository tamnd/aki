package sqlo1a

import (
	"context"
	"fmt"

	"github.com/ncruces/go-sqlite3"
)

// reapBatchDefault is the doc 11 section 3.1 bounded batch: the reaper
// runs on the drain cadence and must never stall the command path, so one
// pass touches at most this many rows per table unless the caller says
// otherwise.
const reapBatchDefault = 64

// Reap deletes due rows in bounded batches (doc 11 section 7): expired kv
// roots through the kv_exp partial index, then expired hash fields
// through helem_exp, one transaction per pass. A reaped root takes its
// elem rows with it, exactly like a drain delete, because an expired
// collection root must not strand orphans for a sweep that never comes.
// Lazy expiry on the read path is the correctness layer; this is space
// reclamation, so a lagging reaper is never visible to a reader.
//
// Returns the roots and fields reaped. A full batch in either count means
// there is likely more due work, and the caller can go again immediately.
func (d *DB) Reap(ctx context.Context, limit int) (roots, fields int, err error) {
	if err := ctx.Err(); err != nil {
		return 0, 0, err
	}
	if limit <= 0 {
		limit = reapBatchDefault
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	now := d.now()
	txn, err := d.conn.BeginImmediate()
	if err != nil {
		return 0, 0, err
	}
	due, err := d.dueRootsLocked(now, limit)
	if err != nil {
		txn.Rollback()
		return 0, 0, err
	}
	for _, key := range due {
		if err := d.delRootLocked(key); err != nil {
			txn.Rollback()
			return 0, 0, err
		}
	}
	if err := bindExec(d.st.helemReap, func(s *sqlite3.Stmt) error {
		if err := s.BindInt64(1, now); err != nil {
			return err
		}
		return s.BindInt64(2, int64(limit))
	}); err != nil {
		txn.Rollback()
		return 0, 0, fmt.Errorf("sqlo1a: reap helem: %w", err)
	}
	fields = int(d.conn.Changes())
	if err := txn.Commit(); err != nil {
		return 0, 0, err
	}
	return len(due), fields, nil
}

// dueRootsLocked collects up to limit expired root keys. Collected fully
// before any delete runs, so the select never walks a tree the same pass
// is mutating.
func (d *DB) dueRootsLocked(nowMs int64, limit int) (keys [][]byte, err error) {
	s := d.st.kvExpired
	defer func() {
		if rerr := s.Reset(); rerr != nil && err == nil {
			err = rerr
		}
	}()
	if err := s.BindInt64(1, nowMs); err != nil {
		return nil, err
	}
	if err := s.BindInt64(2, int64(limit)); err != nil {
		return nil, err
	}
	for s.Step() {
		keys = append(keys, append([]byte(nil), s.ColumnRawBlob(0)...))
	}
	if err := s.Err(); err != nil {
		return nil, err
	}
	return keys, nil
}
