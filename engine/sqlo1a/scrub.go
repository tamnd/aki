package sqlo1a

import (
	"context"
	"fmt"

	"github.com/tamnd/aki/engine/sqlo1"
)

// scrubBatchDefault bounds one scrub pass. The rate cap lives here as rows
// per pass; the cadence caller turns it into rows per second by choosing
// how often it calls.
const scrubBatchDefault = 512

// ScrubResult is one pass's findings. Corrupt holds one ErrCorrupt-wrapped
// error per damaged row, because a scrub that stops at the first hit would
// need a full extra sweep per additional bad row to size the damage.
type ScrubResult struct {
	Scanned int
	Corrupt []error
}

// Scrub is the rolling crc sweep (doc 02 section 6, A-I4): it walks kv in
// key order verifying every row's checksum, gated rows included, since
// corruption must not hide inside expired or foreign-tag entries. Each
// call checks at most limit rows and returns a cursor to resume from; nil
// means the sweep wrapped and the next pass starts a new one. Corrupt rows
// are reported, not repaired and not deleted: detection-only is the
// recorded G-safe posture, the data is provably damaged and there is no
// replica to heal from in this spec generation, so the blast radius stays
// one loudly-failing key.
func (d *DB) Scrub(ctx context.Context, cur sqlo1.Cursor, limit int) (next sqlo1.Cursor, res ScrubResult, err error) {
	if err := ctx.Err(); err != nil {
		return nil, res, err
	}
	if limit <= 0 {
		limit = scrubBatchDefault
	}
	fresh := len(cur) == 0
	var after []byte
	if !fresh {
		if cur[0] != cursorTag {
			return nil, res, fmt.Errorf("sqlo1a: unknown scrub cursor tag %#02x", cur[0])
		}
		after = append([]byte(nil), cur[1:]...)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	s := d.st.kvScan
	if fresh {
		s = d.st.kvScanFirst
	}
	defer func() {
		if rerr := s.Reset(); rerr != nil && err == nil {
			next, res.Corrupt, err = nil, nil, rerr
		}
	}()
	if fresh {
		if err := s.BindInt64(1, int64(limit)); err != nil {
			return nil, res, err
		}
	} else {
		if err := s.BindBlob(1, after); err != nil {
			return nil, res, err
		}
		if err := s.BindInt64(2, int64(limit)); err != nil {
			return nil, res, err
		}
	}
	var last []byte
	for s.Step() {
		k := append([]byte(nil), s.ColumnRawBlob(0)...)
		t := s.ColumnInt64(1)
		exp := s.ColumnInt64(2)
		gen := s.ColumnInt64(3)
		v := s.ColumnRawBlob(4)
		crc := uint32(s.ColumnInt64(5))
		if got := rowCRC(k, t, exp, gen, v); got != crc {
			res.Corrupt = append(res.Corrupt,
				fmt.Errorf("%w: key %x has crc %08x, row hashes to %08x", ErrCorrupt, k, crc, got))
		}
		res.Scanned++
		last = k
	}
	if err := s.Err(); err != nil {
		return nil, res, err
	}
	if res.Scanned < limit {
		return nil, res, nil
	}
	return sqlo1.Cursor(append([]byte{cursorTag}, last...)), res, nil
}
