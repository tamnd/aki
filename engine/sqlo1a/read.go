package sqlo1a

import (
	"context"
	"errors"
	"fmt"

	"github.com/tamnd/aki/engine/sqlo1"
)

// ErrCorrupt marks a row whose stored crc32c disagrees with its bytes.
// Detection-only is the recorded G-safe posture for Track A (doc 02
// section 4): the read refuses the row loudly instead of serving it, and
// the scrub slice sweeps for the same condition in the background.
var ErrCorrupt = errors.New("sqlo1a: row checksum mismatch")

// recordTag is the kv.t value for flat Store-seam records. The per-type
// models (docs 05-10) claim other tags for collection roots when their
// slices land; until then a foreign tag on a row means it belongs to logic
// above this seam, and the seam reports the key as absent rather than
// leaking another model's encoding as a plain value.
const recordTag = 0

// scanPage is how many rows Scan pulls per statement execution. Pages keep
// a long scan from pinning the read statement across the whole keyspace;
// the cursor between pages is just the last key, so the size is a latency
// knob, not a correctness one.
const scanPage = 512

// cursorTag prefixes every non-nil cursor Scan hands out. The payload is
// the last key visited, and the prefix keeps a cursor after the zero-length
// key (a legal Redis key) from collapsing into the nil start-of-scan value.
const cursorTag = 0x01

// Get implements the sqlo1.Store read contract on kv: crc verified before
// anything else is trusted, a foreign type tag or a passed expiry is a
// miss, and the returned Record aliases nothing (fresh copies of key and
// value).
func (d *DB) Get(ctx context.Context, key []byte) (sqlo1.Record, error) {
	if err := ctx.Err(); err != nil {
		return sqlo1.Record{}, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.getLocked(key)
}

// BatchGet returns one Record per requested key in order, nil Key marking
// a miss. In the A2 shape this is N point lookups under one lock hold on
// one prepared statement, which beats N Get calls on lock and reset churn;
// turning a cold-miss batch into one IO round is the cache slice's job.
func (d *DB) BatchGet(ctx context.Context, keys [][]byte) ([]sqlo1.Record, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]sqlo1.Record, len(keys))
	for i, k := range keys {
		rec, err := d.getLocked(k)
		if errors.Is(err, sqlo1.ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		out[i] = rec
	}
	return out, nil
}

func (d *DB) getLocked(key []byte) (rec sqlo1.Record, err error) {
	s := d.st.kvGet
	defer func() {
		if rerr := s.Reset(); rerr != nil && err == nil {
			err = rerr
		}
	}()
	if err := s.BindBlob(1, key); err != nil {
		return sqlo1.Record{}, err
	}
	if !s.Step() {
		if err := s.Err(); err != nil {
			return sqlo1.Record{}, err
		}
		return sqlo1.Record{}, sqlo1.ErrNotFound
	}
	t := s.ColumnInt64(0)
	exp := s.ColumnInt64(1)
	gen := s.ColumnInt64(2)
	v := append([]byte(nil), s.ColumnRawBlob(3)...)
	crc := uint32(s.ColumnInt64(4))
	if got := rowCRC(key, t, exp, gen, v); got != crc {
		return sqlo1.Record{}, fmt.Errorf("%w: key %x has crc %08x, row hashes to %08x", ErrCorrupt, key, crc, got)
	}
	if t != recordTag || expired(exp, d.now()) {
		return sqlo1.Record{}, sqlo1.ErrNotFound
	}
	return sqlo1.Record{
		Key:      append([]byte(nil), key...),
		Value:    v,
		ExpireMs: exp,
		Gen:      uint32(gen),
	}, nil
}

// pageRow is one row off a scan page. skip means the row is gated (foreign
// tag or passed expiry) and only advances the cursor; its crc was still
// verified, because a scan is the closest thing the read path has to a
// scrub pass and corruption must not hide inside expired entries.
type pageRow struct {
	rec  sqlo1.Record
	skip bool
}

// Scan visits live flat records in key order until fn returns false or kv
// is exhausted. A nil cursor starts from the top; the returned cursor
// resumes after the last key visited, and nil means done.
func (d *DB) Scan(ctx context.Context, cur sqlo1.Cursor, fn func(sqlo1.Record) bool) (sqlo1.Cursor, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	var after []byte
	fresh := len(cur) == 0
	if !fresh {
		if cur[0] != cursorTag {
			return nil, fmt.Errorf("sqlo1a: unknown scan cursor tag %#02x", cur[0])
		}
		after = append([]byte(nil), cur[1:]...)
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		rows, err := d.scanPageLocked(fresh, after)
		if err != nil {
			return nil, err
		}
		for _, row := range rows {
			after = row.rec.Key
			if row.skip {
				continue
			}
			if !fn(row.rec) {
				return sqlo1.Cursor(append([]byte{cursorTag}, after...)), nil
			}
		}
		if len(rows) < scanPage {
			return nil, nil
		}
		fresh = false
	}
}

func (d *DB) scanPageLocked(fresh bool, after []byte) (rows []pageRow, err error) {
	s := d.st.kvScan
	if fresh {
		s = d.st.kvScanFirst
	}
	defer func() {
		if rerr := s.Reset(); rerr != nil && err == nil {
			err = rerr
		}
	}()
	if fresh {
		if err := s.BindInt64(1, scanPage); err != nil {
			return nil, err
		}
	} else {
		if err := s.BindBlob(1, after); err != nil {
			return nil, err
		}
		if err := s.BindInt64(2, scanPage); err != nil {
			return nil, err
		}
	}
	now := d.now()
	for s.Step() {
		k := append([]byte(nil), s.ColumnRawBlob(0)...)
		t := s.ColumnInt64(1)
		exp := s.ColumnInt64(2)
		gen := s.ColumnInt64(3)
		v := append([]byte(nil), s.ColumnRawBlob(4)...)
		crc := uint32(s.ColumnInt64(5))
		if got := rowCRC(k, t, exp, gen, v); got != crc {
			return nil, fmt.Errorf("%w: key %x has crc %08x, row hashes to %08x", ErrCorrupt, k, crc, got)
		}
		if t != recordTag || expired(exp, now) {
			rows = append(rows, pageRow{rec: sqlo1.Record{Key: k}, skip: true})
			continue
		}
		rows = append(rows, pageRow{rec: sqlo1.Record{Key: k, Value: v, ExpireMs: exp, Gen: uint32(gen)}})
	}
	if err := s.Err(); err != nil {
		return nil, err
	}
	return rows, nil
}

func expired(exp, nowMs int64) bool {
	return exp > 0 && exp <= nowMs
}
