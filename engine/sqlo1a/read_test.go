package sqlo1a

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/tamnd/aki/engine/sqlo1"
)

// rawPut writes a kv row through the prepared upsert with the crc the read
// path expects, or with an explicit bad one when badCRC is set. Writing
// around ApplyBatch keeps these tests about the read contract only.
func rawPut(t *testing.T, db *DB, k []byte, tag, exp, gen int64, v []byte, badCRC bool) {
	t.Helper()
	crc := int64(rowCRC(k, tag, exp, gen, v))
	if badCRC {
		crc ^= 0xdead
	}
	s := db.st.kvPut
	for i, err := range []error{
		s.BindBlob(1, k), s.BindInt64(2, tag), s.BindInt64(3, exp),
		s.BindInt64(4, gen), s.BindBlob(5, v), s.BindInt64(6, crc),
	} {
		if err != nil {
			t.Fatalf("bind %d: %v", i+1, err)
		}
	}
	if s.Step() {
		t.Fatalf("put %q returned a row", k)
	}
	if err := s.Err(); err != nil {
		t.Fatalf("put %q: %v", k, err)
	}
	if err := s.Reset(); err != nil {
		t.Fatalf("put reset: %v", err)
	}
}

func openTest(t *testing.T) *DB {
	t.Helper()
	db, err := Open(filepath.Join(t.TempDir(), "s.sqlo1"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestGetContract(t *testing.T) {
	db := openTest(t)
	db.now = func() int64 { return 1000 }
	ctx := context.Background()

	rawPut(t, db, []byte("live"), recordTag, 0, 7, []byte("v1"), false)
	rawPut(t, db, []byte("timed"), recordTag, 2000, 1, []byte("v2"), false)
	rawPut(t, db, []byte("gone"), recordTag, 1000, 1, []byte("v3"), false)
	rawPut(t, db, []byte("alien"), 5, 0, 1, []byte("v4"), false)
	rawPut(t, db, []byte("rot"), recordTag, 0, 1, []byte("v5"), true)

	rec, err := db.Get(ctx, []byte("live"))
	if err != nil {
		t.Fatalf("live: %v", err)
	}
	if !bytes.Equal(rec.Value, []byte("v1")) || rec.Gen != 7 || rec.ExpireMs != 0 {
		t.Fatalf("live = %+v", rec)
	}
	rec, err = db.Get(ctx, []byte("timed"))
	if err != nil {
		t.Fatalf("timed (expiry in the future): %v", err)
	}
	if rec.ExpireMs != 2000 {
		t.Fatalf("timed exp = %d, want 2000", rec.ExpireMs)
	}
	if _, err := db.Get(ctx, []byte("gone")); !errors.Is(err, sqlo1.ErrNotFound) {
		t.Fatalf("expired row: err = %v, want ErrNotFound", err)
	}
	if _, err := db.Get(ctx, []byte("alien")); !errors.Is(err, sqlo1.ErrNotFound) {
		t.Fatalf("foreign tag row: err = %v, want ErrNotFound", err)
	}
	if _, err := db.Get(ctx, []byte("missing")); !errors.Is(err, sqlo1.ErrNotFound) {
		t.Fatalf("missing key: err = %v, want ErrNotFound", err)
	}
	if _, err := db.Get(ctx, []byte("rot")); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("corrupt row: err = %v, want ErrCorrupt", err)
	}

	// The returned record aliases nothing: mutating it does not change
	// what the next read sees.
	rec, _ = db.Get(ctx, []byte("live"))
	rec.Value[0] = 'X'
	again, _ := db.Get(ctx, []byte("live"))
	if !bytes.Equal(again.Value, []byte("v1")) {
		t.Fatalf("read after caller mutation = %q, want v1", again.Value)
	}
}

func TestBatchGetOrderAndMisses(t *testing.T) {
	db := openTest(t)
	db.now = func() int64 { return 1000 }
	ctx := context.Background()

	rawPut(t, db, []byte("a"), recordTag, 0, 1, []byte("va"), false)
	rawPut(t, db, []byte("c"), recordTag, 500, 1, []byte("vc"), false)

	recs, err := db.BatchGet(ctx, [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("a")})
	if err != nil {
		t.Fatalf("BatchGet: %v", err)
	}
	if len(recs) != 4 {
		t.Fatalf("len = %d, want 4", len(recs))
	}
	if !bytes.Equal(recs[0].Value, []byte("va")) || !bytes.Equal(recs[3].Value, []byte("va")) {
		t.Fatalf("hits = %+v", recs)
	}
	if recs[1].Key != nil {
		t.Fatalf("missing key slot has Key %q, want nil", recs[1].Key)
	}
	if recs[2].Key != nil {
		t.Fatalf("expired key slot has Key %q, want nil", recs[2].Key)
	}

	rawPut(t, db, []byte("bad"), recordTag, 0, 1, []byte("x"), true)
	if _, err := db.BatchGet(ctx, [][]byte{[]byte("a"), []byte("bad")}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("corrupt in batch: err = %v, want ErrCorrupt", err)
	}
}

func TestScanOrderGatingAndResume(t *testing.T) {
	db := openTest(t)
	db.now = func() int64 { return 1000 }
	ctx := context.Background()

	// More rows than one scan page, so the pagination path runs. The
	// zero-length key is legal and must be the first row out.
	var want []string
	rawPut(t, db, []byte(""), recordTag, 0, 1, []byte("empty"), false)
	want = append(want, "")
	for i := range 3 * scanPage / 2 {
		k := fmt.Sprintf("k%05d", i)
		switch i % 100 {
		case 17:
			rawPut(t, db, []byte(k), recordTag, 999, 1, []byte("dead"), false)
		case 43:
			rawPut(t, db, []byte(k), 9, 0, 1, []byte("alien"), false)
		default:
			rawPut(t, db, []byte(k), recordTag, 0, 1, []byte("v"), false)
			want = append(want, k)
		}
	}

	var got []string
	cur, err := db.Scan(ctx, nil, func(r sqlo1.Record) bool {
		got = append(got, string(r.Key))
		return true
	})
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if cur != nil {
		t.Fatalf("exhausted scan returned cursor %x", cur)
	}
	if len(got) != len(want) {
		t.Fatalf("scan visited %d records, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("scan[%d] = %q, want %q", i, got[i], want[i])
		}
	}

	// Stop partway, resume from the cursor, and the two halves must
	// stitch into the same sequence with no overlap and no gap.
	stopAt := len(want) / 3
	got = nil
	cur, err = db.Scan(ctx, nil, func(r sqlo1.Record) bool {
		got = append(got, string(r.Key))
		return len(got) < stopAt
	})
	if err != nil {
		t.Fatalf("partial scan: %v", err)
	}
	if cur == nil {
		t.Fatal("stopped scan returned nil cursor")
	}
	cur2, err := db.Scan(ctx, cur, func(r sqlo1.Record) bool {
		got = append(got, string(r.Key))
		return true
	})
	if err != nil {
		t.Fatalf("resumed scan: %v", err)
	}
	if cur2 != nil {
		t.Fatalf("resumed scan returned cursor %x", cur2)
	}
	if len(got) != len(want) {
		t.Fatalf("stitched scan visited %d records, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("stitched[%d] = %q, want %q", i, got[i], want[i])
		}
	}

	if _, err := db.Scan(ctx, sqlo1.Cursor{0xff, 'k'}, func(sqlo1.Record) bool { return true }); err == nil {
		t.Fatal("Scan accepted a cursor with an unknown tag")
	}
}

func TestScanVerifiesGatedRows(t *testing.T) {
	db := openTest(t)
	db.now = func() int64 { return 1000 }
	ctx := context.Background()

	rawPut(t, db, []byte("fine"), recordTag, 0, 1, []byte("v"), false)
	// Expired AND corrupt: gating must not skip verification.
	rawPut(t, db, []byte("rotting"), recordTag, 500, 1, []byte("v"), true)

	_, err := db.Scan(ctx, nil, func(sqlo1.Record) bool { return true })
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("scan over corrupt expired row: err = %v, want ErrCorrupt", err)
	}
}
