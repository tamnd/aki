package sqlo1a

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/ncruces/go-sqlite3"
	"github.com/tamnd/aki/engine/sqlo1"
)

func execSQL(t *testing.T, db *DB, sql string, bind func(*sqlite3.Stmt) error) {
	t.Helper()
	stmt, _, err := db.conn.Prepare(sql)
	if err != nil {
		t.Fatalf("prepare %s: %v", sql, err)
	}
	defer stmt.Close()
	if bind != nil {
		if err := bind(stmt); err != nil {
			t.Fatalf("bind %s: %v", sql, err)
		}
	}
	stmt.Step()
	if err := stmt.Err(); err != nil {
		t.Fatalf("step %s: %v", sql, err)
	}
}

func countWhereKey(t *testing.T, db *DB, sql string, k []byte) int64 {
	t.Helper()
	stmt, _, err := db.conn.Prepare(sql)
	if err != nil {
		t.Fatalf("prepare %s: %v", sql, err)
	}
	defer stmt.Close()
	if err := stmt.BindBlob(1, k); err != nil {
		t.Fatalf("bind %s: %v", sql, err)
	}
	if !stmt.Step() {
		t.Fatalf("step %s: %v", sql, stmt.Err())
	}
	return stmt.ColumnInt64(0)
}

func TestApplyBatchRoundTrip(t *testing.T) {
	db := openTest(t)
	ctx := context.Background()

	err := db.ApplyBatch(ctx, &sqlo1.DrainBatch{Seq: 5, Ops: []sqlo1.Op{
		{Rec: sqlo1.Record{Key: []byte("a"), Value: []byte("va"), Gen: 2}},
		{Rec: sqlo1.Record{Key: []byte("b"), Value: []byte("vb"), ExpireMs: 1 << 60}},
		{Rec: sqlo1.Record{Key: []byte("c"), Value: []byte("vc")}},
		{Del: true, Rec: sqlo1.Record{Key: []byte("c")}},
	}})
	if err != nil {
		t.Fatalf("ApplyBatch: %v", err)
	}

	rec, err := db.Get(ctx, []byte("a"))
	if err != nil {
		t.Fatalf("get a: %v", err)
	}
	if !bytes.Equal(rec.Value, []byte("va")) || rec.Gen != 2 {
		t.Fatalf("a = %+v", rec)
	}
	if rec, err = db.Get(ctx, []byte("b")); err != nil || rec.ExpireMs != 1<<60 {
		t.Fatalf("b = %+v, err %v", rec, err)
	}
	if _, err := db.Get(ctx, []byte("c")); !errors.Is(err, sqlo1.ErrNotFound) {
		t.Fatalf("c after delete in same batch: err = %v, want ErrNotFound", err)
	}

	st := db.Stats()
	if st.HighWater != 5 {
		t.Fatalf("HighWater = %d, want 5", st.HighWater)
	}
	if st.Keys != 2 {
		t.Fatalf("Keys = %d, want 2", st.Keys)
	}
	if st.DiskBytes == 0 {
		t.Fatal("DiskBytes = 0 on a written store")
	}
}

func TestApplyBatchExactlyOnce(t *testing.T) {
	db := openTest(t)
	ctx := context.Background()

	put := func(seq int64, v string) error {
		return db.ApplyBatch(ctx, &sqlo1.DrainBatch{Seq: seq, Ops: []sqlo1.Op{
			{Rec: sqlo1.Record{Key: []byte("k"), Value: []byte(v)}},
		}})
	}
	if err := put(5, "first"); err != nil {
		t.Fatalf("seq 5: %v", err)
	}
	// A replayed batch at the mark and one below it must both be no-op
	// successes: recovery replays the aki WAL from the mark and the store
	// shrugs off what it already holds.
	if err := put(5, "replayed"); err != nil {
		t.Fatalf("replay at mark: %v", err)
	}
	if err := put(4, "older"); err != nil {
		t.Fatalf("replay below mark: %v", err)
	}
	rec, err := db.Get(ctx, []byte("k"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !bytes.Equal(rec.Value, []byte("first")) {
		t.Fatalf("value after replays = %q, want first", rec.Value)
	}
	if hw := db.Stats().HighWater; hw != 5 {
		t.Fatalf("HighWater after replays = %d, want 5", hw)
	}
	if err := put(6, "second"); err != nil {
		t.Fatalf("seq 6: %v", err)
	}
	if rec, _ = db.Get(ctx, []byte("k")); !bytes.Equal(rec.Value, []byte("second")) {
		t.Fatalf("value after seq 6 = %q, want second", rec.Value)
	}
}

func TestApplyBatchSweepsElemRows(t *testing.T) {
	db := openTest(t)
	ctx := context.Background()

	blobs := func(bs ...[]byte) func(*sqlite3.Stmt) error {
		return func(s *sqlite3.Stmt) error {
			for i, b := range bs {
				if err := s.BindBlob(i+1, b); err != nil {
					return err
				}
			}
			return nil
		}
	}
	for _, root := range [][]byte{[]byte("drop"), []byte("keep")} {
		execSQL(t, db, `INSERT INTO helem (k, f, v, exp) VALUES (?1, ?2, ?3, 0)`, blobs(root, []byte("f"), []byte("v")))
		execSQL(t, db, `INSERT INTO selem (k, m) VALUES (?1, ?2)`, blobs(root, []byte("m")))
		execSQL(t, db, `INSERT INTO zmem (k, m, s) VALUES (?1, ?2, 1.5)`, blobs(root, []byte("m")))
		execSQL(t, db, `INSERT INTO lelem (k, ord, v) VALUES (?1, 1, ?2)`, blobs(root, []byte("v")))
		execSQL(t, db, `INSERT INTO chunk (k, cid, v, pc) VALUES (?1, 1, ?2, 0)`, blobs(root, []byte("v")))
		execSQL(t, db, `INSERT INTO xent (k, ms, seq, data) VALUES (?1, 1, 1, ?2)`, blobs(root, []byte("d")))
		execSQL(t, db, `INSERT INTO xpel (k, grp, ms, seq, c, dc, dt) VALUES (?1, ?2, 1, 1, 0, 0, 0)`, blobs(root, []byte("g")))
	}

	err := db.ApplyBatch(ctx, &sqlo1.DrainBatch{Seq: 1, Ops: []sqlo1.Op{
		{Del: true, Rec: sqlo1.Record{Key: []byte("drop")}},
	}})
	if err != nil {
		t.Fatalf("ApplyBatch: %v", err)
	}

	for _, q := range []string{
		`SELECT count(*) FROM helem WHERE k = ?1`,
		`SELECT count(*) FROM selem WHERE k = ?1`,
		`SELECT count(*) FROM zmem WHERE k = ?1`,
		`SELECT count(*) FROM lelem WHERE k = ?1`,
		`SELECT count(*) FROM chunk WHERE k = ?1`,
		`SELECT count(*) FROM xent WHERE k = ?1`,
		`SELECT count(*) FROM xpel WHERE k = ?1`,
	} {
		if n := countWhereKey(t, db, q, []byte("drop")); n != 0 {
			t.Fatalf("%s left %d rows for the deleted root", q, n)
		}
		if n := countWhereKey(t, db, q, []byte("keep")); n != 1 {
			t.Fatalf("%s has %d rows for the surviving root, want 1", q, n)
		}
	}
}

func TestApplyBatchRetainsNoOpMemory(t *testing.T) {
	db := openTest(t)
	ctx := context.Background()

	key := []byte("arena-key")
	val := []byte("arena-val")
	if err := db.ApplyBatch(ctx, &sqlo1.DrainBatch{Seq: 1, Ops: []sqlo1.Op{
		{Rec: sqlo1.Record{Key: key, Value: val}},
	}}); err != nil {
		t.Fatalf("ApplyBatch: %v", err)
	}
	// The seam contract: the drain scheduler's arenas may be rewritten the
	// moment ApplyBatch returns. Scribble over the op memory and the store
	// must still hold the original bytes.
	for i := range key {
		key[i] = 'X'
	}
	for i := range val {
		val[i] = 'Y'
	}
	rec, err := db.Get(ctx, []byte("arena-key"))
	if err != nil {
		t.Fatalf("get after scribble: %v", err)
	}
	if !bytes.Equal(rec.Value, []byte("arena-val")) {
		t.Fatalf("value after scribble = %q, want arena-val", rec.Value)
	}
}
