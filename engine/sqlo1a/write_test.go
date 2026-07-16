package sqlo1a

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
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

// TestApplyBatchRootRecords: a Root op lands under rootTag, reads back
// with the flag through Get and Scan, and a Root op carrying a seam gen
// rejects its whole batch with the transaction rolled back.
func TestApplyBatchRootRecords(t *testing.T) {
	db := openTest(t)
	ctx := context.Background()

	err := db.ApplyBatch(ctx, &sqlo1.DrainBatch{Seq: 1, Ops: []sqlo1.Op{
		{Rec: sqlo1.Record{Key: []byte("wide"), Value: []byte("root payload"), Root: true}},
		{Rec: sqlo1.Record{Key: []byte("plain"), Value: []byte("v")}},
	}})
	if err != nil {
		t.Fatalf("ApplyBatch: %v", err)
	}
	rec, err := db.Get(ctx, []byte("wide"))
	if err != nil {
		t.Fatalf("get wide: %v", err)
	}
	if !rec.Root || rec.Gen != 0 || !bytes.Equal(rec.Value, []byte("root payload")) {
		t.Fatalf("wide = %+v", rec)
	}
	if rec, err = db.Get(ctx, []byte("plain")); err != nil || rec.Root {
		t.Fatalf("plain = %+v, err %v", rec, err)
	}
	rootSeen := false
	if _, err := db.Scan(ctx, nil, func(r sqlo1.Record) bool {
		if string(r.Key) == "wide" {
			rootSeen = r.Root
		} else if r.Root {
			t.Fatalf("scan flagged %q as root", r.Key)
		}
		return true
	}); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if !rootSeen {
		t.Fatal("scan dropped the root flag")
	}

	err = db.ApplyBatch(ctx, &sqlo1.DrainBatch{Seq: 2, Ops: []sqlo1.Op{
		{Rec: sqlo1.Record{Key: []byte("early"), Value: []byte("x")}},
		{Rec: sqlo1.Record{Key: []byte("bad"), Value: []byte("p"), Root: true, Gen: 3}},
	}})
	if err == nil {
		t.Fatal("root op with a seam gen applied")
	}
	err = db.ApplyBatch(ctx, &sqlo1.DrainBatch{Seq: 2, Ops: []sqlo1.Op{
		{Rec: sqlo1.Record{Key: []byte("bare"), Value: []byte("p"), Delta: true}},
	}})
	if err == nil {
		t.Fatal("delta op without the root flag applied")
	}
	for _, k := range []string{"early", "bad", "bare"} {
		if _, err := db.Get(ctx, []byte(k)); !errors.Is(err, sqlo1.ErrNotFound) {
			t.Fatalf("rejected batch left %q behind: %v", k, err)
		}
	}
	if hw := db.Stats().HighWater; hw != 1 {
		t.Fatalf("HighWater after rejected batches = %d, want 1", hw)
	}

	// Delta is advisory and Track A has no frame to elide: a delta
	// root stores exactly like any root.
	err = db.ApplyBatch(ctx, &sqlo1.DrainBatch{Seq: 2, Ops: []sqlo1.Op{
		{Rec: sqlo1.Record{Key: []byte("wide2"), Value: []byte("delta image"), Root: true, Delta: true}},
	}})
	if err != nil {
		t.Fatalf("delta root refused: %v", err)
	}
	if rec, err := db.Get(ctx, []byte("wide2")); err != nil || !rec.Root || !bytes.Equal(rec.Value, []byte("delta image")) {
		t.Fatalf("delta root read back %+v, err %v", rec, err)
	}
}

// genRow asserts the row under GenKey(rooth) is a genTag row holding
// want, through the raw read the bump apply itself uses.
func genRow(t *testing.T, db *DB, rooth uint64, want int64) {
	t.Helper()
	tag, gen, found, err := db.rowTagGenLocked(sqlo1.GenKey(rooth))
	if err != nil {
		t.Fatalf("gen row for %#x: %v", rooth, err)
	}
	if !found || tag != genTag || gen != want {
		t.Fatalf("gen row for %#x: tag %d gen %d found %v, want genTag gen %d", rooth, tag, gen, found, want)
	}
}

// TestApplyBatchBumps: seam Bumps land as genTag rows in the batch
// transaction, invisible to reads, monotonic, replayed exactly once,
// persistent across a reopen, and both a zero-generation bump and a
// user record squatting on the GenKey bytes reject the whole batch.
func TestApplyBatchBumps(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bumps.sqlo1")
	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	ctx := context.Background()

	err = db.ApplyBatch(ctx, &sqlo1.DrainBatch{
		Seq:   1,
		Ops:   []sqlo1.Op{{Rec: sqlo1.Record{Key: []byte("wide"), Value: []byte("img"), Root: true}}},
		Bumps: []sqlo1.Bump{{Rooth: 7, NewGen: 2}},
	})
	if err != nil {
		t.Fatalf("ApplyBatch: %v", err)
	}
	genRow(t, db, 7, 2)
	if _, err := db.Get(ctx, sqlo1.GenKey(7)); !errors.Is(err, sqlo1.ErrNotFound) {
		t.Fatalf("gen row leaked through Get: %v", err)
	}
	if _, err := db.Scan(ctx, nil, func(r sqlo1.Record) bool {
		if bytes.Equal(r.Key, sqlo1.GenKey(7)) {
			t.Fatal("gen row leaked through Scan")
		}
		return true
	}); err != nil {
		t.Fatalf("scan: %v", err)
	}

	// Lower bump in a later batch is a monotonic no-op; a replayed Seq
	// applies nothing at all.
	if err := db.ApplyBatch(ctx, &sqlo1.DrainBatch{Seq: 2, Bumps: []sqlo1.Bump{{Rooth: 7, NewGen: 1}}}); err != nil {
		t.Fatalf("stale bump: %v", err)
	}
	genRow(t, db, 7, 2)
	if err := db.ApplyBatch(ctx, &sqlo1.DrainBatch{Seq: 2, Bumps: []sqlo1.Bump{{Rooth: 7, NewGen: 9}}}); err != nil {
		t.Fatalf("replayed batch: %v", err)
	}
	genRow(t, db, 7, 2)

	// A zero-generation bump rolls the whole batch back.
	err = db.ApplyBatch(ctx, &sqlo1.DrainBatch{
		Seq:   3,
		Ops:   []sqlo1.Op{{Rec: sqlo1.Record{Key: []byte("early"), Value: []byte("x")}}},
		Bumps: []sqlo1.Bump{{Rooth: 7, NewGen: 0}},
	})
	if err == nil {
		t.Fatal("bump to generation 0 applied")
	}
	if _, err := db.Get(ctx, []byte("early")); !errors.Is(err, sqlo1.ErrNotFound) {
		t.Fatalf("rejected batch left an op behind: %v", err)
	}

	// A user record occupying the GenKey bytes is the loud aliasing
	// failure, not a silent clobber, and it rolls the batch back too.
	rawPut(t, db, sqlo1.GenKey(9), recordTag, 0, 0, []byte("squatter"), false)
	err = db.ApplyBatch(ctx, &sqlo1.DrainBatch{
		Seq:   3,
		Ops:   []sqlo1.Op{{Rec: sqlo1.Record{Key: []byte("early"), Value: []byte("x")}}},
		Bumps: []sqlo1.Bump{{Rooth: 9, NewGen: 1}},
	})
	if err == nil {
		t.Fatal("bump clobbered a non-generation row")
	}
	if _, err := db.Get(ctx, []byte("early")); !errors.Is(err, sqlo1.ErrNotFound) {
		t.Fatalf("aliasing reject left an op behind: %v", err)
	}
	if hw := db.Stats().HighWater; hw != 2 {
		t.Fatalf("HighWater after rejects = %d, want 2", hw)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	db, err = Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db.Close()
	genRow(t, db, 7, 2)
	if err := db.ApplyBatch(ctx, &sqlo1.DrainBatch{Seq: 3, Bumps: []sqlo1.Bump{{Rooth: 7, NewGen: 3}}}); err != nil {
		t.Fatalf("bump after reopen: %v", err)
	}
	genRow(t, db, 7, 3)
}
