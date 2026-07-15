package sqlo1a

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/ncruces/go-sqlite3"
)

func TestReapExpiredRoots(t *testing.T) {
	ctx := context.Background()
	db := openTest(t)
	db.now = func() int64 { return 1000 }

	rawPut(t, db, []byte("live"), recordTag, 0, 0, []byte("v"), false)
	rawPut(t, db, []byte("later"), recordTag, 5000, 0, []byte("v"), false)
	rawPut(t, db, []byte("gone1"), recordTag, 1000, 0, []byte("v"), false)
	rawPut(t, db, []byte("gone2"), recordTag, 999, 0, []byte("v"), false)
	// Elem rows under an expiring root and a surviving one. exp 0 keeps
	// the field reaper away; this test is about the root sweep.
	for _, k := range []string{"gone1", "later"} {
		execSQL(t, db, `INSERT INTO helem (k, f, v, exp) VALUES (?1, 'f', 'x', 0)`,
			func(s *sqlite3.Stmt) error { return s.BindBlob(1, []byte(k)) })
	}
	execSQL(t, db, `INSERT INTO selem (k, m) VALUES (?1, 'm')`,
		func(s *sqlite3.Stmt) error { return s.BindBlob(1, []byte("gone1")) })

	roots, fields, err := db.Reap(ctx, 0)
	if err != nil {
		t.Fatalf("Reap: %v", err)
	}
	if roots != 2 || fields != 0 {
		t.Fatalf("reaped %d roots, %d fields, want 2 and 0", roots, fields)
	}
	for k, want := range map[string]int64{"live": 1, "later": 1, "gone1": 0, "gone2": 0} {
		if n := countWhereKey(t, db, `SELECT count(*) FROM kv WHERE k = ?1`, []byte(k)); n != want {
			t.Errorf("kv rows for %s = %d, want %d", k, n, want)
		}
	}
	if n := countWhereKey(t, db, `SELECT count(*) FROM helem WHERE k = ?1`, []byte("gone1")); n != 0 {
		t.Errorf("helem rows under reaped root = %d, want 0", n)
	}
	if n := countWhereKey(t, db, `SELECT count(*) FROM selem WHERE k = ?1`, []byte("gone1")); n != 0 {
		t.Errorf("selem rows under reaped root = %d, want 0", n)
	}
	if n := countWhereKey(t, db, `SELECT count(*) FROM helem WHERE k = ?1`, []byte("later")); n != 1 {
		t.Errorf("helem rows under live root = %d, want 1", n)
	}

	roots, fields, err = db.Reap(ctx, 0)
	if err != nil || roots != 0 || fields != 0 {
		t.Fatalf("second pass reaped %d roots, %d fields, %v; want all zero", roots, fields, err)
	}
}

func TestReapHonorsLimit(t *testing.T) {
	ctx := context.Background()
	db := openTest(t)
	db.now = func() int64 { return 1000 }
	for i := range 5 {
		rawPut(t, db, fmt.Appendf(nil, "k%d", i), recordTag, 10, 0, []byte("v"), false)
	}
	for _, want := range []int{2, 2, 1, 0} {
		roots, _, err := db.Reap(ctx, 2)
		if err != nil {
			t.Fatalf("Reap: %v", err)
		}
		if roots != want {
			t.Fatalf("pass reaped %d roots, want %d", roots, want)
		}
	}
}

func TestReapHelemFields(t *testing.T) {
	ctx := context.Background()
	db := openTest(t)
	db.now = func() int64 { return 1000 }
	rawPut(t, db, []byte("h"), recordTag, 0, 0, []byte("root"), false)
	for _, f := range []struct {
		name string
		exp  int64
	}{
		{"due-old", 999}, {"due-now", 1000}, {"live", 5000}, {"never", 0},
	} {
		execSQL(t, db, `INSERT INTO helem (k, f, v, exp) VALUES (?1, ?2, 'x', ?3)`,
			func(s *sqlite3.Stmt) error {
				if err := s.BindBlob(1, []byte("h")); err != nil {
					return err
				}
				if err := s.BindText(2, f.name); err != nil {
					return err
				}
				return s.BindInt64(3, f.exp)
			})
	}

	roots, fields, err := db.Reap(ctx, 0)
	if err != nil {
		t.Fatalf("Reap: %v", err)
	}
	if roots != 0 || fields != 2 {
		t.Fatalf("reaped %d roots, %d fields, want 0 and 2", roots, fields)
	}
	if n := countWhereKey(t, db, `SELECT count(*) FROM helem WHERE k = ?1`, []byte("h")); n != 2 {
		t.Fatalf("surviving fields = %d, want 2 (live and never)", n)
	}
	if n := countWhereKey(t, db, `SELECT count(*) FROM kv WHERE k = ?1`, []byte("h")); n != 1 {
		t.Fatalf("field reap touched the root row")
	}
}

func TestReapFieldLimit(t *testing.T) {
	ctx := context.Background()
	db := openTest(t)
	db.now = func() int64 { return 1000 }
	for i := range 3 {
		execSQL(t, db, `INSERT INTO helem (k, f, v, exp) VALUES ('h', ?1, 'x', 5)`,
			func(s *sqlite3.Stmt) error { return s.BindText(1, fmt.Sprintf("f%d", i)) })
	}
	for _, want := range []int{2, 1, 0} {
		_, fields, err := db.Reap(ctx, 2)
		if err != nil {
			t.Fatalf("Reap: %v", err)
		}
		if fields != want {
			t.Fatalf("pass reaped %d fields, want %d", fields, want)
		}
	}
}

// The milestone line is "against the partial indexes"; this pins it so a
// schema or statement change that silently degrades the reaper to a full
// table scan fails loudly.
func TestReapPlansUsePartialIndexes(t *testing.T) {
	db := openTest(t)
	for _, tc := range []struct{ sql, index string }{
		{sqlKVExpired, "kv_exp"},
		{sqlHElemReap, "helem_exp"},
	} {
		plan := queryPlan(t, db, tc.sql)
		if !strings.Contains(plan, tc.index) {
			t.Errorf("plan for %q does not use %s:\n%s", tc.sql, tc.index, plan)
		}
	}
}

func queryPlan(t *testing.T, db *DB, sql string) string {
	t.Helper()
	stmt, _, err := db.conn.Prepare("EXPLAIN QUERY PLAN " + sql)
	if err != nil {
		t.Fatalf("prepare plan: %v", err)
	}
	defer stmt.Close()
	var b strings.Builder
	for stmt.Step() {
		b.WriteString(stmt.ColumnText(3))
		b.WriteByte('\n')
	}
	if err := stmt.Err(); err != nil {
		t.Fatalf("step plan: %v", err)
	}
	return b.String()
}
