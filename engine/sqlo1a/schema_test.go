package sqlo1a

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/ncruces/go-sqlite3"
)

func TestOpenCreatesGenerationOne(t *testing.T) {
	path := filepath.Join(t.TempDir(), "a.sqlo1")
	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()
	ver, err := pragmaInt(db.conn, "PRAGMA user_version")
	if err != nil {
		t.Fatalf("user_version: %v", err)
	}
	if ver != schemaVersion {
		t.Fatalf("user_version = %d, want %d", ver, schemaVersion)
	}
	tables, err := listTables(db.conn)
	if err != nil {
		t.Fatalf("listTables: %v", err)
	}
	if got, want := strings.Join(tables, " "), strings.Join(schemaTables, " "); got != want {
		t.Fatalf("tables = [%s], want [%s]", got, want)
	}
	// The partial expiry indexes and the score index are part of the doc 02
	// contract, not decoration; a schema that lost them would still pass a
	// table-set check and then quietly scan.
	stmt, _, err := db.conn.Prepare(
		`SELECT name FROM sqlite_schema WHERE type = 'index' AND name NOT LIKE 'sqlite_%' ORDER BY name`)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	defer stmt.Close()
	var idx []string
	for stmt.Step() {
		idx = append(idx, stmt.ColumnText(0))
	}
	if err := stmt.Err(); err != nil {
		t.Fatalf("step: %v", err)
	}
	if got, want := strings.Join(idx, " "), "helem_exp kv_exp z_score"; got != want {
		t.Fatalf("indexes = [%s], want [%s]", got, want)
	}
}

func TestOpenRefusesForeignSQLiteFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "b.sqlo1")
	conn, err := sqlite3.Open(path)
	if err != nil {
		t.Fatalf("raw open: %v", err)
	}
	for _, p := range []string{
		"PRAGMA page_size = 8192",
		"CREATE TABLE somebody_elses (id INTEGER PRIMARY KEY)",
	} {
		if err := conn.Exec(p); err != nil {
			t.Fatalf("%s: %v", p, err)
		}
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("raw close: %v", err)
	}
	if _, err := Open(path); err == nil {
		t.Fatal("Open accepted a foreign SQLite file with no user_version stamp")
	}
}

func TestOpenRefusesFutureGeneration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c.sqlo1")
	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := db.conn.Exec("PRAGMA user_version = 99"); err != nil {
		t.Fatalf("bump: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	_, err = Open(path)
	if err == nil {
		t.Fatal("Open accepted schema generation 99")
	}
	if !strings.Contains(err.Error(), "generation 99") {
		t.Fatalf("error does not name the generation: %v", err)
	}
}

func TestOpenRefusesMissingTable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "d.sqlo1")
	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := db.conn.Exec("DROP TABLE xpel"); err != nil {
		t.Fatalf("drop: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if _, err := Open(path); err == nil {
		t.Fatal("Open accepted a store missing the xpel table")
	}
}

func TestOpenRefusesExtraTable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "e.sqlo1")
	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := db.conn.Exec("CREATE TABLE stray (k BLOB PRIMARY KEY) WITHOUT ROWID"); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if _, err := Open(path); err == nil {
		t.Fatal("Open accepted a store with a stray table")
	}
}
