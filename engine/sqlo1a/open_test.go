package sqlo1a

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/ncruces/go-sqlite3"
)

// headerPageSize reads the page size from the SQLite file header (bytes
// 16-17, big endian). This is the same assertion that caught two real
// creation-pragma bugs in the drivershoot lab: a pragma that silently
// fails to land leaves the default in the header, not an error anywhere.
func headerPageSize(t *testing.T, path string) int {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if len(b) < 18 {
		t.Fatalf("%s: %d bytes, no header", path, len(b))
	}
	return int(binary.BigEndian.Uint16(b[16:18]))
}

func TestOpenFreezesHeader(t *testing.T) {
	path := filepath.Join(t.TempDir(), "a.sqlo1")
	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := db.conn.Exec("CREATE TABLE probe (k BLOB PRIMARY KEY) WITHOUT ROWID"); err != nil {
		t.Fatalf("create: %v", err)
	}
	mode, err := pragmaText(db.conn, "PRAGMA journal_mode")
	if err != nil {
		t.Fatalf("journal_mode: %v", err)
	}
	if mode != "wal" {
		t.Fatalf("journal_mode = %q, want wal", mode)
	}
	av, err := pragmaInt(db.conn, "PRAGMA auto_vacuum")
	if err != nil {
		t.Fatalf("auto_vacuum: %v", err)
	}
	if av != 2 {
		t.Fatalf("auto_vacuum = %d, want 2 (INCREMENTAL)", av)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if got := headerPageSize(t, path); got != PageSize {
		t.Fatalf("header page size = %d, want %d", got, PageSize)
	}

	// Reopen the same file: the creation pragmas are no-ops and the check
	// passes.
	db, err = Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close after reopen: %v", err)
	}
}

func TestOpenRefusesForeignPageSize(t *testing.T) {
	path := filepath.Join(t.TempDir(), "b.sqlo1")
	conn, err := sqlite3.Open(path)
	if err != nil {
		t.Fatalf("raw open: %v", err)
	}
	for _, p := range []string{
		"PRAGMA page_size = 4096",
		"CREATE TABLE probe (k BLOB PRIMARY KEY) WITHOUT ROWID",
	} {
		if err := conn.Exec(p); err != nil {
			t.Fatalf("%s: %v", p, err)
		}
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("raw close: %v", err)
	}
	if got := headerPageSize(t, path); got != 4096 {
		t.Fatalf("fixture header page size = %d, want 4096", got)
	}
	if _, err := Open(path); err == nil {
		t.Fatal("Open accepted a 4096-page file; the freeze is 8192")
	}
}

func pragmaText(conn *sqlite3.Conn, sql string) (string, error) {
	stmt, _, err := conn.Prepare(sql)
	if err != nil {
		return "", err
	}
	defer stmt.Close()
	if !stmt.Step() {
		return "", stmt.Err()
	}
	return stmt.ColumnText(0), nil
}
