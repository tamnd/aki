package sqlo1a

import (
	"fmt"
	"sort"
	"strings"

	"github.com/ncruces/go-sqlite3"
)

// schemaVersion is the sqlo1a schema generation, stamped into the SQLite
// user_version header field. Open refuses any nonzero value it does not
// know: a newer generation means a newer sqlo1a wrote this file, and
// guessing at its tables would corrupt it politely.
const schemaVersion = 1

// schemaSQL is doc 02 section 4 verbatim. All tables WITHOUT ROWID so the
// clustered PK is the only B-tree, keys BLOB, times INTEGER milliseconds.
// gen implements rootgen (collection DEL bumps gen in kv, a background
// sweep reclaims elem rows), crc is sqlo1's own crc32c row checksum since
// SQLite mainline has no page checksums. Inline collections below the doc
// 05-10 thresholds stay as one kv blob. meta is the single-row home of the
// drain high-water mark; it cannot live in kv because kv's primary key is
// the bare user key and any reserved key would be one user SET away from
// clobbering the mark. The seed insert makes the row's existence a schema
// invariant, so the high-water read never has a missing-row case.
const schemaSQL = `
CREATE TABLE kv (
  k BLOB PRIMARY KEY, t INTEGER, exp INTEGER, gen INTEGER,
  v BLOB, crc INTEGER
) WITHOUT ROWID;
CREATE INDEX kv_exp ON kv(exp) WHERE exp > 0;

CREATE TABLE helem (k BLOB, f BLOB, v BLOB, exp INTEGER,
  PRIMARY KEY (k, f)) WITHOUT ROWID;
CREATE INDEX helem_exp ON helem(exp) WHERE exp > 0;

CREATE TABLE selem (k BLOB, m BLOB, PRIMARY KEY (k, m)) WITHOUT ROWID;

CREATE TABLE zmem (k BLOB, m BLOB, s REAL, PRIMARY KEY (k, m)) WITHOUT ROWID;
CREATE INDEX z_score ON zmem(k, s, m);

CREATE TABLE lelem (k BLOB, ord INTEGER, v BLOB,
  PRIMARY KEY (k, ord)) WITHOUT ROWID;

CREATE TABLE chunk (k BLOB, cid INTEGER, v BLOB, pc INTEGER,
  PRIMARY KEY (k, cid)) WITHOUT ROWID;

CREATE TABLE xent (k BLOB, ms INTEGER, seq INTEGER, data BLOB,
  PRIMARY KEY (k, ms, seq)) WITHOUT ROWID;

CREATE TABLE xpel (k BLOB, grp BLOB, ms INTEGER, seq INTEGER,
  c INTEGER, dc INTEGER, dt INTEGER,
  PRIMARY KEY (k, grp, ms, seq)) WITHOUT ROWID;

CREATE TABLE meta (id INTEGER PRIMARY KEY CHECK (id = 0),
  hw INTEGER) WITHOUT ROWID;
INSERT INTO meta (id, hw) VALUES (0, 0);
`

// schemaTables is what a generation-1 store must contain, exactly. The
// guard checks the set both ways: a missing table means a truncated or
// hand-edited file, an extra one means something else has been writing
// into the store, and either way running on top of it hides the problem.
var schemaTables = []string{
	"chunk", "helem", "kv", "lelem", "meta", "selem", "xent", "xpel", "zmem",
}

// ensureSchema brings a freshly opened connection to the current schema
// generation or refuses. A brand new database (user_version 0, no tables)
// gets the DDL and the stamp in one transaction, so a crash mid-create
// leaves either nothing or a complete generation, never a half-schema.
// user_version 0 with tables present is some other SQLite file, not ours.
func ensureSchema(conn *sqlite3.Conn) error {
	ver, err := pragmaInt(conn, "PRAGMA user_version")
	if err != nil {
		return err
	}
	tables, err := listTables(conn)
	if err != nil {
		return err
	}
	switch {
	case ver == 0 && len(tables) == 0:
		txn, err := conn.BeginImmediate()
		if err != nil {
			return err
		}
		if err := conn.Exec(schemaSQL); err != nil {
			txn.Rollback()
			return fmt.Errorf("sqlo1a: create schema: %w", err)
		}
		if err := conn.Exec(fmt.Sprintf("PRAGMA user_version = %d", schemaVersion)); err != nil {
			txn.Rollback()
			return fmt.Errorf("sqlo1a: stamp user_version: %w", err)
		}
		return txn.Commit()
	case ver == 0:
		return fmt.Errorf("sqlo1a: database has %d tables but no user_version stamp; not an sqlo1a store", len(tables))
	case ver == schemaVersion:
		if got, want := strings.Join(tables, " "), strings.Join(schemaTables, " "); got != want {
			return fmt.Errorf("sqlo1a: generation %d store has tables [%s], want [%s]", ver, got, want)
		}
		return nil
	default:
		return fmt.Errorf("sqlo1a: store is schema generation %d, this build speaks %d", ver, schemaVersion)
	}
}

func listTables(conn *sqlite3.Conn) ([]string, error) {
	stmt, _, err := conn.Prepare(sqlSchemaTables)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	var names []string
	for stmt.Step() {
		names = append(names, stmt.ColumnText(0))
	}
	if err := stmt.Err(); err != nil {
		return nil, err
	}
	sort.Strings(names)
	return names, nil
}
