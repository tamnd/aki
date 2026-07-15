package sqlo1a

import (
	"fmt"

	"github.com/ncruces/go-sqlite3"
)

// PageSize is the frozen SQLite page size, decided by the drivershoot
// verdict (results/sqlo1/drivershoot.md): 8192 beat 4096 on the bulk arms
// and gave up nothing, and 16384's extra bulk margin was not worth doubled
// worst-case cold-read and WAL bytes. A2's apragma lab re-sweeps this on
// the real store; until that verdict lands, 8192 is the only value Open
// accepts.
const PageSize = 8192

// DB is one sqlo1a connection to the store file. The A2 slices grow the
// schema, prepared statements, and the Store implementation on top of it;
// the freeze only pins which driver this is (ncruces, native API) and the
// shape of the file it produces.
type DB struct {
	conn *sqlite3.Conn
	st   *stmts
}

// Open opens or creates the store file at path. The creation-time pragmas
// run before anything writes the database header: page_size and auto_vacuum
// are header decisions, and the drivershoot lab showed what replaying them
// on live connections costs (a pooled reader queueing behind the writer for
// a lock on every open). On an existing file they are no-ops, and the
// page-size check afterwards refuses a file whose header disagrees with the
// freeze rather than silently running a different database than the one the
// verdict measured.
func Open(path string) (*DB, error) {
	conn, err := sqlite3.Open(path)
	if err != nil {
		return nil, err
	}
	for _, p := range []string{
		fmt.Sprintf("PRAGMA page_size = %d", PageSize),
		"PRAGMA auto_vacuum = INCREMENTAL",
		"PRAGMA journal_mode = WAL",
	} {
		if err := conn.Exec(p); err != nil {
			conn.Close()
			return nil, fmt.Errorf("sqlo1a: %s: %w", p, err)
		}
	}
	got, err := pragmaInt(conn, "PRAGMA page_size")
	if err != nil {
		conn.Close()
		return nil, err
	}
	if got != PageSize {
		conn.Close()
		return nil, fmt.Errorf("sqlo1a: %s has page_size %d, the freeze is %d", path, got, PageSize)
	}
	if err := ensureSchema(conn); err != nil {
		conn.Close()
		return nil, err
	}
	st, err := prepareStmts(conn)
	if err != nil {
		conn.Close()
		return nil, err
	}
	return &DB{conn: conn, st: st}, nil
}

// Close finalizes the prepared statements before closing the connection;
// ncruces treats a close with live statements as an error, and it is right
// to, so the ordering here is a contract, not a courtesy.
func (d *DB) Close() error {
	err := d.st.close()
	if cerr := d.conn.Close(); err == nil {
		err = cerr
	}
	return err
}

func pragmaInt(conn *sqlite3.Conn, sql string) (int, error) {
	stmt, _, err := conn.Prepare(sql)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()
	if !stmt.Step() {
		if err := stmt.Err(); err != nil {
			return 0, err
		}
		return 0, fmt.Errorf("sqlo1a: %s returned no row", sql)
	}
	return stmt.ColumnInt(0), nil
}
