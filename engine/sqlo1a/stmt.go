package sqlo1a

import (
	"github.com/ncruces/go-sqlite3"
)

// This file is the statement catalog: every query sqlo1a runs lives here
// as a named const, and each connection prepares the catalog once at open.
// The import-boundary script greps that no query verb appears in a string
// literal anywhere else in the package (tests excepted, they poke fixtures)
// and that no SQL is ever assembled with Sprintf. Query building is how a
// storage engine grows a per-request parse tax and an injection surface at
// the same time; a closed catalog makes both impossible by construction.
//
// The catalog grows one slice at a time. What is here now covers schema
// introspection and the kv paths the read and ApplyBatch slices consume;
// elem-table statements land with the slices that speak them.
const (
	// sqlSchemaTables lists user tables for the ensureSchema guard.
	sqlSchemaTables = `SELECT name FROM sqlite_schema WHERE type = 'table' AND name NOT LIKE 'sqlite_%'`

	// sqlKVGet fetches one row for Get and BatchGet. Column order matches
	// the schema: type tag, expiry, rootgen, value, crc.
	sqlKVGet = `SELECT t, exp, gen, v, crc FROM kv WHERE k = ?1`

	// sqlKVPut is the ApplyBatch upsert. A drain batch replays after a
	// crash, so a plain INSERT would conflict on the second pass; the
	// upsert makes every op idempotent at the row level.
	sqlKVPut = `INSERT INTO kv (k, t, exp, gen, v, crc) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
		ON CONFLICT (k) DO UPDATE SET t = excluded.t, exp = excluded.exp,
		gen = excluded.gen, v = excluded.v, crc = excluded.crc`

	// sqlKVDel removes one key for ApplyBatch delete ops. Deleting an
	// absent key is a no-op, which is exactly the replay semantics the
	// exactly-once contract wants.
	sqlKVDel = `DELETE FROM kv WHERE k = ?1`

	// sqlKVScan walks kv in key order from an exclusive cursor. The
	// cursor is the last key of the previous page, so k > ?1 resumes
	// without overlap and an empty blob starts from the beginning.
	sqlKVScan = `SELECT k, t, exp, gen, v, crc FROM kv WHERE k > ?1 ORDER BY k LIMIT ?2`
)

// stmts is one connection's prepared form of the catalog. Prepared eagerly
// when the connection joins the store, so no request ever pays SQL
// compilation, and finalized before the connection closes because ncruces
// refuses to close a connection with live statements.
type stmts struct {
	kvGet  *sqlite3.Stmt
	kvPut  *sqlite3.Stmt
	kvDel  *sqlite3.Stmt
	kvScan *sqlite3.Stmt

	all []*sqlite3.Stmt
}

func prepareStmts(conn *sqlite3.Conn) (*stmts, error) {
	s := &stmts{}
	for _, p := range []struct {
		sql string
		dst **sqlite3.Stmt
	}{
		{sqlKVGet, &s.kvGet},
		{sqlKVPut, &s.kvPut},
		{sqlKVDel, &s.kvDel},
		{sqlKVScan, &s.kvScan},
	} {
		stmt, _, err := conn.Prepare(p.sql)
		if err != nil {
			s.close()
			return nil, err
		}
		*p.dst = stmt
		s.all = append(s.all, stmt)
	}
	return s, nil
}

// close finalizes every prepared statement. First error wins but every
// statement still gets its Close, so one failure cannot strand the rest
// and block the connection from closing.
func (s *stmts) close() error {
	var first error
	for _, stmt := range s.all {
		if err := stmt.Close(); err != nil && first == nil {
			first = err
		}
	}
	s.all = nil
	return first
}
