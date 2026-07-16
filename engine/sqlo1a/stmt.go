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

	// sqlKVScan walks kv in key order from an exclusive cursor, the last
	// key of the previous page, so pages resume without overlap.
	sqlKVScan = `SELECT k, t, exp, gen, v, crc FROM kv WHERE k > ?1 ORDER BY k LIMIT ?2`

	// sqlKVScanFirst starts a scan from the top. It exists because the
	// zero-length key is a legal Redis key and k > x'' would skip it; a
	// fresh scan must not need a cursor smaller than every key.
	sqlKVScanFirst = `SELECT k, t, exp, gen, v, crc FROM kv ORDER BY k LIMIT ?1`

	// sqlKVCount backs Stats.Keys. It counts every kv row including gated
	// ones, which is the honest INFO-level number until the reaper slice
	// keeps expired rows from accumulating.
	sqlKVCount = `SELECT count(*) FROM kv`

	// sqlMetaHW and sqlMetaSetHW read and move the drain high-water mark.
	// The row exists from schema creation (seeded in schemaSQL), so the
	// read has no missing-row case and the write is always an UPDATE.
	sqlMetaHW    = `SELECT hw FROM meta WHERE id = 0`
	sqlMetaSetHW = `UPDATE meta SET hw = ?1 WHERE id = 0`

	// sqlMetaLease and sqlMetaSetLease read and move the rooth mint-lease
	// mark, the seam Minter capability's durable counter. Same single-row
	// discipline as the high-water mark: seeded at schema creation, so the
	// write is always an UPDATE.
	sqlMetaLease    = `SELECT lease FROM meta WHERE id = 0`
	sqlMetaSetLease = `UPDATE meta SET lease = ?1 WHERE id = 0`

	// The elem-root deletes are the gen-sweep shape ApplyBatch batches
	// into a drain transaction when a root key is deleted: every elem
	// table clears the root's rows by primary-key prefix. Pacing against
	// large collections is the per-type slices' problem, priced by the
	// batchdrain lab; at the flat seam these are index seeks on empty
	// tables.
	// sqlKVExpired feeds the reaper the next batch of due roots. The
	// explicit exp > 0 term is what lets the planner use the kv_exp
	// partial index; exp <= now matches the read path's gate boundary
	// exactly, so a row this returns is one Get already refuses.
	sqlKVExpired = `SELECT k FROM kv WHERE exp > 0 AND exp <= ?1 LIMIT ?2`

	// sqlHElemReap drops due hash fields through the helem_exp partial
	// index in one bounded statement. The row-value subquery stands in
	// for DELETE ... LIMIT, which needs a nonstandard compile-time flag.
	sqlHElemReap = `DELETE FROM helem WHERE (k, f) IN
		(SELECT k, f FROM helem WHERE exp > 0 AND exp <= ?1 LIMIT ?2)`

	sqlHElemDelRoot = `DELETE FROM helem WHERE k = ?1`
	sqlSElemDelRoot = `DELETE FROM selem WHERE k = ?1`
	sqlZMemDelRoot  = `DELETE FROM zmem WHERE k = ?1`
	sqlLElemDelRoot = `DELETE FROM lelem WHERE k = ?1`
	sqlChunkDelRoot = `DELETE FROM chunk WHERE k = ?1`
	sqlXEntDelRoot  = `DELETE FROM xent WHERE k = ?1`
	sqlXPelDelRoot  = `DELETE FROM xpel WHERE k = ?1`
)

// stmts is one connection's prepared form of the catalog. Prepared eagerly
// when the connection joins the store, so no request ever pays SQL
// compilation, and finalized before the connection closes because ncruces
// refuses to close a connection with live statements.
type stmts struct {
	kvGet        *sqlite3.Stmt
	kvPut        *sqlite3.Stmt
	kvDel        *sqlite3.Stmt
	kvScan       *sqlite3.Stmt
	kvScanFirst  *sqlite3.Stmt
	kvCount      *sqlite3.Stmt
	kvExpired    *sqlite3.Stmt
	helemReap    *sqlite3.Stmt
	metaHW       *sqlite3.Stmt
	metaSetHW    *sqlite3.Stmt
	metaLease    *sqlite3.Stmt
	metaSetLease *sqlite3.Stmt

	// elemDelRoot holds the per-elem-table root deletes ApplyBatch loops
	// over for a deleted key, in schemaSQL declaration order.
	elemDelRoot []*sqlite3.Stmt

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
		{sqlKVScanFirst, &s.kvScanFirst},
		{sqlKVCount, &s.kvCount},
		{sqlKVExpired, &s.kvExpired},
		{sqlHElemReap, &s.helemReap},
		{sqlMetaHW, &s.metaHW},
		{sqlMetaSetHW, &s.metaSetHW},
		{sqlMetaLease, &s.metaLease},
		{sqlMetaSetLease, &s.metaSetLease},
	} {
		stmt, _, err := conn.Prepare(p.sql)
		if err != nil {
			s.close()
			return nil, err
		}
		*p.dst = stmt
		s.all = append(s.all, stmt)
	}
	for _, sql := range []string{
		sqlHElemDelRoot, sqlSElemDelRoot, sqlZMemDelRoot, sqlLElemDelRoot,
		sqlChunkDelRoot, sqlXEntDelRoot, sqlXPelDelRoot,
	} {
		stmt, _, err := conn.Prepare(sql)
		if err != nil {
			s.close()
			return nil, err
		}
		s.elemDelRoot = append(s.elemDelRoot, stmt)
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
