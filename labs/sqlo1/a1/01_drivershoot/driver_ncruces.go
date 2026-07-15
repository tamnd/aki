//go:build drv_ncruces

package main

import "github.com/ncruces/go-sqlite3"

const driverName = "ncruces"

// The ncruces adapter runs real upstream SQLite compiled to WASM under
// wazero; every call crosses the WASM boundary, and the lab exists to
// price exactly that against the workload. Statements are prepared once
// and reused; readers are their own connections.
type ncrDB struct {
	conn     *sqlite3.Conn
	path     string
	cacheKiB int

	getStmt      *sqlite3.Stmt
	setStmt      *sqlite3.Stmt
	helemGetStmt *sqlite3.Stmt
	helemSetStmt *sqlite3.Stmt
	stepStmt     *sqlite3.Stmt
	stmts        []*sqlite3.Stmt
}

func openShootDB(path string, pageSize, cacheKiB int) (shootDB, error) {
	conn, err := sqlite3.Open(path)
	if err != nil {
		return nil, err
	}
	for _, p := range append(createPragmas(pageSize), writerPragmas(cacheKiB)...) {
		if err := conn.Exec(p); err != nil {
			conn.Close()
			return nil, err
		}
	}
	if err := conn.Exec(schemaSQL); err != nil {
		conn.Close()
		return nil, err
	}
	db := &ncrDB{conn: conn, path: path, cacheKiB: cacheKiB}
	for _, s := range []struct {
		dst **sqlite3.Stmt
		sql string
	}{
		{&db.getStmt, getSQL},
		{&db.setStmt, setSQL},
		{&db.helemGetStmt, helemGetSQL},
		{&db.helemSetStmt, helemSetSQL},
		{&db.stepStmt, stepSQL},
	} {
		stmt, _, err := conn.Prepare(s.sql)
		if err != nil {
			conn.Close()
			return nil, err
		}
		*s.dst = stmt
		db.stmts = append(db.stmts, stmt)
	}
	return db, nil
}

// close finalizes the prepared statements first; the driver refuses to
// close a connection that still holds them.
func (n *ncrDB) close() error {
	for _, stmt := range n.stmts {
		stmt.Close()
	}
	return n.conn.Close()
}

func (n *ncrDB) checkpoint() error {
	return n.conn.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
}

func ncrStep(stmt *sqlite3.Stmt) (int, bool, error) {
	ok := stmt.Step()
	ln := 0
	if ok {
		ln = len(stmt.ColumnRawBlob(0))
	}
	err := stmt.Err()
	if rerr := stmt.Reset(); err == nil {
		err = rerr
	}
	return ln, ok, err
}

func (n *ncrDB) get(key []byte) (int, bool, error) {
	if err := n.getStmt.BindBlob(1, key); err != nil {
		return 0, false, err
	}
	return ncrStep(n.getStmt)
}

func (n *ncrDB) setOne(key, val []byte) error {
	if err := n.setStmt.BindBlob(1, key); err != nil {
		return err
	}
	if err := n.setStmt.BindInt64(2, 1); err != nil {
		return err
	}
	if err := n.setStmt.BindBlob(3, val); err != nil {
		return err
	}
	_, _, err := ncrStep(n.setStmt)
	return err
}

func (n *ncrDB) set(key, val []byte) error { return n.setOne(key, val) }

func (n *ncrDB) drain(keys, vals [][]byte) error {
	txn, err := n.conn.BeginImmediate()
	if err != nil {
		return err
	}
	for i := range keys {
		if err := n.setOne(keys[i], vals[i]); err != nil {
			txn.Rollback()
			return err
		}
	}
	return txn.Commit()
}

func (n *ncrDB) helemGet(k, f []byte) (int, bool, error) {
	if err := n.helemGetStmt.BindBlob(1, k); err != nil {
		return 0, false, err
	}
	if err := n.helemGetStmt.BindBlob(2, f); err != nil {
		return 0, false, err
	}
	return ncrStep(n.helemGetStmt)
}

func (n *ncrDB) helemDrain(k []byte, fs, vs [][]byte) error {
	txn, err := n.conn.BeginImmediate()
	if err != nil {
		return err
	}
	for i := range fs {
		if err := n.helemSetStmt.BindBlob(1, k); err != nil {
			txn.Rollback()
			return err
		}
		if err := n.helemSetStmt.BindBlob(2, fs[i]); err != nil {
			txn.Rollback()
			return err
		}
		if err := n.helemSetStmt.BindBlob(3, vs[i]); err != nil {
			txn.Rollback()
			return err
		}
		if _, _, err := ncrStep(n.helemSetStmt); err != nil {
			txn.Rollback()
			return err
		}
	}
	return txn.Commit()
}

func (n *ncrDB) step() error {
	if err := n.stepStmt.BindInt64(1, 1); err != nil {
		return err
	}
	_, _, err := ncrStep(n.stepStmt)
	return err
}

type ncrReader struct {
	conn *sqlite3.Conn
	get1 *sqlite3.Stmt
}

func (n *ncrDB) newReader() (shootReader, error) {
	conn, err := sqlite3.OpenFlags(n.path, sqlite3.OPEN_READONLY)
	if err != nil {
		return nil, err
	}
	for _, p := range readerPragmas(n.cacheKiB) {
		if err := conn.Exec(p); err != nil {
			conn.Close()
			return nil, err
		}
	}
	stmt, _, err := conn.Prepare(getSQL)
	if err != nil {
		conn.Close()
		return nil, err
	}
	return &ncrReader{conn: conn, get1: stmt}, nil
}

func (r *ncrReader) get(key []byte) (int, bool, error) {
	if err := r.get1.BindBlob(1, key); err != nil {
		return 0, false, err
	}
	return ncrStep(r.get1)
}

func (r *ncrReader) close() error {
	r.get1.Close()
	return r.conn.Close()
}
