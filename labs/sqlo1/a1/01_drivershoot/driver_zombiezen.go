//go:build drv_zombiezen

package main

import (
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

const driverName = "zombiezen"

// The zombiezen adapter speaks the crawshaw-style API: one write
// connection with Prep-cached statements, readers as their own
// connections. No database/sql anywhere, which is the point of this
// candidate.
type zenDB struct {
	conn     *sqlite.Conn
	path     string
	cacheKiB int
}

func openShootDB(path string, pageSize, cacheKiB int) (shootDB, error) {
	// No OpenWAL flag here: it writes the database header at open,
	// before page_size can run, and the header page size sticks at the
	// default. WAL mode arrives with writerPragmas instead.
	conn, err := sqlite.OpenConn(path, sqlite.OpenReadWrite|sqlite.OpenCreate)
	if err != nil {
		return nil, err
	}
	for _, p := range append(createPragmas(pageSize), writerPragmas(cacheKiB)...) {
		if err := sqlitex.ExecuteTransient(conn, p, nil); err != nil {
			conn.Close()
			return nil, err
		}
	}
	if err := sqlitex.ExecuteScript(conn, schemaSQL, nil); err != nil {
		conn.Close()
		return nil, err
	}
	return &zenDB{conn: conn, path: path, cacheKiB: cacheKiB}, nil
}

func (z *zenDB) close() error { return z.conn.Close() }

func zenGet(conn *sqlite.Conn, sql string, key []byte) (int, bool, error) {
	stmt := conn.Prep(sql)
	stmt.BindBytes(1, key)
	ok, err := stmt.Step()
	n := 0
	if ok {
		n = stmt.ColumnLen(0)
	}
	if rerr := stmt.Reset(); err == nil {
		err = rerr
	}
	return n, ok, err
}

func (z *zenDB) get(key []byte) (int, bool, error) {
	return zenGet(z.conn, getSQL, key)
}

func (z *zenDB) setOne(key, val []byte) error {
	stmt := z.conn.Prep(setSQL)
	stmt.BindBytes(1, key)
	stmt.BindInt64(2, 1)
	stmt.BindBytes(3, val)
	_, err := stmt.Step()
	if rerr := stmt.Reset(); err == nil {
		err = rerr
	}
	return err
}

func (z *zenDB) set(key, val []byte) error { return z.setOne(key, val) }

func (z *zenDB) drain(keys, vals [][]byte) error {
	if err := sqlitex.ExecuteTransient(z.conn, "BEGIN IMMEDIATE", nil); err != nil {
		return err
	}
	for i := range keys {
		if err := z.setOne(keys[i], vals[i]); err != nil {
			sqlitex.ExecuteTransient(z.conn, "ROLLBACK", nil)
			return err
		}
	}
	return sqlitex.ExecuteTransient(z.conn, "COMMIT", nil)
}

func (z *zenDB) helemGet(k, f []byte) (int, bool, error) {
	stmt := z.conn.Prep(helemGetSQL)
	stmt.BindBytes(1, k)
	stmt.BindBytes(2, f)
	ok, err := stmt.Step()
	n := 0
	if ok {
		n = stmt.ColumnLen(0)
	}
	if rerr := stmt.Reset(); err == nil {
		err = rerr
	}
	return n, ok, err
}

func (z *zenDB) helemDrain(k []byte, fs, vs [][]byte) error {
	if err := sqlitex.ExecuteTransient(z.conn, "BEGIN IMMEDIATE", nil); err != nil {
		return err
	}
	for i := range fs {
		stmt := z.conn.Prep(helemSetSQL)
		stmt.BindBytes(1, k)
		stmt.BindBytes(2, fs[i])
		stmt.BindBytes(3, vs[i])
		if _, err := stmt.Step(); err != nil {
			stmt.Reset()
			sqlitex.ExecuteTransient(z.conn, "ROLLBACK", nil)
			return err
		}
		if err := stmt.Reset(); err != nil {
			sqlitex.ExecuteTransient(z.conn, "ROLLBACK", nil)
			return err
		}
	}
	return sqlitex.ExecuteTransient(z.conn, "COMMIT", nil)
}

func (z *zenDB) checkpoint() error {
	return sqlitex.ExecuteTransient(z.conn, "PRAGMA wal_checkpoint(TRUNCATE)", nil)
}

func (z *zenDB) step() error {
	stmt := z.conn.Prep(stepSQL)
	stmt.BindInt64(1, 1)
	_, err := stmt.Step()
	if rerr := stmt.Reset(); err == nil {
		err = rerr
	}
	return err
}

type zenReader struct{ conn *sqlite.Conn }

func (z *zenDB) newReader() (shootReader, error) {
	conn, err := sqlite.OpenConn(z.path, sqlite.OpenReadOnly)
	if err != nil {
		return nil, err
	}
	for _, p := range readerPragmas(z.cacheKiB) {
		if err := sqlitex.ExecuteTransient(conn, p, nil); err != nil {
			conn.Close()
			return nil, err
		}
	}
	return &zenReader{conn: conn}, nil
}

func (r *zenReader) get(key []byte) (int, bool, error) {
	return zenGet(r.conn, getSQL, key)
}

func (r *zenReader) close() error { return r.conn.Close() }
