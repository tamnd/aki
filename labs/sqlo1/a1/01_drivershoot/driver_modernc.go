//go:build drv_modernc

package main

import (
	"database/sql"
	"fmt"
	"net/url"
	"strings"

	_ "modernc.org/sqlite"
)

const driverName = "modernc"

// The modernc adapter goes through database/sql because that is the
// driver's documented posture and the compatibility floor doc 02 names;
// the pool overhead and per-row Scan copies it pays are part of the
// measurement, not an accident. Pragmas ride the DSN so every pooled
// connection gets them.
type modDB struct {
	db           *sql.DB
	getStmt      *sql.Stmt
	setStmt      *sql.Stmt
	helemGetStmt *sql.Stmt
	helemSetStmt *sql.Stmt
	stepStmt     *sql.Stmt
	buf          sql.RawBytes
}

func modDSN(path string, ps []string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "file:%s", url.PathEscape(path))
	sep := "?"
	for _, p := range ps {
		// "PRAGMA name = value" becomes _pragma=name(value).
		rest := strings.TrimPrefix(p, "PRAGMA ")
		name, value, _ := strings.Cut(rest, " = ")
		fmt.Fprintf(&b, "%s_pragma=%s", sep, url.QueryEscape(fmt.Sprintf("%s(%s)", name, value)))
		sep = "&"
	}
	return b.String()
}

func openShootDB(path string, pageSize, cacheKiB int) (shootDB, error) {
	// Creation runs on its own single-connection handle with explicit
	// ordered Execs: the header pragmas must precede the WAL switch,
	// and DSN pragma order is not a contract. Keeping them off the
	// pooled DSN also matters; every pooled connection replaying
	// auto_vacuum is the read-pool collapse recorded in main.go.
	create, err := sql.Open("sqlite", modDSN(path, nil))
	if err != nil {
		return nil, err
	}
	create.SetMaxOpenConns(1)
	for _, p := range createPragmas(pageSize) {
		if _, err := create.Exec(p); err != nil {
			create.Close()
			return nil, err
		}
	}
	var mode string
	if err := create.QueryRow("PRAGMA journal_mode = WAL").Scan(&mode); err != nil {
		create.Close()
		return nil, err
	}
	if _, err := create.Exec(schemaSQL); err != nil {
		create.Close()
		return nil, err
	}
	if err := create.Close(); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", modDSN(path, writerPragmas(cacheKiB)))
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(64)
	m := &modDB{db: db}
	for _, s := range []struct {
		dst **sql.Stmt
		sql string
	}{
		{&m.getStmt, getSQL},
		{&m.setStmt, setSQL},
		{&m.helemGetStmt, helemGetSQL},
		{&m.helemSetStmt, helemSetSQL},
		{&m.stepStmt, stepSQL},
	} {
		stmt, err := db.Prepare(s.sql)
		if err != nil {
			db.Close()
			return nil, err
		}
		*s.dst = stmt
	}
	return m, nil
}

func (m *modDB) close() error { return m.db.Close() }

func modGet(stmt *sql.Stmt, args ...any) (int, bool, error) {
	var v []byte
	err := stmt.QueryRow(args...).Scan(&v)
	if err == sql.ErrNoRows {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return len(v), true, nil
}

func (m *modDB) get(key []byte) (int, bool, error) {
	return modGet(m.getStmt, key)
}

func (m *modDB) set(key, val []byte) error {
	_, err := m.setStmt.Exec(key, 1, val)
	return err
}

func (m *modDB) drain(keys, vals [][]byte) error {
	tx, err := m.db.Begin()
	if err != nil {
		return err
	}
	stmt := tx.Stmt(m.setStmt)
	for i := range keys {
		if _, err := stmt.Exec(keys[i], 1, vals[i]); err != nil {
			tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

func (m *modDB) helemGet(k, f []byte) (int, bool, error) {
	return modGet(m.helemGetStmt, k, f)
}

func (m *modDB) helemDrain(k []byte, fs, vs [][]byte) error {
	tx, err := m.db.Begin()
	if err != nil {
		return err
	}
	stmt := tx.Stmt(m.helemSetStmt)
	for i := range fs {
		if _, err := stmt.Exec(k, fs[i], vs[i]); err != nil {
			tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

func (m *modDB) step() error {
	var v int64
	return m.stepStmt.QueryRow(1).Scan(&v)
}

func (m *modDB) checkpoint() error {
	_, err := m.db.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
	return err
}

// modReader shares the pooled handle: database/sql's pool is the
// concurrency model this driver is measured under.
type modReader struct{ m *modDB }

func (m *modDB) newReader() (shootReader, error) {
	return &modReader{m: m}, nil
}

func (r *modReader) get(key []byte) (int, bool, error) {
	return modGet(r.m.getStmt, key)
}

func (r *modReader) close() error { return nil }
