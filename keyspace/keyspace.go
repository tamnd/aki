// Package keyspace is aki's logical key dictionary (spec 2064 doc 05). It maps
// N independent logical databases onto one .aki file. Each database is an ordered
// map from a binary key to a ValueHeader plus its inline body, stored in a
// per-DB B-tree keyed by a composite (hash slot, key length, key) tuple. The
// package tracks a per-DB catalog (root page, key count, expire count, average
// TTL) on a dedicated catalog page referenced from the meta page, and it applies
// lazy TTL expiry on read.
//
// This slice is the storage layer the command dispatch layer sits on. It assumes
// a single writer at a time; the sharded writer model and MVCC snapshot
// filtering from doc 05 §7 and §12 come in later slices.
package keyspace

import (
	"errors"
	"time"

	"github.com/tamnd/aki/btree"
	"github.com/tamnd/aki/encoding"
	"github.com/tamnd/aki/format"
	"github.com/tamnd/aki/pager"
)

// recordSize is the on-disk size of one per-DB catalog record (doc 05 §2.2).
const recordSize = 32

// catalogDataStart is the byte offset of the first catalog record on the catalog
// page, just past the common 16-byte page header.
const catalogDataStart = format.CommonHeaderSize

// ErrDBRange is returned when a database index is outside [0, DBCount).
var ErrDBRange = errors.New("aki/keyspace: database index out of range")

// nowMillis returns the current wall clock in Unix epoch milliseconds. It is a
// variable so tests can pin time and exercise TTL expiry deterministically.
var nowMillis = func() int64 { return time.Now().UnixMilli() }

// Keyspace owns every logical database in one .aki file.
type Keyspace struct {
	pgr     *pager.Pager
	dbs     []*DB
	catRoot uint32 // catalog page number, NullPage until first persisted
	version uint64 // monotonic write version assigned to each write
}

// DB is one logical database: a B-tree of keys plus its catalog counters.
type DB struct {
	ks       *Keyspace
	index    int
	rootPage uint32 // btree root page, NullPage when the DB has no keys yet
	tree     *btree.Tree

	keyCount    uint64
	expireCount uint64
	avgTTL      uint32
}

// Open binds a Keyspace to a pager and loads the catalog. The number of
// databases comes from the file header; a fresh file with no catalog page yields
// empty databases that materialize their B-trees on first write.
func Open(pgr *pager.Pager) (*Keyspace, error) {
	dbCount := int(pgr.Header().DBCount)
	if dbCount <= 0 {
		dbCount = int(format.DefaultDBCount)
	}
	ks := &Keyspace{
		pgr:     pgr,
		dbs:     make([]*DB, dbCount),
		catRoot: pgr.Meta().CatalogRoot,
	}
	for i := range ks.dbs {
		ks.dbs[i] = &DB{ks: ks, index: i, rootPage: format.NullPage}
	}
	if ks.catRoot != format.NullPage {
		if err := ks.loadCatalog(); err != nil {
			return nil, err
		}
	}
	return ks, nil
}

// DBCount returns the number of logical databases.
func (ks *Keyspace) DBCount() int { return len(ks.dbs) }

// DB returns the database at index, or an error if the index is out of range.
func (ks *Keyspace) DB(index int) (*DB, error) {
	if index < 0 || index >= len(ks.dbs) {
		return nil, ErrDBRange
	}
	return ks.dbs[index], nil
}

// loadCatalog reads the catalog page and fills each DB's counters and root.
func (ks *Keyspace) loadCatalog() error {
	pg, err := ks.pgr.Get(ks.catRoot)
	if err != nil {
		return err
	}
	defer ks.pgr.Unpin(pg, false)
	for i, db := range ks.dbs {
		off := catalogDataStart + i*recordSize
		if off+recordSize > len(pg.Data) {
			break
		}
		rec := pg.Data[off:]
		db.rootPage = uint32(encoding.U64(rec[0:]))
		db.keyCount = encoding.U64(rec[8:])
		db.expireCount = encoding.U64(rec[16:])
		db.avgTTL = encoding.U32(rec[24:])
	}
	return nil
}

// Index returns the database's index.
func (db *DB) Index() int { return db.index }

// Len returns the number of live keys, the value DBSIZE reports.
func (db *DB) Len() uint64 { return db.keyCount }

// loadTree returns the DB's B-tree, opening it from the stored root. It returns
// nil when the DB has never been written.
func (db *DB) loadTree() *btree.Tree {
	if db.tree != nil {
		return db.tree
	}
	if db.rootPage == format.NullPage {
		return nil
	}
	db.tree = btree.Open(db.ks.pgr, db.rootPage)
	return db.tree
}

// ensureTree returns the DB's B-tree, creating one if the DB is empty.
func (db *DB) ensureTree() (*btree.Tree, error) {
	if t := db.loadTree(); t != nil {
		return t, nil
	}
	t, err := btree.Create(db.ks.pgr)
	if err != nil {
		return nil, err
	}
	db.tree = t
	db.rootPage = t.Root()
	return t, nil
}

// Set writes key with the given body, type, encoding and TTL. A ttlMs of -1
// means no expiry; a positive ttlMs is an absolute Unix epoch in milliseconds.
// A key whose absolute TTL is already in the past is not written and any
// existing key under that name is removed, matching Redis's write-time expiry.
func (db *DB) Set(key, body []byte, typ, enc uint8, ttlMs int64) error {
	if ttlMs >= 0 && ttlMs <= nowMillis() {
		_, err := db.Delete(key)
		return err
	}
	t, err := db.ensureTree()
	if err != nil {
		return err
	}
	ck := compositeKey(key)

	prev, existed, err := db.lookup(t, ck)
	if err != nil {
		return err
	}
	isNew := !existed
	hadTTL := existed && prev.HasTTL()

	db.ks.version++
	h := ValueHeader{
		Type:     typ,
		Encoding: enc,
		Flags:    FlagInlineBody,
		TTLms:    -1,
		Version:  db.ks.version,
		BodyLen:  uint32(len(body)),
		RefCount: 1,
	}
	if ttlMs >= 0 {
		h.Flags |= FlagHasTTL
		h.TTLms = ttlMs
	}

	cell := h.AppendTo(make([]byte, 0, HeaderSize+len(body)))
	cell = append(cell, body...)
	if err := t.Put(ck, cell); err != nil {
		return err
	}
	db.rootPage = t.Root()

	if isNew {
		db.keyCount++
	}
	if h.HasTTL() && !hadTTL {
		db.expireCount++
	} else if !h.HasTTL() && hadTTL {
		db.expireCount--
	}
	return nil
}

// Get returns the body and header for key. found is false when the key is
// absent or has expired; an expired key is deleted as a side effect (lazy
// expiry).
func (db *DB) Get(key []byte) (body []byte, hdr ValueHeader, found bool, err error) {
	t := db.loadTree()
	if t == nil {
		return nil, ValueHeader{}, false, nil
	}
	ck := compositeKey(key)
	h, cell, ok, err := db.read(t, ck)
	if err != nil || !ok {
		return nil, ValueHeader{}, false, err
	}
	if db.expired(h) {
		_, err := db.Delete(key)
		return nil, ValueHeader{}, false, err
	}
	out := make([]byte, len(cell))
	copy(out, cell)
	return out, h, true, nil
}

// Exists reports whether key is present and unexpired. An expired key is deleted.
func (db *DB) Exists(key []byte) (bool, error) {
	_, _, found, err := db.Get(key)
	return found, err
}

// Delete removes key. It returns whether a key was present.
func (db *DB) Delete(key []byte) (bool, error) {
	t := db.loadTree()
	if t == nil {
		return false, nil
	}
	ck := compositeKey(key)
	prev, existed, err := db.lookup(t, ck)
	if err != nil || !existed {
		return false, err
	}
	ok, err := t.Delete(ck)
	if err != nil {
		return false, err
	}
	if ok {
		db.rootPage = t.Root()
		db.keyCount--
		if prev.HasTTL() {
			db.expireCount--
		}
	}
	return ok, nil
}

// read fetches the raw cell for a composite key and splits off its header.
func (db *DB) read(t *btree.Tree, ck []byte) (ValueHeader, []byte, bool, error) {
	cell, ok, err := t.Get(ck)
	if err != nil || !ok {
		return ValueHeader{}, nil, false, err
	}
	h, n, valid := parseHeader(cell)
	if !valid {
		return ValueHeader{}, nil, false, nil
	}
	return h, cell[n:], true, nil
}

// lookup returns just the header for a composite key.
func (db *DB) lookup(t *btree.Tree, ck []byte) (ValueHeader, bool, error) {
	h, _, ok, err := db.read(t, ck)
	return h, ok, err
}

// expired reports whether a header's TTL has passed.
func (db *DB) expired(h ValueHeader) bool {
	return h.HasTTL() && h.TTLms <= nowMillis()
}

// Commit persists the catalog and every DB root, then commits the pager. The
// catalog page is allocated on first commit that has data to record.
func (ks *Keyspace) Commit() error {
	if ks.catRoot == format.NullPage {
		pg, err := ks.pgr.Allocate()
		if err != nil {
			return err
		}
		ks.catRoot = pg.No
		ks.pgr.Unpin(pg, false)
	}
	pg, err := ks.pgr.Get(ks.catRoot)
	if err != nil {
		return err
	}
	for i := range pg.Data {
		pg.Data[i] = 0
	}
	hdr := format.PageHeader{Type: format.PageTypeCatalog, FreeStart: catalogDataStart, FreeEnd: uint16(len(pg.Data))}
	if err := pg.PutHeader(hdr); err != nil {
		ks.pgr.Unpin(pg, false)
		return err
	}
	for i, db := range ks.dbs {
		off := catalogDataStart + i*recordSize
		if off+recordSize > len(pg.Data) {
			break
		}
		rec := pg.Data[off:]
		encoding.PutU64(rec[0:], uint64(db.rootPage))
		encoding.PutU64(rec[8:], db.keyCount)
		encoding.PutU64(rec[16:], db.expireCount)
		encoding.PutU32(rec[24:], db.avgTTL)
	}
	ks.pgr.Unpin(pg, true)

	return ks.pgr.Commit(pager.CommitInfo{
		CatalogRoot:    ks.catRoot,
		SetCatalogRoot: true,
	})
}
