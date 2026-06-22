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
	"math/rand/v2"
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

// NowMillis returns the keyspace clock in Unix epoch milliseconds. The command
// layer uses it to turn a relative TTL like EX seconds into the absolute
// millisecond deadline that Set stores, so both layers read the same clock.
func NowMillis() int64 { return nowMillis() }

// ExpiredKey names a key that lazy expiry removed, tagged with the database it
// lived in. The command layer drains these after a keyspace access to fire the
// "expired" notification, which the keyspace layer cannot fire on its own.
type ExpiredKey struct {
	DB  int
	Key []byte
}

// entryOverhead is the fixed per-key cost folded into the live-data estimate on
// top of the key name and the body. It stands in for the value header and the
// B-tree cell bookkeeping so used memory tracks roughly with what a key occupies.
const entryOverhead = 64

// Keyspace owns every logical database in one .aki file.
type Keyspace struct {
	pgr     *pager.Pager
	dbs     []*DB
	catRoot uint32 // catalog page number, NullPage until first persisted
	sysRoot uint32 // system table B-tree root, NullPage until first SystemPut
	sysTree *btree.Tree
	version uint64 // monotonic write version assigned to each write

	// expiredLog collects keys deleted by lazy expiry since the last drain. The
	// command layer empties it with TakeExpired after each access to fire the
	// "expired" keyspace event. Access is serialized by the command engine lock.
	expiredLog []ExpiredKey

	// dataBytes is the running estimate of live key and value bytes, the figure
	// INFO reports as used_memory and the maxmemory eviction loop compares against
	// the limit. Set and Delete keep it current.
	dataBytes int64
}

// UsedMemory returns the live-data estimate in bytes, the value compared against
// maxmemory. It is the sum of key name, body and per-key overhead across every
// live key, which shrinks as keys are deleted or evicted.
func (ks *Keyspace) UsedMemory() int64 { return ks.dataBytes }

// EvictionCandidate is a key the eviction loop may remove, carrying the fields the
// policies sort on: the expiry for volatile-ttl, the last-access time for the lru
// policies, and the decayed frequency for the lfu policies.
type EvictionCandidate struct {
	DB     int
	Key    []byte
	TTLms  int64
	HasTTL bool
	Atime  uint32 // unix seconds of last access; smaller is older, evicted first by LRU
	Freq   uint8  // decayed LFU counter; smaller is colder, evicted first by LFU
}

// SampleForEviction reservoir-samples up to n eviction candidates across every
// database. When volatileOnly is set it considers only keys that carry a TTL,
// which is what the volatile-* policies evict from.
func (ks *Keyspace) SampleForEviction(n int, volatileOnly bool) []EvictionCandidate {
	if n <= 0 {
		n = 1
	}
	out := make([]EvictionCandidate, 0, n)
	seen := 0
	for _, db := range ks.dbs {
		if volatileOnly && db.expireCount == 0 {
			continue
		}
		_ = db.forEachLive(func(ck []byte, h ValueHeader) error {
			if volatileOnly && !h.HasTTL() {
				return nil
			}
			raw := copyRaw(ck)
			atime, freq := db.accessMetrics(raw)
			cand := EvictionCandidate{
				DB:     db.index,
				Key:    raw,
				TTLms:  h.TTLms,
				HasTTL: h.HasTTL(),
				Atime:  atime,
				Freq:   freq,
			}
			if len(out) < n {
				out = append(out, cand)
			} else if j := rand.IntN(seen + 1); j < n {
				out[j] = cand
			}
			seen++
			return nil
		})
	}
	return out
}

// TakeExpired returns the keys lazily expired since the last call and clears the
// log. The command engine calls it under its own lock so there is no concurrent
// appender.
func (ks *Keyspace) TakeExpired() []ExpiredKey {
	if len(ks.expiredLog) == 0 {
		return nil
	}
	out := ks.expiredLog
	ks.expiredLog = nil
	return out
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

	// access holds the in-memory LRU and LFU bookkeeping per key, keyed by the raw
	// key name. It is built up as keys are read and written and is not persisted,
	// matching how Redis treats approximate eviction state across a restart.
	access map[string]keyAccess
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
		sysRoot: normalizeRoot(pgr.Meta().SystemRoot),
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

// PagerStats returns the underlying pager's counters for the file-growth INFO
// fields. It is a passthrough so the command layer does not reach into the pager.
func (k *Keyspace) PagerStats() pager.Stats { return k.pgr.Stats() }

// PagerName returns the file path the underlying pager was opened with, empty
// for an in-memory backing.
func (k *Keyspace) PagerName() string { return k.pgr.Name() }

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
		TTLms:    -1,
		Version:  db.ks.version,
		BodyLen:  uint32(len(body)),
		RefCount: 1,
	}
	if ttlMs >= 0 {
		h.Flags |= FlagHasTTL
		h.TTLms = ttlMs
	}

	// A body up to maxInlineBody rides in the leaf cell; a larger one goes to an
	// overflow chain and the cell carries only the header with BodyRef set.
	var cell []byte
	if len(body) <= maxInlineBody {
		h.Flags |= FlagInlineBody
		cell = h.AppendTo(make([]byte, 0, HeaderSize+len(body)))
		cell = append(cell, body...)
	} else {
		head, werr := db.ks.writeOverflow(body)
		if werr != nil {
			return werr
		}
		h.BodyRef = uint64(head)
		cell = h.AppendTo(make([]byte, 0, HeaderSize))
	}
	if err := t.Put(ck, cell); err != nil {
		return err
	}
	db.rootPage = t.Root()

	// The previous value's overflow pages are now unreferenced.
	if existed && prev.Flags&FlagInlineBody == 0 && prev.BodyRef != 0 {
		if err := db.ks.freeOverflow(uint32(prev.BodyRef)); err != nil {
			return err
		}
	}

	if isNew {
		db.keyCount++
	} else {
		db.ks.dataBytes -= int64(len(key)) + int64(prev.BodyLen) + entryOverhead
	}
	db.ks.dataBytes += int64(len(key)) + int64(len(body)) + entryOverhead
	if h.HasTTL() && !hadTTL {
		db.expireCount++
	} else if !h.HasTTL() && hadTTL {
		db.expireCount--
	}
	db.recordAccess(key, isNew)
	return nil
}

// Get returns the body and header for key and records an LRU and LFU access. It
// is the read path data commands use. found is false when the key is absent or
// has expired; an expired key is deleted as a side effect (lazy expiry).
func (db *DB) Get(key []byte) (body []byte, hdr ValueHeader, found bool, err error) {
	return db.get(key, true)
}

// Peek is Get without recording an access, the read path introspection commands
// use so OBJECT, EXISTS and friends do not reset a key's idle time or bump its
// frequency.
func (db *DB) Peek(key []byte) (body []byte, hdr ValueHeader, found bool, err error) {
	return db.get(key, false)
}

// get is the shared read path. When touch is set it records an access for the
// eviction bookkeeping; lazy expiry runs either way.
func (db *DB) get(key []byte, touch bool) (body []byte, hdr ValueHeader, found bool, err error) {
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
		if err == nil {
			db.ks.expiredLog = append(db.ks.expiredLog, ExpiredKey{
				DB:  db.index,
				Key: append([]byte(nil), key...),
			})
		}
		return nil, ValueHeader{}, false, err
	}
	if touch {
		db.recordAccess(key, false)
	}
	if h.Flags&FlagInlineBody == 0 {
		body, err := db.ks.readOverflow(uint32(h.BodyRef), int(h.BodyLen))
		if err != nil {
			return nil, ValueHeader{}, false, err
		}
		return body, h, true, nil
	}
	out := make([]byte, len(cell))
	copy(out, cell)
	return out, h, true, nil
}

// Exists reports whether key is present and unexpired without recording an
// access. An expired key is deleted.
func (db *DB) Exists(key []byte) (bool, error) {
	_, _, found, err := db.Peek(key)
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
		db.ks.dataBytes -= int64(len(key)) + int64(prev.BodyLen) + entryOverhead
		db.dropAccess(key)
		if prev.HasTTL() {
			db.expireCount--
		}
		if prev.Flags&FlagInlineBody == 0 && prev.BodyRef != 0 {
			if err := db.ks.freeOverflow(uint32(prev.BodyRef)); err != nil {
				return ok, err
			}
		}
	}
	return ok, nil
}

// ActiveExpireCycle walks every database for volatile keys whose TTL has passed,
// deletes them, and records each in the expired log so the command layer can fire
// the "expired" event. It returns the number of keys removed. A database with no
// volatile keys is skipped on the cheap expireCount guard.
func (ks *Keyspace) ActiveExpireCycle() (int, error) {
	now := nowMillis()
	total := 0
	for _, db := range ks.dbs {
		if db.expireCount == 0 {
			continue
		}
		keys, err := db.expiredVolatileKeys(now)
		if err != nil {
			return total, err
		}
		for _, k := range keys {
			ok, err := db.Delete(k)
			if err != nil {
				return total, err
			}
			if ok {
				ks.expiredLog = append(ks.expiredLog, ExpiredKey{DB: db.index, Key: k})
				total++
			}
		}
	}
	return total, nil
}

// expiredVolatileKeys returns the raw names of every key in the DB whose absolute
// TTL is at or before now. It collects the names in one pass rather than deleting
// during the walk, since deleting under the cursor would disturb the iteration.
func (db *DB) expiredVolatileKeys(now int64) ([][]byte, error) {
	t := db.loadTree()
	if t == nil {
		return nil, nil
	}
	var out [][]byte
	c := t.Cursor()
	if err := c.First(); err != nil {
		return nil, err
	}
	for c.Valid() {
		h, _, ok := parseHeader(c.Value())
		if ok && h.HasTTL() && h.TTLms <= now {
			out = append(out, copyRaw(c.Key()))
		}
		if err := c.Next(); err != nil {
			return nil, err
		}
	}
	return out, nil
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

	if err := ks.pgr.Commit(pager.CommitInfo{
		CatalogRoot:    ks.catRoot,
		SetCatalogRoot: true,
		SystemRoot:     ks.sysRoot,
		SetSystemRoot:  true,
	}); err != nil {
		return err
	}
	ks.assertConsistent()
	return nil
}
