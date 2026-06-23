package pager

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/tamnd/aki/encoding"
	"github.com/tamnd/aki/format"
	"github.com/tamnd/aki/vfs"
)

// Errors returned by the pager.
var (
	ErrClosed       = errors.New("aki/pager: pager is closed")
	ErrPinned       = errors.New("aki/pager: page still pinned")
	ErrInvalidPage  = errors.New("aki/pager: invalid page number")
	ErrNoMeta       = errors.New("aki/pager: no valid meta page")
	ErrReadOnlyMeta = errors.New("aki/pager: page 0/1/2 are reserved")
)

// Options configure a new or opened pager.
type Options struct {
	// PageSize is used only by Create; Open reads it from the header. Zero means
	// format.DefaultPageSize.
	PageSize uint32
	// DBCount is used only by Create. Zero means format.DefaultDBCount.
	DBCount uint32
	// CachePages is the buffer-pool capacity in frames. Zero means a default.
	CachePages int
	// CreateTimeUS stamps the header at create time. Tests pass a fixed value;
	// the engine passes the wall clock. Zero is allowed.
	CreateTimeUS uint64
}

// Pager owns a single .aki file and its in-memory page cache.
type Pager struct {
	vfs  vfs.VFS
	name string

	mu       sync.RWMutex
	file     vfs.File
	pageSize uint32
	header   format.FileHeader
	meta     format.MetaPage
	pool     *frameTable

	// freelist is the in-memory stack of free page numbers; the persistent form
	// is an intrusive linked list through the free pages (doc 03 §6).
	freelist      []uint32
	freelistDirty bool

	// cacheHits and cacheMisses count buffer-pool lookups served from memory
	// versus those that had to read the page off disk. They drive the
	// aki_page_cache_hit_ratio growth field. Updated on the read path so they use
	// atomics rather than the pager mutex.
	cacheHits   atomic.Uint64
	cacheMisses atomic.Uint64

	closed bool
}

// Create initialises a fresh .aki file: page 0 header, meta pages 1 and 2, and
// an empty freelist. It fails if the file already exists with content.
func Create(fsys vfs.VFS, name string, opts Options) (*Pager, error) {
	pageSize := opts.PageSize
	if pageSize == 0 {
		pageSize = format.DefaultPageSize
	}
	if !format.ValidPageSize(pageSize) {
		return nil, format.ErrBadPageSize
	}
	dbCount := opts.DBCount
	if dbCount == 0 {
		dbCount = format.DefaultDBCount
	}
	f, err := fsys.Open(name, true)
	if err != nil {
		return nil, err
	}
	hdr := format.NewFileHeader(pageSize, dbCount, opts.CreateTimeUS)
	p := &Pager{
		vfs:      fsys,
		name:     name,
		file:     f,
		pageSize: pageSize,
		header:   hdr,
		pool:     newFrameTable(cacheCap(opts.CachePages)),
	}
	// Write page 0 (header), then meta A (seq 1, live) and meta B (seq 0).
	page0 := make([]byte, pageSize)
	if err := hdr.MarshalTo(page0); err != nil {
		return nil, err
	}
	metaA := format.NewMetaPage(hdr, 1)
	metaB := format.NewMetaPage(hdr, 0)
	bufA := make([]byte, pageSize)
	bufB := make([]byte, pageSize)
	if err := metaA.MarshalTo(bufA, pageSize); err != nil {
		return nil, err
	}
	if err := metaB.MarshalTo(bufB, pageSize); err != nil {
		return nil, err
	}
	if err := p.writeRaw(0, page0); err != nil {
		return nil, err
	}
	if err := p.writeRaw(format.MetaPageA, bufA); err != nil {
		return nil, err
	}
	if err := p.writeRaw(format.MetaPageB, bufB); err != nil {
		return nil, err
	}
	if err := f.Sync(); err != nil {
		return nil, err
	}
	p.meta = metaA
	return p, nil
}

// Open opens an existing .aki file, validates the header, and selects the live
// meta page.
func Open(fsys vfs.VFS, name string, opts Options) (*Pager, error) {
	f, err := fsys.Open(name, false)
	if err != nil {
		return nil, err
	}
	// Read enough to parse the header; page size is not yet known, so read the
	// minimum page (4096) which always covers the 128-byte header.
	probe := make([]byte, format.MinPageSize)
	if _, err := f.ReadAt(probe, 0); err != nil {
		_ = f.Close()
		return nil, err
	}
	hdr, err := format.ParseFileHeader(probe)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	if !format.ValidPageSize(hdr.PageSize) {
		_ = f.Close()
		return nil, format.ErrBadPageSize
	}
	p := &Pager{
		vfs:      fsys,
		name:     name,
		file:     f,
		pageSize: hdr.PageSize,
		header:   hdr,
		pool:     newFrameTable(cacheCap(opts.CachePages)),
	}
	if err := p.loadMeta(); err != nil {
		_ = f.Close()
		return nil, err
	}
	if err := p.loadFreelist(); err != nil {
		_ = f.Close()
		return nil, err
	}
	return p, nil
}

func cacheCap(n int) int {
	if n <= 0 {
		return 1024
	}
	return n
}

// PageSize returns the file's page size.
func (p *Pager) PageSize() uint32 { return p.pageSize }

// Name returns the file path this pager was opened with. It is empty for an
// in-memory backing.
func (p *Pager) Name() string { return p.name }

// PageCount returns the current total page count.
func (p *Pager) PageCount() uint32 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.meta.PageCount
}

// Meta returns a copy of the live meta snapshot.
func (p *Pager) Meta() format.MetaPage {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.meta
}

// Stats is a point-in-time snapshot of pager and buffer-pool counters. The
// server reads it for the file-growth INFO fields in doc 20 section 9.8.
type Stats struct {
	PageSize      uint32
	PageCount     uint32
	FreeCount     int
	FileBytes     int64
	ResidentPages int
	DirtyPages    int
	CacheHits     uint64
	CacheMisses   uint64
}

// CheckFreelist walks the on-disk freelist chain from the meta head, following
// the next-link in each free page. It detects a cycle and an out-of-range link,
// which a plain load cannot since it would loop or read garbage. It returns the
// number of free pages on a healthy chain. The integrity checker calls it.
func (p *Pager) CheckFreelist() (int, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	seen := make(map[uint32]struct{})
	head := p.meta.FreelistHead
	count := 0
	for head != format.NullPage {
		if head < 3 || head >= p.meta.PageCount {
			return count, fmt.Errorf("freelist link %d out of range (page count %d)", head, p.meta.PageCount)
		}
		if _, dup := seen[head]; dup {
			return count, fmt.Errorf("freelist cycle at page %d", head)
		}
		seen[head] = struct{}{}
		buf, err := p.readRaw(head)
		if err != nil {
			return count, err
		}
		count++
		head = encoding.U32(buf[16:])
	}
	return count, nil
}

// Stats returns the current pager counters. FileBytes is the on-disk size the
// page count implies, which is what the dataset-file growth field reports.
func (p *Pager) Stats() Stats {
	p.mu.RLock()
	defer p.mu.RUnlock()
	resident, dirty := p.pool.counts()
	return Stats{
		PageSize:      p.pageSize,
		PageCount:     p.meta.PageCount,
		FreeCount:     len(p.freelist),
		FileBytes:     int64(p.meta.PageCount) * int64(p.pageSize),
		ResidentPages: resident,
		DirtyPages:    dirty,
		CacheHits:     p.cacheHits.Load(),
		CacheMisses:   p.cacheMisses.Load(),
	}
}

// Header returns a copy of the file header.
func (p *Pager) Header() format.FileHeader {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.header
}

func (p *Pager) loadMeta() error {
	bufA := make([]byte, p.pageSize)
	bufB := make([]byte, p.pageSize)
	if _, err := p.file.ReadAt(bufA, int64(format.MetaPageA)*int64(p.pageSize)); err != nil {
		return err
	}
	if _, err := p.file.ReadAt(bufB, int64(format.MetaPageB)*int64(p.pageSize)); err != nil {
		return err
	}
	mA, errA := format.ParseMetaPage(bufA)
	mB, errB := format.ParseMetaPage(bufB)
	live, ok := format.LiveMeta(mA, mB, errA == nil, errB == nil)
	if !ok {
		return ErrNoMeta
	}
	p.meta = live
	return nil
}

// writeRaw writes a full page to disk without touching the buffer pool.
func (p *Pager) writeRaw(pgno uint32, buf []byte) error {
	_, err := p.file.WriteAt(buf, int64(pgno)*int64(p.pageSize))
	return err
}

// readRaw reads a full page from disk into a fresh buffer.
func (p *Pager) readRaw(pgno uint32) ([]byte, error) {
	buf := make([]byte, p.pageSize)
	if _, err := p.file.ReadAt(buf, int64(pgno)*int64(p.pageSize)); err != nil {
		return nil, err
	}
	return buf, nil
}

// Get pins and returns the page numbered pgno, faulting it in from disk if it is
// not resident. The caller must Unpin it when done.
// Multiple goroutines may call Get concurrently; the outer RLock guards the
// closed flag and the page-count bound, and the inner pool.mu guards the frame
// table. A cache miss reads the page outside either lock so the file read runs
// in parallel with other Get calls and with the pool.mu critical section.
func (p *Pager) Get(pgno uint32) (*Page, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return nil, ErrClosed
	}
	if pgno >= p.meta.PageCount {
		return nil, ErrInvalidPage
	}
	return p.getLocked(pgno)
}

func (p *Pager) getLocked(pgno uint32) (*Page, error) {
	p.pool.mu.Lock()
	if pg := p.pool.get(pgno); pg != nil {
		pg.pins++
		p.pool.mu.Unlock()
		p.cacheHits.Add(1)
		return pg, nil
	}
	p.pool.mu.Unlock()
	p.cacheMisses.Add(1)

	buf, err := p.readRaw(pgno)
	if err != nil {
		return nil, err
	}
	pg := &Page{No: pgno, Data: buf, ref: true, pins: 1}

	p.pool.mu.Lock()
	// Another goroutine may have loaded it meanwhile; prefer the existing frame.
	if existing := p.pool.get(pgno); existing != nil {
		existing.pins++
		p.pool.mu.Unlock()
		return existing, nil
	}
	p.maybeEvictLocked()
	p.pool.put(pg)
	p.pool.mu.Unlock()
	return pg, nil
}

// maybeEvictLocked drops one clean victim if the pool is at capacity. Caller
// holds pool.mu.
func (p *Pager) maybeEvictLocked() {
	if len(p.pool.frames) < p.pool.cap {
		return
	}
	if victim, ok := p.pool.evictable(); ok {
		p.pool.drop(victim)
	}
}

// Unpin releases a pin on pg. If dirty is true the page is marked for write-back
// at the next Commit.
func (p *Pager) Unpin(pg *Page, dirty bool) {
	p.pool.mu.Lock()
	defer p.pool.mu.Unlock()
	if dirty {
		pg.dirty = true
	}
	if pg.pins > 0 {
		pg.pins--
	}
}

// Allocate returns a fresh page for writing, reusing a freelist page when one is
// available or extending the file otherwise. The returned page is pinned and
// already marked dirty; its contents are zeroed.
func (p *Pager) Allocate() (*Page, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, ErrClosed
	}
	var pgno uint32
	if n := len(p.freelist); n > 0 {
		pgno = p.freelist[n-1]
		p.freelist = p.freelist[:n-1]
		p.freelistDirty = true
	} else {
		pgno = p.meta.PageCount
		p.meta.PageCount++
	}
	pg := &Page{No: pgno, Data: make([]byte, p.pageSize), ref: true, pins: 1, dirty: true}
	p.pool.mu.Lock()
	// A freed-then-reallocated page may still be cached; replace it.
	if old := p.pool.get(pgno); old != nil {
		p.pool.drop(pgno)
	}
	p.maybeEvictLocked()
	p.pool.put(pg)
	p.pool.mu.Unlock()
	return pg, nil
}

// Free returns pgno to the freelist. Reserved pages (0, 1, 2) cannot be freed.
func (p *Pager) Free(pgno uint32) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if pgno <= format.MetaPageB {
		return ErrReadOnlyMeta
	}
	p.freelist = append(p.freelist, pgno)
	p.freelistDirty = true
	p.pool.mu.Lock()
	if pg := p.pool.frames[pgno]; pg != nil && pg.pins == 0 {
		p.pool.drop(pgno)
	}
	p.pool.mu.Unlock()
	return nil
}

// FreeCount returns the number of pages currently on the freelist.
func (p *Pager) FreeCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.freelist)
}

// FreePages returns a copy of the in-memory freelist. The page-accounting check
// uses it to prove no live page is also free, and to find leaked pages that are
// neither live nor free.
func (p *Pager) FreePages() []uint32 {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]uint32, len(p.freelist))
	copy(out, p.freelist)
	return out
}

// PinnedPages returns the page numbers currently held with a non-zero pin count.
// After a command finishes every page should be unpinned, so a debug build calls
// this to catch a Get that was never matched by an Unpin (doc 23 section 9.4).
func (p *Pager) PinnedPages() []uint32 {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.pool.mu.Lock()
	defer p.pool.mu.Unlock()
	var out []uint32
	for pgno, pg := range p.pool.frames {
		if pg.pins > 0 {
			out = append(out, pgno)
		}
	}
	return out
}

// loadFreelist walks the persistent intrusive free-page chain into memory.
func (p *Pager) loadFreelist() error {
	head := p.meta.FreelistHead
	p.freelist = p.freelist[:0]
	for head != format.NullPage {
		buf, err := p.readRaw(head)
		if err != nil {
			return err
		}
		p.freelist = append(p.freelist, head)
		head = encoding.U32(buf[16:])
	}
	// The chain is read head-first; reverse so pops mirror the original order.
	for i, j := 0, len(p.freelist)-1; i < j; i, j = i+1, j-1 {
		p.freelist[i], p.freelist[j] = p.freelist[j], p.freelist[i]
	}
	return nil
}

// persistFreelist writes the intrusive chain through the free pages and returns
// the new head. Each free page stores Type=PageTypeFree and a next pointer at
// offset 16 (doc 03 §6).
func (p *Pager) persistFreelist() (uint32, error) {
	head := format.NullPage
	// Link pages so that walking from head yields reverse-insertion order; on
	// load we reverse again, restoring the stack order.
	for _, pgno := range p.freelist {
		buf := make([]byte, p.pageSize)
		h := format.PageHeader{Type: format.PageTypeFree, FreeStart: format.CommonHeaderSize, FreeEnd: uint16(p.pageSize)}
		if err := h.MarshalTo(buf); err != nil {
			return format.NullPage, err
		}
		encoding.PutU32(buf[16:], head)
		if err := p.writeRaw(pgno, buf); err != nil {
			return format.NullPage, err
		}
		head = pgno
	}
	return head, nil
}
