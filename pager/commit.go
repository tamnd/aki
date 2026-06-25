package pager

import "github.com/tamnd/aki/format"

// CommitInfo carries the per-commit values the caller wants recorded in the new
// meta snapshot: the updated catalog root, the per-DB B-tree roots, and the WAL
// commit LSN (zero when the WAL is not in use). Fields left at their zero value
// inherit the current live meta.
type CommitInfo struct {
	CatalogRoot   uint32
	SystemRoot    uint32
	DBRootPages   [8]uint32
	WALCommitLSN  uint64
	SchemaVersion uint32
	// SetDBRoots, when true, replaces the DB root array; otherwise the existing
	// roots are kept.
	SetDBRoots bool
	// SetCatalogRoot, when true, replaces the catalog root.
	SetCatalogRoot bool
	// SetSystemRoot, when true, replaces the system table root.
	SetSystemRoot bool
}

// Commit makes the current set of dirty pages durable and atomically advances
// the live meta snapshot (doc 02 §9.1, doc 03 §8). The protocol is:
//
//  1. flush every dirty data page to its slot and fsync, so the page images are
//     durable before the meta pointer that references them;
//  2. persist the freelist chain if it changed;
//  3. build the next meta with seq = live+1, write it to the non-live meta slot,
//     and fsync, which is the linearization point: the commit is visible exactly
//     when this fsync returns;
//  4. best-effort refresh of the file header's mutable fields.
//
// A crash before step 3's fsync leaves the previous meta live, so the whole
// transaction rolls back atomically with no journal.
func (p *Pager) Commit(info CommitInfo) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed.Load() {
		return ErrClosed
	}

	if err := p.flushDirtyLocked(); err != nil {
		return err
	}

	if p.freelistDirty {
		head, err := p.persistFreelist()
		if err != nil {
			return err
		}
		p.meta.FreelistHead = head
		p.meta.FreelistCount = uint32(len(p.freelist))
		p.freelistDirty = false
	}

	if err := p.file.Sync(); err != nil {
		return err
	}

	next := p.meta
	next.MetaSeq = p.meta.MetaSeq + 1
	next.TxnID = p.meta.TxnID + 1
	next.ChangeCounter = p.meta.ChangeCounter + 1
	next.PageCount = p.meta.PageCount
	next.FreelistHead = p.meta.FreelistHead
	next.FreelistCount = p.meta.FreelistCount
	if info.SetCatalogRoot {
		next.CatalogRoot = info.CatalogRoot
	}
	if info.SetSystemRoot {
		next.SystemRoot = info.SystemRoot
	}
	if info.SetDBRoots {
		next.DBRootPages = info.DBRootPages
	}
	if info.WALCommitLSN != 0 {
		next.WALCommitLSN = info.WALCommitLSN
	}
	if info.SchemaVersion != 0 {
		next.SchemaVersion = info.SchemaVersion
	}

	// The non-live slot is the one whose page number differs from the live
	// page's parity; with only two pages, write to whichever is NOT live. We
	// track liveness by seq, and the two physical pages alternate by seq parity.
	deadPage := deadMetaPage(next.MetaSeq)
	buf := make([]byte, p.pageSize)
	if err := next.MarshalTo(buf, p.pageSize); err != nil {
		return err
	}
	if err := p.writeRaw(deadPage, buf); err != nil {
		return err
	}
	if err := p.file.Sync(); err != nil {
		return err
	}
	p.meta = next
	p.pageCount.Store(next.PageCount)

	// Best-effort header refresh; not required for correctness because recovery
	// reads the meta pages, not these fields.
	p.header.PageCount = next.PageCount
	p.header.FreelistHead = next.FreelistHead
	p.header.FreelistCount = next.FreelistCount
	p.header.CatalogRoot = next.CatalogRoot
	p.header.ChangeCounter = next.ChangeCounter
	p.header.SchemaVersion = next.SchemaVersion
	hbuf := make([]byte, format.HeaderSize)
	if err := p.header.MarshalTo(hbuf); err == nil {
		_, _ = p.file.WriteAt(hbuf, 0)
	}
	return nil
}

// deadMetaPage returns the physical meta page number to write for a commit
// reaching sequence seq. Sequence N lands on page A when N is odd and page B
// when N is even, so the newest commit always overwrites the older physical
// slot and the previous snapshot survives a crash mid-write.
func deadMetaPage(seq uint64) uint32 {
	if seq%2 == 1 {
		return format.MetaPageA
	}
	return format.MetaPageB
}

// flushDirtyLocked writes back every dirty cached page. Caller holds p.mu. Each
// stripe is flushed under its own lock; commit already runs with the write path
// quiesced, so no globally consistent snapshot across stripes is needed.
func (p *Pager) flushDirtyLocked() error {
	for i := range p.pool.stripes {
		ft := &p.pool.stripes[i]
		ft.mu.Lock()
		for pgno, pg := range ft.frames {
			if !pg.dirty.Load() {
				continue
			}
			if err := p.writeRaw(pgno, pg.Data); err != nil {
				ft.mu.Unlock()
				return err
			}
			pg.dirty.Store(false)
		}
		ft.mu.Unlock()
	}
	return nil
}

// Close flushes nothing implicitly (callers Commit first) and releases the file
// handle. It errors if any page is still pinned.
func (p *Pager) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed.Load() {
		return nil
	}
	for i := range p.pool.stripes {
		ft := &p.pool.stripes[i]
		ft.mu.Lock()
		for _, pg := range ft.frames {
			if pg.pins.Load() > 0 {
				ft.mu.Unlock()
				return ErrPinned
			}
		}
		ft.mu.Unlock()
	}
	p.closed.Store(true)
	return p.file.Close()
}
