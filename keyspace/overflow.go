package keyspace

import (
	"github.com/tamnd/aki/encoding"
	"github.com/tamnd/aki/format"
)

// maxInlineBody is the largest value body kept in the B-tree leaf cell next to
// its header (doc 05 §3.4). A body of this size or smaller is inline; anything
// larger is written to an overflow page chain and the leaf cell holds only the
// 40-byte header with BodyRef pointing at the head page.
//
// The threshold is MinPageSize/4 (4096/4 = 1024). An overflow body claims a
// whole page chain of its own, so a mid-size value (a small collection blob, a
// few-hundred-byte string) used to burn a >=4KB page each: 100k such keys make a
// dirty set large enough to force a checkpoint every couple thousand writes, and
// each overwrite first walks the old chain to free it. Keeping bodies up to a
// quarter page inline packs many per leaf instead, which removes that page
// explosion and the per-overwrite chain read. A quarter of the smallest legal
// page still leaves at least three cells per leaf on a MinPageSize file, so the
// tree stays splittable; on the 16KB default page a leaf holds about sixteen
// max-size cells. The choice is per write and recorded in each cell's
// FlagInlineBody, so raising it stays compatible with files written under the
// old 128-byte threshold (their larger cells simply remain in overflow until
// rewritten).
const maxInlineBody = 1024

// Overflow page layout. The 16-byte common header is followed by a 4-byte
// little-endian next-page pointer (NullPage on the last page), then the body
// chunk. So each page carries pageSize-ovDataStart payload bytes.
const (
	ovNextOffset = format.CommonHeaderSize
	ovDataStart  = ovNextOffset + 4
)

// ovChunkCap is the number of body bytes one overflow page holds.
func (ks *Keyspace) ovChunkCap() int { return int(ks.pgr.PageSize()) - ovDataStart }

// writeOverflow stores body across a chain of overflow pages and returns the
// head page number. The chain is built from the tail so each page already knows
// the page that follows it.
func (ks *Keyspace) writeOverflow(body []byte) (uint32, error) {
	cap := ks.ovChunkCap()
	next := format.NullPage
	head := format.NullPage
	for end := len(body); end > 0; {
		start := max(end-cap, 0)
		chunk := body[start:end]
		pg, err := ks.pgr.Allocate()
		if err != nil {
			return format.NullPage, err
		}
		h := format.PageHeader{
			Type:      format.PageTypeOverflow,
			FreeStart: ovDataStart,
			FreeEnd:   uint16(ovDataStart + len(chunk)),
		}
		if err := pg.PutHeader(h); err != nil {
			ks.pgr.Unpin(pg, false)
			return format.NullPage, err
		}
		encoding.PutU32(pg.Data[ovNextOffset:], next)
		copy(pg.Data[ovDataStart:], chunk)
		next = pg.No
		head = pg.No
		ks.pgr.Unpin(pg, true)
		end = start
	}
	return head, nil
}

// readOverflow walks the chain starting at head and returns the first n body
// bytes. n is the BodyLen recorded in the header, which bounds the read so a
// partly-filled last page contributes only its real bytes.
func (ks *Keyspace) readOverflow(head uint32, n int) ([]byte, error) {
	out := make([]byte, 0, n)
	pgno := head
	for pgno != format.NullPage && len(out) < n {
		pg, err := ks.pgr.Get(pgno)
		if err != nil {
			return nil, err
		}
		hdr, err := pg.Header()
		if err != nil {
			ks.pgr.Unpin(pg, false)
			return nil, err
		}
		chunk := pg.Data[ovDataStart:hdr.FreeEnd]
		want := n - len(out)
		if len(chunk) > want {
			chunk = chunk[:want]
		}
		out = append(out, chunk...)
		pgno = encoding.U32(pg.Data[ovNextOffset:])
		ks.pgr.Unpin(pg, false)
	}
	return out, nil
}

// freeOverflow returns every page in the chain starting at head to the freelist.
func (ks *Keyspace) freeOverflow(head uint32) error {
	pgno := head
	for pgno != format.NullPage {
		pg, err := ks.pgr.Get(pgno)
		if err != nil {
			return err
		}
		next := encoding.U32(pg.Data[ovNextOffset:])
		ks.pgr.Unpin(pg, false)
		if err := ks.pgr.Free(pgno); err != nil {
			return err
		}
		pgno = next
	}
	return nil
}
