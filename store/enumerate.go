package store

import (
	"encoding/binary"
	"errors"
)

// Each calls fn once for every live key with the raw value bytes stored under
// it. The key and value slices are valid only for the duration of the call: fn
// must copy any bytes it keeps, because in the full-resident path they alias the
// log page and in the spilled path they are reused scratch buffers. Iteration
// order is unspecified (it is the index-slot order of each shard in turn). fn
// returning false stops the walk early and Each returns nil. fn must not call
// back into the store on the same goroutine, as the visited shard's read lock is
// held across the call.
//
// This is the enumerate surface the keyspace needs for KEYS, SCAN, RANDOMKEY,
// DBSIZE-by-walk and the RDB writer once the engine is gated on, where the point
// Get/Set/Delete API alone leaves the keyspace unenumerable.
func (s *Store) Each(fn func(key, value []byte) bool) error {
	for _, sh := range s.shards {
		cont, err := sh.each(fn)
		if err != nil {
			return err
		}
		if !cont {
			return nil
		}
	}
	return nil
}

// each walks one shard's index in slot order, dereferencing each live entry to
// its key and value. It holds the shard read lock for the whole walk, so a
// concurrent writer on this shard waits, the same cost the B-tree KEYS/SCAN pays
// per shard. cont is false when fn asked to stop.
func (sh *shard) each(fn func(key, value []byte) bool) (cont bool, err error) {
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	v := sh.view.Load()
	for _, e := range v.slots {
		if e == 0 || e == tombstone {
			continue
		}
		key, val, derefErr := sh.recordKV(v, entryAddr(e))
		if derefErr != nil {
			return false, derefErr
		}
		if !fn(key, val) {
			return false, nil
		}
	}
	return true, nil
}

// recordKV returns the key and value bytes of the record at logical address
// addr. On a resident page it slices both in place (no copy); on a spilled page
// it reads the record off disk into fresh buffers. Caller holds at least the
// shard read lock. It mirrors recordKeyAny but also recovers the value, which
// enumeration needs and a key-only tag compare does not.
func (sh *shard) recordKV(v *view, addr uint64) (key, value []byte, err error) {
	pid := int64(addr) >> sh.pageShift
	off := int(int64(addr) & sh.pageMask)
	if page := v.pages[pid]; page != nil {
		klen := int(binary.LittleEndian.Uint32(page[off:]))
		vlen := int(binary.LittleEndian.Uint32(page[off+4:]))
		kstart := off + recHdr
		vstart := kstart + klen
		return page[kstart : kstart+klen], page[vstart : vstart+vlen], nil
	}
	if sh.file == nil {
		return nil, nil, errors.New("store: address neither resident nor on disk")
	}
	dOff := sh.diskOff[pid]
	var hdr [recHdr]byte
	if _, rerr := sh.file.ReadAt(hdr[:], dOff+int64(off)); rerr != nil {
		return nil, nil, rerr
	}
	klen := int(binary.LittleEndian.Uint32(hdr[0:]))
	vlen := int(binary.LittleEndian.Uint32(hdr[4:]))
	buf := make([]byte, klen+vlen)
	if _, rerr := sh.file.ReadAt(buf, dOff+int64(off)+int64(recHdr)); rerr != nil {
		return nil, nil, rerr
	}
	return buf[:klen], buf[klen:], nil
}

// Clear drops every key and returns each shard's log to its freshly-built empty
// state, reclaiming the resident pages. It is safe for concurrent use: it takes
// every shard's write lock in turn. This is the truncate surface FLUSHDB,
// FLUSHALL and SWAPDB (via a rebuild) need, which the point API cannot express.
func (s *Store) Clear() error {
	for _, sh := range s.shards {
		if err := sh.clear(); err != nil {
			return err
		}
	}
	return nil
}

// clear resets one shard to the state newShard leaves it in: an empty index and
// a single empty resident page. A spill log file, if any, is truncated and
// reused so the shard keeps its file handle. Caller must not hold the lock.
func (sh *shard) clear() error {
	sh.mu.Lock()
	defer sh.mu.Unlock()
	v := sh.view.Load()
	// Publish a fresh empty view: a zeroed index of the same size (keeping the
	// grown capacity for a FLUSH that will be refilled) and a single empty resident
	// page with the first recHdr bytes reserved so address 0 stays the unambiguous
	// empty marker. A new slots array, not an in-place wipe, so any lock-free reader
	// still walking the old view sees a consistent snapshot.
	nv := &view{
		slots: make([]uint64, len(v.slots)),
		mask:  v.mask,
		pages: [][]byte{make([]byte, sh.pageSize)},
	}
	sh.view.Store(nv)
	sh.icount = 0
	sh.tombs = 0
	sh.diskOff = sh.diskOff[:0]
	sh.residentOrder = sh.residentOrder[:0]
	sh.diskOff = append(sh.diskOff, 0)
	sh.residentOrder = append(sh.residentOrder, 0)
	sh.tailPage = 0
	sh.tailPos = recHdr
	sh.spilledPages = 0
	if sh.file != nil {
		if err := sh.file.Truncate(0); err != nil {
			return err
		}
		sh.fileEnd = 0
	}
	return nil
}
