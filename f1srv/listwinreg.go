package f1srv

import "sync"

// listWinShard is one shard of the resident hot-list window registry. The registry is striped by
// the same key hash as the stripe locks (incrMu), so a key's window shard and its stripe lock line
// up and admission or eviction under the stripe lock never crosses into another key's shard. The
// shard lock is an RWMutex because the lookup on the push fast path is by far the common access and
// wants a shared lock, while admission and eviction (both under the key's exclusive stripe lock) are
// rare and take the exclusive shard lock.
type listWinShard struct {
	mu sync.RWMutex
	m  map[string]*listWindow
}

// listWinShardFor returns the registry shard for a key, the same index as its stripe lock.
func (s *Server) listWinShardFor(key []byte) *listWinShard {
	return &s.listWin[s.stripe(key)]
}

// listWinLookup returns the resident window for a key, or nil. It is gated on listWinLive so the
// all-cold workload pays a single atomic load and never touches a shard lock: when no list has a
// window, every push and every read short-circuits here.
func (s *Server) listWinLookup(key []byte) *listWindow {
	if s.listWinLive.Load() == 0 {
		return nil
	}
	sh := s.listWinShardFor(key)
	sh.mu.RLock()
	w := sh.m[string(key)]
	sh.mu.RUnlock()
	return w
}

// listWinAdmit installs a window for a key, seeded from the header row the cold-key push just wrote,
// and returns it. The caller must hold the key's exclusive stripe lock, which serializes admission
// against eviction (also under that lock) and against another connection admitting the same key. If
// a window somehow already exists it is returned as-is rather than replaced, so a racing admit under
// the same lock is idempotent.
func (s *Server) listWinAdmit(key []byte, head, tail int64, lpBytes uint64, everLarge bool) *listWindow {
	sh := s.listWinShardFor(key)
	sh.mu.Lock()
	if w := sh.m[string(key)]; w != nil {
		sh.mu.Unlock()
		return w
	}
	w := newListWindow(head, tail)
	w.seedBytes(lpBytes, everLarge)
	sh.m[string(key)] = w
	sh.mu.Unlock()
	s.listWinLive.Add(1)
	return w
}

// listWinDrainEvict retires a key's window, flushing its committed bounds and size back to the
// persistent header row so every later read and recovery sees the true list, then dropping the
// entry. The caller must hold the key's exclusive stripe lock. The gate.Lock waits out every
// in-flight lock-free push (each holds gate.RLock across its reserve, element writes, and commit),
// so when it is acquired the committed bounds equal the reserved bounds and no reserved slot is
// left unpublished; the header it writes is therefore complete. A push that raced in and took
// gate.RLock after this set the evicted flag sees the flag and falls back to the stripe-lock path,
// which blocks on the stripe lock this caller holds and re-admits once it is free. It is a no-op
// when no window is resident, gated on listWinLive so a non-push command on an all-cold keyspace
// pays one atomic load.
func (c *connState) listWinDrainEvict(lkey []byte) {
	s := c.srv
	if s.listWinLive.Load() == 0 {
		return
	}
	sh := s.listWinShardFor(lkey)
	sh.mu.RLock()
	w := sh.m[string(lkey)]
	sh.mu.RUnlock()
	if w == nil {
		return
	}
	w.gate.Lock()
	head, tail := w.bounds()
	lpBytes, everLarge := w.sizeState()
	// Flush every resident position back to its f1raw row before the header, so the persisted image
	// is whole and a later read (or recovery) finds the element bytes a lock-free push left only in
	// the ring (slice 3, impl/34). A resident push skipped PutKind, so these rows do not yet exist and
	// their records are uncounted; PutKind here creates the row and increments the count, restoring the
	// pre-window invariant that every live element is a counted row. Pre-block positions already have
	// their rows (the admitting push wrote them), so resident() skips them and no row is written twice.
	// gate.Lock has waited out every in-flight push and pop, so the ring is quiescent and read without
	// the commit mutex. The window is being retired, so resetting slots is unnecessary.
	for pos := head; pos < tail; pos++ {
		if w.resident(pos) {
			_, _ = c.srv.store.PutKind(c.listElemKey(lkey, pos), w.ring.get(pos), kindListElem)
		}
	}
	// Flush the committed window to the header row, or delete the header when the window drained to
	// empty, exactly the write-back listPutHeader already does for a pop that empties a list.
	_ = c.listPutHeader(lkey, head, tail, lpBytes, everLarge)
	w.evicted.Store(true)
	sh.mu.Lock()
	delete(sh.m, string(lkey))
	sh.mu.Unlock()
	s.listWinLive.Add(-1)
	w.gate.Unlock()
}
