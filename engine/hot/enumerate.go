package hot

// Each calls fn for every live key/value in the store, stopping early if fn
// returns false. It runs lock-free: it loads each shard's table pointer and reads
// slots with atomic loads, so a concurrent write may or may not be reflected,
// which matches the weak snapshot SCAN and KEYS already give. The key and value
// slices alias immutable entries and must not be retained past the callback or
// mutated.
func (s *Store) Each(fn func(key, value []byte) bool) error {
	for _, sh := range s.shards {
		t := sh.table.Load()
		for i := range t.slots {
			e := t.slots[i].e.Load()
			if e == nil || e == tombstone {
				continue
			}
			if !fn(e.key(), e.val()) {
				return nil
			}
		}
	}
	return nil
}

// Clear drops every key, resetting each shard to an empty table. It takes each
// shard's write lock in turn, so a concurrent reader sees either the old contents
// or the empty table, never a torn state.
func (s *Store) Clear() error {
	for _, sh := range s.shards {
		sh.mu.Lock()
		sh.table.Store(newHTable(minTableCap))
		sh.count = 0
		sh.tomb = 0
		sh.bytes = 0
		sh.mu.Unlock()
	}
	return nil
}
