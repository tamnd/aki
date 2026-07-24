package shard

// The collection root-deadline hint index (spec 2064/obs1 doc 08 sections
// 2 and 8): one absolute unix-ms deadline per key, owner-local like the
// type registries hanging off Coll. This map is only a fast index for the
// keyspace lazy-expiry guard; the authoritative deadline lives on each
// type's root struct, where it dies with the root, so an entry here can go
// stale when a key is dropped and recreated. The guard treats a hit as a
// hint: it re-reads the root's own deadline before acting, and clears the
// entry when the root is gone or carries no deadline. Strings are not
// here; the string store carries its deadline inline on the record
// (str.go flagHasTTL), the form that already demotes and folds.

// RootDeadline reports key's hinted collection root deadline in absolute
// unix ms, 0 when none is recorded. It does not check the clock and it
// does not validate against the root; callers that act on a fired hint
// confirm it through the owning type's Deadline first. Owner goroutine
// only.
func (cx *Ctx) RootDeadline(key []byte) int64 {
	if cx.rootExp == nil {
		return 0
	}
	return cx.rootExp[string(key)]
}

// SetRootDeadline records or replaces key's hint; at 0 removes it
// (PERSIST). Callers set it alongside the root struct's own deadline so
// the index stays warm; a keyspace with no collection TTLs never
// allocates the map. Owner goroutine only.
func (cx *Ctx) SetRootDeadline(key []byte, at int64) {
	if at == 0 {
		cx.DropRootDeadline(key)
		return
	}
	if cx.rootExp == nil {
		cx.rootExp = make(map[string]int64)
	}
	cx.rootExp[string(key)] = at
}

// DropRootDeadline removes key's hint if one is recorded. The guard calls
// it when a hint turns out stale and the delete paths that know the key
// call it eagerly; paths that miss it only leave a stale hint, which the
// guard tolerates. Owner goroutine only.
func (cx *Ctx) DropRootDeadline(key []byte) {
	if cx.rootExp != nil {
		delete(cx.rootExp, string(key))
	}
}
