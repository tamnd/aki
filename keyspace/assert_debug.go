//go:build debug

package keyspace

import "fmt"

// assertConsistent runs the pin check and panics if a commit left a page pinned.
// It is compiled in only under the debug build tag, so debug and test builds catch
// a Get without a matching Unpin the moment a commit creates it. Production builds
// get the no-op in assert_prod.go.
//
// Page accounting is deliberately not asserted here. It is a true invariant for
// every path except FLUSHDB and FLUSHALL, which drop a whole database tree's root
// without freeing the pages under it. That is the known leak the page-reclamation
// milestone will close. Until then page accounting runs on demand through
// CheckPageAccounting, wired into aki check and the accounting tests, so the leak
// is still observable without crashing a normal FLUSHDB.
func (ks *Keyspace) assertConsistent() {
	if pinned := ks.pgr.PinnedPages(); len(pinned) != 0 {
		panic(fmt.Sprintf("keyspace: pages still pinned after commit: %v", pinned))
	}
}
