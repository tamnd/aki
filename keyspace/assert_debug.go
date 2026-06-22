//go:build debug

package keyspace

import "fmt"

// assertConsistent runs the pin and page-accounting checks and panics if a commit
// left the file inconsistent. It is compiled in only under the debug build tag, so
// debug and test builds catch a Get without a matching Unpin, a leak, a double
// reference, or a use-after-free the moment a commit creates one. Production builds
// get the no-op in assert_prod.go.
//
// Page accounting is now a global invariant on every commit. It used to be exempt
// for FLUSHDB and FLUSHALL, which dropped a whole tree's root without freeing the
// pages under it, but the page-reclamation milestone taught Flush to return those
// pages to the freelist, so the last path that leaked is closed.
func (ks *Keyspace) assertConsistent() {
	if pinned := ks.pgr.PinnedPages(); len(pinned) != 0 {
		panic(fmt.Sprintf("keyspace: pages still pinned after commit: %v", pinned))
	}
	if err := ks.CheckPageAccounting(); err != nil {
		panic(fmt.Sprintf("keyspace: page accounting failed after commit: %v", err))
	}
}
