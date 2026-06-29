package keyspace

import (
	"strconv"
	"testing"
)

// BenchmarkRecordAccessParallel measures the per-read LFU/idle bookkeeping
// recordAccess does on every GET hit, under the cross-core contention a saturating
// GET-P64 load puts on it. Every hybrid GET routes through recordAccess, which
// today takes a single per-DB mutex (TryLock) and updates one shared map, so under
// many cores every reader CAS-es the same mutex word. This benchmark drives that
// path from every core against a spread of pre-seeded keys, the read-side analogue
// of the GET saturation load, so its scaling across -cpu shows whether the per-DB
// access lock is a cross-core wall the way the call counter was.
func BenchmarkRecordAccessParallel(b *testing.B) {
	db := benchDB(b, false)
	const nkeys = 4096
	keys := make([][]byte, nkeys)
	for i := range keys {
		keys[i] = []byte("k:" + strconv.Itoa(i))
		db.recordAccess(keys[i], true) // seed the entry so the read path takes the repeat branch
	}
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			db.recordAccess(keys[i&(nkeys-1)], false)
			i++
		}
	})
}
