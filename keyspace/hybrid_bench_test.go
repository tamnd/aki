package keyspace

import (
	"fmt"
	"testing"

	"github.com/tamnd/aki/pager"
	"github.com/tamnd/aki/store"
	"github.com/tamnd/aki/vfs"
)

// benchDB opens a keyspace database for benchmarking. With hl set it engages the
// hybrid-log string path; otherwise it runs the default paged B-tree. Both use an
// in-memory pager so the measurement is the engine's CPU path, not disk.
func benchDB(b *testing.B, hl bool) *DB {
	b.Helper()
	fs := vfs.NewMem()
	p, err := pager.Create(fs, "bench.aki", pager.Options{PageSize: 4096, DBCount: 16})
	if err != nil {
		b.Fatalf("create pager: %v", err)
	}
	b.Cleanup(func() { _ = p.Close() })
	var opts []Option
	if hl {
		opts = append(opts, WithHybridLog(store.Tunables{Shards: 64, PageSize: 1 << 20, IndexHintPerShard: bnKeys / 64}))
	}
	ks, err := Open(p, opts...)
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	db, _ := ks.DB(0)
	return db
}

// bnKeys is the working set both engines are loaded with before the timed loop.
const bnKeys = 200_000

func loadKeys(b *testing.B, db *DB) [][]byte {
	keys := make([][]byte, bnKeys)
	val := []byte("0123456789abcdef0123456789abcdef") // 32-byte value
	for i := 0; i < bnKeys; i++ {
		keys[i] = []byte(fmt.Sprintf("user:session:%08d", i))
		if err := db.Set(keys[i], val, TypeString, EncRaw, -1); err != nil {
			b.Fatalf("Set %d: %v", i, err)
		}
	}
	return keys
}

func benchGet(b *testing.B, hl bool) {
	db := benchDB(b, hl)
	keys := loadKeys(b, db)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var x uint32 = 0x9e3779b9
		for pb.Next() {
			x ^= x << 13
			x ^= x >> 17
			x ^= x << 5
			k := keys[int(x)%bnKeys]
			if _, _, found, err := db.Get(k); err != nil || !found {
				b.Fatalf("Get miss: found=%v err=%v", found, err)
			}
		}
	})
}

func benchSet(b *testing.B, hl bool) {
	db := benchDB(b, hl)
	keys := loadKeys(b, db)
	val := []byte("0123456789abcdef0123456789abcdef")
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var x uint32 = 0x12345678
		for pb.Next() {
			x ^= x << 13
			x ^= x >> 17
			x ^= x << 5
			k := keys[int(x)%bnKeys]
			if err := db.Set(k, val, TypeString, EncRaw, -1); err != nil {
				b.Fatalf("Set: %v", err)
			}
		}
	})
}

func BenchmarkKeyspaceGetBtree(b *testing.B)  { benchGet(b, false) }
func BenchmarkKeyspaceGetHybrid(b *testing.B) { benchGet(b, true) }
func BenchmarkKeyspaceSetBtree(b *testing.B)  { benchSet(b, false) }
func BenchmarkKeyspaceSetHybrid(b *testing.B) { benchSet(b, true) }
