package keyspace

import (
	"testing"

	"github.com/tamnd/aki/engine/hot"
	"github.com/tamnd/aki/pager"
	"github.com/tamnd/aki/vfs"
)

// benchHotDB opens a keyspace database routed through the clean hot/ engine, the
// F2 hot tier the saturation benchmarks run with (--aki-engine hot). It is here so
// a CPU and mutex profile can see exactly what the wire benchmark's GET/SET path
// spends its time and coordination on, isolated from the network and the client.
func benchHotDB(b *testing.B) *DB {
	b.Helper()
	fs := vfs.NewMem()
	p, err := pager.Create(fs, "bench.aki", pager.Options{PageSize: 4096, DBCount: 16})
	if err != nil {
		b.Fatalf("create pager: %v", err)
	}
	b.Cleanup(func() { _ = p.Close() })
	ks, err := Open(p, WithHotEngine(hot.Tunables{Shards: 256, IndexHintPerShard: bnKeys / 256}))
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	db, _ := ks.DB(0)
	return db
}

func benchHotGet(b *testing.B) {
	db := benchHotDB(b)
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

func benchHotSet(b *testing.B) {
	db := benchHotDB(b)
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

func BenchmarkKeyspaceGetHot(b *testing.B) { benchHotGet(b) }
func BenchmarkKeyspaceSetHot(b *testing.B) { benchHotSet(b) }
