// Command akiv2 runs the v2 engine behind a minimal RESP front end so the
// standard redis-benchmark saturation harness can drive it. It exists to measure
// the v2 read/write path on its own, separate from the v1 server.
package main

import (
	"flag"
	"log"
	"runtime"

	"github.com/tamnd/aki/v2/server"
	"github.com/tamnd/aki/v2/store"
)

func main() {
	addr := flag.String("addr", ":6399", "listen address")
	shards := flag.Int("shards", 256, "number of index+log shards (power of two)")
	pageKiB := flag.Int("page-kib", 1024, "log page size in KiB")
	residentPages := flag.Int("resident-pages", 0, "resident pages per shard (0 = unbounded, memory-only)")
	dir := flag.String("dir", "", "spill directory (empty = memory-only)")
	flag.Parse()

	s, err := store.New(store.Tunables{
		Shards:                *shards,
		PageSize:              *pageKiB << 10,
		ResidentPagesPerShard: *residentPages,
		Dir:                   *dir,
	})
	if err != nil {
		log.Fatalf("akiv2: store: %v", err)
	}
	defer func() { _ = s.Close() }()

	srv := server.New(s)
	log.Printf("akiv2 listening on %s (shards=%d page=%dKiB resident-pages=%d dir=%q GOMAXPROCS=%d)",
		*addr, *shards, *pageKiB, *residentPages, *dir, runtime.GOMAXPROCS(0))
	if err := srv.ListenAndServe(*addr); err != nil {
		log.Fatalf("akiv2: serve: %v", err)
	}
}
