// Command f3srv is the f3 server binary (spec 2064/f3). What runs today is
// the M0 point surface: the shard runtime behind a TCP listener speaking
// RESP2, with PING, ECHO, and the string commands in the dispatch table; the
// remaining M0 slices grow the table from here.
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/f3srv/drivers"
)

func main() {
	addr := flag.String("addr", ":6379", "TCP listen address")
	shards := flag.Int("shards", shard.DefaultShards(),
		"owner workers, one pinned thread and one store each; the default is the 60 percent core split of spec 2064/f3/03 section 2.2")
	arenaMiB := flag.Int("arena-mib", 256, "arena MiB per shard")
	vlogDir := flag.String("vlog-dir", "",
		"directory for per-shard value logs; empty keeps values in memory")
	residentMiB := flag.Int("resident-cap-mib", 0,
		"resident byte budget MiB per shard; past it, large values spill to the shard's value log (needs -vlog-dir; 0 means uncapped)")
	pinWorkers := flag.Bool("pin-workers", false,
		"lock each shard worker to an OS thread; off by default, the locked-M park/unpark handoff costs more than thread residency buys (labs/f3/m0/11_transport)")
	pprofAddr := flag.String("pprof-addr", "",
		"listen address for net/http/pprof, e.g. 127.0.0.1:6060; the endpoint has no auth, so keep it on loopback; empty leaves it off")
	connShape := flag.String("conn-shape", drivers.ShapeSingle,
		"per-connection goroutine shape: single (one goroutine reads, dispatches, drains, and flushes) or pair (the M0 reader/writer pair, kept for the labs/f3/m0/15_conn_single A/B)")
	netDriver := flag.String("net", drivers.NetGoroutine,
		"network driver: goroutine (the default, one shape-selected handler per connection) or reactor (raw epoll event loops, Linux only; elsewhere it logs a notice and serves on the goroutine driver)")
	netLoops := flag.Int("net-loops", 0,
		"reactor event loops; 0 takes the 2/5 network share of the core split (labs/f3/m0/19_loop_count; only the reactor driver reads this)")
	flag.Parse()

	srv, err := drivers.Listen(drivers.Options{
		Addr:             *addr,
		Shards:           *shards,
		ArenaBytes:       *arenaMiB << 20,
		VlogDir:          *vlogDir,
		ResidentCapBytes: uint64(*residentMiB) << 20,
		PinWorkers:       *pinWorkers,
		PprofAddr:        *pprofAddr,
		ConnShape:        *connShape,
		NetDriver:        *netDriver,
		NetLoops:         *netLoops,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "f3srv:", err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "f3srv: serving on %s with %d shards\n", srv.Addr(), *shards)
	if pa := srv.PprofAddr(); pa != nil {
		fmt.Fprintf(os.Stderr, "f3srv: pprof on http://%s/debug/pprof/\n", pa)
	}
	if err := srv.Serve(); err != nil {
		fmt.Fprintln(os.Stderr, "f3srv:", err)
		os.Exit(1)
	}
}
