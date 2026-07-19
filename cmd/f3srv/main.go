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
	akiPath := flag.String("aki", "",
		"path to the shared durable .aki file (the M8 durable arc): every write commits through the group-commit writer and a restart rebuilds the whole keyspace, strings and all five collection types, from the file; empty keeps the server non-durable; mutually exclusive with -vlog-dir, -aki wins")
	residentMiB := flag.Int("resident-cap-mib", 0,
		"resident byte budget MiB per shard; past it, large values spill to the shard's value log (needs -vlog-dir; 0 means uncapped)")
	pinWorkers := flag.Bool("pin-workers", false,
		"lock each shard worker to an OS thread; off by default, the locked-M park/unpark handoff costs more than thread residency buys (labs/f3/m0/11_transport)")
	pprofAddr := flag.String("pprof-addr", "",
		"listen address for net/http/pprof, e.g. 127.0.0.1:6060; the endpoint has no auth, so keep it on loopback; empty leaves it off")
	connShape := flag.String("conn-shape", drivers.ShapeSingle,
		"per-connection goroutine shape: single (one goroutine reads, dispatches, drains, and flushes) or pair (the M0 reader/writer pair, kept for the labs/f3/m0/15_conn_single A/B)")
	netDriver := flag.String("net", drivers.NetGoroutine,
		"network driver: goroutine (the default, one shape-selected handler per connection), reactor (raw epoll event loops, Linux only), or uring (io_uring event loops, Linux with a probed kernel); where an event-loop driver cannot run it logs a notice and serves on the goroutine driver")
	netLoops := flag.Int("net-loops", 0,
		"event loops for the reactor and uring drivers; 0 takes GOMAXPROCS/2, the loop-count knee re-swept on the current surface (labs/f3/m0/26_loop_knee; the goroutine driver ignores this)")
	connSpinHighWater := flag.Int("conn-spin-highwater", 0,
		"live connections at or above which a connection writer parks immediately instead of spinning (labs/f3/m0/22_conn_spin); 0 keeps the GOMAXPROCS*6 default, 1 always parks fast, a huge value restores unconditional spin")
	readBufKiB := flag.Int("read-buf-kib", 0,
		"initial per-connection read buffer KiB; 0 takes the 64KiB default. It grows on demand for a larger command, so a smaller value only trims idle/point-op connections (labs/f3/m0/24_conn_buffers)")
	replyBufKiB := flag.Int("reply-buf-kib", 0,
		"per-connection reply writer buffer KiB; 0 takes the 64KiB default. One pipeline round of replies should fit or the writer flushes mid-drain (labs/f3/m0/24_conn_buffers)")
	batchDataCap := flag.Int("batch-data-cap", 0,
		"per-hop-node starting data-buffer bytes; 0 takes the tuning.go default. It grows on demand for a bigger command, so a smaller start only trims the steady small-value path (labs/f3/m0/25_conn_caps)")
	repCap := flag.Int("rep-cap", 0,
		"per-hop-node starting reply-buffer bytes; 0 takes the tuning.go default (batchDataCap+64*batchCap). It grows on demand for a bigger reply, so a smaller start only trims the steady write-heavy path (labs/f3/m0/27_rep_headroom cut 15MiB off the c512 SET cell at rep-cap 1024)")
	replyRing := flag.Int("reply-ring", 0,
		"per-connection reply reorder window in commands; 0 takes the tuning.go default. It must cover the pipeline depth or the reader throttles (labs/f3/m0/25_conn_caps)")
	freeListCap := flag.Int("free-list-cap", 0,
		"per-connection hop-node free-list size; 0 takes the tuning.go default. It bounds the pooled idle nodes each connection retains (labs/f3/m0/25_conn_caps)")
	flag.Parse()
	if *connSpinHighWater > 0 {
		shard.SetConnSpinHighWater(*connSpinHighWater)
	}

	srv, err := drivers.Listen(drivers.Options{
		Addr:             *addr,
		Shards:           *shards,
		ArenaBytes:       *arenaMiB << 20,
		VlogDir:          *vlogDir,
		AkiPath:          *akiPath,
		ResidentCapBytes: uint64(*residentMiB) << 20,
		PinWorkers:       *pinWorkers,
		PprofAddr:        *pprofAddr,
		ConnShape:        *connShape,
		NetDriver:        *netDriver,
		NetLoops:         *netLoops,
		ReadBufBytes:     *readBufKiB << 10,
		ReplyBufBytes:    *replyBufKiB << 10,
		BatchDataCap:     *batchDataCap,
		RepCap:           *repCap,
		ReplyRing:        *replyRing,
		FreeListCap:      *freeListCap,
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
