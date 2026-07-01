// Command f1srv runs the clean-room f1raw string server. It is the from-first-
// principles in-memory wire path the 2x claim is measured on, separate from the
// historical aki server binary. The flags it accepts are a subset of the aki/redis
// server flags so an existing benchmark harness can launch or connect to it without
// special casing.
package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"

	"github.com/tamnd/aki/f1srv"
)

func main() {
	// "server" subcommand is accepted and skipped so the harness's
	// `f1srv server --addr ...` invocation matches the aki binary's shape.
	args := os.Args[1:]
	if len(args) > 0 && args[0] == "server" {
		args = args[1:]
	}

	fs := flag.NewFlagSet("f1srv", flag.ExitOnError)
	addr := fs.String("addr", "127.0.0.1:6390", "listen address host:port")
	dir := fs.String("dir", ".", "working directory; holds the cold value log when --ltm-cold is set")
	_ = fs.String("appendonly", "no", "append-only file (accepted, no durability yet)")
	_ = fs.String("appendfsync", "everysec", "fsync policy (accepted, no durability yet)")
	_ = fs.String("aki-engine", "f1raw", "engine name (accepted; this binary is always f1raw)")
	netMode := fs.String("aki-net", "go", "net model: go (goroutine-per-conn, default) or reactor (Linux epoll)")
	indexBuckets := fs.Int("index-buckets", 1<<22, "f1raw index buckets")
	arenaBytes := fs.Int("arena-bytes", 2<<30, "f1raw arena size in bytes")
	stripes := fs.Int("incr-stripes", 1<<10, "INCR-family RMW lock stripes")
	ltmCold := fs.Bool("ltm-cold", false, "engage the larger-than-memory string tier: separate large values to a cold log under --dir")
	sepThreshold := fs.Int("sep-threshold", 0, "inline-vs-separated value cutoff in bytes for --ltm-cold (0 = engine default)")
	pprofAddr := fs.String("pprof", "", "if set, serve net/http/pprof on this host:port (profiling only, off by default)")
	// ExitOnError means a bad flag exits the process, so Parse never returns a
	// non-nil error here. Blank-assign to satisfy errcheck without dead handling.
	_ = fs.Parse(args)

	cfg := f1srv.DefaultConfig(*addr)
	cfg.IndexBuckets = *indexBuckets
	cfg.ArenaBytes = *arenaBytes
	cfg.IncrStripes = *stripes
	cfg.NetMode = *netMode
	if *ltmCold {
		cfg.ColdPath = filepath.Join(*dir, "f1raw-cold.vlog")
		cfg.SepThreshold = *sepThreshold
	}

	if *pprofAddr != "" {
		go func() {
			log.Printf("f1srv: pprof on http://%s/debug/pprof/", *pprofAddr)
			if err := http.ListenAndServe(*pprofAddr, nil); err != nil {
				log.Printf("f1srv: pprof server: %v", err)
			}
		}()
	}

	srv := f1srv.New(cfg)
	if err := srv.Listen(); err != nil {
		log.Fatalf("f1srv: listen %s: %v", *addr, err)
	}
	cold := "off"
	if *ltmCold {
		cold = cfg.ColdPath
	}
	fmt.Printf("f1srv listening on %s (index-buckets=%d arena=%dMiB cold=%s)\n",
		srv.Addr(), *indexBuckets, *arenaBytes>>20, cold)
	if err := srv.ListenAndServe(); err != nil {
		log.Fatalf("f1srv: serve: %v", err)
	}
}
