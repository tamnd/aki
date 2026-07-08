// Command f2srv runs the minimal f2raw string server: one lock-free hash index
// over one in-memory hybrid log, wrapped in the thinnest RESP layer that answers
// GET/SET/INCR/DEL. It exists to measure the base point path over the wire against
// Redis and Valkey before any collection or tiering machinery is layered on. The
// flags it accepts are a subset of the aki/redis server flags so an existing
// benchmark harness can launch it without special casing.
package main

import (
	"flag"
	"log"
	"os"
	"runtime/debug"

	"github.com/tamnd/aki/engine/f2raw"
	"github.com/tamnd/aki/f2srv"
)

func main() {
	// "server" subcommand is accepted and skipped so a harness's
	// `f2srv server --addr ...` invocation matches the aki binary's shape.
	args := os.Args[1:]
	if len(args) > 0 && args[0] == "server" {
		args = args[1:]
	}

	fs := flag.NewFlagSet("f2srv", flag.ExitOnError)
	addr := fs.String("addr", "127.0.0.1:6392", "listen address host:port")
	indexBuckets := fs.Int("index-buckets", 1<<22, "f2raw index buckets")
	arenaBytes := fs.Int("arena-bytes", 2<<30, "f2raw arena size in bytes")
	gogc := fs.Int("gogc", 0, "GOGC percent (0 = Go default 100); raise to trade memory for fewer GC cycles")
	// Flags the harness passes to the redis/aki servers; accepted and ignored so a
	// shared launch line does not need special casing for this binary.
	_ = fs.String("dir", ".", "working directory (accepted, unused)")
	_ = fs.String("appendonly", "no", "append-only file (accepted, no durability)")
	_ = fs.String("appendfsync", "everysec", "fsync policy (accepted, no durability)")
	_ = fs.String("aki-engine", "f2raw", "engine name (accepted; this binary is always f2raw)")
	_ = fs.String("aki-net", "go", "net model (accepted; this binary is always goroutine-per-conn)")
	_ = fs.String("save", "", "RDB save points (accepted, unused)")
	if err := fs.Parse(args); err != nil {
		log.Fatal(err)
	}
	if *gogc > 0 {
		debug.SetGCPercent(*gogc)
	}

	store := f2raw.New(*indexBuckets, *arenaBytes)
	srv := f2srv.New(store)
	log.Printf("f2srv listening on %s (index-buckets=%d arena-bytes=%d)", *addr, *indexBuckets, *arenaBytes)
	if err := srv.ListenAndServe(*addr); err != nil {
		log.Fatal(err)
	}
}
