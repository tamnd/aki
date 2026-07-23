// Command sqlo1srv serves the Redis protocol over the sqlo1 engine.
//
// One command, no config file: sqlo1srv -addr :6379 starts serving on
// the placeholder memory store. The real thing is the single-file
// store: sqlo1srv -store file -path data.aki opens the file if it
// exists (running recovery) or creates it, and the dataset moves
// between machines as that file plus its WAL sidecar.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/tamnd/aki/engine/sqlo1"
	"github.com/tamnd/aki/engine/sqlo1b"
)

// walSegSize matches the engine's production ring segment.
const walSegSize = 64 << 20

func main() {
	addr := flag.String("addr", ":6379", "listen address")
	store := flag.String("store", "mem", "store backend: mem or file")
	path := flag.String("path", "", "data file for -store file; the WAL sidecar sits next to it")
	maxBytes := flag.Int64("max-bytes", 0, "data file budget in bytes for -store file; 0 is unbounded and disables the free-extent pressure rung")
	ioBackend := flag.String("io-backend", "auto", "cold-read IO backend for -store file: auto (ring where supported, iopool otherwise) or iopool; INFO reports which one is live")
	flag.Parse()

	switch *ioBackend {
	case "auto":
	case "iopool":
		sqlo1b.ForceIOPool = true
	default:
		fmt.Fprintf(os.Stderr, "sqlo1srv: unknown io backend %q\n", *ioBackend)
		os.Exit(2)
	}

	var st sqlo1.Store
	var db *sqlo1b.Store
	switch *store {
	case "mem":
		st = sqlo1.NewMemStore()
	case "file":
		if *path == "" {
			fmt.Fprintln(os.Stderr, "sqlo1srv: -store file needs -path")
			os.Exit(2)
		}
		var err error
		if _, serr := os.Stat(*path); os.IsNotExist(serr) {
			db, err = sqlo1b.CreateStore(*path, walSegSize)
		} else {
			db, err = sqlo1b.OpenStore(*path, walSegSize)
		}
		if err != nil {
			log.Fatalf("sqlo1srv: %v", err)
		}
		if *maxBytes > 0 {
			db.SetMaxBytes(*maxBytes)
		}
		st = db
	default:
		fmt.Fprintf(os.Stderr, "sqlo1srv: unknown store %q\n", *store)
		os.Exit(2)
	}

	l, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("sqlo1srv: %v", err)
	}
	// The bound address on its own line, so a script that started us on
	// port 0 can find the port.
	fmt.Printf("sqlo1srv listening on %s\n", l.Addr())

	srv, err := sqlo1.NewServer(st)
	if err != nil {
		log.Fatalf("sqlo1srv: %v", err)
	}

	// Clean shutdown on SIGINT or SIGTERM: close the listener so Serve
	// returns, drain the hot tier, checkpoint, and close the store. An
	// acked write lives only in the hot tier until a drain carries it
	// down, so skipping the Flush here would keep only the last drained
	// prefix, which is the crash contract, not the shutdown contract.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sig
		l.Close()
	}()

	if err := srv.Serve(l); err != nil {
		log.Fatalf("sqlo1srv: %v", err)
	}
	if db != nil {
		if err := srv.Flush(context.Background()); err != nil {
			log.Fatalf("sqlo1srv: flush: %v", err)
		}
		if err := db.Checkpoint(); err != nil {
			log.Fatalf("sqlo1srv: checkpoint: %v", err)
		}
		if err := db.Close(); err != nil {
			log.Fatalf("sqlo1srv: close: %v", err)
		}
	}
}
