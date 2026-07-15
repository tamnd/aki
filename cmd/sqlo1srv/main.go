// Command sqlo1srv serves the Redis protocol over the sqlo1 engine.
//
// One command, no config file: sqlo1srv -addr :6379 starts serving. The
// only store at S0 is the placeholder memory store; -store grows sqlite
// and file values as the tracks land.
package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"

	"github.com/tamnd/aki/engine/sqlo1"
)

func main() {
	addr := flag.String("addr", ":6379", "listen address")
	store := flag.String("store", "mem", "store backend: mem")
	flag.Parse()

	var st sqlo1.Store
	switch *store {
	case "mem":
		st = sqlo1.NewMemStore()
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

	if err := sqlo1.NewServer(st).Serve(l); err != nil {
		log.Fatalf("sqlo1srv: %v", err)
	}
}
