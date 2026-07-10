// Command f3srv is the f3 server binary (spec 2064/f3). What runs today is
// the M0 smoke surface: the shard runtime behind a TCP listener answering
// PING and ECHO; the RESP2 slice grows the parser and the string slices grow
// the command table.
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
	flag.Parse()

	srv, err := drivers.Listen(drivers.Options{
		Addr:       *addr,
		Shards:     *shards,
		ArenaBytes: *arenaMiB << 20,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "f3srv:", err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "f3srv: serving on %s with %d shards\n", srv.Addr(), *shards)
	if err := srv.Serve(); err != nil {
		fmt.Fprintln(os.Stderr, "f3srv:", err)
		os.Exit(1)
	}
}
