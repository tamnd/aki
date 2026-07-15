// Command sqlo1crash is the G4 crash harness for the sqlo1 driver: a kill
// -9 loop with a shadow oracle and a keyspace checksum diff (spec
// 2064/sqlo1 doc 13). Each iteration starts the server, hammers it with
// SET/DEL/EXPIRE while every reply is checked against the oracle, kills it
// with SIGKILL at a random point, restarts it, and then classifies every
// key: matched, in-flight op applied, lost, or corrupt.
//
// Corruption fails the run always. Lost acked writes fail it only under
// -durable, which is what lets the same binary gate the S0 memory
// placeholder (clean restart, nothing corrupt, everything lost) and the A2
// SQLite store (nothing lost either) without changing the verifier. The
// full 1000-iteration G4 pass runs on the gate box from A2 onward.
package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"
)

func main() {
	bin := flag.String("bin", "sqlo1srv", "server binary to launch and kill")
	serverArgs := flag.String("server-args", "", "extra server arguments, space separated (data dir flags go here when a store gains one)")
	iterations := flag.Int("iterations", 5, "kill cycles to run")
	workers := flag.Int("workers", 4, "concurrent load connections")
	keys := flag.Int("keys", 256, "keys per worker")
	killMin := flag.Duration("kill-min", 200*time.Millisecond, "earliest kill point after load starts")
	killMax := flag.Duration("kill-max", 1500*time.Millisecond, "latest kill point after load starts")
	durable := flag.Bool("durable", false, "fail on lost acked writes, not only on corruption")
	seed := flag.Int64("seed", 1, "rng seed, printed so a failure reproduces")
	flag.Parse()

	if *killMax < *killMin {
		fmt.Fprintln(os.Stderr, "sqlo1crash: -kill-max must be at least -kill-min")
		os.Exit(2)
	}
	cfg := config{
		bin:        *bin,
		args:       strings.Fields(*serverArgs),
		iterations: *iterations,
		workers:    *workers,
		keys:       *keys,
		killMin:    *killMin,
		killMax:    *killMax,
		durable:    *durable,
		seed:       *seed,
	}

	mode := "non-durable (corruption fails, loss reported)"
	if cfg.durable {
		mode = "durable (corruption or loss fails)"
	}
	fmt.Printf("sqlo1crash: %d iterations, %d workers x %d keys, seed %d, %s\n",
		cfg.iterations, cfg.workers, cfg.keys, cfg.seed, mode)

	failures := 0
	var worstRecovery time.Duration
	for iter := range cfg.iterations {
		res, err := runIteration(cfg, iter)
		if err != nil {
			fmt.Fprintf(os.Stderr, "sqlo1crash: %v\n", err)
			failures++
			continue
		}
		ok := res.pass(cfg.durable)
		status := "ok"
		if !ok {
			status = "FAIL"
			failures++
		}
		if res.recovery > worstRecovery {
			worstRecovery = res.recovery
		}
		fmt.Printf("iter %d: %d ops, killed after %v, recovery %v, matched %d, pending-applied %d, lost %d, corrupt %d [%s]\n",
			iter, res.ops, res.killAfter.Round(time.Millisecond), res.recovery.Round(time.Millisecond),
			res.matched, res.pendingApplied, res.lost, res.corrupt, status)
		if res.corrupt > 0 {
			fmt.Printf("iter %d: oracle digest %s, observed digest %s\n", iter, res.oracleDigest, res.observedDigest)
			for _, k := range res.corruptKeys {
				fmt.Printf("iter %d: corrupt key %s\n", iter, k)
			}
		}
	}

	if failures > 0 {
		fmt.Printf("sqlo1crash: %d of %d iterations failed\n", failures, cfg.iterations)
		os.Exit(1)
	}
	fmt.Printf("sqlo1crash: %d iterations clean, worst recovery %v\n", cfg.iterations, worstRecovery.Round(time.Millisecond))
}
