// Command aki is the aki server and toolbox. aki speaks the Redis wire protocol
// over a single .aki file (spec 2064). The full subcommand surface (server, cli,
// check, dump, import, bench) is built milestone by milestone; this M0 build
// ships the storage-substrate tooling: version reporting and a file inspector.
package main

import (
	"fmt"
	"os"
)

// Build metadata, injected at release time via -ldflags -X main.Version=...
var (
	Version = "dev"
	Commit  = "none"
	Date    = "unknown"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "aki:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		usage()
		return nil
	}
	switch args[0] {
	case "version", "-v", "--version":
		fmt.Printf("aki %s (commit %s, built %s)\n", Version, Commit, Date)
		return nil
	case "server":
		return cmdServer(args[1:])
	case "check":
		return cmdCheck(args[1:])
	case "import":
		return cmdImport(args[1:])
	case "dump":
		return cmdDump(args[1:])
	case "bench":
		return cmdBench(args[1:])
	case "help", "-h", "--help":
		usage()
		return nil
	default:
		return fmt.Errorf("unknown command %q (try: aki help)", args[0])
	}
}

func usage() {
	fmt.Print(`aki - a Redis-compatible database in a single file

Usage:
  aki <command> [arguments]

Commands:
  server         Start the aki server (Redis wire protocol)
  check <file>   Inspect an .aki file, or validate an RDB with --rdb <file>
  import <file>  Import a Redis dump.rdb into an .aki file
  dump --file f  Export an .aki file to a Redis dump.rdb
  bench run      Run a load test against a server and report latency
  version        Print version information
  help           Show this help

More commands (cli) arrive as the engine is built.
See the specification under notes/Spec/2064.
`)
}
