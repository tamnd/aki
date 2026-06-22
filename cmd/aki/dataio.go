package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/tamnd/aki/command"
	"github.com/tamnd/aki/rdb"
)

// cmdImport ingests a Redis dump.rdb into an aki database file, or ships it to a
// running instance with --addr. AOF input is not handled yet; the format is
// detected from the leading magic bytes.
func cmdImport(args []string) error {
	fs := flag.NewFlagSet("import", flag.ContinueOnError)
	format := fs.String("format", "detect", "input format: rdb or detect")
	target := fs.String("target", "aki.aki", "path to the .aki file to write")
	addr := fs.String("addr", "", "ship the keys to this running instance instead of a file")
	auth := fs.String("auth", "", "password to send to the --addr instance")
	db := fs.Int("db", -1, "import only this source database (default all)")
	replace := fs.Bool("replace", false, "overwrite keys that already exist")
	dryRun := fs.Bool("dry-run", false, "parse and count without writing")
	pos, err := parseInterspersed(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return fmt.Errorf("usage: aki import <file> [--target path | --addr host:port] [--db N] [--replace] [--dry-run]")
	}
	src := pos[0]

	blob, err := os.ReadFile(src)
	if err != nil {
		return fmt.Errorf("read %s: %w", src, err)
	}
	if err := requireRDB(*format, blob); err != nil {
		return err
	}

	snap, err := rdb.UnmarshalFile(blob)
	if err != nil {
		return fmt.Errorf("parse RDB %s: %w", src, err)
	}

	if *dryRun {
		fmt.Printf("dry run: %d keys in %s would be imported\n", command.CountSnapshot(snap, *db), src)
		return nil
	}

	if *addr != "" {
		n, ierr := importToServer(*addr, *auth, snap, *db, *replace, 30*time.Second)
		if ierr != nil {
			return fmt.Errorf("import into %s: %w", *addr, ierr)
		}
		fmt.Printf("imported %d keys from %s into %s\n", n, src, *addr)
		return nil
	}

	ks, closeKS, err := openKeyspace(*target, snapshotDBCount(snap))
	if err != nil {
		return err
	}
	defer closeKS()

	n, err := command.LoadSnapshot(ks, snap, *db, *replace)
	if err != nil {
		return fmt.Errorf("import into %s: %w", *target, err)
	}
	if err := ks.Commit(); err != nil {
		return fmt.Errorf("commit %s: %w", *target, err)
	}
	fmt.Printf("imported %d keys from %s into %s\n", n, src, *target)
	return nil
}

// cmdDump exports a keyspace to a Redis dump.rdb. It reads from an offline .aki
// file with --file, or from a running instance over the wire with --addr.
func cmdDump(args []string) error {
	fs := flag.NewFlagSet("dump", flag.ContinueOnError)
	format := fs.String("format", "rdb", "output format: rdb")
	output := fs.String("output", "dump.rdb", "output file path")
	db := fs.Int("db", -1, "export only this database (default all)")
	file := fs.String("file", "", "read directly from this .aki file (offline)")
	addr := fs.String("addr", "", "connect to a running instance over the wire")
	auth := fs.String("auth", "", "password to send to the --addr instance")
	databases := fs.Int("databases", 16, "with --addr, number of databases to scan")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *format != "rdb" {
		return fmt.Errorf("only --format rdb is supported")
	}
	if *file == "" && *addr == "" {
		return fmt.Errorf("usage: aki dump (--file <.aki> | --addr host:port) [--output path] [--db N]")
	}
	if *file != "" && *addr != "" {
		return fmt.Errorf("use either --file or --addr, not both")
	}

	var (
		snap   rdb.Snapshot
		source string
		err    error
	)
	if *addr != "" {
		snap, err = dumpFromServer(*addr, *auth, *databases, *db, 30*time.Second)
		source = *addr
	} else {
		snap, err = dumpFromFile(*file, *db)
		source = *file
	}
	if err != nil {
		return err
	}

	blob, err := rdb.MarshalFile(snap)
	if err != nil {
		return fmt.Errorf("encode RDB: %w", err)
	}
	if err := os.WriteFile(*output, blob, 0o644); err != nil {
		return fmt.Errorf("write %s: %w", *output, err)
	}
	fmt.Printf("dumped %d keys from %s to %s\n", command.CountSnapshot(snap, -1), source, *output)
	return nil
}

// dumpFromFile reads a snapshot out of an offline .aki file, optionally limited
// to one database.
func dumpFromFile(file string, db int) (rdb.Snapshot, error) {
	ks, closeKS, err := openKeyspace(file, 16)
	if err != nil {
		return rdb.Snapshot{}, err
	}
	defer closeKS()

	snap, err := command.SnapshotKeyspace(ks)
	if err != nil {
		return rdb.Snapshot{}, fmt.Errorf("read %s: %w", file, err)
	}
	if db >= 0 {
		snap = filterDB(snap, db)
	}
	return snap, nil
}

// parseInterspersed parses a flag set that allows positional arguments to appear
// before, after, or between flags, which the stdlib flag package does not do on
// its own. It returns the positional arguments in order.
func parseInterspersed(fs *flag.FlagSet, args []string) ([]string, error) {
	var positional []string
	rest := args
	for len(rest) > 0 {
		if err := fs.Parse(rest); err != nil {
			return nil, err
		}
		rest = fs.Args()
		if len(rest) == 0 {
			break
		}
		positional = append(positional, rest[0])
		rest = rest[1:]
	}
	return positional, nil
}

// requireRDB checks the input is an RDB file. The detect form looks at the magic;
// an explicit rdb still verifies the magic so a mislabeled file fails clearly.
func requireRDB(format string, blob []byte) error {
	switch format {
	case "rdb", "detect":
		if len(blob) < 5 || string(blob[:5]) != "REDIS" {
			if format == "detect" {
				return fmt.Errorf("cannot detect format: not an RDB file (AOF import is not supported yet)")
			}
			return fmt.Errorf("not an RDB file: missing REDIS magic")
		}
		return nil
	case "aof":
		return fmt.Errorf("AOF import is not supported yet")
	default:
		return fmt.Errorf("unknown format %q", format)
	}
}

// snapshotDBCount returns a database count large enough to hold every index in the
// snapshot, so a fresh target file is created with room for them.
func snapshotDBCount(snap rdb.Snapshot) int {
	count := 16
	for _, db := range snap.DBs {
		if db.Index+1 > count {
			count = db.Index + 1
		}
	}
	return count
}

// filterDB keeps only the named database, renumbered to 0, the way a single-db
// export is expected to land.
func filterDB(snap rdb.Snapshot, index int) rdb.Snapshot {
	out := rdb.Snapshot{Aux: snap.Aux}
	for _, db := range snap.DBs {
		if db.Index == index {
			out.DBs = append(out.DBs, rdb.DBData{Index: 0, Entries: db.Entries})
		}
	}
	return out
}
