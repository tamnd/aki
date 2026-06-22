package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/tamnd/aki/format"
	"github.com/tamnd/aki/keyspace"
	"github.com/tamnd/aki/pager"
	"github.com/tamnd/aki/rdb"
	"github.com/tamnd/aki/vfs"
)

// cmdCheck verifies a file without starting a server. With --rdb it validates a
// Redis RDB file. Otherwise it runs the .aki integrity checker from doc 20
// section 9.2 and exits with 0 (healthy), 1 (warnings), 2 (errors), or 3
// (critical, file not usable).
func cmdCheck(args []string) error {
	fs := flag.NewFlagSet("check", flag.ContinueOnError)
	rdbPath := fs.String("rdb", "", "validate a Redis RDB file instead of an .aki file")
	file := fs.String("file", "", "path to the .aki file (or pass it positionally)")
	fix := fs.Bool("fix", false, "repair safe issues (clear impossibly-future TTLs)")
	verbose := fs.Bool("verbose", false, "print per-database detail")
	if err := fs.Parse(args); err != nil {
		return err
	}

	if *rdbPath != "" {
		return checkRDB(*rdbPath)
	}

	name := *file
	if name == "" {
		if fs.NArg() != 1 {
			return errors.New("usage: aki check <file> | aki check --rdb <file>")
		}
		name = fs.Arg(0)
	}

	if code := checkAki(name, *fix, *verbose, os.Stdout); code != 0 {
		os.Exit(code)
	}
	return nil
}

// checkResult collects the running output and the worst severity seen so the
// caller can map it to an exit code.
type checkResult struct {
	w    io.Writer
	code int
}

func (r *checkResult) ok(msg string, a ...any)   { _, _ = fmt.Fprintf(r.w, "  [OK] "+msg+"\n", a...) }
func (r *checkResult) warn(msg string, a ...any) { r.line(1, "WARN", msg, a...) }
func (r *checkResult) err(msg string, a ...any)  { r.line(2, "ERROR", msg, a...) }
func (r *checkResult) crit(msg string, a ...any) { r.line(3, "CRIT", msg, a...) }

func (r *checkResult) line(code int, tag, msg string, a ...any) {
	_, _ = fmt.Fprintf(r.w, "  ["+tag+"] "+msg+"\n", a...)
	if code > r.code {
		r.code = code
	}
}

// checkAki runs every integrity check on the .aki file at name and returns the
// exit code. It writes a line per check as it goes.
func checkAki(name string, fix, verbose bool, w io.Writer) int {
	_, _ = fmt.Fprintf(w, "aki check: checking %s\n", name)
	res := &checkResult{w: w}

	// Open validates the magic, header CRC, page size, and meta snapshot, so a
	// failure here means the file is not usable.
	p, err := pager.Open(vfs.NewOS(), name, pager.Options{})
	if err != nil {
		res.crit("open file: %v", err)
		summarize(w, res.code)
		return res.code
	}
	defer func() { _ = p.Close() }()

	h := p.Header()
	res.ok("magic bytes")
	res.ok("header CRC32")
	if h.FormatVersion > format.FormatVersion || h.MinReadVersion > format.FormatVersion {
		res.err("format version %d not supported by this build (max %d)", h.FormatVersion, format.FormatVersion)
	} else {
		res.ok("format version %d", h.FormatVersion)
	}
	res.ok("page size %d", h.PageSize)

	if free, ferr := p.CheckFreelist(); ferr != nil {
		res.err("free list: %v", ferr)
	} else {
		res.ok("free list (%d pages)", free)
	}

	ks, err := keyspace.Open(p)
	if err != nil {
		res.crit("open keyspace: %v", err)
		summarize(w, res.code)
		return res.code
	}

	checks, err := ks.Check()
	if err != nil {
		res.err("B-tree traversal: %v", err)
		summarize(w, res.code)
		return res.code
	}

	var entries, live, expires, badHeaders, orderErrors, stale, future, dbsWithKeys, structErrs int
	for _, c := range checks {
		entries += c.Entries
		live += c.Live
		expires += c.Expires
		badHeaders += c.BadHeaders
		orderErrors += c.OrderErrors
		stale += c.StaleTTL
		future += c.FutureTTL
		if c.Live > 0 {
			dbsWithKeys++
		}
		if c.StructErr != nil {
			structErrs++
			res.err("db%d B-tree structure: %v", c.Index, c.StructErr)
		}
	}

	if structErrs == 0 {
		res.ok("B-tree structure (%d databases)", len(checks))
	}

	if err := ks.CheckPageAccounting(); err != nil {
		res.err("page accounting: %v", err)
	} else {
		res.ok("page accounting (no leaks, no double-free)")
	}

	if orderErrors > 0 {
		res.err("B-tree key ordering (%d out-of-order entries)", orderErrors)
	} else {
		res.ok("B-tree integrity (%d keys in %d databases)", live, dbsWithKeys)
	}

	if badHeaders > 0 {
		res.err("value headers (%d bad of %d)", badHeaders, entries)
	} else {
		res.ok("value headers (%d/%d)", entries, entries)
	}

	if stale > 0 {
		res.warn("%d keys have a TTL in the past (will expire on next access)", stale)
	}

	switch {
	case future > 0 && fix:
		if n, ferr := ks.FixFutureTTLs(); ferr != nil {
			res.err("fix future TTLs: %v", ferr)
		} else {
			res.ok("cleared %d impossibly-future TTLs", n)
		}
	case future > 0:
		res.warn("%d keys have an impossibly far-future TTL (run with --fix to clear)", future)
	}

	// The WAL sidecar is not wired into the pager yet, so there is nothing to
	// validate. Report it so the run is unambiguous rather than silently skipping.
	res.ok("WAL: no sidecar (main-file commits)")

	if verbose {
		for _, c := range checks {
			if c.Entries == 0 {
				continue
			}
			_, _ = fmt.Fprintf(w, "  db%d: entries=%d live=%d expires=%d stale=%d\n",
				c.Index, c.Entries, c.Live, c.Expires, c.StaleTTL)
		}
	}

	summarize(w, res.code)
	return res.code
}

// summarize prints the final status line for the worst severity seen.
func summarize(w io.Writer, code int) {
	switch code {
	case 0:
		_, _ = fmt.Fprintln(w, "aki check: PASSED")
	case 1:
		_, _ = fmt.Fprintln(w, "aki check: PASSED with warnings")
	case 2:
		_, _ = fmt.Fprintln(w, "aki check: FAILED (errors found)")
	default:
		_, _ = fmt.Fprintln(w, "aki check: FAILED (critical corruption)")
	}
}

// checkRDB parses an RDB file and reports how many keys it holds across how many
// databases. A bad magic, version, opcode, or CRC comes back as an error so the
// process exits non-zero.
func checkRDB(name string) error {
	blob, err := os.ReadFile(name)
	if err != nil {
		return fmt.Errorf("read %s: %w", name, err)
	}
	snap, err := rdb.UnmarshalFile(blob)
	if err != nil {
		return fmt.Errorf("invalid RDB %s: %w", name, err)
	}
	keys := 0
	for _, db := range snap.DBs {
		keys += len(db.Entries)
	}
	fmt.Printf("file:       %s\n", name)
	fmt.Printf("format:     RDB\n")
	fmt.Printf("databases:  %d\n", len(snap.DBs))
	fmt.Printf("keys:       %d\n", keys)
	fmt.Printf("status:     OK\n")
	return nil
}
