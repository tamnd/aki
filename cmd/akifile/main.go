// Command akifile inspects an .aki store file (spec 2064/f3/07 section 6). It never
// writes: the file is opened read-only, so it is safe to run against a live or a
// damaged file.
//
//	akifile file-info <path>   prints the report and exits 0
//	akifile verify <path>      prints the report and exits nonzero on any finding
//	akifile dump <path>        prints the file's live records as JSONL and exits 0
//
// file-info always exits 0 (it reports whatever it finds); verify turns the same
// report into an exit code so a script can gate on a clean file; dump streams the
// logical contents so a crash-matrix cell can diff a before and after image.
package main

import (
	"fmt"
	"os"

	"github.com/tamnd/aki/engine/f3/akifile"
)

func main() {
	if len(os.Args) != 3 || !known(os.Args[1]) {
		fmt.Fprintln(os.Stderr, "usage: akifile <file-info|verify|dump> <path>")
		os.Exit(2)
	}
	cmd, path := os.Args[1], os.Args[2]

	if cmd == "dump" {
		if err := dump(path); err != nil {
			fmt.Fprintln(os.Stderr, "akifile:", err)
			os.Exit(1)
		}
		return
	}

	rep, err := akifile.InspectPath(path)
	if err != nil {
		fmt.Fprintln(os.Stderr, "akifile:", err)
		os.Exit(1)
	}
	if err := akifile.WriteReport(os.Stdout, rep); err != nil {
		fmt.Fprintln(os.Stderr, "akifile:", err)
		os.Exit(1)
	}
	if cmd == "verify" && len(rep.Findings()) > 0 {
		os.Exit(1)
	}
}

func known(cmd string) bool {
	return cmd == "file-info" || cmd == "verify" || cmd == "dump"
}

// dump opens the file, streams its live records to stdout as JSONL, and closes it. The
// open only reads, and WriteDump walks the append space without writing, so a dump is
// safe against a live file.
func dump(path string) error {
	f, err := akifile.Open(path, akifile.OpenOptions{Sync: akifile.SyncNo})
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	return akifile.WriteDump(os.Stdout, f)
}
