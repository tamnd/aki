// Command akifile inspects an .aki store file (spec 2064/f3/07 section 6). It
// never writes: the file is opened read-only, so it is safe to run against a live
// or a damaged file.
//
//	akifile file-info <path>   prints the report and exits 0
//	akifile verify <path>      prints the report and exits nonzero on any finding
//
// file-info always exits 0 (it reports whatever it finds); verify turns the same
// report into an exit code so a script can gate on a clean file.
package main

import (
	"fmt"
	"os"

	"github.com/tamnd/aki/engine/f3/akifile"
)

func main() {
	if len(os.Args) != 3 || (os.Args[1] != "file-info" && os.Args[1] != "verify") {
		fmt.Fprintln(os.Stderr, "usage: akifile <file-info|verify> <path>")
		os.Exit(2)
	}
	cmd, path := os.Args[1], os.Args[2]

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
