// Command obs1srv runs an obs1 node.
//
// One command, no config file, same startup story as the other drivers.
// Nothing serves yet: the binary exists so the CI lane, the boundary
// check, and the release plumbing have their target from day one.
package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Fprintln(os.Stderr, "obs1srv: scaffold only, no serving surface yet (spec 2064/obs1, milestone O0a)")
	os.Exit(2)
}
