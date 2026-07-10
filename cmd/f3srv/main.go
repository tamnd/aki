// Command f3srv is the f3 server binary (spec 2064/f3). The shard runtime and
// the network drivers land in later M0 slices; until the smoke server exists
// this binary only names itself, so the harness launch target and the release
// wiring have a stable path to point at.
package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Fprintln(os.Stderr, "f3srv: the shard runtime has not landed yet; this binary does not serve")
	os.Exit(1)
}
