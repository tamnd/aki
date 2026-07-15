// Command sqlo1srv serves the Redis protocol over the sqlo1 engine.
//
// The S0 skeleton pins the binary name and the import boundary only; the
// listener and the first seven commands arrive with the server slice of S0.
package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Fprintln(os.Stderr, "sqlo1srv: scaffold only, the S0 server slice adds the listener")
	os.Exit(2)
}
