package main

import "testing"

// The harness itself at tiny counts: each in-process arm produces a row
// without erroring. Numbers are not asserted; this is a does-it-run test.
func TestHarness(t *testing.T) {
	for _, arm := range []string{"inproc-http", "inproc-tls", "fresh"} {
		run(arm, "", "", "", 2, 4, 40, 512)
	}
}
