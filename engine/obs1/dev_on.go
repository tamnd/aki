//go:build obs1dev

package obs1

// devBuild turns invariant-violation rows of the doc 04 section 10
// taxonomy (frame encode failure, emission before a grant) into panics,
// so a dev run stops at the bug instead of failing one command. Release
// builds (no obs1dev tag) fail just the command; see dev_off.go.
const devBuild = true
