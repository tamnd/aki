//go:build !obs1dev

package obs1

// devBuild is off in release builds: an encode failure fails its one
// command with the doc 04 section 10 reply and never acks it. Build
// with -tags obs1dev to panic at the bug instead.
const devBuild = false
