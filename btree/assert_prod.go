//go:build !debug

package btree

// assertInvariants is a no-op in production builds. The debug build replaces it
// with the real check in assert_debug.go. The parameter is unnamed so the empty
// body carries no unused-parameter cost.
func assertInvariants(*Tree) {}
