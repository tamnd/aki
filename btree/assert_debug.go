//go:build debug

package btree

// assertInvariants runs the full invariant check and panics on the first
// violation. It is compiled in only under the debug build tag, so debug and test
// builds catch a structural bug the moment an operation creates it. Production
// builds get the no-op in assert_prod.go and pay nothing.
func assertInvariants(t *Tree) {
	if err := CheckInvariants(t); err != nil {
		panic(err)
	}
}
