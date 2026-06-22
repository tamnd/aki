//go:build !debug

package keyspace

// assertConsistent is a no-op in production builds. The debug build in
// assert_debug.go runs the real page-accounting and pin checks.
func (ks *Keyspace) assertConsistent() {}
