// Package sqlo1 is the shared runtime of the sqlo1 driver: the hot tier,
// the write-ahead log, the timer wheel, and the per-type logic, all built
// against a pluggable Store backend (spec 2064/sqlo1 doc 02 section 1).
//
// The package starts empty on purpose. The S0 skeleton pins the layout and
// the import boundary before any code lands: an sqlo1 package may depend on
// the other sqlo1 packages and the standard library, nothing else in this
// module. In particular nothing here imports engine/f3 or any other f-series
// package; sqlo1 is a fresh driver, not a fork.
package sqlo1
