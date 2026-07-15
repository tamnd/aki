// Package sqlo1b is the Track B store backend of the sqlo1 driver: a
// single-file format designed end to end for the aki bars, extent-based
// with a sub-byte-per-key cold index (spec 2064/sqlo1 docs 03 and 04).
//
// Empty at S0; the first code lands with the B1 format-core slices.
package sqlo1b
