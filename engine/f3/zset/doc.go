// Package zset is the sorted-set type (spec 2064/f3/12). This slice builds the
// inline listpack-class band: one packed (member, score) blob per key kept in
// score-then-member order, the point and rank/range-by-index commands over it,
// and the one-way conversion to the native band at Redis's listpack thresholds.
//
// The native band here is a map-plus-ordered-slice placeholder behind the
// nativeStore seam (skiplist.go): it makes the OBJECT ENCODING transition to
// "skiplist" and every command's cross-band behavior testable now. The counted
// B+ tree of doc 12 section 2 replaces the placeholder in a later M2 slice by
// implementing the same seam; nothing else in the package changes.
package zset
