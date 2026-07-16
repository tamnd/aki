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
//
// Ported by copy from engine/f3/zset per the 2064/obs1 doc 11 section 2
// inventory (the sqlo1 rule: obs1 imports obs1 and the standard library,
// nothing else); every file except this one is byte-identical to the f3
// original, so a future re-sync is a clean diff. The f3 design docs stay
// authoritative for everything the copy does not change.
package zset
