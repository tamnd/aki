// Package list is the list type (spec 2064/f3/13): the inline listpack-parity
// band, its one-way conversion into the native form, and the native form itself,
// an owner-local ring-backed chunked byte deque (native.go, section 2). The
// inline band is a single packed element blob with the point ops over it; a
// write crossing Redis's byte boundary promotes it to the deque, which carries
// position by chunk layout so the ends stay O(1) and positional access rides a
// per-chunk count directory. The Fenwick directory above flatMax chunks is the
// next slice; this package runs the flat scan at every ring size.
//
// Ported by copy from engine/f3/list per the 2064/obs1 doc 11 section 2
// inventory (the sqlo1 rule: obs1 imports obs1 and the standard library,
// nothing else); every file except this one is byte-identical to the f3
// original, so a future re-sync is a clean diff. The f3 design docs stay
// authoritative for everything the copy does not change.
package list
