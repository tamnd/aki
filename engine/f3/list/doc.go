// Package list is the list type (spec 2064/f3/13): the inline listpack-parity
// band, its one-way conversion into the native form, and the native form itself,
// an owner-local ring-backed chunked byte deque (native.go, section 2). The
// inline band is a single packed element blob with the point ops over it; a
// write crossing Redis's byte boundary promotes it to the deque, which carries
// position by chunk layout so the ends stay O(1) and positional access rides a
// per-chunk count directory. The Fenwick directory above flatMax chunks is the
// next slice; this package runs the flat scan at every ring size.
package list
