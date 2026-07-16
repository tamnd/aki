// Package structs holds the shared structures more than one type imports
// (spec 2064/f3/19 section 4.1): the open-addressed table, the counted tree,
// and the chunk directory. The types import these, never duplicate them. The
// directory is engine/f3/struct per the doc map; the package is named structs
// because struct is a Go keyword.
//
// Ported by copy from engine/f3/struct per the 2064/obs1 doc 11 section 2
// inventory (the sqlo1 rule: obs1 imports obs1 and the standard library,
// nothing else); every file except this one is byte-identical to the f3
// original, so a future re-sync is a clean diff. The f3 design docs stay
// authoritative for everything the copy does not change.
package structs
