// Package str is the string type (spec 2064/f3/09): the point surface, the
// INCR int cell, APPEND growth, and the value placement bands from embedded
// through chunked giant values.
//
// Ported by copy from engine/f3/str per the 2064/obs1 doc 11 section 2
// inventory (the sqlo1 rule: obs1 imports obs1 and the standard library,
// nothing else); every file except this one is byte-identical to the f3
// original, so a future re-sync is a clean diff. The f3 design docs stay
// authoritative for everything the copy does not change.
package str
