// Package derived holds the derived types (spec 2064/f3/15): bitmaps as bit
// pages over the chunked string, HLL bit-identical to the HYLL format, and geo
// as geohash scores on the zset tree.
//
// Ported by copy from engine/f3/derived per the 2064/obs1 doc 11 section 2
// inventory (the sqlo1 rule: obs1 imports obs1 and the standard library,
// nothing else); every file except this one is byte-identical to the f3
// original, so a future re-sync is a clean diff. The f3 design docs stay
// authoritative for everything the copy does not change.
package derived
