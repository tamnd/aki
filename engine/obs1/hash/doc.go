// Package hash is the hash type (spec 2064/f3/10): the open-addressed field
// table, the dense field vector for HRANDFIELD and the downward HSCAN cursor,
// and field TTL on the inline-TTL design.
//
// Ported by copy from engine/f3/hash per the 2064/obs1 doc 11 section 2
// inventory (the sqlo1 rule: obs1 imports obs1 and the standard library,
// nothing else); every file except this one is byte-identical to the f3
// original, so a future re-sync is a clean diff. The f3 design docs stay
// authoritative for everything the copy does not change.
package hash
