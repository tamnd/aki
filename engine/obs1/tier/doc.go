// Package tier is the larger-than-memory engine (spec 2064/f3/06): the cold
// migrator on the owner's schedule, packed cold chunks with their resident
// directories, and block-not-drop backpressure. None of it runs below memory
// pressure; a fitting dataset stays fully resident.
//
// Ported by copy from engine/f3/tier per the 2064/obs1 doc 11 section 2
// inventory (the sqlo1 rule: obs1 imports obs1 and the standard library,
// nothing else); every file except this one is byte-identical to the f3
// original, so a future re-sync is a clean diff. The f3 design docs stay
// authoritative for everything the copy does not change.
package tier
