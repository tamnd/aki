// Package dispatch is the command table and per-command routing for f3srv:
// parse once, hash once, route to the owning shard, and reorder replies per
// connection (spec 2064/f3/03).
//
// Ported by copy from f3srv/dispatch per the 2064/obs1 doc 11 section 2
// inventory (the sqlo1 rule); every file except this one is byte-identical
// to the f3 original, so a future re-sync is a clean diff. The f3 design
// docs stay authoritative for everything the copy does not change.
package dispatch
