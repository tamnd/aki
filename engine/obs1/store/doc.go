// Package store is the per-shard memory engine, ported by copy from
// engine/f3/store per the 2064/obs1 doc 11 section 2 inventory (the sqlo1
// rule: obs1 imports obs1 and the standard library, nothing else). The
// design docs it cites are f3's (spec 2064/f3/04 for the index and arena,
// 09 for the value bands); those stay authoritative for everything the
// copy does not change.
//
// The shape is f3's: the open-addressed bucket index with dashtable
// segment-split growth, the segmented bump arena, and the 16-byte record
// frame. Everything here is single-owner by contract: exactly one
// goroutine reads and writes a shard's store, so every load is a plain
// load and every store is a plain store, and the package holds no mutex,
// no CAS, and no atomic on any path.
//
// One reinterpretation (2064/obs1 doc 05 section 2): the index entry's
// tier bit still means resident-or-not, but not-resident no longer means
// on-disk forever. In obs1 a non-resident record is cooled (in this
// package's file-backed cold tier, awaiting fold into a bucket segment)
// or cold (only in a bucket segment, named by the keymap); the cold file
// this package carries is the cooled staging area, and the planned-GET
// read path that replaces pread for truly cold records arrives with the
// fold milestone. The MaybeDemote, ResidentOver, and spillCold seams in
// resid.go are the hooks that milestone wires; they arrive intact and
// unwired here.
//
// The demotion and promotion policy rides the same copy: the SIEVE hand
// (resid.go, migrate.go, and the second-chance pass in coldstage.go)
// picks what leaves the resident tier, and the two-generation Bloom
// doorkeeper (colddoor.go) decides what a cold read brings back on its
// second sighting. obs1 re-aims both rather than replacing them: the
// hand's selection order becomes the fold pass's selection order, so
// what folds first is what the hot tier wants to evict first (2064/obs1
// doc 06 section 1.4), and the doorkeeper's two-touch discipline guards
// GET dollars instead of pread counts once cold reads go to the bucket
// (doc 05 section 4). In the diskless serving shape both are inert by
// construction: no cold path is configured, ltmOn stays false,
// MaybeDemote declines at its first check, and the doorkeeper is never
// consulted, which is the O1a persistence-off contract the shard tests
// pin (engine/obs1/shard/foldpolicy_test.go).
package store
