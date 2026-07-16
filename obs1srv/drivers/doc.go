// Package drivers holds the network front ends for f3srv. The
// goroutine-per-connection driver is the only driver through M9 (spec
// 2064/f3/08 F16); the P1 campaign re-decides the driver question at M10.
//
// Ported by copy from f3srv/drivers per the 2064/obs1 doc 11 section 2
// inventory (the sqlo1 rule); every file except this one is byte-identical
// to the f3 original, so a future re-sync is a clean diff. The f3 design
// docs stay authoritative for everything the copy does not change.
package drivers
