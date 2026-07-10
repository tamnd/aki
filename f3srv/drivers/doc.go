// Package drivers holds the network front ends for f3srv. The
// goroutine-per-connection driver is the only driver through M9 (spec
// 2064/f3/08 F16); the P1 campaign re-decides the driver question at M10.
package drivers
