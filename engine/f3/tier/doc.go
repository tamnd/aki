// Package tier is the larger-than-memory engine (spec 2064/f3/06): the cold
// migrator on the owner's schedule, packed cold chunks with their resident
// directories, and block-not-drop backpressure. None of it runs below memory
// pressure; a fitting dataset stays fully resident.
package tier
