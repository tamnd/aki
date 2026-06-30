// Package lab is a library of small, self-contained microbenchmarks that settle one
// engine technique choice each. It is a record of experiments, not engine code: nothing
// here is imported by the store. Each file isolates a single decision (which hash, tag
// or no tag, which value-update primitive) with minimal stand-ins so the benchmark
// times the technique and nothing else, and each decision is written up in the
// implementation notes so the reasoning outlives the numbers.
//
// Run one family:
//
//	go test -bench Hash   -benchmem ./engine/lab/
//	go test -bench Probe  -benchmem ./engine/lab/
//	go test -bench Update -benchmem ./engine/lab/ -cpu 1,10
//
// When a future engine decision comes up (a new probe layout, a different latch, an
// allocator trick), add a file here and a note, so the choice is backed by a number on
// this hardware rather than a hunch. That is the point of the library: every technique
// the engine commits to should have a reproducible measurement behind it.
package lab
