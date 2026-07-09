// Package mergeresolve prices one specific cost in the f1raw SINTER merge: how many times the
// two-pointer intersection resolves a matched member's offset back to its bytes in the arena.
//
// The merge keeps each set as a sorted array of (member-hash, arena-offset). On a hash match it must
// (1) byte-confirm the pair really names the same member, to reject the astronomically rare 64-bit
// hash collision, and (2) emit the member's bytes into the reply. The confirm needs both operands'
// bytes; the emit needs A's bytes. The original code ran these as two separate steps: the confirm
// resolved offA and offB, and the emit then resolved offA a SECOND time to materialize it, so every
// matched member cost three arena resolutions. Folding confirm and emit into one callback resolves
// offA once and reuses those bytes for both the compare and the emit, so a matched member costs two.
//
// Each resolution is a random read into a multi-member arena (the member row can sit anywhere), so it
// is a DRAM round trip, not a rounding error. On a high-overlap SINTER almost every driver member
// matches, so the third resolution runs ~|A| times per command. This lab models the arena as a byte
// slab of scattered length-prefixed member rows and runs the merge both ways, so the gap is exactly
// the one saved resolution per matched member with nothing else changing.
package mergeresolve
