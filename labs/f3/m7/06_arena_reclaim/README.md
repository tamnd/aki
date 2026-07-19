# m7/06 arena reclaim: peak-fill overshoot over the resident cap

The readback gate row M7-G4 owes: how far the arena fill peaks over the nominal cap under churn, and what the boundary reclaim brings back.

## The question

G1 pins aki's peak resident at 0.25-0.27x an evicting rival's for the same data, the product pitch.
But that peak sits about 1.5x over aki's own nominal resident cap, and G4 asks whether the overshoot is an unbuilt reclaim lever or the bounded churn-cycle headroom a compacting store carries between its boundaries.

The mechanism: the cap gates admission, so the live charge parks just under it.
An overwrite of a resident value leaves its old bytes dead in place, and those dead bytes are arena fill the cap does not count, so under churn the fill climbs above the cap until an idle-boundary compaction reclaims the dead segments and `MADV_DONTNEED` returns their pages.
The overshoot is therefore the dead bytes one boundary interval accumulates: bounded by how often the owner runs compaction, and by the pass budgets once a pass fires.

## Run

    go run ./labs/f3/m7/06_arena_reclaim          # full churn
    go run ./labs/f3/m7/06_arena_reclaim -quick   # smaller

## Numbers (2026-07-19, mac local disk, cap 16 MiB, resident 11k x ~1 KiB working set, 400k size-varying overwrites)

| boundary cadence | peak fill / cap | settled fill / cap | reclaimed at boundary |
|---|---|---|---|
| every 256 writes | 0.96x | 0.90x | 0.07x |
| every 1024 writes | 0.96x | 0.87x | 0.10x |
| every 4096 writes | 1.05x | 0.85x | 0.20x |
| every 16384 writes | 1.28x | 0.92x | 0.36x |

The overshoot tracks the boundary interval: a busy owner compacting every 256 writes holds the peak at 0.96x the cap, a slack owner running a long write burst before its next idle seam peaks at 1.28x.
Every cadence's boundary reclaims the fill back to at or under the cap (settled 0.85-0.92x), and the reclaimed column rises with the interval, exactly the churn-headroom signature: the dead bytes pile up between boundaries and the boundary returns them.

## Verdict (feeds M7-G4)

The overshoot is bounded churn headroom, not a missing reclaim lever.
The reclaim path is wired (`releasePages` / `MADV_DONTNEED`, arena.go; `CompactArena` / `MaybeDemote` at the owner boundary), the peak never runs away toward the arena size, and a boundary always returns the fill to at or under the cap.
The knob is the boundary cadence, an owner-scheduling choice, not an unbuilt feature.

And the peak clears the product bar with room to spare.
The worst-case peak here is 1.28x aki's nominal cap; the box saw ~1.5x under a heavier churn.
Multiplied onto the G1 ratio (aki 0.25-0.27x the rival's peak), even a 1.5x peak is 0.38-0.41x the rival, so the memory pitch, the whole point of the LTM regime, holds with about 2.5x of headroom.
G4 is structural: the overshoot is over aki's own cap, invisible at the product level where the peak is a fraction of the rival's.

The lab is real-store: it drives `engine/f3/store` Set under a live value log and samples `ArenaBytes` for the true fill, running the owner-boundary `MaybeDemote` / `CompactArena` on a swept cadence. `main_test.go` pins the bounded-and-cadence-driven shape for CI.
