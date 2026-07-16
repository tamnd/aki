# ropechunk local provisional verdict: log2chunk 13 (8 KiB), not the doc 05 default 15

Lab: labs/sqlo1/t1/01_ropechunk (#807), provisional run 2026-07-16 on the dev Mac, raw rows in t1-ropechunk-local.csv next to this note.
This is the local read that T1 slice 1 bakes its default from; the gate-box run confirms or moves it before the T1 exit gate, and it owns the two axes this machine cannot measure (VmHWM and box IO).
The doc 05 prior was 32 KiB; the sweep rejects it, and the margin is large enough that machine noise cannot be the story.

## Headline

8 KiB wins every mix, on both arms, on every metric this machine can read: per-op latency, p99, write amplification, worst flush stall, and final file size.
There is no knee inside the swept range; cost is monotonic in chunk size at roughly 1.6 to 1.9x per doubling on the b arm, and the a arm agrees on the shape at higher absolute cost.
The verdict arm (b, the backend that carries the constant) reads, for the SETRANGE uniform mix: write 4.7/7.0/12.7/22.2 us per op across 8/16/32/64 KiB, with p99 following the same curve.
GETRANGE reads rise too (5.7/6.9/10.3/15.4 us), which kills the one argument for bigger chunks before it starts.

## Why the locality story never shows up

The case for a big chunk is fewer row probes per long read span, but the doc's GETRANGE span is 4 KiB, so even an 8 KiB chunk resolves most spans in one probe and two at worst.
Meanwhile every fresh chunk touch pays a read-modify base of a full chunk and every dirty chunk drains as a full image, both linear in chunk size, so the RMW and drain bills scale while the probe count barely moves.
A span distribution heavy in hundreds-of-KiB reads would tilt back toward bigger chunks, but that is not the operator mix Redis strings and bitmaps see.

## Write amplification is the chunk-size multiple, as predicted

WA is workload arithmetic and came out identical on both arms, doubling per chunk doubling: SETRANGE uniform 127/253/505/1009, SETBIT uniform 8.1k/16.1k/32.2k/64.5k.
The zipf SETBIT arm was the one place a big chunk could earn its image back through hot-chunk coalescing, and the discount runs the wrong way: 35% off uniform WA at 8 KiB but only 12% at 64 KiB.
Concentration helps small chunks more because the hot set in chunk units stays small enough to coalesce inside one flush window; at 64 KiB the same zipf mass dirties images too large for the window to absorb.

## Stalls and disk follow

Worst flush stall on the b arm grows from 77 ms at 8 KiB to 3.5 s at 64 KiB on SETRANGE uniform, and the zipf SETBIT arm hits 11.9 s at 64 KiB.
Final file size for the same logical work: 2.5 GiB at 8 KiB against 12.1 GiB at 64 KiB on the setrange mix, which is the WA showing up as vlog growth before compaction earns any of it back.

## What slice 1 bakes

- log2chunk default 13 (8 KiB), as a constant a lab can move, not a format invariant: the superblock field stays, so a box verdict that disagrees changes a default, not a file format.
- Chunk addressing, absent-chunks-read-zero, last-chunk trim, and the popcount cache are unaffected; the pc cache overhead at 8 KiB is 4 B per 8 KiB, 0.05%, still noise.

## What the box run must settle before the T1 exit gate

- VmHWM: 8 KiB means 4x the segment records of 32 KiB, so index chunks and directory RAM grow; macOS cannot read peak RSS the way the harness wants, so the memory bar (the product pitch) is unpriced here. If the box shows the index tax eating the latency win, the default moves back up one notch, and the note next to the constant says so.
- A 4 KiB arm: the curve is monotonic down to the sweep floor, and 4 KiB is one group, the natural floor of the blob-run layout. The box sweep should extend down one step to see whether 8 KiB is the minimum or merely the smallest value swept.
- Single rep per cell here; the box runs the usual rep discipline. The per-step margins are 1.6x and up, far past local noise, but the stall tails deserve real reps.

## Caveats on this machine

Shared dev Mac with other agent workloads running; absolute numbers are not comparable to box numbers and are not meant to be.
The load and flush rows include APFS behavior a Linux box will not show.
Both arms ran the full grid (40 cells); arm agreement on shape is the reason to trust the direction locally.
