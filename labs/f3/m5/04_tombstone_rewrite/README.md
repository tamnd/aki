# Lab 04: the gc rewrite of a partially-tombstoned block, and its gc-ratio

Part of issue #547, the M5 stream milestone, lab 04, the tombstone-rewrite decision (doc 14 section 6.5). This is the lab the section 6.5 gc rewrite depends on: it settles the `stream-block-gc-ratio` (default 0.5) before the slice bakes the rewrite trigger into the owner's between-batches step, per the labs-per-perf-change rule. It is the companion to lab 02: lab 02 priced the front whole-block drop that reclaims trimmed entries, this one prices the rewrite that reclaims interior tombstones the front drop can never reach.

## Question

XDEL tombstones an entry in a sealed block, it does not rewrite the block (section 6.5, the tombstone side of the tombstone-vs-rewrite choice), because a sealed block is append-frozen and a mid-block splice would force a full re-encode on the reply path of a point command. So an interior block, neither at the front nor empty, accumulates dead bytes that lab 02's whole-block front drop can never reclaim. Section 6.5 reclaims them with a deferred, threshold-gated rewrite: a block whose `deleted/count` crosses `stream-block-gc-ratio` is rewritten by the owner's between-batches step, its live entries re-encoded into a fresh block with a new master, the directory repointed, the old block freed; a block whose live count hits zero is dropped whole with no rewrite.

So the questions: does a rewrite actually reclaim the dead fraction's bytes, what does it cost to re-encode the live entries it keeps, and at what dead fraction does the reclaim pay for the copy. And downstream: what does the ratio buy under sustained interior churn, how much dead memory does each setting retain and how much copy work does it charge.

The break-even is structural. Rewriting a block whose dead fraction is `f` reclaims `f` of its bytes and copies the other `1-f`, so reclaimed-over-copied is `f/(1-f)`, which crosses 1.0 at `f=0.5`: below half-dead a rewrite copies more than it frees, above it frees more than it copies. That is the first-principles case for a 0.5 default, and the lab checks the real encoded bytes bend that knee only slightly (a rewrite re-masters, so the accounting is not exactly `f/(1-f)`).

## Method

In-process, no server, no wire, no engine import, the same lab-local model labs 01 and 02 use. Blocks carry real byte blobs encoded as section 3.3 lays out (master whole, same-schema entries as a flags byte plus the two ID deltas against the block firstID plus value frames), and each block also keeps its per-entry IDs and a live/dead bit so a rewrite reconstructs the survivors exactly. A rewrite builds a real fresh blob by re-encoding the surviving entries against a new base, so the reclaim figure is the true byte difference, re-mastering included. IDs are dense auto-IDs at 1000 entries per millisecond, the benchmark-shaped case. Resident cost counts the 48-byte block header and the 32-byte directory leaf per block plus the entry bytes, matching labs 01 and 02.

The gc pass walks the sealed blocks (the open tail is left alone, it is still filling): an empty sealed block is dropped whole, a sealed block at or past the ratio is rewritten to its live entries. Sweep B drives sustained interior churn: it deletes random live entries in the sealed blocks in batches, runs a gc pass at the swept ratio after each batch, and reports the end-state memory and the cumulative copy work.

`go run .` runs the whole sweep. `-quick` shrinks the op counts for the shared runner. `TestRewriteReclaimsAndKeepsLive`, `TestRewriteBreakEvenAtHalf`, `TestGcRatioBoundsRetainedDead`, `TestGcDropsEmptyBlock`, and `TestGcLeavesTailBlock` are what CI drives.

## Results

Apple M4 (darwin/arm64), go 1.26, 2026-07-13, one process. 4096/128 block geometry, 3x8B fixed-schema entries (24-byte payload), dense IDs 1000/ms. Byte columns are exact; the nanosecond columns are single-box and timing-noise sensitive, read them as the shape, not to three digits.

Sweep A, one full 128-entry block, rewrite reclaim vs copy by dead fraction:

| dead f | live | reclB | copiedB | recl/copy | rewrNs |
|---|---|---|---|---|---|
| 0.10 | 116 | 372 | 3608 | 0.103 | 5659 |
| 0.25 | 96 | 992 | 2988 | 0.332 | 3200 |
| 0.40 | 77 | 1581 | 2399 | 0.659 | 3357 |
| 0.50 | 64 | 1984 | 1996 | 0.994 | 3370 |
| 0.60 | 52 | 2344 | 1636 | 1.433 | 3578 |
| 0.75 | 32 | 2944 | 1036 | 2.842 | 2209 |
| 0.90 | 13 | 3514 | 466 | 7.541 | 1228 |

Reclaimed-over-copied crosses 1.0 right at `f=0.5` (0.994): re-mastering the fresh block costs a few bytes the analytic `f/(1-f)` ignores, so the true break-even sits a hair above 0.5, close enough that 0.5 is the honest default. Below 0.5 a rewrite copies more live bytes than it frees dead ones; above it, the reverse. Rewrite latency tracks the live count it re-encodes (fewer survivors at higher `f`), not the block's original fill.

Sweep B, sustained interior churn to 60 percent deleted, gc-ratio sweep (n=200000):

| ratio | copied | rewrites | drops | deadFrac | B/live |
|---|---|---|---|---|---|
| 0.25 | 303440 | 4063 | 0 | 0.130 | 37.17 |
| 0.50 | 89505 | 1424 | 0 | 0.258 | 43.20 |
| 0.75 | 0 | 0 | 0 | 0.600 | 78.39 |
| never | 0 | 0 | 0 | 0.600 | 78.39 |

Three findings. First, never-rewrite leaks: it retains the full 0.60 churn fraction as dead entries and inflates the footprint to 78 B/live against the ~31 B/entry floor (lab 01), so the gc rewrite is not optional, it is what keeps interior XDEL from leaking memory the front drop cannot touch. Second, running gc every batch holds the retained dead at roughly half the ratio (0.258 at r=0.5, 0.130 at r=0.25): a block sawtooths between 0 dead just after a rewrite and the ratio just before the next, so the steady-state average across blocks is about ratio/2. Third, a ratio above the churn fraction never fires: at r=0.75, uniform 60 percent churn never pushes a block past 0.75, so it behaves exactly like never-rewrite. Tightening from 0.5 to 0.25 buys about 6 B/live (43 to 37) for 3.4x the copy work (89k to 303k entries re-encoded), a poor trade.

Sweep C, rewrite latency by surviving live entries (dead f=0.5):

| liveKept | rewrNs |
|---|---|
| 8 | 805 |
| 16 | 1451 |
| 32 | 1678 |
| 64 | 3225 |

A rewrite re-encodes only the survivors, so its cost scales with the live count, not the block's original fill: half a full block is a low-single-digit-microsecond re-encode, and it runs on the owner's between-batches step, off the command path, one block at a time.

## Verdict

The gc-ratio default is 0.5, the reclaim-equals-copy break-even. A rewrite of a block that is exactly half-dead reclaims as many bytes as it copies (recl/copy 0.994 with real re-mastering); below half-dead a rewrite is a net copy, above it a net reclaim, so 0.5 is the point past which the owner's work is paid for by the memory it frees. So:

- The section 6.5 rewrite fires when a sealed block's `deleted/count` reaches `stream-block-gc-ratio` (default 0.5), re-encoding the live entries into a fresh block and freeing the old one on the owner's between-batches step; a sealed block that reaches zero live is dropped whole with no rewrite, and the open tail is never touched.
- 0.5 holds the retained dead at about ratio/2 (~0.26 of encoded entries under steady churn) and the footprint near 43 B/live, against 78 B/live if interior tombstones are never collected. Tightening to 0.25 recovers only ~6 more B/live for 3.4x the copy work, so 0.5 is the knee, not a floor to chase.
- A ratio at or above the sustained churn fraction never fires, which is the correct behavior: light uniform churn leaves a small bounded waste no rewrite could pay for, and the rewrite spends the owner's cycles only where a block has actually gone half-dead.

This closes the section 6.5 lab. The rewrite trigger lands in the stream slice on the owner's between-batches maintenance seam (worker.go's idle and between-drain boundary, where no ChunkStream snapshot can name the bytes a rewrite moves), gated on this 0.5 ratio.
