# sqlocache: hot-tier-first vs page-cache-only RAM split

Milestone A2 lab 04 (spec 2064/sqlo1 doc 02 section 6, doc 04 section 15).

## Question

At the same total cache budget, does a record-granular hot cache in front of the store beat handing SQLite the whole budget as cache_size?
Doc 02 section 6 claims it does, because a record costs its bytes while an 8 KiB page costs 8 KiB however little of it is hot, and the exit gate bakes this verdict into the budget split.

## Method

Two arms, same store file, same pragma posture (the apragma shape), same workload, same total budget.
The hot arm splits the budget by the doc 04 shares rescaled to the same total: 55/70 to the record cache, 15/70 to SQLite cache_size.
The page arm gives everything to cache_size.

The record cache is a lab-side model, not the engine's HotTable, whose evictor and drain machinery are package-internal and not what this lab prices.
The model charges the doc 04 per-entry overhead (71 B) plus key and value bytes, promotes read misses at p=0.5, is write-aware, pins dirty entries until their batch flushes, and evicts at random.
Random eviction only underestimates the hot arm against the engine's sampled scoring, so a hot-arm win is a safe verdict and a loss is not final; a pinned unit test keeps the model honest.

The dataset is 2M keys at 128 B (roughly 430 MB of file), far past both swept budgets (64 and 256 MiB).
The measured phase is a 90/10 mixed loop at the swept distribution (zipf 1.1 where a hot set exists; uniform as the control where record caching should not help), with writes batched into identical 1024-row drain-shaped transactions in both arms.

Read the mixed-read rows across arms at equal budget: ns/op and p99 for speed, hit_pct for the mechanism, and VmHWM for whether the split actually spent equal RAM (the hot arm's cache lives on the Go heap, the page arm's inside the wasm allocation; the nominal budgets match, the process peak decides).
The preload is reused across runs in one work dir, so a sweep pays the 2M-row build once.

## Run

    ./run.sh            # both arms x {64, 256 MiB} x {zipf, uniform}, gate box
    go run . -quick     # smoke
    go test ./...       # tiny-count both-arm test plus the model pin

## Results

Pending: runs on the gate box after the S0 self-proof frees it.

## Verdict

Pending.
