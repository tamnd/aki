# Lab: prefetch depth

Spec 2064/f3/03 section 3.4 and 04 section 2, M0 lab 4.

## The question

The batch drain's whole bet is that issuing index-bucket prefetches ahead of the probes converts two dependent cache misses per point op into overlapped misses across the batch (F6). How far ahead does the prefetch have to run before the stalls are actually hidden, and where does more depth stop paying? Doc 04 pre-registers <= 8ns amortized tag-path probes at depth 16; the survey's convergent industry answer is depth 16.

## Method

`go run .` probes a lab-local clone of the ported index bucket: the exact 64-byte bucket of `engine/f3/store/index.go` (7 tag+address entry words, one link word, 12-bit tags from the same bit range) as one flat power-of-two array over records in a flat arena (16B header, 16B key, 64B value), keyed by the ported `store.Hash`. The clone exists because the sweep needs raw bucket and record addresses for the prefetch instruction, which the store rightly does not export. The probe loop is a software pipeline at distance d: hash key i and prefetch its bucket line, complete the bucket probe for key i-d and prefetch its two record lines, finish the record read (key verify plus value touch) for key i-2d. Tag collisions re-resolve through a full probe, mirroring the drain's recheck. d=0 is the fully serial baseline; prefetch is a real PRFM (PLDL1KEEP) via a one-instruction asm shim, with an amd64 PREFETCHT0 twin. 8M uniform random probes per row, keyspaces 1M and 10M (the 10M arena is ~1GB, far past LLC). A serial `store.Get` row on the same keys anchors the clone against the real ported engine.

## Results

Apple M4 (4P + 6E), macOS, Go 1.26, single thread.

keys = 1048576 (load 0.57, 13k overflow buckets):

| distance | ns/probe | vs serial |
|---|---|---|
| 0 | 114.2 | 1.00x |
| 2 | 46.2 | 2.48x |
| 4 | 35.5 | 3.22x |
| 8 | 31.5 | 3.62x |
| 16 | 30.7 | 3.72x |
| store.Get serial | 228.0 | anchor |

keys = 10485760 (load 0.71, 280k overflow buckets):

| distance | ns/probe | vs serial |
|---|---|---|
| 0 | 143.3 | 1.00x |
| 2 | 55.8 | 2.57x |
| 4 | 38.9 | 3.68x |
| 8 | 35.3 | 4.06x |
| 16 | 35.3 | 4.06x |
| store.Get serial | 298.3 | anchor |

The serial baseline lands where doc 04 predicted from note 355 (121.8ns random-key Get, two dependent misses). Distance 2 already buys the biggest single step (2.5x), the curve knees at 8, and 16 adds 2 percent at 1M keys and nothing at 10M. No pollution penalty appears at 16 either, so the depth is forgiving upward. The `store.Get` anchor runs about 2x the clone's serial row because the real store adds the directory indirection (dir word, segment pointer, then bucket), the full value copy into dst, and its own key build per call; that gap is the drain's headroom on the real index, not a defect of the clone.

The doc 04 target of <= 8ns amortized is for the tag-path probe alone; these rows carry hashing, key build, key verify, and a value touch, so 30-35ns amortized for the whole point read is consistent with it rather than a miss.

## Provisional verdict

Laptop numbers, hypothesis until the GamingPC rerun. Prefetch depth 8 is the knee on this box: it captures 97 percent of the win at 1M keys and 100 percent at 10M. Depth 16, the doc 03 default and the batch cap's natural partner, costs nothing measurable over 8, so the default stands and there is no case for going past it. The two-stage shape earned its keep: bucket prefetch alone cannot hide the record miss, and pipelining both stages is what produced the 3.7-4x over serial. The gate box (Zen 4, different LLC and memory latency) reruns this before the drain slice bakes the constant, and should also run the depth-16 row against doc 04's 8ns tag-path prediction with the value touch stripped.

## Gate box results

The spec guessed the gate box was Zen 4; it is not. The box is an i9-13900K (Raptor Lake, 8P + 16E, 36MB LLC), WSL2 Debian on Windows 11 (kernel 6.18.33.2-microsoft-standard-WSL2), Go 1.26.0, aki fc4a79f, single thread pinned with `taskset -c 2`.

keys = 1048576 (load 0.57, 13k overflow buckets):

| distance | ns/probe | vs serial |
|---|---|---|
| 0 | 150.7 | 1.00x |
| 2 | 51.2 | 2.94x |
| 4 | 49.7 | 3.03x |
| 8 | 47.8 | 3.15x |
| 16 | 45.0 | 3.35x |
| store.Get serial | 209.3 | anchor |

keys = 10485760 (load 0.71, 280k overflow buckets):

| distance | ns/probe | vs serial |
|---|---|---|
| 0 | 249.3 | 1.00x |
| 2 | 77.7 | 3.21x |
| 4 | 71.9 | 3.47x |
| 8 | 71.2 | 3.50x |
| 16 | 72.9 | 3.42x |
| store.Get serial | 324.1 | anchor |

Same curve as the M4, shifted by this box's memory latency: the serial chain costs more here (150.7ns vs 114.2ns at 1M, 249.3 vs 143.3 at 10M, DRAM under WSL2 rather than the M4's package memory), distance 2 buys the big step, and the knee sits at 2-4 at 1M and 4-8 at 10M. Depth 16 is the best 1M row (45.0ns, 3.35x) and costs nothing at 10M (72.9 vs 71.2 at depth 8, inside run noise), so it stays forgiving upward on the second microarchitecture. The laptop verdict asked this rerun to also time the depth-16 tag path with the value touch stripped; the harness has no such mode, and the closest bound is the port-lab microbench row (results/f3/m0-portlab.md): a cache-resident serial findEntry walk nets out to ~15ns on this box, hash included.

## Gate box verdict

Depth 16 stands on Raptor Lake: 3.35x over serial at 1M keys and 3.4-3.5x at 10M, knee at 2-8 with no pollution penalty at 16. The constant the drain slice baked in is confirmed on the second box, and the Zen 4 line in doc 04 should be read as this box, an i9-13900K.
