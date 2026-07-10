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
