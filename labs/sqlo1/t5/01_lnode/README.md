# lnode: the list node size and element cap

Milestone T5 lab 01 (spec 2064/sqlo1 doc 07 sections 2, 3, and 7).

## Question

T5 slice 2 bakes two split thresholds that bind whichever comes first: node_max in encoded bytes and ecap in elements.
The trade is doc 06's W4 bandwidth knob on the queue type: every push or pop bills the amended head or tail node's full post-image in its frame group, so bigger nodes make steady queue traffic carry more WAL bytes per op, while smaller ones lengthen the fence, cut and drop nodes more often (each a fence-shape bill), page the fence earlier, and make an LRANGE window touch more nodes for the same output.
The element cap only binds when elements are small, which is why the sweep carries a 16-byte ID deque beside the payload-sized arms.
PRED-SQLO1-T5-QUEUE and PRED-SQLO1-T5-FEED take their inputs here: WAL frames and bytes per queue op at steady depth, and the amortized bill of the capped feed's LPUSH plus LTRIM pair.

## Method

The model is the doc 07 shape resident, no store underneath (the salgebra pattern; the drain-substrate half of the trade was priced by T2's hseg lab on the real backends, and what T5 adds is the queue-shaped bill, which is arithmetic).
Nodes hold contiguous element runs in list order behind an ordered fence of (segid, count) entries; positional math prefix-sums the fence, two-level once the fence pages past the inline root.
The WAL column bills every mutating command its full post-images plus an inline root or one fence page and the root page index when the fence changes shape; a dropped node bills a tombstone; drain traffic accumulates deduped dirty post-images against the 8 MiB threshold for the WA column, which is where the queue's hot-tier coalescing shows up.
Mixes: deque (50/50 LPUSH/RPOP, 200 B elements, steady depth 10^4), dequeid (the same at 16 B, where ecap binds), feed (LPUSH 600 B plus LTRIM to cap 1000, every op), page (static 10^5 x 100 B, 90 percent LRANGE-100 at random offsets with an LINDEX probe each, 10 percent RPUSH churn).
An oracle test pins the model against a reference slice through pushes, pops, trims, ranges, indexes, counts, encoded sizes, and the fence partition, at thresholds small enough to cross the cut, drop, trim, and both paging transitions thousands of times; a second test holds the two-level paged seek to the flat answer.

## Run

    ./run.sh            # 4 mixes x node_max {2016, 4032, 8064} x ecap {64, 128, 256}
    go run . -quick     # smoke
    go test ./...       # oracle, feed shape, paged seek agreement

## Results (local, 2026-07-17, macbook; the model is deterministic and the trade is arithmetic, so the shape is box-independent)

The queue bill scales with the node, not the op: 1394 / 2168 / 4096 WAL bytes per op at node_max 2016 / 4032 / 8064 for 200 B elements (the half-full edge node is the post-image), at 1.111 / 1.053 / 1.026 frames per op since only a cut or drop adds a second frame.
Structural bills stay rare at every size (26 per 1000 ops at 4032) because a steady queue only touches its edges.
The write-behind coalescing is the counterweight on record: WA is 0.1 at every size, one node image per full queue rotation against the ~19 WAL images the same node's elements billed, so the WAL is the whole queue bandwidth story and drains are noise.

The ID deque is where ecap earns its keep: at node_max 4032 the bill is 682 / 1300 / 2026 WAL bytes per op at ecap 64 / 128 / 256 against 8 logical bytes, since for 16 B elements the cap is what bounds the post-image.
The cost of the small cap is fence length on exactly those lists: 157 nodes per 10^4 elements at ecap 64 against 79 at 128, which halves the inline fence's reach (a 16 B deque pages at ~10^4 elements at ecap 64, ~2 x 10^4 at 128).

The feed has a real knee at 2016: the cap-1000 feed of 600 B entries sits at 334 nodes there, past the 168-entry inline fence, so every third op bills a 4 KiB fence page and the amortized bill lands at 4603 B per push-trim pair against 4328 at 4032, where the same feed is 167 nodes and stays inline.
At 8064 the pair costs 8024 B because the edge rewrite carries a twice-as-large node image; frames run 2.667 / 2.333 / 2.154.
Trim itself is O(edges) as doc 07 claims: one edge rewrite per trim worst case, whole-node drops amortized to one per node-fill (167 drops per 1000 pairs at 4032), and the oracle test pins that bound.

The page mix follows node count: a 100-element window touches 6.21 / 3.61 / 2.55 nodes and cold-reads 12.3 / 14.3 / 17.0 KB at 2016 / 4032 / 8064, with seek p99 at 8.3 / 4.7 / 3.2 us on the 5264 / 2632 / 1563-node paged fences.

## Verdict

node_max is 4032, the T2 family constant, and the feed knee makes it a measured choice rather than an inheritance: 2016 pays a 6 percent worse amortized feed bill despite half-size nodes because realistic capped feeds page its fence, plus 72 percent more nodes per range window; 8064 pays 89 percent more WAL per queue op and 85 percent more per feed pair to save half a node per window.
ecap is 128: it matches the inline-to-noded boundary (the first cut out of inline moves the whole 128-element inline payload into exactly one node), keeps the fence half the length of ecap 64 on small-element lists, and matches the packing Redis's own quicklist defaults to.
The ID-deque column shows what 128 leaves on the table (682 vs 1300 B per op at 16 B elements), and that delta is the smallest absolute bill in the sweep; a per-type ecap override stays a priced v2 candidate if ID queues ever dominate a gate profile.
Slice 2 bakes 4032/128.

For the prediction notes: a steady 200 B queue op bills 1.05 frames and ~2.2 KB WAL at the chosen sizes with WA 0.1, and a 600 B push-trim feed pair bills 2.33 frames and ~4.3 KB; those are the numbers PRED-SQLO1-T5-QUEUE and PRED-SQLO1-T5-FEED will put on record.

The sweep CSV (lnode.csv) stays untracked, like every lab CSV.
