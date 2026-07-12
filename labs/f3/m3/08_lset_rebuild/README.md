# M3 lab 08: LSET spurious directory rebuild versus a guarded stale flag

## Question

The M2/M3 exit gate measured deep LSET on a one-million-element list at **0.028x** of the rival (`lset_c1m`), 35x slower, while LINDEX on the identical list ran **4.06x**, a fast indexed read.
Same list, same locate, opposite outcomes: the seek is not the problem, the write path is.

LSET resolves the index and, when the new value differs in length from the old, repacks the one chunk that holds it (the bounded in-chunk surgery, doc 13 section 5.6).
That repack replaces one element with another, so the chunk's element count is unchanged and every other chunk is untouched: the Fenwick chunk directory, which maps a dense index to a `(chunk, ordinal)` pair off the per-chunk counts, is still exactly correct.
But the shipped `setAt` marked the directory stale after every length-changing repack, so the **next** `locate` above the flat crossover ran `chunkDir.sync`: zero the tree, refill it from every chunk's count, re-link the Fenwick blocks, an O(chunks) rebuild.
On a 1M list that is roughly seventeen thousand chunks rebuilt on every single LSET, an O(n/CAP) tax bolted onto an O(CAP) surgery.

The fix marks the directory stale only when the repack actually splits the chunk (an overflow that inserts a chunk and so renumbers the ring); a no-split length change leaves the flag clear and the next `locate` is a plain O(log chunks) descent over the still-valid tree.
This lab prices that tax.

## Method

In-process, no server, no wire, no engine import.
A `chunkDir` byte-identical in shape to `engine/f3/list/native.go`'s directory (1-indexed Fenwick BIT over per-chunk counts, a `stale` flag, the same `sync` rebuild and the same power-of-two `rank` descent) over a ring of `chunks` chunks.
The directory is built once up front and then a stream of simulated LSETs runs against it, so the persistence matters:

- `rebuild` is the shipped path. Each LSET does the descent, a fixed O(CAP) repack (a widening sum over one chunk's 4096 blob bytes, standing in for the surgery both variants share), and then marks the tree stale, so the **next** LSET's descent pays a full `sync`.
- `guarded` is the fix. Each LSET does the same descent and the same repack but never marks the tree stale, so its per-op `sync` is a no-op and the directory is built exactly once for the whole stream.

The fixed 4096-byte repack is deliberately larger than a real small-element chunk's live bytes, so it overstates the shared floor and makes the reported speedup conservative.
The deep index is strided across the whole list, not a single hot slot, matching a keyspace-spread LSET workload.
`main_test.go` proves the guarded descent resolves every index against the flat scan after a no-split LSET without a rebuild, so skipping the rebuild is correct, not just fast.

## Result (Apple M4, go1.26)

```
  chunks     elems     rebuild_ns   guarded_ns     speedup
     128     16384       1317.3        984.0       1.34x
    1024    131072       2886.5        987.9       2.92x
    4096    524288       8614.2       1006.1       8.56x
   17408   2228224      34882.6       1023.3      34.09x
```

Two readings.

**guarded is flat in the chunk count.** It holds at ~1000 ns per LSET from 128 chunks to 17408, because the only per-op cost is the descent (O(log chunks), a handful of steps) and the fixed repack. That is the O(CAP) shape a bounded surgery should have.

**rebuild grows linearly with the chunk count.** It climbs from 1.3 us at 128 chunks to 34.9 us at 17408, the O(chunks) `sync` paid once per LSET. At the gate cell (17408 chunks, the 1M-at-64B case) the guarded path is **34.09x** faster, and that 34x matches the gate's measured `lset_c1m` 0.028x (1/35.7) almost exactly: the spurious rebuild is the deep-LSET regression, nothing else.

## Verdict (frozen)

A length-changing LSET must invalidate the chunk directory only when it changes the ring's chunk structure (a split), never on a plain in-chunk repack.
The repack changes no chunk's element count, so the Fenwick tree stays valid and the next locate is an O(log chunks) descent; marking it stale forced an O(chunks) rebuild on every deep LSET and is the whole of the 0.028x regression.
`setAt` now guards the stale flag on an actual `ring.n` change (an overflow split), which the `sync` length check would catch on its own regardless, so the guard is safe under both branches.

This lab bakes no constant; it justifies deleting one unconditional line and guarding it.
The same "mark stale only on a real structural change" discipline does not extend to LINSERT or LREM, whose repacks do change a chunk's count and so genuinely need the directory updated (and whose dominant cost is the inherent O(n) value scan, a separate matter); it is specific to LSET, the one interior write that leaves the counts alone.
The end-to-end deep-LSET gain is smaller than this walk-kernel figure because the op also pays the reactor dispatch and the reply this kernel does not, and it is measured by a box A/B of the `lset_c1m` aki-bench cell on the old versus new f3srv binary.
