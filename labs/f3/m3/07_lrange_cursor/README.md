# M3 lab 07: LRANGE per-element locate versus cursor walk

## Question

f3's native list is a chunked resident byte deque: a ring of fixed-capacity chunks, each holding a contiguous run of consecutive list positions (lab 02 froze the flat-versus-Fenwick directory that resolves a dense index to a (chunk, ordinal) pair).
LRANGE key start stop streams a window of the list.
The shipped M3 handler resolved every element in the window with its own index lookup, `get(i)` once per position, and `get` resolves `i` through the directory.
Above the flat crossover that directory read is an O(log chunks) Fenwick descent, so a window of `w` elements paid `w` descents: the range read was O(w log n) when it should be O(w).

The M2/M3 exit gate measured LRANGE at **0.51x** of Valkey on a 100-element window over a one-million-element list, aki slower than the rival, a clean regression against a range read that should walk the window at contiguous speed.
This lab prices the fix: seek to the first element once, then advance the (chunk, ordinal) cursor by layout across chunk boundaries, no directory read per element.

## Method

In-process, no server, no wire, no engine import.
The model is byte-identical in shape to `engine/f3/list`: `chunkDir` is the 1-indexed Fenwick BIT with the same power-of-two rank descent as `native.go`'s directory, and a per-chunk backing of frame values stands in for the packed frames so the emit reads a real value the way `frameAt` does.
Two range kernels:

- `perElem` is the old shape: `dir.rank(i)` for every `i` in the window, each a fresh descent from the root, then emit. This is `get(i)` in a loop, the LRANGE the gate measured.
- `cursor` is the new shape (`native.rangeInto`): one `dir.rank(lo)` to seek the window start, then walk (chunk, ordinal) forward, emitting the rest of a chunk and stepping to the next.

Both emit identical per-element work (a checksum fold over the modeled frame), so the measured delta is purely the index resolution the cursor deletes.
`main_test.go` proves the two kernels yield the identical (chunk, ordinal) sequence for every window across several chunk counts, so the walk is a pure performance change and LRANGE returns byte-for-byte what it did before.

The sweep runs chunk count in {16, 128, 256, 1024, 4096, 17408} (17408 is the spec's 17K-chunk, one-million-element-at-64B case) and window length in {10, 100, 1000}.
The window sits mid-list so the seek is a full descent and the walk crosses chunk boundaries, the gate shape.
The build is deterministic (no PRNG in the timed loop), so the table reproduces.

## Result (Apple M4, go1.26)

```
  chunks   elems   window   perElem_ns    cursor_ns  perE_ns/e  curs_ns/e     speedup
      16     940       10         79.5         15.1      7.950      1.508       5.27x
      16     940      100        386.1         62.1      3.861      0.621       6.21x
     128    7662      100        552.5         64.0      5.525      0.640       8.63x
     128    7662     1000       5418.9        631.0      5.419      0.631       8.59x
     256   15343      100        575.7         65.1      5.757      0.651       8.84x
    1024   61425      100        668.7         66.0      6.687      0.660      10.13x
    1024   61425     1000       8451.7        651.5      8.452      0.651      12.97x
    4096  245742      100        891.8         69.1      8.918      0.691      12.90x
   17408 1044464      100        873.4         68.1      8.734      0.681      12.82x
   17408 1044464     1000       9883.6        654.1      9.884      0.654      15.11x
```

Two readings.

**cursor's per-element cost is flat in the chunk count.** It holds at 0.62 to 0.69 ns per emitted element from 16 chunks to 17408, because the one seek amortizes over the window and the rest is a contiguous walk. That is the O(w) shape a range read should have.

**perElem's per-element cost grows with log(chunks).** It climbs from 3.9 ns/element at 16 chunks to 8.7 ns/element at 17408, the Fenwick descent depth paid once per position. That is the O(w log n) shape the gate measured.

At the gate cell (17408 chunks, one million elements, window 100) the cursor walk is **12.82x** faster on the index resolution, and the speedup widens with both the chunk count and the window because the per-element descent is the whole extra cost.

## Verdict (frozen)

LRANGE must resolve the window with a single seek plus a cursor advance, never a per-element locate.
The per-element `get(i)` loop is O(w log n) in the chunk count and is the root of the gate's 0.51x LRANGE regression; the cursor walk is O(w) and flat in the chunk count.

This lab bakes no constant; it justifies the shape of `list.appendRange` / `native.rangeInto` (the slice this lab ships with).
The lab isolates and bounds the index-resolution win only; the end-to-end LRANGE gain is smaller because the op also pays the RESP array encode, the reply delivery, and the reactor dispatch that this walk does not touch, and it is measured by a box A/B of the `lrange_c10k` / `lrange_c1m` aki-bench cells on the old versus new f3srv binary.
The same per-element-locate shape does not affect the head and tail ops (LINDEX is a single locate, LPUSH/LPOP never locate), so this is a range-read-only change.
