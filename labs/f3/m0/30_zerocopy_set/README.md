# M0 lab 30: the SET value-copy ceiling (zero-copy net→store arc)

Spec: 2064/f3 doc 09 (value bands) + doc 08 (net drivers). Gate rows: M0-G5
(1 KiB SET 1.83x), and the write side of M0-G6 (64 KiB) and M0-G10.

## Question

The reactor writes a SET value twice. The loop goroutine copies it out of the
socket read buffer into the batch span table so it can free the socket buffer
immediately (copy 1, `engine/f3/shard/batch.go:265`), then the shard worker
copies that staging into the arena run the record owns (copy 2,
`engine/f3/store/str.go:590` embedded / `bands.go:116` separated). redis writes
the value once, out of its per-connection query buffer into the value's sds.

Copy 1 is the decouple tax: it exists only so the reader does not wait on the
worker goroutine. The zero-copy design removes it by giving each in-flight batch
its own read buffer (per-conn buffer rotation, like redis's query buffer), so a
value span references the handed-off socket buffer and the worker copies once,
net→arena, with no reader/worker stall.

This lab isolates the memcpy ceiling that removes: how large is copy 1 next to
the rest of the per-op value handling, and does the per-conn buffer handoff cost
stay small next to the saved copy. It does NOT model the reactor's cross-batch
pipelining; the per-conn pool design avoids the serialization a single pinned
buffer would cause, so the only real-system unknowns are the memory of the pool
and the dispatch-relative fraction the copy occupies, which the box settles.

## Result (go test -bench -benchmem, apple dev box)

| value  | band      | two-copy ns/op | one-copy ns/op | delta |
|--------|-----------|---------------:|---------------:|------:|
| 64     | embedded  |          11.1  |          14.2  | -27%  |
| 1024   | embedded  |          85.3  |          52.5  | +38%  |
| 4096   | separated |          1292  |           326  | +75%  |
| 16384  | separated |          3667  |          1113  | +70%  |

## Verdict: the arc must be size-gated, and it is marginal at the gate cell

Two findings decide the shape of the engine change:

1. At 64B the one-copy path is SLOWER (11.1 → 14.2 ns): the per-conn buffer
   handoff bookkeeping costs more than the 64-byte copy it removes. Applied
   unconditionally the arc would regress the 2.71x/2.14x 64B SET/GET headline,
   the binding M0 gate. So the reference/handoff path must be gated to values
   above a threshold (≈512B); small values keep the copy-both path untouched.

2. Size-gated, the win is real but marginal at the 1 KiB gate cell: the copy
   drops ~33 ns/op (85 → 52). Against a ~350 ns end-to-end per-op budget that is
   ~9%, and M0-G5 needs +9% (1.83x → 2.0x) to clear valkey. The arc lands the
   1 KiB row exactly on the line, so the box A/B is the decider, not a
   comfortable pass. Past the gate cell (4–16 KiB) the saving is 70–75% of the
   copy and helps the G6/G10 write coverage more.

The cost side: the per-conn buffer pool raises resident memory, and the M0
memory row already sits at 1.19x valkey peak (declared structural in #1159).
Adding in-flight read buffers per conn (c512) pushes the wrong way on a bar that
is already tight.

Net: the arc is worth attempting only as a size-gated change with a box A/B
gating both the 1 KiB lift AND no 64B headline / memory regression. If the box
shows the 1 KiB row does not clear 2x or the headline/memory regresses, this
lab IS the earned verdict for G5: the second copy is the reader/worker decouple
that buys the 2.71x small-value headline, its removal is size-gated and marginal
at 1 KiB and fights the memory bar, so the 1.83x sits on the copy-vs-decouple
tradeoff, not a free optimization.
