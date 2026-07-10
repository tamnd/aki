# Lab: value copies on the GET reply path

Spec 2064/f3, M0 gate follow-up, lab 9.

## The question

The M0 gate profile put readSep plus the reply-arena append at about 10 percent of CPU at 4KiB values (9.24s + 3.38s of 121.6s).
The GET handler read every band through the shard scratch (GetString appends the arena bytes into cx.Val) and Reply.Bulk then copied that scratch into the reply arena, so a resident value crossed memory twice where redis crosses once.
The handler runs on the single owner inside the epoch bracket, value-log compaction fires only at the idle boundary, and arena.freeSegment has no caller on the command path, so nothing moves arena bytes mid-command and the resident bands can hand the arena view straight to the reply builder.
What does dropping the scratch copy buy, per value size?

## Method

`go run .` measures the exact pair of calls the owner runs per GET, in process with no server and no wire: a store read plus resp.AppendBulk into a reused 64KiB reply buffer, in rounds of 16 appends (the P16 batch shape) over uniform-random keys.
Per value size {64B, 512B, 1KiB, 4KiB} it fills a store (1M keys, 256K at 4KiB) and runs two lanes for 2 seconds each: copy is GetString through a reused scratch, the read GET ran before this lab; view is GetView, the arena view.
Lanes alternate over three reps per cell so thermal drift lands on both sides of the ratio.

## Results

Apple M4 (4P + 6E), macOS, Go 1.26, in process.
Two runs on a quiet box; the ratio shape was stable, the absolute numbers moved with the machine.

Run 1:

| value | copy ops/s | view ops/s | view/copy |
|---|---|---|---|
| 64B | 4.20M | 4.27M | 1.02x |
| 512B | 2.75M | 3.26M | 1.18x |
| 1KiB | 2.06M | 2.38M | 1.16x |
| 4KiB | 1.63M | 1.83M | 1.13x |

Run 2:

| value | copy ops/s | view ops/s | view/copy |
|---|---|---|---|
| 64B | 3.70M | 3.69M | 1.00x |
| 512B | 2.64M | 3.30M | 1.25x |
| 1KiB | 2.14M | 2.34M | 1.09x |
| 4KiB | 1.64M | 1.81M | 1.10x |

The shape: at 64B the copy is a fraction of the probe-plus-header cost and the view path is level.
From 512B up the scratch copy is a real share of the reply build and eliding it buys 10 to 25 percent; the residual copy in AppendBulk is the one redis also pays, so this is the whole first copy, not half of it.
The pure store read without the reply append (BenchmarkReadValue in engine/f3/store) shows the same elision undiluted: 215ns to 172ns at 1KiB and 162ns to 83ns at 4KiB per read.

## Verdict

Serve resident reads as arena views: GET, MGET, and GETRANGE read through store.GetView / GetViewStream, which return the embedded bytes and an arena-resident separated run without the scratch copy.
The view is valid until the owner returns from the current command execution and dies at the next store write on the shard; Reply.Bulk and AppendFanValue consume it immediately, and f3srv/drivers/view_test.go pins the same-batch overwrite cases.
The int cell, a log-resident run, and the chunked band keep one copy through the store scratch: an int cell has no text in the arena to view, a log run is a pread and cannot alias the arena, and the chunked band streams.
SET with GET and INCRBYFLOAT stay on the copying read because a write runs between the read and its use, and an in-place overwrite would mutate a view.
