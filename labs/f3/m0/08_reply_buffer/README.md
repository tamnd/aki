# Lab: reply write buffer size

Spec 2064/f3, M0 gate follow-up, lab 8.

## The question

The connection writer drains replies through a bufio.Writer that was left at the 4096-byte default. The M0 gate profile put 56.7s of 76.1s of write time at 1KiB values in mid-drain buffer-full flushes (one write() per ~3.9 replies), and at 4KiB values every reply overflows the buffer, so it is one write() per reply. The buffer should hold a full pipelined round so a P16 burst amortizes into about one syscall per drained boundary. What does each size actually buy, per reply size, over a real socket?

## Method

`go run .` starts one f3srv per writer size {4KiB, 16KiB, 64KiB, 256KiB} on loopback with default shards, via the drivers.Options.ReplyBufBytes knob this lab added. Per value size {64B, 1KiB, 4KiB}: load 16384 keys, then 8 client connections each run pipelined GET rounds of depth 16 against random keys for 2 seconds. All values in a cell are the same size, so every reply is a fixed-shape `$<len>\r\n<val>\r\n` and the client reads each round by exact byte count. The cell reports total GETs per second across the 8 connections.

## Results

Apple M4 (4P + 6E), macOS, Go 1.26, loopback. Two runs; the shape was stable, 64B wobbled a few percent between runs.

Run 1:

| writer buf | GET 64B ops/s | GET 1KiB ops/s | GET 4KiB ops/s |
|---|---|---|---|
| 4KiB | 1.10M | 0.91M | 0.49M |
| 16KiB | 1.08M | 1.03M | 0.72M |
| 64KiB | 1.09M | 0.98M | 0.81M |
| 256KiB | 1.07M | 1.02M | 0.82M |

Run 2:

| writer buf | GET 64B ops/s | GET 1KiB ops/s | GET 4KiB ops/s |
|---|---|---|---|
| 4KiB | 1.11M | 0.90M | 0.48M |
| 16KiB | 1.04M | 0.99M | 0.73M |
| 64KiB | 1.05M | 1.01M | 0.83M |
| 256KiB | 1.01M | 0.97M | 0.81M |

The shape: at 64B replies the 4KiB default already holds a P16 round, so the buffer size does not matter. At 1KiB a P16 round is 16KiB and the 4KiB buffer flushes mid-drain four times per round; 16KiB and up recovers ~10 percent. At 4KiB every reply overflows the 4KiB buffer and the cell runs at one write() per reply; 64KiB holds the full 64KiB round and recovers ~1.7x. 256KiB buys nothing over 64KiB anywhere: past one full round there are no mid-drain flushes left to remove.

## Verdict

Freeze the writer buffer at 64KiB (`replyBufSize` in f3srv/drivers/server.go). The sizing rule: at least pipeline depth times typical reply size, and the gate shape is P16 with separated-band values to 4KiB, so 16 x 4KiB = 64KiB. Cost is per-connection writer memory: 64KiB x 512 connections = 32MiB, which is fine against the arena budget; 256KiB would quadruple that for zero return.
