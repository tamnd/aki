# ringpool: ring depth and batch threshold vs iopool

Milestone B5 lab 01 (spec 2064/sqlo1 doc 04 section 12, tracking issue #725).

## Question

What ring depth and batch-submit threshold should the ioring backend bake, and where does the ring actually beat the worker-pool backend it replaces?
The spec carries batch 16 as the syscall amortization point and bans 128 for its tail cost; both numbers came from outside measurements and need our own curve through the Backend seam.

## Method

One binary drives either backend through the same seam with the same request stream.
The coldread shape issues batches of random group reads against a filled file and waits each batch out, so in-flight depth equals the batch under test.
The drain shape writes groups sequentially with an fsync barrier at every extent boundary, the checkpoint rhythm the store's drain path pays.
Every request is stamped with its batch's submit time, so a batch that queues on its own tail pays for it in p99.

Reads are page-cache warm unless -direct is set, so warm read rows price the submission path (syscalls, wakeups, copies), not device latency; the gate box reruns the sweep with -direct for the cold shape.
The slice-2 axes: -regbuf N switches the ring arm's batch slots to the registered pool so READ_FIXED and WRITE_FIXED carry the IO, and -direct reopens the file O_DIRECT (ring only, requires -regbuf since the pool is the aligned memory; tmpfs refuses it, so run.sh tolerates that arm exiting 3).

## Run

    ./run.sh > ringpool.csv

The ring arm needs a Linux kernel with io_uring; run.sh probes first and skips those rows elsewhere.
Depth sweeps 2 to 32, batch sweeps 1 to 128, both shapes, both backends.

## CSV

backend, workload, depth, batch, regbuf, direct, n, secs, ops_s, mb_s, p50_us, p99_us.

## Verdict (provisional, server3 kernel 6.8, 2026-07-24)

The harness is proven end to end: 140 rows, both backends, both shapes, the ring arm live on kernel 6.8.
The numbers are not: server3 sat at load average 137 on 8 cores during the sweep, so every latency column is mostly scheduler delay and the depth-by-batch curve does not reproduce cell to cell.
Under that starvation the ring loses badly to iopool almost everywhere, which is expected rather than informative; the ring pays more wakeups per completion (reaper to mailbox to collector) and CPU starvation taxes exactly that.

Two observations survived the noise.
Ring depth 8 was the only depth where the ring competed at all (55k ops per second coldread at batch 16, its best cell by far, with depths 2, 4, and 32 collapsing), so depth 8 goes in as the starting point for the gate box sweep.
And both backends show the batch-128 p99 blowup the spec bans it for, tens to hundreds of milliseconds against sub-millisecond at small batches, on the ring and the pool alike.

No constant gets baked from this run.
The gate box sweep on an idle machine, plus the O_DIRECT and registered-buffer axes from slice 2, is the verdict.
