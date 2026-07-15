# unitsize: the sqlo1b IO unit on the gate box NVMe

Milestone B1 lab 01 (spec 2064/sqlo1 doc 03, principle P2).

## Question

Is 4 KiB the right group size to bake into extent addressing?
Doc 03 adopts LeanStore's finding that 4 KiB has the lowest NVMe latency and lowest random write amplification, against the aki base convention of 16 KiB pages; the superblock stores io_unit as data, so an amended default costs no version bump if this sweep says otherwise.
The group is the read quantum, the compression quantum, and the addressing quantum, so the number this lab confirms prices every cold point lookup the format will ever serve.

## Method

One big file of random bytes on the target disk, opened with O_DIRECT (F_NOCACHE on darwin, smoke only), so every op hits the device.
Cells are op x unit x queue depth: random unit-aligned preads and pwrites at 4/8/16 KiB across depths 1 to 32, each cell timed for a fixed window with per-op latencies kept.
The random read is the shape of a cold point lookup (one group read); the random write is the in-place map page shape and the write-amplification probe.
The fill is untimed and the buffers are 4096-aligned so the direct path never falls back.
The direct CSV column records whether direct IO was actually on; a 0 there means the row is not quotable.

Read the sweep as pricing: 4 KiB is confirmed unless 16 KiB random reads land within about 10 percent of 4 KiB per-op time at the working depths and 16 KiB random writes show no amplification penalty, in which case the io_unit default gets amended before the superblock slice bakes the geometry.

## Run

    ./run.sh            # 8 GiB file, 5 s per cell, full qd sweep
    go run . -quick     # 256 MiB, 1 s, depths 1 and 8
    go test ./...       # CSV shape, alignment, direct-open, fill

The gate box runs it on the ext4 root disk (WSL2, so virtio overhead is in the numbers, which is fine: that is the box every other gate number comes from).
Darwin smoke numbers are not evidence; F_NOCACHE is advisory and the reads still land in device cache.

## Results

Pending the gate box run.

## Verdict

Pending.
