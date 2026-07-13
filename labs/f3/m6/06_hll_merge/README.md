# HLL register-merge kernel: the fold under PFMERGE and multi-key PFCOUNT

The register merge is, for each of 16384 registers, `dst[i] = max(dst[i], src[i])`.
It is the dominant term in `PFMERGE` and in multi-key `PFCOUNT`, so it gets the
same treatment the BITCOUNT kernel got: a specified scalar strategy, the
arithmetic that makes it branchless, and a measured verdict before the command
slice bakes it in.

Two shapes. The naive shape works on the packed 6-bit array: unpack a register,
compare, repack, 16384 times per source. The branch on the comparison is the
cost the vendors' SIMD rewrites deleted. The branchless shape unpacks each input
once into a one-byte-per-register scratch (word-at-a-time, 16 registers per 12
bytes, the reghisto layout), folds with a SWAR byte-max on 8-byte words, and
repacks once word-at-a-time, or skips the repack entirely and feeds the value
histogram straight from the scratch when the caller is `PFCOUNT`.

The SWAR byte-max needs no branch and no lane crossing because every register is
at most 63, so the high bit of every lane is clear. `(a | H) - b` with
`H = 0x8080...80` keeps each lane's high bit set exactly when `a >= b` and the
borrow never leaves the lane, so `h = ((a|H)-b) & H` is a per-lane a-ge-b flag;
`m = h | (h - (h>>7))` expands it to a full-byte select mask, and
`(a & m) | (b &^ m)` picks the larger byte in all eight lanes at once.

```
two-source fold, per cardinality (naive packed vs SWAR unpack-fold-repack):
card              naive         swar   speedup     estNaive      estSwar
100            22.515µs      14.73µs     1.53x          200          200
1000           25.726µs     14.619µs     1.76x         1987         1987
10000          24.768µs     14.928µs     1.66x        19973        19973
100000          25.44µs     14.969µs     1.70x       199892       199892
1000000        25.872µs     15.444µs     1.68x      2014825      2014825

N-source fold at card=100000 each (PFMERGE fan-in):
sources           naive         swar   speedup
2              24.698µs     14.874µs     1.66x
4              94.114µs     27.944µs     3.37x
8             227.562µs     53.526µs     4.25x
16            461.149µs    105.688µs     4.36x

multi-key PFCOUNT read union, no repack, card=100000 each:
keys              naive         swar   speedup   estAgree
2              40.451µs     24.643µs     1.64x       true
4              109.96µs     36.943µs     2.98x       true
8             242.961µs     63.348µs     3.84x       true
16            478.909µs    115.387µs     4.15x       true
```

`estNaive == estSwar` and the merged sketches are byte-identical on every row, so
the SWAR path is a pure speedup: the two shapes commit to the same bytes and the
same count.

Where the win comes from. A two-source fold is already 1.5x-1.8x once the repack
is word-at-a-time rather than per-register (the per-register repack was the whole
reason an earlier draft showed no two-source win). The bigger numbers are the
fan-in: the SWAR path pays the unpack and one repack once and adds one cheap word
fold per extra source, while the naive path pays a full per-register pass per
source, so the gap widens to ~4.3x at 16 sources. The `PFCOUNT` read path never
repacks, so its 2-key case already wins 1.6x and its 16-key case 4.2x.

Verdict for the slice: fold on the unpacked one-byte-per-register scratch with the
SWAR byte-max, repack word-at-a-time for `PFMERGE`, feed the histogram directly
for multi-key `PFCOUNT`. This is the scalar kernel the command slice ships; the
vendors' 12x is measured against the branchy baseline and overstates the gap
against this form. An AVX2 `vpmaxub` fold takes the same unpacked scratch as
input and is the recorded next step only if the gate box shows the scalar fold
sinking a cell (P15.11's falsifier: the scratch traffic dominating). The packed
four-registers-at-a-time fold is the parked fallback if unpack traffic ever wins.
