# GEOSEARCH covering ranges: proximity as a handful of 1D score walks

A geo set is a sorted set whose score is a 52-bit geohash interleave, so a 2D
proximity query is a 1D question in disguise. The naive answer scans every
member, decodes its score, and exact-filters, so it is O(N) in the cardinality
no matter how small the search area is. The covering answer picks a geohash
precision from the shape's extent so a 3-by-3 block of cells covers the whole
search area, turns the nine cells into a few contiguous score ranges, walks only
those ranges on the ordered scores, and exact-filters the handful of candidates
the block holds. Its work scales with the points inside the block, never with
the set size.

The geohash math is Redis's, ported cell for cell: `geohashEstimateStepsByRadius`
picks the bit depth, `geohash_move_x`/`geohash_move_y` step to the eight
neighbors, the decrease-step probe drops one precision level when the block does
not reach the shape, and the bounding-box exclusion zeroes the neighbors that
fall entirely outside the shape once the cells are small enough. Each surviving
cell aligns to a `[bits << (52 - 2*step), (bits+1) << (52 - 2*step))` score band,
the bands sort and merge, and the walk descends each one on the counted tree.
The exact filter at the end is the same haversine `GEODIST` runs, so the covering
set and the survivors match a same-version Redis.

This lab pins three things before the read path ships the algorithm: that the
covering walk returns the byte-identical survivor set the full scan does across
cardinalities, region sizes, latitudes, and radii; that a fixed-size search over
a growing map touches a small and roughly constant candidate count while the full
scan grows with N; and that the exact-filter over-fetch stays bounded, the
precision of the cover.

## The structural win

A fixed 30 km search over a map that grows at fixed density (a larger set means a
larger region, not a denser one), the shape that separates the two algorithms.
The full scan decodes and filters every member; the covering walk touches only
the points in the 3-by-3 block, so its candidate count and its time stay flat as
the map grows a thousandfold.

```
fixed 30 km search over a growing map at fixed density (full-scan vs covering):
n           sideDeg survivors   scanCand  coverCand     fullScan     covering   speedup
1000          1.581       129       1000        491     38.025µs     19.835µs      1.9x
5000          3.536        99       5000        541     132.35µs     15.498µs      8.5x
25000         7.906       107      25000        551    513.732µs     14.536µs     35.3x
100000       15.811       133     100000        559   2.075912ms     14.887µs    139.4x
500000       35.355       107     500000        599  10.318328ms     15.513µs    665.1x
1000000      50.000       105    1000000        557  20.721562ms     14.303µs   1448.8x
```

The covering candidate count sits near 550 from a thousand members to a million,
because the search area is fixed and the density is fixed, so the block always
holds about the same number of points. The full scan's candidate count is the
cardinality by definition. The covering time is flat near 15 µs; the full scan's
time tracks N, from 38 µs to 20.7 ms, and the speedup grows with it, past 1400x
at a million members. This is the whole reason the geo read path walks covering
ranges instead of scanning: the cost follows the search area, not the set.

## Cover precision

The over-fetch is the candidates the walk examines per survivor it keeps, the
looseness of the cover-to-circle fit. A 3-by-3 block of square cells around a
circle is a bounded over-cover, so the ratio stays single digit across the radius
sweep, and the exact haversine filter discards the surplus at about 30 ns each.

```
cover precision: candidates per survivor over the radius sweep (n=500000, fixed density):
radiusKm     step     cells  survivors  coverCand  overfetch
1              14         6          0          1       NaNx
5              11         3          2         11      5.50x
10             10         2         11         39      3.55x
30              9         6        107        599      5.60x
60              8         6        481       2321      4.83x
120             7         4       1910       6037      3.16x
```

The step drops as the radius grows, from 14 bits per coordinate at 1 km to 7 at
120 km, so each cell widens to match the reach and the block still covers the
circle in nine cells or fewer after the exclusion prunes the useless neighbors
(the `cells` column is the used count, 2 to 6, never the full 9). The over-fetch
holds between 3x and 6x, so the walk never examines more than a small multiple of
the true survivors. The 1 km row finds no member in this half-million-point map,
the density is too low for so tight a circle, and the walk still costs a single
candidate.

## Verdict

The covering-range engine is the read path. It returns exactly the full scan's
survivors on every shape tested, its work scales with the search area rather than
the cardinality (flat candidate count and flat time as the map grows a thousandfold),
and its cover is tight enough that the exact filter discards only a 3x to 6x
over-fetch. The full scan is kept only as the correctness oracle in the tests,
never as a code path.
