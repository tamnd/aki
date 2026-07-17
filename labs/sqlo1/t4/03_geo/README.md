# geo: the search cell cover

Milestone T4 lab 03 (spec 2064/sqlo1 doc 09 sections 7 and 10).

## Question

Geo search covers the search area with cells at a precision derived from the radius, scans each cell as a sortable-score run range, and passes candidates through the exact distance filter.
Redis's cover (geohashEstimateStepsByRadius plus the neighbor ring) bounds the cell count at nine but admits candidates well outside the circle, and doc 09 section 10 asks for the over-read ratio those candidates cost.
A finer precision cuts the over-read, but every extra cell is another fence-guided range scan, and our unit of cost is runs read: a run is a ~2.7 KB record that may be a cold pread.
The slice needs the cover precision baked on numbers, so the lab prices Redis's step, one step finer, and one step coarser, on candidates per result, distinct runs read per search, and hot walk latency.

## Method

The model is engine-faithful and resident (the salgebra pattern): points live as one sorted array of 52-bit scores, coordinates are decoded from the score on every probe exactly as the engine will decode them, and cell scans are binary searches over the array.
The codec, the step estimator, and the distance are Redis's own (geohash.c arithmetic term for term, the haversine on Redis's earth radius), because filter parity on boundary points needs the same floating point path.
Run accounting routes the way the score fence does: a scan starts at the run whose separator owns the range's low bound, an empty interior range still reads that run because separators cannot prove emptiness, and only a range below the first separator reads nothing.
Runs deduplicate across the cells of one search; run size is 104 entries, the hsegz occupancy.
Datasets: 10^6 points uniform over a 1200 km square and 200-town gaussian clusters at 2 km sigma, at latitude bands 0, 45, and 70 (the estimator widens past 66); radii 100 m to 500 km; 64 searches per cell; a 10^7 scale check on the two live arms.
The -parity mode GEOADDs a 6000-point fixture into a live Redis 8.8.0 and demands bit-identical ZSCOREs on every point, GEOPOS decodes within 1e-9, GEODIST at its printed grain, and GEOSEARCH result sets identical to both a brute filter over decoded scores and the cover walk itself.

## Run

    ./run.sh                    # sweep + parity if redis answers on $REDIS_PORT (default 7799)
    go run . -quick             # smoke
    go run . -parity -port 7799 # parity phase alone
    go test ./...               # interleave ref, codec round trip, pinned estimator steps, cover completeness

## Results (local, 2026-07-17, macbook; parity against a live Redis 8.8.0)

Parity: 6000 points, zero score mismatches, zero GEOPOS, zero GEODIST, 200/200 searches set-identical.

The arms separate by radius regime, and the split is the same on uniform and clustered data and at every latitude band.

Below about 1 km every arm reads one to two runs, because the whole search area routes into a couple of runs regardless of cell count.
Fine's extra cells cost only fence probes there: 15 cells against Redis's 6 at 500 m, and the walk stays within a microsecond of the Redis arm.

From 5 km up the arms diverge on the numbers that matter.
At 100 km on 10^6 uniform points the Redis-step cover examines 104K candidates for 21.9K results (4.77x over-read) and reads 1006 runs; one step finer examines 61K (2.80x) and reads 594 runs; one step coarser examines 197K (9.00x) and reads 1894.
The same 1.6 to 1.7x run cut holds at 10 km and 50 km, at latitudes 0 and 70, on clusters, and at 10^7 points (10.5K runs against 5.8K at 100 km), and the hot walk p50 tracks the runs cut, 5.3 ms against 3.2 ms at the 100 km cell.
At 500 km every arm converges (over-read 1.9 against 1.6) because half the circle is results.

Cell counts stay bounded: the fine cover peaked at 15.4 cells in the whole sweep against the geometric bound of 25, and coarse never beat Redis's step anywhere, on any metric, in any regime.

## Verdict

Slice 11 covers at estimateStep plus one, keeping the bounding-box trim rather than the neighbor ring, so the cover is the up-to-25 cells at one step finer that intersect the circle's bounding box.
The price is a handful of extra fence probes and boundary runs where searches are small and already cheap; the payoff is 1.4 to 1.7x fewer runs read and roughly half the candidates decoded everywhere past 5 km, and runs are the unit that goes cold.
Coarse is dominated everywhere and dies.
The published over-read ratio (doc 09 section 10) lands at 2.0 to 3.6x across the mid radii for the baked cover, against 3.9 to 7.2x for Redis's own cover.
The precision choice cannot move results, only cost: parity is the filter's, and the cover-completeness oracle plus the 200/200 live GEOSEARCH matches pin that.

The sweep CSV (geo.csv) stays untracked, like every lab CSV.
