// Command 07_geo_covering prices the GEOSEARCH covering-range engine against the
// full-scan baseline, the structural claim behind the geo read path.
//
// A proximity query is a 2D question the sorted set answers in 1D. The naive
// shape scans every member, decodes its score, and exact-filters, so it is O(N)
// in the set cardinality regardless of how small the search area is. The
// covering shape picks a geohash precision from the shape's extent so a 3-by-3
// block of cells covers the whole search area, turns the nine cells into a few
// contiguous score ranges, walks only those ranges on the ordered scores, and
// exact-filters the handful of candidates the block holds. Its work scales with
// the points inside the block, never with the cardinality.
//
// This lab pins three things before the read path bakes the algorithm in: that
// the two shapes return the byte-identical survivor set, that the covering walk
// touches a small and roughly constant candidate count as the set grows around a
// fixed-size search, and that the exact-filter over-fetch (candidates per
// survivor) stays bounded, the precision of the cover. The geohash kernel here
// is the same interleave, step estimator, neighbor arithmetic, and exact filter
// the engine's zset/geosearch.go ships, duplicated because a lab cannot import
// the package-private codec.
package main

import (
	"fmt"
	"math"
	"sort"
	"time"
)

const (
	geoLonMin       = -180.0
	geoLatMin       = -85.05112878
	geoLonMax       = 180.0
	geoLatMax       = 85.05112878
	geoStep         = 26
	geoEarthRadiusM = 6372797.560856
	mercatorMax     = 20037726.37
)

// geoSpread scatters a 26-bit value into the even bit positions, the forward
// half of Redis's interleave64.
func geoSpread(v uint32) uint64 {
	x := uint64(v) & 0x3FFFFFF
	x = (x | (x << 16)) & 0x0000FFFF0000FFFF
	x = (x | (x << 8)) & 0x00FF00FF00FF00FF
	x = (x | (x << 4)) & 0x0F0F0F0F0F0F0F0F
	x = (x | (x << 2)) & 0x3333333333333333
	x = (x | (x << 1)) & 0x5555555555555555
	return x
}

// geoSquash gathers an even-bit plane back into a dense 26-bit value.
func geoSquash(x uint64) uint32 {
	x &= 0x5555555555555555
	x = (x | (x >> 1)) & 0x3333333333333333
	x = (x | (x >> 2)) & 0x0F0F0F0F0F0F0F0F
	x = (x | (x >> 4)) & 0x00FF00FF00FF00FF
	x = (x | (x >> 8)) & 0x0000FFFF0000FFFF
	x = (x | (x >> 16)) & 0x00000000FFFFFFFF
	return uint32(x)
}

// geoEncode maps a coordinate to its 52-bit geohash at step 26.
func geoEncode(lon, lat float64) uint64 {
	latOff := (lat - geoLatMin) / (geoLatMax - geoLatMin)
	lonOff := (lon - geoLonMin) / (geoLonMax - geoLonMin)
	scale := float64(uint64(1) << geoStep)
	return geoSpread(uint32(latOff*scale)) | (geoSpread(uint32(lonOff*scale)) << 1)
}

// geoDecode maps a 52-bit geohash back to the center of its cell.
func geoDecode(bits uint64) (lon, lat float64) {
	ilat := geoSquash(bits)
	ilon := geoSquash(bits >> 1)
	scale := float64(uint64(1) << geoStep)
	latSpan := geoLatMax - geoLatMin
	lonSpan := geoLonMax - geoLonMin
	lat = geoLatMin + ((float64(ilat)+0.5)/scale)*latSpan
	lon = geoLonMin + ((float64(ilon)+0.5)/scale)*lonSpan
	return
}

// geoDistM is the haversine distance in meters on Redis's sphere.
func geoDistM(lon1, lat1, lon2, lat2 float64) float64 {
	lon1r := lon1 * math.Pi / 180
	lon2r := lon2 * math.Pi / 180
	v := math.Sin((lon2r - lon1r) / 2)
	if v == 0 {
		return geoEarthRadiusM * math.Abs((lat2-lat1)*math.Pi/180)
	}
	lat1r := lat1 * math.Pi / 180
	lat2r := lat2 * math.Pi / 180
	u := math.Sin((lat2r - lat1r) / 2)
	a := u*u + math.Cos(lat1r)*math.Cos(lat2r)*v*v
	return 2.0 * geoEarthRadiusM * math.Asin(math.Sqrt(a))
}

// geoEncodeStep maps a coordinate to its geohash at an arbitrary bit depth.
func geoEncodeStep(lon, lat float64, step uint) uint64 {
	latOff := (lat - geoLatMin) / (geoLatMax - geoLatMin)
	lonOff := (lon - geoLonMin) / (geoLonMax - geoLonMin)
	scale := float64(uint64(1) << step)
	return geoSpread(uint32(latOff*scale)) | (geoSpread(uint32(lonOff*scale)) << 1)
}

// geoDecodeArea returns the coordinate bounds of a step-depth cell.
func geoDecodeArea(bits uint64, step uint) (lonMin, lonMax, latMin, latMax float64) {
	ilat := geoSquash(bits)
	ilon := geoSquash(bits >> 1)
	scale := float64(uint64(1) << step)
	latSpan := geoLatMax - geoLatMin
	lonSpan := geoLonMax - geoLonMin
	latMin = geoLatMin + (float64(ilat)/scale)*latSpan
	latMax = geoLatMin + (float64(ilat+1)/scale)*latSpan
	lonMin = geoLonMin + (float64(ilon)/scale)*lonSpan
	lonMax = geoLonMin + (float64(ilon+1)/scale)*lonSpan
	return
}

// geoEstimateSteps picks the geohash bit depth for a search reach.
func geoEstimateSteps(rangeMeters, lat float64) uint {
	if rangeMeters == 0 {
		return 26
	}
	step := 1
	for rangeMeters < mercatorMax {
		rangeMeters *= 2
		step++
	}
	step -= 2
	if lat > 66 || lat < -66 {
		step--
		if lat > 80 || lat < -80 {
			step--
		}
	}
	if step < 1 {
		step = 1
	}
	if step > 26 {
		step = 26
	}
	return uint(step)
}

func geoMoveX(bits uint64, step uint, d int) uint64 {
	if d == 0 {
		return bits
	}
	x := bits & 0xaaaaaaaaaaaaaaaa
	y := bits & 0x5555555555555555
	zz := uint64(0x5555555555555555) >> (64 - step*2)
	if d > 0 {
		x = x + (zz + 1)
	} else {
		x = x | zz
		x = x - (zz + 1)
	}
	x &= 0xaaaaaaaaaaaaaaaa >> (64 - step*2)
	return x | y
}

func geoMoveY(bits uint64, step uint, d int) uint64 {
	if d == 0 {
		return bits
	}
	x := bits & 0xaaaaaaaaaaaaaaaa
	y := bits & 0x5555555555555555
	zz := uint64(0xaaaaaaaaaaaaaaaa) >> (64 - step*2)
	if d > 0 {
		y = y + (zz + 1)
	} else {
		y = y | zz
		y = y - (zz + 1)
	}
	y &= 0x5555555555555555 >> (64 - step*2)
	return x | y
}

func geoAlign52(bits uint64, step uint) uint64 { return bits << (52 - step*2) }

func degRad(d float64) float64 { return d * math.Pi / 180 }
func radDeg(r float64) float64 { return r * 180 / math.Pi }

// geoBoundingBox returns the lon/lat bounds a radius reaches at a latitude.
func geoBoundingBox(lon, lat, halfWidthM, halfHeightM float64) (minLon, maxLon, minLat, maxLat float64) {
	latDelta := radDeg(halfHeightM / geoEarthRadiusM)
	lonDeltaTop := radDeg(halfWidthM / geoEarthRadiusM / math.Cos(degRad(lat+latDelta)))
	lonDeltaBottom := radDeg(halfWidthM / geoEarthRadiusM / math.Cos(degRad(lat-latDelta)))
	lonDelta := lonDeltaTop
	if lat < 0 {
		lonDelta = lonDeltaBottom
	}
	return lon - lonDelta, lon + lonDelta, lat - latDelta, lat + latDelta
}

const (
	idxCenter = iota
	idxNorth
	idxSouth
	idxEast
	idxWest
	idxNorthEast
	idxNorthWest
	idxSouthEast
	idxSouthWest
)

type geoCell struct {
	bits uint64
	used bool
}

// geoCovering computes the nine covering cells for a circle of radiusM at the
// center, the port of Redis's geohashCalculateAreasByShapeWGS84.
func geoCovering(lon, lat, radiusM float64) (uint, [9]geoCell) {
	minLon, maxLon, minLat, maxLat := geoBoundingBox(lon, lat, radiusM, radiusM)
	step := geoEstimateSteps(radiusM, lat)
	hash := geoEncodeStep(lon, lat, step)

	if step > 1 && geoNeedsCoarserStep(lon, lat, radiusM, hash, step) {
		step--
		hash = geoEncodeStep(lon, lat, step)
	}

	var c [9]geoCell
	c[idxCenter] = geoCell{hash, true}
	c[idxNorth] = geoCell{geoMoveY(hash, step, 1), true}
	c[idxSouth] = geoCell{geoMoveY(hash, step, -1), true}
	c[idxEast] = geoCell{geoMoveX(hash, step, 1), true}
	c[idxWest] = geoCell{geoMoveX(hash, step, -1), true}
	c[idxNorthEast] = geoCell{geoMoveX(geoMoveY(hash, step, 1), step, 1), true}
	c[idxNorthWest] = geoCell{geoMoveX(geoMoveY(hash, step, 1), step, -1), true}
	c[idxSouthEast] = geoCell{geoMoveX(geoMoveY(hash, step, -1), step, 1), true}
	c[idxSouthWest] = geoCell{geoMoveX(geoMoveY(hash, step, -1), step, -1), true}

	if step >= 2 {
		lonMin, lonMax, latMin, latMax := geoDecodeArea(hash, step)
		if latMin < minLat {
			c[idxSouth].used, c[idxSouthWest].used, c[idxSouthEast].used = false, false, false
		}
		if latMax > maxLat {
			c[idxNorth].used, c[idxNorthWest].used, c[idxNorthEast].used = false, false, false
		}
		if lonMin < minLon {
			c[idxWest].used, c[idxNorthWest].used, c[idxSouthWest].used = false, false, false
		}
		if lonMax > maxLon {
			c[idxEast].used, c[idxNorthEast].used, c[idxSouthEast].used = false, false, false
		}
	}
	return step, c
}

func geoNeedsCoarserStep(lon, lat, reach float64, hash uint64, step uint) bool {
	_, _, _, nLatMax := geoDecodeArea(geoMoveY(hash, step, 1), step)
	_, _, sLatMin, _ := geoDecodeArea(geoMoveY(hash, step, -1), step)
	_, eLonMax, _, _ := geoDecodeArea(geoMoveX(hash, step, 1), step)
	wLonMin, _, _, _ := geoDecodeArea(geoMoveX(hash, step, -1), step)
	if geoDistM(lon, lat, lon, nLatMax) < reach {
		return true
	}
	if geoDistM(lon, lat, lon, sLatMin) < reach {
		return true
	}
	if geoDistM(lon, lat, eLonMax, lat) < reach {
		return true
	}
	if geoDistM(lon, lat, wLonMin, lat) < reach {
		return true
	}
	return false
}

type scoreRange struct{ min, maxExcl uint64 }

// mergeRanges turns the used cells into sorted, merged score bands.
func mergeRanges(step uint, cells [9]geoCell) []scoreRange {
	ranges := make([]scoreRange, 0, 9)
	for _, c := range cells {
		if !c.used {
			continue
		}
		ranges = append(ranges, scoreRange{geoAlign52(c.bits, step), geoAlign52(c.bits+1, step)})
	}
	sort.Slice(ranges, func(i, j int) bool { return ranges[i].min < ranges[j].min })
	merged := ranges[:0]
	for _, rg := range ranges {
		if len(merged) > 0 && rg.min <= merged[len(merged)-1].maxExcl {
			if rg.maxExcl > merged[len(merged)-1].maxExcl {
				merged[len(merged)-1].maxExcl = rg.maxExcl
			}
			continue
		}
		merged = append(merged, rg)
	}
	return merged
}

// point is a stored member: its score and its true coordinate.
type point struct {
	score    uint64
	lon, lat float64
	id       int
}

// geoSet is the substrate the walk descends: members sorted by geohash score,
// the ordered form the counted tree presents.
type geoSet struct {
	pts []point
}

func newGeoSet(pts []point) *geoSet {
	sorted := append([]point(nil), pts...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].score < sorted[j].score })
	return &geoSet{pts: sorted}
}

// coveringSearch runs the covering-range engine and returns the survivor ids and
// the number of candidates the range walk examined.
func coveringSearch(gs *geoSet, lon, lat, radiusM float64) (ids []int, candidates int) {
	step, cells := geoCovering(lon, lat, radiusM)
	for _, rg := range mergeRanges(step, cells) {
		lo := sort.Search(len(gs.pts), func(i int) bool { return gs.pts[i].score >= rg.min })
		for i := lo; i < len(gs.pts) && gs.pts[i].score < rg.maxExcl; i++ {
			candidates++
			p := gs.pts[i]
			plon, plat := geoDecode(p.score)
			if geoDistM(lon, lat, plon, plat) <= radiusM {
				ids = append(ids, p.id)
			}
		}
	}
	sort.Ints(ids)
	return ids, candidates
}

// fullScan is the O(N) baseline: decode and exact-filter every member.
func fullScan(gs *geoSet, lon, lat, radiusM float64) (ids []int, candidates int) {
	for _, p := range gs.pts {
		candidates++
		plon, plat := geoDecode(p.score)
		if geoDistM(lon, lat, plon, plat) <= radiusM {
			ids = append(ids, p.id)
		}
	}
	sort.Ints(ids)
	return ids, candidates
}

// splitmix64 gives deterministic pseudo-random values per index.
func splitmix64(x uint64) uint64 {
	x += 0x9e3779b97f4d7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

// u01 maps a hash to a float in [0,1).
func u01(h uint64) float64 { return float64(h>>11) / float64(uint64(1)<<53) }

// makeSet builds n points uniformly over a square region of the given side in
// degrees centered on (clon, clat), at a fixed density so a larger n means a
// larger region, not a denser one. That is the shape that separates the two
// algorithms: a fixed-size search over a growing map.
func makeSet(n int, clon, clat, sideDeg float64) *geoSet {
	pts := make([]point, n)
	half := sideDeg / 2
	for i := 0; i < n; i++ {
		hx := splitmix64(uint64(i)*2 + 1)
		hy := splitmix64(uint64(i)*2 + 2)
		lon := clon - half + u01(hx)*sideDeg
		lat := clat - half + u01(hy)*sideDeg
		pts[i] = point{score: geoEncode(lon, lat), lon: lon, lat: lat, id: i}
	}
	return newGeoSet(pts)
}

func intsEqual(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func timeSearch(f func() ([]int, int)) time.Duration {
	const iters = 200
	start := time.Now()
	for i := 0; i < iters; i++ {
		f()
	}
	return time.Since(start) / iters
}

func main() {
	const (
		clon, clat = 15.0, 37.0
		density    = 400.0    // points per square degree, held fixed
		radiusM    = 30_000.0 // fixed 30 km search
	)

	fmt.Println("fixed 30 km search over a growing map at fixed density (full-scan vs covering):")
	fmt.Printf("%-10s %8s %9s %10s %10s %12s %12s %9s\n",
		"n", "sideDeg", "survivors", "scanCand", "coverCand", "fullScan", "covering", "speedup")
	for _, n := range []int{1000, 5000, 25000, 100000, 500000, 1000000} {
		side := math.Sqrt(float64(n) / density)
		gs := makeSet(n, clon, clat, side)

		wantIDs, scanCand := fullScan(gs, clon, clat, radiusM)
		gotIDs, coverCand := coveringSearch(gs, clon, clat, radiusM)
		if !intsEqual(wantIDs, gotIDs) {
			fmt.Printf("MISMATCH at n=%d: survivor sets differ (%d vs %d)\n", n, len(wantIDs), len(gotIDs))
			return
		}
		tf := timeSearch(func() ([]int, int) { return fullScan(gs, clon, clat, radiusM) })
		tc := timeSearch(func() ([]int, int) { return coveringSearch(gs, clon, clat, radiusM) })
		fmt.Printf("%-10d %8.3f %9d %10d %10d %12v %12v %8.1fx\n",
			n, side, len(wantIDs), scanCand, coverCand, tf, tc, float64(tf)/float64(tc))
	}

	fmt.Println("\ncover precision: candidates per survivor over the radius sweep (n=500000, fixed density):")
	fmt.Printf("%-10s %6s %9s %10s %10s %10s\n",
		"radiusKm", "step", "cells", "survivors", "coverCand", "overfetch")
	side := math.Sqrt(500000.0 / density)
	gs := makeSet(500000, clon, clat, side)
	for _, rk := range []float64{1, 5, 10, 30, 60, 120} {
		rm := rk * 1000
		step, cells := geoCovering(clon, clat, rm)
		used := 0
		for _, c := range cells {
			if c.used {
				used++
			}
		}
		ids, coverCand := coveringSearch(gs, clon, clat, rm)
		over := math.NaN()
		if len(ids) > 0 {
			over = float64(coverCand) / float64(len(ids))
		}
		fmt.Printf("%-10.0f %6d %9d %10d %10d %9.2fx\n", rk, step, used, len(ids), coverCand, over)
	}
}
