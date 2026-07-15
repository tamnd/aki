package zset

// GEOSEARCH as bounded tree ranges (spec 2064/f3/15 section 11). A 2D proximity
// query reduces to a handful of 1D score ranges: pick a geohash precision from
// the shape's extent so a 3-by-3 block of cells at that precision covers the
// whole search area, turn the nine cells into contiguous score ranges, walk each
// range on the counted tree, and exact-filter every candidate against the true
// shape. The candidate over-fetch is the cover-to-circle area ratio, discarded
// by the ~30ns haversine per candidate, so the work scales with the points in
// the 3-by-3 block, never with the cardinality of the geo set.
//
// The geohash helpers (step estimation, encode at a step, neighbor arithmetic,
// the useless-cell exclusion) are ported from Redis's geohash_helper.c so the
// candidate set and the survivors match a same-version Redis.

import (
	"math"
	"sort"
	"strconv"

	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/f3srv/resp"
)

// mercatorMax is the meter extent Redis's step estimator halves against, the
// Mercator projection's half-span at the equator.
const mercatorMax = 20037726.37

// geoShape is a resolved GEOSEARCH search area: a center and either a circle
// (radiusM) or an axis-aligned box (widthM by heightM, full extents), all in
// meters. toMeters is the requested unit's multiplier, kept for formatting the
// WITHDIST reply back into the caller's unit.
type geoShape struct {
	lon, lat float64
	box      bool
	radiusM  float64
	widthM   float64
	heightM  float64
	toMeters float64
}

// geoEncodeStep maps a coordinate to its geohash at an arbitrary bit depth,
// latitude in the even interleave bits and longitude in the odd, the generalized
// form of geoEncode (which is this at step 26).
func geoEncodeStep(lon, lat float64, step uint) uint64 {
	latOff := (lat - geoLatMin) / (geoLatMax - geoLatMin)
	lonOff := (lon - geoLonMin) / (geoLonMax - geoLonMin)
	scale := float64(uint64(1) << step)
	latBits := uint32(latOff * scale)
	lonBits := uint32(lonOff * scale)
	return geoSpread(latBits) | (geoSpread(lonBits) << 1)
}

// geoDecodeArea returns the coordinate bounds of the cell a step-depth geohash
// names, the generalized geoDecode. The center is the midpoint of the bounds.
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

// geoEstimateSteps picks the geohash bit depth for a search reach, Redis's
// geohashEstimateStepsByRadius: halve the Mercator span until the reach fits,
// back off two steps so the reach lands inside the 3-by-3 block, and widen one
// or two more steps toward the poles where cells crowd together. The result is
// clamped to 1..26.
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

// geoMoveX returns the geohash of the cell d cells east (d>0) or west (d<0) of
// hash at the given step, Redis's geohash_move_x: increment the longitude plane
// (the odd bits) with carry contained to that plane, wrapping at the seam.
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

// geoMoveY returns the geohash of the cell d cells north (d>0) or south (d<0) of
// hash, Redis's geohash_move_y over the latitude plane (the even bits).
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

// geoAlign52 left-justifies a step-depth geohash into the 52-bit score space, so
// a cell at depth s covers the score band [align52(bits), align52(bits+1)).
func geoAlign52(bits uint64, step uint) uint64 {
	return bits << (52 - step*2)
}

// geoCell is one of the nine covering cells: its step-depth geohash and whether
// it survived the useless-cell exclusion (the center always does).
type geoCell struct {
	bits uint64
	used bool
}

// geoCovering computes the search's covering cells: the center's geohash at the
// estimated step plus its eight neighbors, with the step decreased once if the
// 3-by-3 block does not reach the shape and the neighbors that fall entirely
// outside the shape's bounding box zeroed. It is a port of Redis's
// geohashCalculateAreasByShapeWGS84, and returns the final step and the nine
// cells.
func geoCovering(sh geoShape) (uint, [9]geoCell) {
	// The bounding box half-extents in meters and the step-estimation reach.
	var hMeters, wMeters, reach float64
	if sh.box {
		hMeters = sh.heightM / 2
		wMeters = sh.widthM / 2
		reach = math.Sqrt(wMeters*wMeters + hMeters*hMeters)
	} else {
		hMeters = sh.radiusM
		wMeters = sh.radiusM
		reach = sh.radiusM
	}
	minLon, maxLon, minLat, maxLat := geoBoundingBox(sh.lon, sh.lat, wMeters, hMeters)

	step := geoEstimateSteps(reach, sh.lat)
	hash := geoEncodeStep(sh.lon, sh.lat, step)

	// If the 3-by-3 block does not reach the shape in some direction, one coarser
	// step is needed. Redis probes the four edge neighbors against the reach.
	if step > 1 && geoNeedsCoarserStep(sh.lon, sh.lat, reach, hash, step) {
		step--
		hash = geoEncodeStep(sh.lon, sh.lat, step)
	}

	cells := geoNineCells(hash, step)

	// Exclude neighbors whose whole cell falls outside the shape's bounding box.
	// Only meaningful once cells are small enough (step >= 2), matching Redis.
	if step >= 2 {
		lonMin, lonMax, latMin, latMax := geoDecodeArea(hash, step)
		if latMin < minLat {
			cells[idxSouth].used = false
			cells[idxSouthWest].used = false
			cells[idxSouthEast].used = false
		}
		if latMax > maxLat {
			cells[idxNorth].used = false
			cells[idxNorthWest].used = false
			cells[idxNorthEast].used = false
		}
		if lonMin < minLon {
			cells[idxWest].used = false
			cells[idxNorthWest].used = false
			cells[idxSouthWest].used = false
		}
		if lonMax > maxLon {
			cells[idxEast].used = false
			cells[idxNorthEast].used = false
			cells[idxSouthEast].used = false
		}
	}
	return step, cells
}

// The fixed cell order geoNineCells fills and geoCovering excludes by.
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

// geoNineCells fills the center plus the eight neighbors at the given step.
func geoNineCells(hash uint64, step uint) [9]geoCell {
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
	return c
}

// geoNeedsCoarserStep reports whether the search reach spills past the center
// cell's edge neighbors, Redis's decrease_step probe: measure the distance from
// the center to each of the four edge neighbors' far edges, and if any is inside
// the reach the block is too tight and the step must drop.
func geoNeedsCoarserStep(lon, lat, reach float64, hash uint64, step uint) bool {
	_, _, nLatMin, nLatMax := geoDecodeArea(geoMoveY(hash, step, 1), step)
	_, _, sLatMin, _ := geoDecodeArea(geoMoveY(hash, step, -1), step)
	_, eLonMax, _, _ := geoDecodeArea(geoMoveX(hash, step, 1), step)
	wLonMin, _, _, _ := geoDecodeArea(geoMoveX(hash, step, -1), step)
	_ = nLatMin
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

// geoBoundingBox returns the lon/lat bounds of the shape, Redis's
// geohashBoundingBox: the latitude delta is the meridian arc of the half-height,
// and the longitude delta widens with the cosine of the latitude nearer the
// equator (the wider of the top and bottom edges), so the box conservatively
// contains the circle or rectangle.
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

func degRad(d float64) float64 { return d * math.Pi / 180 }
func radDeg(r float64) float64 { return r * 180 / math.Pi }

// geoScoreRange is a covering cell's score band, half-open [min, maxExcl).
type geoScoreRange struct {
	min, maxExcl uint64
}

// geoMergeRanges turns the used cells into sorted, merged score ranges. Cells at
// one step are disjoint, so this only collapses the identical ranges a huge
// radius can produce (adjacent neighbors coinciding) and fuses abutting bands so
// nine cells become the five to seven distinct ranges the walk descends.
func geoMergeRanges(step uint, cells [9]geoCell) []geoScoreRange {
	ranges := make([]geoScoreRange, 0, 9)
	for _, c := range cells {
		if !c.used {
			continue
		}
		ranges = append(ranges, geoScoreRange{geoAlign52(c.bits, step), geoAlign52(c.bits+1, step)})
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

// geoHit is one survivor: the member (copied, since the reply outlives the walk's
// aliases once sorting reorders them), its decoded coordinate, its distance from
// the center in meters, and its raw geohash score for WITHHASH.
type geoHit struct {
	member []byte
	lon    float64
	lat    float64
	distM  float64
	score  uint64
}

// geoInShape reports whether a candidate at (lon, lat) is inside the shape and
// returns its distance from the center in meters. The circle is a haversine
// compare; the box checks the longitude-independent latitude distance and the
// same-latitude longitude distance against the half-extents, the
// longitude-shrinks-with-latitude correction Redis applies.
func geoInShape(sh geoShape, lon, lat float64) (float64, bool) {
	if !sh.box {
		d := geoDistM(sh.lon, sh.lat, lon, lat)
		return d, d <= sh.radiusM
	}
	latDist := geoEarthRadiusM * math.Abs((lat-sh.lat)*math.Pi/180)
	if latDist > sh.heightM/2 {
		return 0, false
	}
	lonDist := geoDistM(sh.lon, lat, lon, lat)
	if lonDist > sh.widthM/2 {
		return 0, false
	}
	return geoDistM(sh.lon, sh.lat, lon, lat), true
}

// geoSearchHits runs the section 11 engine on z and returns the survivors,
// unsorted. It walks each merged covering range on the counted tree and
// exact-filters every candidate, so the candidate count is the 3-by-3 block's
// occupancy and the survivor count is the shape's true contents.
func geoSearchHits(z *zset, sh geoShape) []geoHit {
	step, cells := geoCovering(sh)
	ranges := geoMergeRanges(step, cells)
	var hits []geoHit
	for _, rg := range ranges {
		z.forEachInScoreRange(float64(rg.min), float64(rg.maxExcl), func(m []byte, s float64) {
			score := uint64(s)
			lon, lat := geoDecode(score)
			if d, ok := geoInShape(sh, lon, lat); ok {
				hits = append(hits, geoHit{member: append([]byte(nil), m...), lon: lon, lat: lat, distM: d, score: score})
			}
		})
	}
	return hits
}

// geoSearchOpts is the parsed GEOSEARCH tail beyond the center and shape. The
// WITH flags belong to the reading GEOSEARCH; storeDist belongs to the writing
// GEOSEARCHSTORE, which the parser keeps mutually exclusive.
type geoSearchOpts struct {
	sortAsc   bool
	sortDesc  bool
	count     int
	any       bool
	withCord  bool
	withDist  bool
	withHash  bool
	storeDist bool
}

// Geosearch answers GEOSEARCH key <FROMMEMBER m | FROMLONLAT lon lat> <BYRADIUS r
// unit | BYBOX w h unit> [ASC|DESC] [COUNT n [ANY]] [WITHCOORD] [WITHDIST]
// [WITHHASH]: resolve the center and shape, run the covering-range engine, sort
// and cut per the options, and build the annotated reply. A missing key is an
// empty array, never an error.
func Geosearch(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	g := registry(cx)
	z, wrong := g.lookup(cx, args[0])
	if wrong {
		r.Err(wrongType)
		return
	}
	sh, opts, errMsg := parseGeoSearch(z, args[1:], false)
	if errMsg != "" {
		r.Err(errMsg)
		return
	}
	var hits []geoHit
	if z != nil {
		hits = geoSearchHits(z, sh)
	}
	geoReply(cx, r, hits, opts, sh.toMeters)
}

// parseGeoSearch parses a GEOSEARCH or GEOSEARCHSTORE tail (the arguments after
// the key, or after the destination and source) against the geo set z (needed to
// resolve FROMMEMBER) and returns the resolved shape, the options, and a
// non-empty Redis error on a malformed request. It requires exactly one center
// source and exactly one shape. When store is set the WITH flags are rejected and
// STOREDIST is accepted; when it is clear the reverse holds, so the reading and
// writing commands cannot borrow each other's annotations.
func parseGeoSearch(z *zset, tail [][]byte, store bool) (geoShape, geoSearchOpts, string) {
	var (
		sh          geoShape
		opts        geoSearchOpts
		haveCenter  bool
		haveShape   bool
		fromMember  []byte
		fromLonLat  bool
		haveCountTk bool
	)
	i := 0
	for i < len(tail) {
		switch {
		case eqFold(tail[i], "FROMMEMBER"):
			if i+1 >= len(tail) {
				return sh, opts, "ERR syntax error"
			}
			fromMember = tail[i+1]
			haveCenter = true
			i += 2
		case eqFold(tail[i], "FROMLONLAT"):
			if i+2 >= len(tail) {
				return sh, opts, "ERR syntax error"
			}
			lon, ok1 := parseScore(tail[i+1])
			lat, ok2 := parseScore(tail[i+2])
			if !ok1 || !ok2 {
				return sh, opts, "ERR value is not a valid float"
			}
			sh.lon, sh.lat = lon, lat
			fromLonLat = true
			haveCenter = true
			i += 3
		case eqFold(tail[i], "BYRADIUS"):
			if i+2 >= len(tail) {
				return sh, opts, "ERR syntax error"
			}
			radius, ok := parseScore(tail[i+1])
			if !ok {
				return sh, opts, "ERR value is not a valid float"
			}
			unit, ok := geoUnit(tail[i+2])
			if !ok {
				return sh, opts, errGeoUnit
			}
			sh.box = false
			sh.radiusM = radius * unit
			sh.toMeters = unit
			haveShape = true
			i += 3
		case eqFold(tail[i], "BYBOX"):
			if i+3 >= len(tail) {
				return sh, opts, "ERR syntax error"
			}
			w, ok1 := parseScore(tail[i+1])
			h, ok2 := parseScore(tail[i+2])
			if !ok1 || !ok2 {
				return sh, opts, "ERR value is not a valid float"
			}
			unit, ok := geoUnit(tail[i+3])
			if !ok {
				return sh, opts, errGeoUnit
			}
			sh.box = true
			sh.widthM = w * unit
			sh.heightM = h * unit
			sh.toMeters = unit
			haveShape = true
			i += 4
		case eqFold(tail[i], "ASC"):
			opts.sortAsc = true
			i++
		case eqFold(tail[i], "DESC"):
			opts.sortDesc = true
			i++
		case eqFold(tail[i], "COUNT"):
			if i+1 >= len(tail) {
				return sh, opts, "ERR syntax error"
			}
			n, ok := parseIndex(tail[i+1])
			if !ok || n <= 0 {
				return sh, opts, "ERR COUNT must be > 0"
			}
			opts.count = n
			haveCountTk = true
			i += 2
			if i < len(tail) && eqFold(tail[i], "ANY") {
				opts.any = true
				i++
			}
		case eqFold(tail[i], "WITHCOORD"):
			if store {
				return sh, opts, "ERR syntax error"
			}
			opts.withCord = true
			i++
		case eqFold(tail[i], "WITHDIST"):
			if store {
				return sh, opts, "ERR syntax error"
			}
			opts.withDist = true
			i++
		case eqFold(tail[i], "WITHHASH"):
			if store {
				return sh, opts, "ERR syntax error"
			}
			opts.withHash = true
			i++
		case eqFold(tail[i], "STOREDIST"):
			if !store {
				return sh, opts, "ERR syntax error"
			}
			opts.storeDist = true
			i++
		default:
			return sh, opts, "ERR syntax error"
		}
	}
	if opts.any && !haveCountTk {
		return sh, opts, "ERR syntax error"
	}
	if opts.sortAsc && opts.sortDesc {
		return sh, opts, "ERR syntax error"
	}
	cmd := "GEOSEARCH"
	if store {
		cmd = "GEOSEARCHSTORE"
	}
	if !haveCenter {
		return sh, opts, "ERR exactly one of FROMMEMBER or FROMLONLAT can be specified for " + cmd
	}
	if !haveShape {
		return sh, opts, "ERR exactly one of BYRADIUS and BYBOX can be specified for " + cmd
	}
	if fromMember != nil {
		if z == nil {
			return sh, opts, errGeoNoMember
		}
		s, ok := z.score(fromMember)
		if !ok {
			return sh, opts, errGeoNoMember
		}
		sh.lon, sh.lat = geoDecode(uint64(s))
	} else if fromLonLat {
		if sh.lon < geoLonMin || sh.lon > geoLonMax || sh.lat < geoLatMin || sh.lat > geoLatMax {
			var buf [64]byte
			msg := append(buf[:0], "ERR invalid longitude,latitude pair "...)
			msg = strconv.AppendFloat(msg, sh.lon, 'f', 6, 64)
			msg = append(msg, ',')
			msg = strconv.AppendFloat(msg, sh.lat, 'f', 6, 64)
			return sh, opts, string(msg)
		}
	}
	return sh, opts, ""
}

const (
	errGeoUnit     = "ERR unsupported unit provided. please use M, KM, FT, MI"
	errGeoNoMember = "ERR could not decode requested zset member"
)

// geoOrderCut sorts the survivors by distance and cuts to COUNT, the ordering
// step GEOSEARCH and GEOSEARCHSTORE share. The set sorts only when ASC or DESC is
// asked, except that a COUNT without ANY sorts ascending so the nearest n are the
// deterministic ones kept; ANY takes the first n the walk found in cell order.
func geoOrderCut(hits []geoHit, opts geoSearchOpts) []geoHit {
	if opts.sortAsc || opts.sortDesc || (opts.count > 0 && !opts.any) {
		desc := opts.sortDesc
		sort.SliceStable(hits, func(i, j int) bool {
			if desc {
				return hits[i].distM > hits[j].distM
			}
			return hits[i].distM < hits[j].distM
		})
	}
	if opts.count > 0 && len(hits) > opts.count {
		hits = hits[:opts.count]
	}
	return hits
}

// geoReply sorts, cuts, and emits the survivor set. GEOSEARCH sorts only when
// ASC or DESC is asked, except that a COUNT without ANY sorts ascending so the
// nearest n are returned deterministically. With no WITH option each element is
// a member bulk string; with any, each is an array of the member then, in Redis
// order, the distance, the raw hash, and the [lon, lat] pair.
func geoReply(cx *shard.Ctx, r shard.Reply, hits []geoHit, opts geoSearchOpts, toMeters float64) {
	hits = geoOrderCut(hits, opts)

	withAny := opts.withCord || opts.withDist || opts.withHash
	out := resp.AppendArrayHeader(cx.Aux[:0], len(hits))
	var nb [40]byte
	for _, h := range hits {
		if !withAny {
			out = resp.AppendBulk(out, h.member)
			continue
		}
		n := 1
		if opts.withDist {
			n++
		}
		if opts.withHash {
			n++
		}
		if opts.withCord {
			n++
		}
		out = resp.AppendArrayHeader(out, n)
		out = resp.AppendBulk(out, h.member)
		if opts.withDist {
			out = resp.AppendBulk(out, strconv.AppendFloat(nb[:0], h.distM/toMeters, 'f', 4, 64))
		}
		if opts.withHash {
			out = resp.AppendInt(out, int64(h.score))
		}
		if opts.withCord {
			out = resp.AppendArrayHeader(out, 2)
			out = resp.AppendBulk(out, strconv.AppendFloat(nb[:0], h.lon, 'g', 17, 64))
			out = resp.AppendBulk(out, strconv.AppendFloat(nb[:0], h.lat, 'g', 17, 64))
		}
	}
	cx.Aux = out
	r.Raw(out)
}
