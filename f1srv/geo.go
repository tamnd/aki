package f1srv

import (
	"encoding/binary"
	"fmt"
	"math"
	"sort"
	"strconv"
)

// Geo commands are the last M9 slice: GEOADD/GEOPOS/GEODIST/GEOHASH and the radius/search family
// (spec 2064/f1_rewrite_ltm/07). A geoset is a sorted set whose score is a 52-bit interleaved
// geohash of a longitude/latitude, so every geo command is a thin layer over the zset model already
// built on f1raw: GEOADD is a ZADD with a computed integer score, GEOPOS/GEODIST/GEOHASH read one
// member's score, and the radius searches are bounded score-range scans over the score family.
//
// The whole geohash core (interleave/deinterleave, encode/decode, neighbor boxes, haversine) is a
// faithful port of Redis 8.8.0's geohash.c + geohash_helper.c + geo.c, so the wire output is
// byte-identical to Redis 8.8 (GEODIST/WITHDIST as %.4f, GEOHASH as 11-char base32, WITHHASH as the
// int64 score, ordering by distance). Coordinates (GEOPOS/WITHCOORD) go through formatScore, the
// d2string port addReplyDouble uses on Redis 8.8, which yields the shortest round-trip form; Valkey
// 9.1 prints coordinates with a legacy %.17Lf and so differs on those bytes alone, a Valkey/Redis
// wire difference, not an aki divergence.
//
// The radius search is the one place the LTM property matters: rather than scanning the whole
// geoset, geohashCalculateAreasByShapeWGS84 computes the nine geohash boxes (center + 8 neighbors)
// that cover the search area, and each box maps to a half-open score interval [min, max). Each
// interval is two order-statistic rank lookups on the score family plus a bounded window read (the
// same zscoreRankBoundary + collectWindow the ZRANGEBYSCORE path uses), so a radius query over a
// billion-member geoset touches only the candidate cells, never the collection.

// Geohash limits and Earth constants, from Redis geohash.h / geohash_helper.c. The latitude range
// is the EPSG:900913 mercator clamp (-85.05..85.05), narrower than the -90..90 a standard geohash
// string uses, which is why GEOHASH re-encodes against -90..90 before emitting.
const (
	geoStepMax = 26 // 26*2 = 52 score bits
	geoLatMin  = -85.05112878
	geoLatMax  = 85.05112878
	geoLongMin = -180.0
	geoLongMax = 180.0
)

const (
	geoDR               = math.Pi / 180.0
	earthRadiusInMeters = 6372797.560856
	geoMercatorMax      = 20037726.37
)

// Search shape types and the georadius option flags, mirroring geo.c.
const (
	geoCircular  = 1
	geoRectangle = 2
)

const (
	geoSortNone = 0
	geoSortAsc  = 1
	geoSortDesc = 2
)

const (
	radiusCoords   = 1 << 0 // GEORADIUS: search around coordinates
	radiusMember   = 1 << 1 // GEORADIUSBYMEMBER: search around a member's position
	radiusNoStore  = 1 << 2 // the _RO variants reject STORE/STOREDIST
	geoSearch      = 1 << 3 // GEOSEARCH / GEOSEARCHSTORE argument grammar
	geoSearchStore = 1 << 4 // GEOSEARCHSTORE accepts only STOREDIST
)

// geoHashBits is a geohash value: the interleaved bits and how many step levels (step*2 bits) are
// significant. A zero value (bits==0 && step==0) is the "no such box" sentinel neighbor pruning
// produces.
type geoHashBits struct {
	bits uint64
	step uint8
}

// geoHashArea is a decoded geohash cell: the longitude/latitude bounds of the box the bits name.
type geoHashArea struct {
	bits           geoHashBits
	lonMin, lonMax float64
	latMin, latMax float64
}

// geoHashNeighbors holds the eight geohash boxes adjacent to a center box.
type geoHashNeighbors struct {
	north, east, west, south                   geoHashBits
	northEast, southEast, northWest, southWest geoHashBits
}

// geoHashRadius is the output of the area calculation: the center box, its neighbors, and the center
// box's decoded area (used to prune neighbors that fall outside the bounding box).
type geoHashRadius struct {
	hash      geoHashBits
	area      geoHashArea
	neighbors geoHashNeighbors
}

// geoShape is the parsed search area: a circle (radius) or a rectangle (width/height), its center,
// the unit-to-meters conversion factor, and the axis-aligned bounding box the calculation fills in.
type geoShape struct {
	typ        int
	xy         [2]float64 // xy[0] lon, xy[1] lat
	conversion float64
	bounds     [4]float64 // minLon, minLat, maxLon, maxLat
	radius     float64
	width      float64
	height     float64
}

// geoPoint is one candidate found in a box scan: its decoded position, distance from the search
// center (in meters, divided by conversion only at reply time), the raw 52-bit score, and a private
// copy of the member bytes (copied off the arena so it survives later box scans and a store clear).
type geoPoint struct {
	longitude, latitude float64
	dist                float64
	score               float64
	member              []byte
}

// --- geohash core: interleave, encode/decode (ports of geohash.c) ---

// interleave64 spreads the low bits of x into even positions and y into odd positions, the Morton
// code the geohash uses. x and y must be below 2^32.
func interleave64(xlo, ylo uint32) uint64 {
	b := [...]uint64{0x5555555555555555, 0x3333333333333333, 0x0F0F0F0F0F0F0F0F, 0x00FF00FF00FF00FF, 0x0000FFFF0000FFFF}
	s := [...]uint{1, 2, 4, 8, 16}
	x := uint64(xlo)
	y := uint64(ylo)
	x = (x | (x << s[4])) & b[4]
	y = (y | (y << s[4])) & b[4]
	x = (x | (x << s[3])) & b[3]
	y = (y | (y << s[3])) & b[3]
	x = (x | (x << s[2])) & b[2]
	y = (y | (y << s[2])) & b[2]
	x = (x | (x << s[1])) & b[1]
	y = (y | (y << s[1])) & b[1]
	x = (x | (x << s[0])) & b[0]
	y = (y | (y << s[0])) & b[0]
	return x | (y << 1)
}

// deinterleave64 reverses interleave64, returning lat in the low 32 bits and lon in the high 32.
func deinterleave64(interleaved uint64) uint64 {
	b := [...]uint64{0x5555555555555555, 0x3333333333333333, 0x0F0F0F0F0F0F0F0F, 0x00FF00FF00FF00FF, 0x0000FFFF0000FFFF, 0x00000000FFFFFFFF}
	s := [...]uint{0, 1, 2, 4, 8, 16}
	x := interleaved
	y := interleaved >> 1
	x = (x | (x >> s[0])) & b[0]
	y = (y | (y >> s[0])) & b[0]
	x = (x | (x >> s[1])) & b[1]
	y = (y | (y >> s[1])) & b[1]
	x = (x | (x >> s[2])) & b[2]
	y = (y | (y >> s[2])) & b[2]
	x = (x | (x >> s[3])) & b[3]
	y = (y | (y >> s[3])) & b[3]
	x = (x | (x >> s[4])) & b[4]
	y = (y | (y >> s[4])) & b[4]
	x = (x | (x >> s[5])) & b[5]
	y = (y | (y >> s[5])) & b[5]
	return x | (y << 32)
}

// geohashEncode encodes longitude/latitude into step*2 interleaved bits over the given ranges. It
// returns ok=false for the same rejects Redis's geohashEncode has (bad step, or a point outside the
// mercator limits). The first bound check uses the fixed geo limits; the second uses the passed
// range, so the GEOHASH re-encode against -90..90 works while a WGS84 encode uses -85..85.
func geohashEncode(lonMin, lonMax, latMin, latMax, longitude, latitude float64, step uint8) (geoHashBits, bool) {
	if step > 32 || step == 0 {
		return geoHashBits{}, false
	}
	if longitude > geoLongMax || longitude < geoLongMin || latitude > geoLatMax || latitude < geoLatMin {
		return geoHashBits{}, false
	}
	if latitude < latMin || latitude > latMax || longitude < lonMin || longitude > lonMax {
		return geoHashBits{}, false
	}
	latOffset := (latitude - latMin) / (latMax - latMin)
	longOffset := (longitude - lonMin) / (lonMax - lonMin)
	latOffset *= float64(uint64(1) << step)
	longOffset *= float64(uint64(1) << step)
	return geoHashBits{bits: interleave64(uint32(latOffset), uint32(longOffset)), step: step}, true
}

// geohashEncodeWGS84 encodes over the WGS84 mercator ranges, the encoding GEOADD stores.
func geohashEncodeWGS84(longitude, latitude float64, step uint8) (geoHashBits, bool) {
	return geohashEncode(geoLongMin, geoLongMax, geoLatMin, geoLatMax, longitude, latitude, step)
}

// geohashDecode expands a geohash back to the cell bounds over the given ranges.
func geohashDecode(lonMin, lonMax, latMin, latMax float64, hash geoHashBits) (geoHashArea, bool) {
	if hash.bits == 0 && hash.step == 0 {
		return geoHashArea{}, false
	}
	step := hash.step
	hashSep := deinterleave64(hash.bits)
	latScale := latMax - latMin
	longScale := lonMax - lonMin
	ilato := uint32(hashSep)
	ilono := uint32(hashSep >> 32)
	var area geoHashArea
	area.bits = hash
	area.latMin = latMin + (float64(ilato)/float64(uint64(1)<<step))*latScale
	area.latMax = latMin + (float64(ilato+1)/float64(uint64(1)<<step))*latScale
	area.lonMin = lonMin + (float64(ilono)/float64(uint64(1)<<step))*longScale
	area.lonMax = lonMin + (float64(ilono+1)/float64(uint64(1)<<step))*longScale
	return area, true
}

// geohashDecodeWGS84 decodes over the WGS84 ranges.
func geohashDecodeWGS84(hash geoHashBits) (geoHashArea, bool) {
	return geohashDecode(geoLongMin, geoLongMax, geoLatMin, geoLatMax, hash)
}

// geohashDecodeAreaToLongLat returns the center of a decoded cell, clamped to the mercator limits.
func geohashDecodeAreaToLongLat(area *geoHashArea) [2]float64 {
	var xy [2]float64
	xy[0] = (area.lonMin + area.lonMax) / 2
	if xy[0] > geoLongMax {
		xy[0] = geoLongMax
	}
	if xy[0] < geoLongMin {
		xy[0] = geoLongMin
	}
	xy[1] = (area.latMin + area.latMax) / 2
	if xy[1] > geoLatMax {
		xy[1] = geoLatMax
	}
	if xy[1] < geoLatMin {
		xy[1] = geoLatMin
	}
	return xy
}

// decodeGeohash turns a stored 52-bit score into a longitude/latitude, the inverse of what GEOADD
// stored. It is geo.c's decodeGeohash: treat the score as a full-precision (step 26) geohash.
func decodeGeohash(score float64) ([2]float64, bool) {
	hash := geoHashBits{bits: uint64(score), step: geoStepMax}
	area, ok := geohashDecodeWGS84(hash)
	if !ok {
		return [2]float64{}, false
	}
	return geohashDecodeAreaToLongLat(&area), true
}

// geohashAlign52Bits left-shifts a step-precision geohash up to the 52-bit score space.
func geohashAlign52Bits(hash geoHashBits) uint64 {
	return hash.bits << (52 - uint(hash.step)*2)
}

// --- neighbor boxes (ports of geohash.c geohash_move_x/y and geohashNeighbors) ---

func geohashMoveX(hash *geoHashBits, d int8) {
	if d == 0 {
		return
	}
	x := hash.bits & 0xaaaaaaaaaaaaaaaa
	y := hash.bits & 0x5555555555555555
	zz := uint64(0x5555555555555555) >> (64 - uint(hash.step)*2)
	if d > 0 {
		x = x + (zz + 1)
	} else {
		x = x | zz
		x = x - (zz + 1)
	}
	x &= uint64(0xaaaaaaaaaaaaaaaa) >> (64 - uint(hash.step)*2)
	hash.bits = x | y
}

func geohashMoveY(hash *geoHashBits, d int8) {
	if d == 0 {
		return
	}
	x := hash.bits & 0xaaaaaaaaaaaaaaaa
	y := hash.bits & 0x5555555555555555
	zz := uint64(0xaaaaaaaaaaaaaaaa) >> (64 - uint(hash.step)*2)
	if d > 0 {
		y = y + (zz + 1)
	} else {
		y = y | zz
		y = y - (zz + 1)
	}
	y &= uint64(0x5555555555555555) >> (64 - uint(hash.step)*2)
	hash.bits = x | y
}

func geohashNeighbors(hash geoHashBits) geoHashNeighbors {
	var n geoHashNeighbors
	n.east = hash
	geohashMoveX(&n.east, 1)
	geohashMoveY(&n.east, 0)

	n.west = hash
	geohashMoveX(&n.west, -1)
	geohashMoveY(&n.west, 0)

	n.south = hash
	geohashMoveX(&n.south, 0)
	geohashMoveY(&n.south, -1)

	n.north = hash
	geohashMoveX(&n.north, 0)
	geohashMoveY(&n.north, 1)

	n.northWest = hash
	geohashMoveX(&n.northWest, -1)
	geohashMoveY(&n.northWest, 1)

	n.northEast = hash
	geohashMoveX(&n.northEast, 1)
	geohashMoveY(&n.northEast, 1)

	n.southEast = hash
	geohashMoveX(&n.southEast, 1)
	geohashMoveY(&n.southEast, -1)

	n.southWest = hash
	geohashMoveX(&n.southWest, -1)
	geohashMoveY(&n.southWest, -1)
	return n
}

func gzero(h *geoHashBits) {
	h.bits = 0
	h.step = 0
}

// --- area calculation (port of geohash_helper.c) ---

func degRad(ang float64) float64 { return ang * geoDR }
func radDeg(ang float64) float64 { return ang / geoDR }

// geohashEstimateStepsByRadius picks the geohash precision whose cells are big enough to cover the
// radius with the nine-box scan, widening (fewer steps) near the poles.
func geohashEstimateStepsByRadius(rangeMeters, lat float64) uint8 {
	if rangeMeters == 0 {
		return 26
	}
	step := 1
	for rangeMeters < geoMercatorMax {
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
	return uint8(step)
}

// geohashBoundingBox returns the axis-aligned lon/lat box that contains the search shape. The box is
// wider at the equator-facing edge because meridians converge toward the poles.
func geohashBoundingBox(shape *geoShape) [4]float64 {
	longitude := shape.xy[0]
	latitude := shape.xy[1]
	var height, width float64
	if shape.typ == geoCircular {
		height = shape.conversion * shape.radius
		width = shape.conversion * shape.radius
	} else {
		height = shape.conversion * (shape.height / 2)
		width = shape.conversion * (shape.width / 2)
	}
	latDelta := radDeg(height / earthRadiusInMeters)
	longDeltaTop := radDeg(width / earthRadiusInMeters / math.Cos(degRad(latitude+latDelta)))
	longDeltaBottom := radDeg(width / earthRadiusInMeters / math.Cos(degRad(latitude-latDelta)))
	southern := latitude < 0
	var bounds [4]float64
	if southern {
		bounds[0] = longitude - longDeltaBottom
		bounds[2] = longitude + longDeltaBottom
	} else {
		bounds[0] = longitude - longDeltaTop
		bounds[2] = longitude + longDeltaTop
	}
	bounds[1] = latitude - latDelta
	bounds[3] = latitude + latDelta
	return bounds
}

// geohashCalculateAreasByShapeWGS84 computes the center box and its eight neighbors that cover the
// search shape, decreasing the step if the estimate is one level too coarse near an edge, and
// zeroing neighbor boxes that lie wholly outside the bounding box so the scan skips them.
func geohashCalculateAreasByShapeWGS84(shape *geoShape) geoHashRadius {
	shape.bounds = geohashBoundingBox(shape)
	minLon := shape.bounds[0]
	minLat := shape.bounds[1]
	maxLon := shape.bounds[2]
	maxLat := shape.bounds[3]

	longitude := shape.xy[0]
	latitude := shape.xy[1]
	var radiusMeters float64
	if shape.typ == geoCircular {
		radiusMeters = shape.radius
	} else {
		radiusMeters = math.Sqrt((shape.width/2)*(shape.width/2) + (shape.height/2)*(shape.height/2))
	}
	radiusMeters *= shape.conversion

	steps := geohashEstimateStepsByRadius(radiusMeters, latitude)
	hash, _ := geohashEncodeWGS84(longitude, latitude, steps)
	neighbors := geohashNeighbors(hash)
	area, _ := geohashDecodeWGS84(hash)

	decreaseStep := false
	{
		north, _ := geohashDecodeWGS84(neighbors.north)
		south, _ := geohashDecodeWGS84(neighbors.south)
		east, _ := geohashDecodeWGS84(neighbors.east)
		west, _ := geohashDecodeWGS84(neighbors.west)
		if north.latMax < maxLat {
			decreaseStep = true
		}
		if south.latMin > minLat {
			decreaseStep = true
		}
		if east.lonMax < maxLon {
			decreaseStep = true
		}
		if west.lonMin > minLon {
			decreaseStep = true
		}
	}

	if steps > 1 && decreaseStep {
		steps--
		hash, _ = geohashEncodeWGS84(longitude, latitude, steps)
		neighbors = geohashNeighbors(hash)
		area, _ = geohashDecodeWGS84(hash)
	}

	if steps >= 2 {
		if area.latMin < minLat {
			gzero(&neighbors.south)
			gzero(&neighbors.southWest)
			gzero(&neighbors.southEast)
		}
		if area.latMax > maxLat {
			gzero(&neighbors.north)
			gzero(&neighbors.northEast)
			gzero(&neighbors.northWest)
		}
		if area.lonMin < minLon {
			gzero(&neighbors.west)
			gzero(&neighbors.southWest)
			gzero(&neighbors.northWest)
		}
		if area.lonMax > maxLon {
			gzero(&neighbors.east)
			gzero(&neighbors.southEast)
			gzero(&neighbors.northEast)
		}
	}
	return geoHashRadius{hash: hash, neighbors: neighbors, area: area}
}

// scoresOfGeoHashBox returns the half-open score interval [min, max) that contains every member
// inside the box: the box aligned to 52 bits, and the next box aligned to 52 bits (exclusive).
func scoresOfGeoHashBox(hash geoHashBits) (uint64, uint64) {
	min := geohashAlign52Bits(hash)
	hash.bits++
	max := geohashAlign52Bits(hash)
	return min, max
}

// --- distance (ports of geohash_helper.c) ---

func geohashGetLatDistance(lat1d, lat2d float64) float64 {
	return earthRadiusInMeters * math.Abs(degRad(lat2d)-degRad(lat1d))
}

// geohashGetDistance is the haversine great-circle distance in meters. It uses the asin(sqrt(a))
// form Redis uses (not atan2), and short-circuits to the latitude-only distance when the longitudes
// coincide, so the bytes match Redis's GEODIST.
func geohashGetDistance(lon1d, lat1d, lon2d, lat2d float64) float64 {
	lon1r := degRad(lon1d)
	lon2r := degRad(lon2d)
	v := math.Sin((lon2r - lon1r) / 2)
	if v == 0.0 {
		return geohashGetLatDistance(lat1d, lat2d)
	}
	lat1r := degRad(lat1d)
	lat2r := degRad(lat2d)
	u := math.Sin((lat2r - lat1r) / 2)
	a := u*u + math.Cos(lat1r)*math.Cos(lat2r)*v*v
	return 2.0 * earthRadiusInMeters * math.Asin(math.Sqrt(a))
}

func geohashGetDistanceIfInRadius(x1, y1, x2, y2, radius float64) (float64, bool) {
	d := geohashGetDistance(x1, y1, x2, y2)
	if d > radius {
		return 0, false
	}
	return d, true
}

// geohashGetDistanceIfInRectangle keeps a point when it is within height/2 in latitude and width/2
// in longitude of the center, then returns the point's true distance.
func geohashGetDistanceIfInRectangle(widthM, heightM, x1, y1, x2, y2 float64) (float64, bool) {
	latDistance := geohashGetLatDistance(y2, y1)
	if latDistance > heightM/2 {
		return 0, false
	}
	lonDistance := geohashGetDistance(x2, y2, x1, y2)
	if lonDistance > widthM/2 {
		return 0, false
	}
	d := geohashGetDistance(x1, y1, x2, y2)
	return d, true
}

// geoWithinShape decodes a member's score and reports whether the point falls inside the search
// shape, returning the decoded position and the distance from the center when it does.
func geoWithinShape(shape *geoShape, score float64) (xy [2]float64, dist float64, ok bool) {
	xy, decoded := decodeGeohash(score)
	if !decoded {
		return xy, 0, false
	}
	if shape.typ == geoCircular {
		d, in := geohashGetDistanceIfInRadius(shape.xy[0], shape.xy[1], xy[0], xy[1], shape.radius*shape.conversion)
		if !in {
			return xy, 0, false
		}
		return xy, d, true
	}
	d, in := geohashGetDistanceIfInRectangle(shape.width*shape.conversion, shape.height*shape.conversion, shape.xy[0], shape.xy[1], xy[0], xy[1])
	if !in {
		return xy, 0, false
	}
	return xy, d, true
}

// --- geoset access on the f1raw zset model ---

// geoScore reads a member's stored score off the zset member family, false when the member (or the
// geoset) is absent.
func (c *connState) geoScore(zkey, member []byte) (float64, bool) {
	mk := c.zmemberKey(zkey, member)
	v, ok := c.srv.store.GetKind(mk, c.vbuf[:0], kindZsetMember)
	c.vbuf = v
	if !ok {
		return 0, false
	}
	return math.Float64frombits(binary.LittleEndian.Uint64(v)), true
}

// unitToMeters maps a geo unit token to its meters conversion factor, matching extractUnitOrReply.
func unitToMeters(u []byte) (float64, bool) {
	switch {
	case eqFold(u, "m"):
		return 1, true
	case eqFold(u, "km"):
		return 1000, true
	case eqFold(u, "ft"):
		return 0.3048, true
	case eqFold(u, "mi"):
		return 1609.34, true
	}
	return 0, false
}

// writeGeoCoord writes a coordinate as a bulk string via formatScore, the d2string port Redis 8.8's
// GEOPOS/WITHCOORD (addReplyDouble) uses. On RESP2 addReplyDouble emits the shortest round-trip form
// through the same grisu2 routine formatScore ports, so the bytes are identical to Redis (e.g.
// 13.361389338970184 for Palermo's longitude). Valkey 9.1 formats geo coordinates differently and so
// diverges on these bytes alone, a Valkey/Redis wire difference rather than an aki divergence.
func (c *connState) writeGeoCoord(v float64) {
	c.sbuf = formatScore(c.sbuf[:0], v)
	c.writeBulk(c.sbuf)
}

// writeGeoDist writes a distance as a bulk string with four fractional digits, matching Redis's
// addReplyDoubleDistance (fixedpoint_d2string with 4 digits).
func (c *connState) writeGeoDist(d float64) {
	c.sbuf = strconv.AppendFloat(c.sbuf[:0], d, 'f', 4, 64)
	c.writeBulk(c.sbuf)
}

// geoMembersInBox scans one geohash box's score interval on the score family and appends every
// member that also passes the shape filter to ga. The interval is min-inclusive, max-exclusive, and
// the scan is bounded to the window between the two rank boundaries, so a box touches only its own
// candidate rows. Member bytes are copied off the arena so they survive later box scans and a store
// clear. A positive limit stops the scan once ga reaches it.
func (c *connState) geoMembersInBox(zkey []byte, hash geoHashBits, shape *geoShape, card int, ga *[]geoPoint, limit int) {
	if hash.bits == 0 && hash.step == 0 {
		return
	}
	minU, maxU := scoresOfGeoHashBox(hash)
	prefix := c.zscorePrefix(zkey)
	plen := len(prefix)
	startIdx := c.zscoreRankBoundary(prefix, float64(minU), false, card)
	endIdx := c.zscoreRankBoundary(prefix, float64(maxU), false, card)
	if startIdx >= endIdx {
		return
	}
	keys := c.collectWindow(prefix, startIdx, endIdx)
	for _, k := range keys {
		score := decodeSortableScore(k[plen : plen+8])
		xy, dist, ok := geoWithinShape(shape, score)
		if !ok {
			continue
		}
		member := append([]byte(nil), k[plen+8:]...)
		*ga = append(*ga, geoPoint{longitude: xy[0], latitude: xy[1], dist: dist, score: score, member: member})
		if limit > 0 && len(*ga) >= limit {
			return
		}
	}
}

// geoMembersOfAllNeighbors scans the center box and its eight neighbors in Redis's order, skipping
// zeroed boxes and a box identical to the one just processed (huge radii can make neighbors
// coincide), and returns the collected candidate points.
func (c *connState) geoMembersOfAllNeighbors(zkey []byte, r *geoHashRadius, shape *geoShape, card, limit int) []geoPoint {
	boxes := [9]geoHashBits{
		r.hash,
		r.neighbors.north,
		r.neighbors.south,
		r.neighbors.east,
		r.neighbors.west,
		r.neighbors.northEast,
		r.neighbors.northWest,
		r.neighbors.southEast,
		r.neighbors.southWest,
	}
	var ga []geoPoint
	lastProcessed := 0
	for i := 0; i < 9; i++ {
		if boxes[i].bits == 0 && boxes[i].step == 0 {
			continue
		}
		if lastProcessed != 0 && boxes[i].bits == boxes[lastProcessed].bits && boxes[i].step == boxes[lastProcessed].step {
			continue
		}
		if limit > 0 && len(ga) >= limit {
			break
		}
		c.geoMembersInBox(zkey, boxes[i], shape, card, &ga, limit)
		lastProcessed = i
	}
	return ga
}

// --- commands ---

// cmdGeoAdd implements GEOADD key [NX|XX] [CH] long lat member [long lat member ...]. It parses the
// options and coordinate triples, encodes each coordinate to its 52-bit score, and hands the work to
// ZADD by building the equivalent argument vector, so all the ZADD option semantics (NX/XX/CH and the
// reply shape) come for free.
func (c *connState) cmdGeoAdd(argv [][]byte) {
	if len(argv) < 5 {
		c.writeErr("ERR wrong number of arguments for 'geoadd' command")
		return
	}
	longidx := 2
	var nx, xx bool
scanOpts:
	for longidx < len(argv) {
		switch {
		case eqFold(argv[longidx], "nx"):
			nx = true
		case eqFold(argv[longidx], "xx"):
			xx = true
		case eqFold(argv[longidx], "ch"):
			// CH is handled by ZADD; just skip past it here.
		default:
			break scanOpts
		}
		longidx++
	}

	rem := len(argv) - longidx
	if rem%3 != 0 || (xx && nx) {
		c.writeErr("ERR syntax error")
		return
	}
	elements := rem / 3

	za := make([][]byte, 0, longidx+elements*2)
	za = append(za, []byte("zadd"))
	for i := 1; i < longidx; i++ {
		za = append(za, argv[i])
	}
	for i := 0; i < elements; i++ {
		lonB := argv[longidx+i*3]
		latB := argv[longidx+i*3+1]
		member := argv[longidx+i*3+2]
		lon, err1 := parseScore(lonB)
		lat, err2 := parseScore(latB)
		if err1 != nil || err2 != nil {
			c.writeErr("ERR value is not a valid float")
			return
		}
		if lon < geoLongMin || lon > geoLongMax || lat < geoLatMin || lat > geoLatMax {
			c.writeErr(fmt.Sprintf("ERR invalid longitude,latitude pair %f,%f", lon, lat))
			return
		}
		hash, _ := geohashEncodeWGS84(lon, lat, geoStepMax)
		bits := geohashAlign52Bits(hash)
		za = append(za, strconv.AppendUint(nil, bits, 10))
		za = append(za, member)
	}
	c.cmdZAdd(za)
}

// cmdGeoPos implements GEOPOS key member [member ...], replying with each member's decoded
// longitude/latitude (a null array for a missing member).
func (c *connState) cmdGeoPos(argv [][]byte) {
	if len(argv) < 2 {
		c.writeErr("ERR wrong number of arguments for 'geopos' command")
		return
	}
	if c.stringConflict(argv[1]) {
		c.writeErr(wrongType)
		return
	}
	c.writeArrayHeader(len(argv) - 2)
	for _, member := range argv[2:] {
		s, ok := c.geoScore(argv[1], member)
		if !ok {
			c.writeNilArray()
			continue
		}
		xy, decoded := decodeGeohash(s)
		if !decoded {
			c.writeNilArray()
			continue
		}
		c.writeArrayHeader(2)
		c.writeGeoCoord(xy[0])
		c.writeGeoCoord(xy[1])
	}
}

// cmdGeoDist implements GEODIST key member1 member2 [unit], replying with the distance between the
// two members (nil if either is missing).
func (c *connState) cmdGeoDist(argv [][]byte) {
	if len(argv) < 4 {
		c.writeErr("ERR wrong number of arguments for 'geodist' command")
		return
	}
	toMeter := 1.0
	if len(argv) == 5 {
		u, ok := unitToMeters(argv[4])
		if !ok {
			c.writeErr("ERR unsupported unit provided. please use M, KM, FT, MI")
			return
		}
		toMeter = u
	} else if len(argv) > 5 {
		c.writeErr("ERR syntax error")
		return
	}
	if c.stringConflict(argv[1]) {
		c.writeErr(wrongType)
		return
	}
	s1, ok1 := c.geoScore(argv[1], argv[2])
	s2, ok2 := c.geoScore(argv[1], argv[3])
	if !ok1 || !ok2 {
		c.writeNil()
		return
	}
	xy1, o1 := decodeGeohash(s1)
	xy2, o2 := decodeGeohash(s2)
	if !o1 || !o2 {
		c.writeNil()
		return
	}
	c.writeGeoDist(geohashGetDistance(xy1[0], xy1[1], xy2[0], xy2[1]) / toMeter)
}

// cmdGeoHash implements GEOHASH key member [member ...], replying with each member's standard
// 11-character geohash string. The stored score uses a -85..85 latitude range, so the position is
// decoded and re-encoded against the standard -90..90 range before base32 encoding.
func (c *connState) cmdGeoHash(argv [][]byte) {
	if len(argv) < 2 {
		c.writeErr("ERR wrong number of arguments for 'geohash' command")
		return
	}
	if c.stringConflict(argv[1]) {
		c.writeErr(wrongType)
		return
	}
	const geoalphabet = "0123456789bcdefghjkmnpqrstuvwxyz"
	c.writeArrayHeader(len(argv) - 2)
	for _, member := range argv[2:] {
		s, ok := c.geoScore(argv[1], member)
		if !ok {
			c.writeNil()
			continue
		}
		xy, decoded := decodeGeohash(s)
		if !decoded {
			c.writeNil()
			continue
		}
		hash, _ := geohashEncode(-180, 180, -90, 90, xy[0], xy[1], 26)
		var buf [11]byte
		for i := 0; i < 11; i++ {
			var idx int
			if i == 10 {
				// Only 52 bits are available; the 11th character is always zero.
				idx = 0
			} else {
				idx = int((hash.bits >> (52 - uint((i+1)*5))) & 0x1f)
			}
			buf[i] = geoalphabet[idx]
		}
		c.writeBulk(buf[:])
	}
}

// extractLongLat parses a longitude/latitude argument pair, writing the wire error and returning
// ok=false on a bad float or an out-of-range pair.
func (c *connState) extractLongLat(lonB, latB []byte) (lon, lat float64, ok bool) {
	l, err1 := parseScore(lonB)
	t, err2 := parseScore(latB)
	if err1 != nil || err2 != nil {
		c.writeErr("ERR value is not a valid float")
		return 0, 0, false
	}
	if l < geoLongMin || l > geoLongMax || t < geoLatMin || t > geoLatMax {
		c.writeErr(fmt.Sprintf("ERR invalid longitude,latitude pair %f,%f", l, t))
		return 0, 0, false
	}
	return l, t, true
}

// extractDistance parses a radius/unit pair into the shape's radius and conversion.
func (c *connState) extractDistance(radB, unitB []byte, shape *geoShape) bool {
	dist, err := parseScore(radB)
	if err != nil {
		c.writeErr("ERR need numeric radius")
		return false
	}
	if dist < 0 {
		c.writeErr("ERR radius cannot be negative")
		return false
	}
	toMeters, ok := unitToMeters(unitB)
	if !ok {
		c.writeErr("ERR unsupported unit provided. please use M, KM, FT, MI")
		return false
	}
	shape.radius = dist
	shape.conversion = toMeters
	return true
}

// extractBox parses a width/height/unit triple into the shape's width, height, and conversion.
func (c *connState) extractBox(wB, hB, unitB []byte, shape *geoShape) bool {
	w, err1 := parseScore(wB)
	if err1 != nil {
		c.writeErr("ERR need numeric width")
		return false
	}
	h, err2 := parseScore(hB)
	if err2 != nil {
		c.writeErr("ERR need numeric height")
		return false
	}
	if h < 0 || w < 0 {
		c.writeErr("ERR height or width cannot be negative")
		return false
	}
	toMeters, ok := unitToMeters(unitB)
	if !ok {
		c.writeErr("ERR unsupported unit provided. please use M, KM, FT, MI")
		return false
	}
	shape.width = w
	shape.height = h
	shape.conversion = toMeters
	return true
}

// cmdGeoRadius wires GEORADIUS and GEORADIUS_RO.
func (c *connState) cmdGeoRadius(argv [][]byte, ro bool) {
	if len(argv) < 6 {
		name := "georadius"
		if ro {
			name = "georadius_ro"
		}
		c.writeErr("ERR wrong number of arguments for '" + name + "' command")
		return
	}
	flags := radiusCoords
	if ro {
		flags |= radiusNoStore
	}
	c.georadiusGeneric(argv, 1, flags)
}

// cmdGeoRadiusByMember wires GEORADIUSBYMEMBER and GEORADIUSBYMEMBER_RO.
func (c *connState) cmdGeoRadiusByMember(argv [][]byte, ro bool) {
	if len(argv) < 5 {
		name := "georadiusbymember"
		if ro {
			name = "georadiusbymember_ro"
		}
		c.writeErr("ERR wrong number of arguments for '" + name + "' command")
		return
	}
	flags := radiusMember
	if ro {
		flags |= radiusNoStore
	}
	c.georadiusGeneric(argv, 1, flags)
}

// cmdGeoSearch wires GEOSEARCH.
func (c *connState) cmdGeoSearch(argv [][]byte) {
	if len(argv) < 7 {
		c.writeErr("ERR wrong number of arguments for 'geosearch' command")
		return
	}
	c.georadiusGeneric(argv, 1, geoSearch)
}

// cmdGeoSearchStore wires GEOSEARCHSTORE.
func (c *connState) cmdGeoSearchStore(argv [][]byte) {
	if len(argv) < 8 {
		c.writeErr("ERR wrong number of arguments for 'geosearchstore' command")
		return
	}
	c.georadiusGeneric(argv, 2, geoSearch|geoSearchStore)
}

// georadiusGeneric is the shared body of GEORADIUS, GEORADIUSBYMEMBER, GEOSEARCH and GEOSEARCHSTORE
// (and their _RO variants), a faithful port of geo.c's georadiusGeneric. It resolves the search
// center and shape, parses the option tail, computes the nine covering boxes, scans them bounded on
// the score family, sorts by distance when asked, and either replies with the results (optionally
// WITHDIST/WITHHASH/WITHCOORD) or stores them in a destination geoset.
func (c *connState) georadiusGeneric(argv [][]byte, srcKeyIndex, flags int) {
	src := argv[srcKeyIndex]
	if c.stringConflict(src) {
		c.writeErr(wrongType)
		return
	}
	card := int(c.zsetCard(src))
	exists := card > 0

	var shape geoShape
	var storekey []byte
	storedist := false
	var baseArgs int

	switch {
	case flags&radiusCoords != 0:
		baseArgs = 6
		shape.typ = geoCircular
		lon, lat, ok := c.extractLongLat(argv[2], argv[3])
		if !ok {
			return
		}
		shape.xy[0], shape.xy[1] = lon, lat
		if !c.extractDistance(argv[baseArgs-2], argv[baseArgs-1], &shape) {
			return
		}
	case flags&radiusMember != 0 && !exists:
		// No source key: still parse arguments so we pick the right reply for the STORE flag.
		baseArgs = 5
	case flags&radiusMember != 0:
		baseArgs = 5
		s, ok := c.geoScore(src, argv[2])
		if !ok {
			c.writeErr("ERR could not decode requested zset member")
			return
		}
		xy, decoded := decodeGeohash(s)
		if !decoded {
			c.writeErr("ERR could not decode requested zset member")
			return
		}
		shape.typ = geoCircular
		shape.xy[0], shape.xy[1] = xy[0], xy[1]
		if !c.extractDistance(argv[baseArgs-2], argv[baseArgs-1], &shape) {
			return
		}
	case flags&geoSearch != 0:
		baseArgs = 2
		if flags&geoSearchStore != 0 {
			baseArgs = 3
			storekey = argv[1]
		}
	default:
		c.writeErr("ERR Unknown georadius search type")
		return
	}

	withdist, withhash, withcoords := false, false, false
	frommember, fromloc, byradius, bybox := false, false, false, false
	sortDir := geoSortNone
	any := false
	count := int64(0)

	if len(argv) > baseArgs {
		remaining := len(argv) - baseArgs
		for i := 0; i < remaining; i++ {
			arg := argv[baseArgs+i]
			switch {
			case eqFold(arg, "withdist"):
				withdist = true
			case eqFold(arg, "withhash"):
				withhash = true
			case eqFold(arg, "withcoord"):
				withcoords = true
			case eqFold(arg, "any"):
				any = true
			case eqFold(arg, "asc"):
				sortDir = geoSortAsc
			case eqFold(arg, "desc"):
				sortDir = geoSortDesc
			case eqFold(arg, "count") && i+1 < remaining:
				n, err := strconv.ParseInt(string(argv[baseArgs+i+1]), 10, 64)
				if err != nil {
					c.writeErr("ERR value is not an integer or out of range")
					return
				}
				if n <= 0 {
					c.writeErr("ERR COUNT must be > 0")
					return
				}
				count = n
				i++
			case eqFold(arg, "store") && i+1 < remaining && flags&radiusNoStore == 0 && flags&geoSearch == 0:
				storekey = argv[baseArgs+i+1]
				storedist = false
				i++
			case eqFold(arg, "storedist") && i+1 < remaining && flags&radiusNoStore == 0 && flags&geoSearch == 0:
				storekey = argv[baseArgs+i+1]
				storedist = true
				i++
			case eqFold(arg, "storedist") && flags&geoSearch != 0 && flags&geoSearchStore != 0:
				storedist = true
			case eqFold(arg, "frommember") && i+1 < remaining && flags&geoSearch != 0 && !fromloc:
				if !exists {
					frommember = true
					i++
					continue
				}
				s, ok := c.geoScore(src, argv[baseArgs+i+1])
				if !ok {
					c.writeErr("ERR could not decode requested zset member")
					return
				}
				xy, decoded := decodeGeohash(s)
				if !decoded {
					c.writeErr("ERR could not decode requested zset member")
					return
				}
				shape.xy[0], shape.xy[1] = xy[0], xy[1]
				frommember = true
				i++
			case eqFold(arg, "fromlonlat") && i+2 < remaining && flags&geoSearch != 0 && !frommember:
				lon, lat, ok := c.extractLongLat(argv[baseArgs+i+1], argv[baseArgs+i+2])
				if !ok {
					return
				}
				shape.xy[0], shape.xy[1] = lon, lat
				fromloc = true
				i += 2
			case eqFold(arg, "byradius") && i+2 < remaining && flags&geoSearch != 0 && !bybox:
				if !c.extractDistance(argv[baseArgs+i+1], argv[baseArgs+i+2], &shape) {
					return
				}
				shape.typ = geoCircular
				byradius = true
				i += 2
			case eqFold(arg, "bybox") && i+3 < remaining && flags&geoSearch != 0 && !byradius:
				if !c.extractBox(argv[baseArgs+i+1], argv[baseArgs+i+2], argv[baseArgs+i+3], &shape) {
					return
				}
				shape.typ = geoRectangle
				bybox = true
				i += 3
			default:
				c.writeErr("ERR syntax error")
				return
			}
		}
	}

	if storekey != nil && (withdist || withhash || withcoords) {
		if flags&geoSearchStore != 0 {
			c.writeErr("ERR GEOSEARCHSTORE is not compatible with WITHDIST, WITHHASH and WITHCOORD options")
		} else {
			c.writeErr("ERR STORE option in GEORADIUS is not compatible with WITHDIST, WITHHASH and WITHCOORD options")
		}
		return
	}
	if flags&geoSearch != 0 && !frommember && !fromloc {
		c.writeErr("ERR exactly one of FROMMEMBER or FROMLONLAT can be specified for " + string(argv[0]))
		return
	}
	if flags&geoSearch != 0 && !byradius && !bybox {
		c.writeErr("ERR exactly one of BYRADIUS and BYBOX can be specified for " + string(argv[0]))
		return
	}
	if any && count == 0 {
		c.writeErr("ERR the ANY argument requires COUNT argument")
		return
	}

	// Source key absent: delete the destination (STORE) and reply 0, or reply an empty array.
	if !exists {
		if storekey != nil {
			mu := &c.srv.incrMu[c.srv.stripe(storekey)]
			mu.Lock()
			c.srv.store.Delete(storekey)
			c.zsetClear(storekey)
			mu.Unlock()
			c.writeInt(0)
		} else {
			c.writeArrayHeader(0)
		}
		return
	}

	// COUNT without ordering forces ASC so the closest N come back (ANY skips this).
	if count != 0 && sortDir == geoSortNone && !any {
		sortDir = geoSortAsc
	}

	georadius := geohashCalculateAreasByShapeWGS84(&shape)
	limit := 0
	if any {
		limit = int(count)
	}
	ga := c.geoMembersOfAllNeighbors(src, &georadius, &shape, card, limit)

	if len(ga) == 0 && storekey == nil {
		c.writeArrayHeader(0)
		return
	}

	resultLength := len(ga)
	returnedItems := resultLength
	if count != 0 && int64(resultLength) > count {
		returnedItems = int(count)
	}

	if sortDir != geoSortNone {
		if sortDir == geoSortAsc {
			sort.SliceStable(ga, func(i, j int) bool { return ga[i].dist < ga[j].dist })
		} else {
			sort.SliceStable(ga, func(i, j int) bool { return ga[i].dist > ga[j].dist })
		}
	}

	if storekey == nil {
		optionLength := 0
		if withdist {
			optionLength++
		}
		if withcoords {
			optionLength++
		}
		if withhash {
			optionLength++
		}
		c.writeArrayHeader(returnedItems)
		for i := 0; i < returnedItems; i++ {
			gp := &ga[i]
			dist := gp.dist / shape.conversion
			if optionLength > 0 {
				c.writeArrayHeader(optionLength + 1)
			}
			c.writeBulk(gp.member)
			if withdist {
				c.writeGeoDist(dist)
			}
			if withhash {
				c.writeInt(int64(gp.score))
			}
			if withcoords {
				c.writeArrayHeader(2)
				c.writeGeoCoord(gp.longitude)
				c.writeGeoCoord(gp.latitude)
			}
		}
		return
	}

	// Store the results in the destination geoset. Members are already private copies, so a
	// destination that aliases the source is safe: the scan finished before the clear.
	res := make([]zScored, 0, returnedItems)
	for i := 0; i < returnedItems; i++ {
		gp := &ga[i]
		score := gp.score
		if storedist {
			score = gp.dist / shape.conversion
		}
		res = append(res, zScored{member: gp.member, score: score})
	}
	mu := &c.srv.incrMu[c.srv.stripe(storekey)]
	mu.Lock()
	c.srv.store.Delete(storekey)
	c.zsetClear(storekey)
	n, err := c.zsetWriteResult(storekey, res)
	mu.Unlock()
	if err != nil {
		c.writeErr("ERR " + err.Error())
		return
	}
	c.writeInt(int64(n))
}
