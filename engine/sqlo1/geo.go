package sqlo1

// The geo family's storage half, doc 09 section 7: geo commands are
// zset commands with the standard 52-bit interleaved geohash as the
// score, bit-identical to Redis's encoding (Z-I6), so geo data
// round-trips against Redis. The codec, the step estimator, and the
// haversine are Redis's geohash.c arithmetic term for term, because
// filter parity on boundary points needs the same floating point
// path; the geo lab (labs/sqlo1/t4/03_geo) verified this port
// bit-for-bit against a live Redis 8.8.0 on 6000 points.
//
// GEOSEARCH covers the search area's bounding box with cells at the
// lab's verdict precision, estimateStep plus one: the bbox trim
// bounds the cover at 25 cells against Redis's fixed nine, reads 1.4
// to 1.7x fewer runs past 5 km, and decodes roughly half the
// candidates. Each cell is a contiguous sortable-score interval, so a
// cell scan is two insertion-rank seeks and one rank-window walk over
// the score fence, and only the runs the cells overlap are read,
// resident or not.

import (
	"context"
	"math"
)

// Redis's geo constants, geohash.c and geohash_helper.c verbatim.
const (
	geoStep     = 26
	geoLatMin   = -85.05112878
	geoLatMax   = 85.05112878
	geoLonMin   = -180.0
	geoLonMax   = 180.0
	geoEarthR   = 6372797.560856
	geoMercator = 20037726.37
	geoBits     = uint64(1)<<(2*geoStep) - 1
)

// geoAlpha is the standard geohash base32 alphabet GEOHASH encodes
// with.
const geoAlpha = "0123456789bcdefghjkmnpqrstuvwxyz"

// The conversion factors are single folded constants, D_R and R_D in
// geohash_helper.c, so every multiply rounds exactly once the way
// Redis's does; the two-step d times pi over 180 shape lands one ulp
// off on some distances.
const (
	geoDR = float64(math.Pi) / 180
	geoRD = 180 / float64(math.Pi)
)

func geoDeg2rad(d float64) float64 { return d * geoDR }
func geoRad2deg(r float64) float64 { return r * geoRD }

// geoInterleave64 spreads x into the even bits and y into the odd
// bits, Redis's magic-number ladder. The encoder passes latitude as
// x.
func geoInterleave64(x, y uint32) uint64 {
	xx, yy := uint64(x), uint64(y)
	xx = (xx | (xx << 16)) & 0x0000FFFF0000FFFF
	xx = (xx | (xx << 8)) & 0x00FF00FF00FF00FF
	xx = (xx | (xx << 4)) & 0x0F0F0F0F0F0F0F0F
	xx = (xx | (xx << 2)) & 0x3333333333333333
	xx = (xx | (xx << 1)) & 0x5555555555555555
	yy = (yy | (yy << 16)) & 0x0000FFFF0000FFFF
	yy = (yy | (yy << 8)) & 0x00FF00FF00FF00FF
	yy = (yy | (yy << 4)) & 0x0F0F0F0F0F0F0F0F
	yy = (yy | (yy << 2)) & 0x3333333333333333
	yy = (yy | (yy << 1)) & 0x5555555555555555
	return xx | (yy << 1)
}

// geoCompressEven collapses the even bits of v back into a 32-bit
// int, the inverse half of geoInterleave64.
func geoCompressEven(v uint64) uint32 {
	v &= 0x5555555555555555
	v = (v | (v >> 1)) & 0x3333333333333333
	v = (v | (v >> 2)) & 0x0F0F0F0F0F0F0F0F
	v = (v | (v >> 4)) & 0x00FF00FF00FF00FF
	v = (v | (v >> 8)) & 0x0000FFFF0000FFFF
	v = (v | (v >> 16)) & 0x00000000FFFFFFFF
	return uint32(v)
}

// geoEncodeRanges is geohashEncode over explicit axis ranges: the
// mercator-limited pair for scores, the world pair for the GEOHASH
// base32 re-encode.
func geoEncodeRanges(lon, lat, lonMin, lonMax, latMin, latMax float64) uint64 {
	latOff := (lat - latMin) / (latMax - latMin)
	lonOff := (lon - lonMin) / (lonMax - lonMin)
	ilat := uint32(latOff * float64(uint64(1)<<geoStep))
	ilon := uint32(lonOff * float64(uint64(1)<<geoStep))
	return geoInterleave64(ilat, ilon)
}

// geoEncode maps a coordinate to its 52-bit score cell.
func geoEncode(lon, lat float64) uint64 {
	return geoEncodeRanges(lon, lat, geoLonMin, geoLonMax, geoLatMin, geoLatMax)
}

// geoDecode returns the cell midpoint, mirroring Redis's decode
// arithmetic term for term so the midpoints agree to the last bit.
func geoDecode(bits uint64) (lon, lat float64) {
	ilat := geoCompressEven(bits)
	ilon := geoCompressEven(bits >> 1)
	latScale := geoLatMax - geoLatMin
	lonScale := geoLonMax - geoLonMin
	latMinC := geoLatMin + (float64(ilat)*1.0/float64(uint64(1)<<geoStep))*latScale
	latMaxC := geoLatMin + (float64(ilat+1)*1.0/float64(uint64(1)<<geoStep))*latScale
	lonMinC := geoLonMin + (float64(ilon)*1.0/float64(uint64(1)<<geoStep))*lonScale
	lonMaxC := geoLonMin + (float64(ilon+1)*1.0/float64(uint64(1)<<geoStep))*lonScale
	return (lonMinC + lonMaxC) / 2, (latMinC + latMaxC) / 2
}

// geoEstimateStep is geohashEstimateStepsByRadius: precision from the
// radius, widened toward the poles where mercator cells narrow.
func geoEstimateStep(r, lat float64) int {
	if r == 0 {
		return geoStep
	}
	step := 1
	for r < geoMercator {
		r *= 2
		step++
	}
	step -= 2
	if lat > 66 || lat < -66 {
		step--
		if lat > 80 || lat < -80 {
			step--
		}
	}
	return min(max(step, 1), geoStep)
}

// geoDist is geohashGetDistance: haversine on Redis's earth radius,
// with the same longitude-only shortcut.
func geoDist(lon1, lat1, lon2, lat2 float64) float64 {
	lon1r, lon2r := geoDeg2rad(lon1), geoDeg2rad(lon2)
	v := math.Sin((lon2r - lon1r) / 2)
	if v == 0 {
		return geoEarthR * math.Abs(geoDeg2rad(lat2)-geoDeg2rad(lat1))
	}
	lat1r, lat2r := geoDeg2rad(lat1), geoDeg2rad(lat2)
	u := math.Sin((lat2r - lat1r) / 2)
	a := u*u + math.Cos(lat1r)*math.Cos(lat2r)*v*v
	return 2 * geoEarthR * math.Asin(math.Sqrt(a))
}

// geoLatDist is geohashGetLatDistance, the latitude-only leg the box
// filter checks first.
func geoLatDist(lat1, lat2 float64) float64 {
	return geoEarthR * math.Abs(geoDeg2rad(lat2)-geoDeg2rad(lat1))
}

// geoCellIdx maps a coordinate to its cell index on one axis at
// step, clamped to the axis.
func geoCellIdx(v, lo, hi float64, step int) uint32 {
	if v < lo {
		v = lo
	}
	if v > hi {
		v = hi
	}
	i := int64((v - lo) / (hi - lo) * float64(uint64(1)<<step))
	if i < 0 {
		i = 0
	}
	if i >= int64(1)<<step {
		i = int64(1)<<step - 1
	}
	return uint32(i)
}

// geoShape is a resolved search area: a circle by radius or a box by
// width and height, all in meters.
type geoShape struct {
	lon, lat float64
	byBox    bool
	radius   float64
	w, h     float64
}

// circum is the radius of the shape's circumscribed circle, the
// value the step estimate and the bounding box derive from, matching
// Redis's rect-to-radius conversion.
func (sh *geoShape) circum() float64 {
	if !sh.byBox {
		return sh.radius
	}
	return math.Sqrt((sh.w/2)*(sh.w/2) + (sh.h/2)*(sh.h/2))
}

// contains is the exact filter: haversine against the radius, or
// Redis's rectangle legs with the longitude distance computed at the
// candidate's latitude. dist is the center distance in meters.
func (sh *geoShape) contains(lon, lat float64) (dist float64, ok bool) {
	if sh.byBox {
		if geoLatDist(sh.lat, lat) > sh.h/2 {
			return 0, false
		}
		if geoDist(sh.lon, lat, lon, lat) > sh.w/2 {
			return 0, false
		}
		return geoDist(sh.lon, sh.lat, lon, lat), true
	}
	d := geoDist(sh.lon, sh.lat, lon, lat)
	if d > sh.radius {
		return 0, false
	}
	return d, true
}

// bbox is geohashBoundingBox: the latitude delta from the meter
// height, the longitude delta at the hemisphere-outward latitude so
// the box only widens. A delta that degenerates near the pole covers
// the whole axis.
func (sh *geoShape) bbox() (lonMin, lonMax, latMin, latMax float64) {
	h, w := sh.radius, sh.radius
	if sh.byBox {
		h, w = sh.h/2, sh.w/2
	}
	latDelta := geoRad2deg(h / geoEarthR)
	edge := sh.lat + latDelta
	if sh.lat < 0 {
		edge = sh.lat - latDelta
	}
	lonDelta := geoRad2deg(w / geoEarthR / math.Cos(geoDeg2rad(edge)))
	if !(lonDelta > 0) || lonDelta > 360 {
		lonDelta = 360
	}
	return sh.lon - lonDelta, sh.lon + lonDelta, sh.lat - latDelta, sh.lat + latDelta
}

// GeoSearch walks the cell cover of sh over key's score runs and
// emits every entry inside the shape with its decoded position and
// center distance in meters. Cells are covered at the lab's verdict
// precision, estimateStep plus one with the bounding-box trim.
// Emitted member bytes alias the run read and die on the next Tiered
// call, so a collector copies them; emit answers false to stop the
// whole search (the ANY door). Entries whose scores were not written
// by GEOADD decode like Redis decodes them: as whatever cell their
// low 52 bits name.
func (z *ZSet) GeoSearch(ctx context.Context, key []byte, sh geoShape, emit func(member []byte, bits uint64, lon, lat, dist float64) bool) error {
	step := min(geoEstimateStep(sh.circum(), sh.lat)+1, geoStep)
	lonMin, lonMax, latMin, latMax := sh.bbox()
	iLat0 := geoCellIdx(latMin, geoLatMin, geoLatMax, step)
	iLat1 := geoCellIdx(latMax, geoLatMin, geoLatMax, step)
	iLonEnd := uint32(1)<<step - 1

	// A box past lon 180 wraps: Redis's neighbor cells wrap modulo the
	// step, and the haversine agrees, so the cover carries the far side
	// as a second lon index interval. The two intervals collapse to the
	// full axis when a coarse step makes them touch.
	lonSpans := [2][2]uint32{}
	nSpans := 1
	switch {
	case lonMax-lonMin >= 360 || (lonMin >= geoLonMin && lonMax <= geoLonMax):
		lonSpans[0] = [2]uint32{geoCellIdx(lonMin, geoLonMin, geoLonMax, step), geoCellIdx(lonMax, geoLonMin, geoLonMax, step)}
	case lonMin < geoLonMin:
		near := [2]uint32{0, geoCellIdx(lonMax, geoLonMin, geoLonMax, step)}
		far := [2]uint32{geoCellIdx(lonMin+360, geoLonMin, geoLonMax, step), iLonEnd}
		if far[0] <= near[1] {
			lonSpans[0] = [2]uint32{0, iLonEnd}
		} else {
			lonSpans[0], lonSpans[1], nSpans = near, far, 2
		}
	default:
		near := [2]uint32{geoCellIdx(lonMin, geoLonMin, geoLonMax, step), iLonEnd}
		far := [2]uint32{0, geoCellIdx(lonMax-360, geoLonMin, geoLonMax, step)}
		if far[1] >= near[0] {
			lonSpans[0] = [2]uint32{0, iLonEnd}
		} else {
			lonSpans[0], lonSpans[1], nSpans = near, far, 2
		}
	}

	shift := uint(2 * (geoStep - step))
	stopped := false
	for ila := iLat0; ila <= iLat1 && !stopped; ila++ {
		for sp := 0; sp < nSpans && !stopped; sp++ {
			iLon0, iLon1 := lonSpans[sp][0], lonSpans[sp][1]
			for ilo := iLon0; ilo <= iLon1 && !stopped; ilo++ {
				cell := geoInterleave64(uint32(ila), uint32(ilo))
				smin := zScoreSortable(float64(cell << shift))
				smax := zScoreSortable(float64((cell + 1) << shift))
				lo, _, err := z.zseekRank(ctx, key, smin, nil)
				if err != nil {
					return err
				}
				hi, _, err := z.zseekRank(ctx, key, smax, nil)
				if err != nil {
					return err
				}
				if hi <= lo {
					continue
				}
				err = z.zwalkRank(ctx, key, lo, hi, func(su uint64, m []byte) bool {
					bits := uint64(zScoreFromSortable(su)) & geoBits
					plon, plat := geoDecode(bits)
					d, ok := sh.contains(plon, plat)
					if !ok {
						return true
					}
					if !emit(m, bits, plon, plat, d) {
						stopped = true
						return false
					}
					return true
				})
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}
