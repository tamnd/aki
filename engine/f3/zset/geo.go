package zset

// Geo on the zset substrate (spec 2064/f3/15 section 10). A geo set is a sorted
// set whose score is a 52-bit geohash interleave of longitude and latitude, so
// every geo command is a zset command underneath plus a coordinate codec on the
// score. GEOADD is a ZADD with an encoded score, GEOPOS/GEODIST/GEOHASH are
// member lookups plus a decode, and TYPE still answers zset. The codec matches
// Redis bit-for-bit so a client that decodes the raw score itself, or an
// interop tool that reads GEOHASH, keeps working: latitude lands in the even
// interleave bits and longitude in the odd bits, the layout Redis's
// interleave64 produces, and GEOHASH re-encodes against the standard -90..90
// latitude range rather than the Mercator-clamped range the score uses.

import (
	"math"
	"strconv"

	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/f3srv/resp"
)

// The coordinate ranges and Earth model, frozen to Redis's constants so scores,
// distances, and hashes agree digit-for-digit where float64 can represent them.
// Longitude spans the full circle; latitude is clamped to the Web Mercator limit
// so the square interleave grid stays square, which is why GEOADD rejects a
// latitude past geoLatMax even though it is a valid Earth coordinate.
const (
	geoLonMin       = -180.0
	geoLatMin       = -85.05112878
	geoLonMax       = 180.0
	geoLatMax       = 85.05112878
	geoStep         = 26 // bits per coordinate; 2*geoStep = 52, exact in a double
	geoEarthRadiusM = 6372797.560856
)

// geoAlpha is the standard geohash base-32 alphabet GEOHASH renders against.
const geoAlpha = "0123456789bcdefghjkmnpqrstuvwxyz"

// geoSpread scatters a 26-bit value into the even bit positions of a u64, the
// forward half of Redis's interleave64. The shifts double the gap between bits
// at each step until every input bit sits two positions apart.
func geoSpread(v uint32) uint64 {
	x := uint64(v) & 0x3FFFFFF
	x = (x | (x << 16)) & 0x0000FFFF0000FFFF
	x = (x | (x << 8)) & 0x00FF00FF00FF00FF
	x = (x | (x << 4)) & 0x0F0F0F0F0F0F0F0F
	x = (x | (x << 2)) & 0x3333333333333333
	x = (x | (x << 1)) & 0x5555555555555555
	return x
}

// geoSquash gathers the even-bit plane of x back into a dense 26-bit value, the
// inverse of geoSpread and the per-plane half of Redis's deinterleave64. The
// caller shifts the odd plane down by one before squashing it.
func geoSquash(x uint64) uint32 {
	x &= 0x5555555555555555
	x = (x | (x >> 1)) & 0x3333333333333333
	x = (x | (x >> 2)) & 0x0F0F0F0F0F0F0F0F
	x = (x | (x >> 4)) & 0x00FF00FF00FF00FF
	x = (x | (x >> 8)) & 0x0000FFFF0000FFFF
	x = (x | (x >> 16)) & 0x00000000FFFFFFFF
	return uint32(x)
}

// geoEncode maps a coordinate to its 52-bit geohash, latitude in the even bits
// and longitude in the odd bits, matching Redis's geohashEncode at step 26. Each
// coordinate quantizes to a 26-bit fraction of its span; the double-to-uint32
// truncation is deliberate and reproduces Redis's fixed-point conversion.
func geoEncode(lon, lat float64) uint64 {
	latOff := (lat - geoLatMin) / (geoLatMax - geoLatMin)
	lonOff := (lon - geoLonMin) / (geoLonMax - geoLonMin)
	scale := float64(uint64(1) << geoStep)
	latBits := uint32(latOff * scale)
	lonBits := uint32(lonOff * scale)
	return geoSpread(latBits) | (geoSpread(lonBits) << 1)
}

// geoDecode maps a 52-bit geohash back to the center of its cell, the inverse of
// geoEncode. GEOPOS of a just-added point returns these near-but-not-exact
// coordinates because a cell has extent, exactly Redis's documented behavior;
// the residual differs from Redis only in the digits float64 cannot hold where
// Redis carries a long double.
func geoDecode(bits uint64) (lon, lat float64) {
	ilat := geoSquash(bits)
	ilon := geoSquash(bits >> 1)
	scale := float64(uint64(1) << geoStep)
	latSpan := geoLatMax - geoLatMin
	lonSpan := geoLonMax - geoLonMin
	latMin := geoLatMin + (float64(ilat)/scale)*latSpan
	latMax := geoLatMin + (float64(ilat+1)/scale)*latSpan
	lonMin := geoLonMin + (float64(ilon)/scale)*lonSpan
	lonMax := geoLonMin + (float64(ilon+1)/scale)*lonSpan
	lat = (latMin + latMax) / 2
	lon = (lonMin + lonMax) / 2
	return
}

// geoDistM is the haversine distance in meters between two coordinates on
// Redis's sphere, a bit-for-bit port of geohashGetDistance including the
// same-longitude shortcut that avoids the haversine when the points share a
// meridian.
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

// geoUnit returns the meters-per-unit multiplier for a GEODIST/GEOSEARCH unit
// token, case-insensitively, and false for an unsupported unit. The defaults and
// spellings match Redis's extractUnitOrReply.
func geoUnit(b []byte) (float64, bool) {
	switch {
	case eqFold(b, "M"):
		return 1, true
	case eqFold(b, "KM"):
		return 1000, true
	case eqFold(b, "FT"):
		return 0.3048, true
	case eqFold(b, "MI"):
		return 1609.34, true
	}
	return 0, false
}

// geoHashString renders the standard eleven-character geohash of a coordinate,
// the geohash.org interop format. It re-encodes against the full -90..90
// latitude range, not the Mercator-clamped score range, then reads the 52-bit
// code five bits at a time from the top; the eleventh character is always the
// zero-padding, since 52 bits fall three short of the 55 the eleven-char format
// carries. This double conversion is Redis's, and skipping it places points
// kilometers off.
func geoHashString(lon, lat float64, out []byte) []byte {
	latOff := (lat - (-90.0)) / 180.0
	lonOff := (lon - (-180.0)) / 360.0
	scale := float64(uint64(1) << geoStep)
	bits := geoSpread(uint32(latOff*scale)) | (geoSpread(uint32(lonOff*scale)) << 1)
	for i := 0; i < 11; i++ {
		var idx uint64
		if i == 10 {
			idx = 0
		} else {
			idx = (bits >> (52 - uint((i+1)*5))) & 0x1f
		}
		out = append(out, geoAlpha[idx])
	}
	return out
}

// geoFlags carries GEOADD's option matrix, the NX/XX/CH subset of ZADD's flags.
type geoFlags struct {
	nx, xx, ch bool
}

// Geoadd answers GEOADD key [NX|XX] [CH] lon lat member [lon lat member ...]:
// validate every coordinate, encode each to a geohash score, and apply the
// triples as ZADDs under the flag matrix. Every coordinate is range-checked
// before any member lands, so a bad tail cannot half-apply a batch (atomic under
// the owner's serial execution). The reply is the number added, or added plus
// changed with CH, exactly like a non-INCR ZADD.
func Geoadd(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	key := args[0]
	var fl geoFlags
	i := 1
flagsLoop:
	for ; i < len(args); i++ {
		switch {
		case eqFold(args[i], "NX"):
			fl.nx = true
		case eqFold(args[i], "XX"):
			fl.xx = true
		case eqFold(args[i], "CH"):
			fl.ch = true
		default:
			break flagsLoop
		}
	}
	if fl.nx && fl.xx {
		r.Err("ERR XX and NX options at the same time are not compatible")
		return
	}
	rest := args[i:]
	if len(rest) == 0 || len(rest)%3 != 0 {
		r.Err("ERR syntax error")
		return
	}

	// Parse and validate every triple before the first write.
	ntriples := len(rest) / 3
	scores := make([]float64, ntriples)
	for t := 0; t < ntriples; t++ {
		lon, ok1 := parseScore(rest[3*t])
		lat, ok2 := parseScore(rest[3*t+1])
		if !ok1 || !ok2 {
			r.Err("ERR value is not a valid float")
			return
		}
		if lon < geoLonMin || lon > geoLonMax || lat < geoLatMin || lat > geoLatMax {
			var buf [64]byte
			msg := append(buf[:0], "ERR invalid longitude,latitude pair "...)
			msg = strconv.AppendFloat(msg, lon, 'f', 6, 64)
			msg = append(msg, ',')
			msg = strconv.AppendFloat(msg, lat, 'f', 6, 64)
			r.Err(string(msg))
			return
		}
		scores[t] = float64(geoEncode(lon, lat))
	}

	g := registry(cx)
	// See Zadd: GEOADD is a ZADD over an encoded score, so the create funnel drops
	// an expired zset before building a fresh one with no stale TTL.
	z := g.live(cx, key)
	created := false
	if z == nil {
		if cx.St.Exists(key, cx.NowMs) {
			r.Err(wrongType)
			return
		}
		z = newZset()
		created = true
	}

	zf := flags{nx: fl.nx, xx: fl.xx, ch: fl.ch}
	var added, changed int64
	for t := 0; t < ntriples; t++ {
		member := rest[3*t+2]
		gotAdded, gotChanged, _, _, _ := z.update(member, scores[t], zf)
		if gotAdded {
			added++
		}
		if gotChanged {
			changed++
		}
	}

	if z.card() == 0 {
		if !created {
			g.drop(key)
		}
	} else {
		if created {
			g.install(cx, key, z)
		}
		g.grewNote(cx, key, z)
	}
	if fl.ch {
		r.Int(added + changed)
		return
	}
	r.Int(added)
}

// Geopos answers GEOPOS key member [member ...]: an array sized to the requested
// members, each element the member's [longitude, latitude] pair as two bulk
// strings, or a null array when the member (or the whole key) is absent. It never
// errors on a missing member, matching Redis.
func Geopos(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	g := registry(cx)
	z, wrong := g.lookup(cx, args[0])
	if wrong {
		r.Err(wrongType)
		return
	}
	members := args[1:]
	resp3 := r.Resp3()
	out := resp.AppendArrayHeader(cx.Aux[:0], len(members))
	var cb [48]byte
	for _, m := range members {
		if z == nil {
			out = resp.AppendNullArray(out)
			continue
		}
		s, ok := z.score(m)
		if !ok {
			out = resp.AppendNullArray(out)
			continue
		}
		lon, lat := geoDecode(uint64(s))
		out = resp.AppendArrayHeader(out, 2)
		out = appendGeoDouble(out, strconv.AppendFloat(cb[:0], lon, 'g', 17, 64), resp3)
		out = appendGeoDouble(out, strconv.AppendFloat(cb[:0], lat, 'g', 17, 64), resp3)
	}
	cx.Aux = out
	r.Raw(out)
}

// Geodist answers GEODIST key member1 member2 [unit]: the haversine distance
// between the two members in the requested unit (default meters), formatted to
// four decimals, or a null when either member or the key is absent. An unknown
// unit is the unsupported-unit error.
func Geodist(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	toMeters := 1.0
	if len(args) == 4 {
		u, ok := geoUnit(args[3])
		if !ok {
			r.Err("ERR unsupported unit provided. please use M, KM, FT, MI")
			return
		}
		toMeters = u
	} else if len(args) != 3 {
		r.Err("ERR syntax error")
		return
	}
	g := registry(cx)
	z, wrong := g.lookup(cx, args[0])
	if wrong {
		r.Err(wrongType)
		return
	}
	if z == nil {
		r.Null()
		return
	}
	s1, ok1 := z.score(args[1])
	s2, ok2 := z.score(args[2])
	if !ok1 || !ok2 {
		r.Null()
		return
	}
	lon1, lat1 := geoDecode(uint64(s1))
	lon2, lat2 := geoDecode(uint64(s2))
	d := geoDistM(lon1, lat1, lon2, lat2) / toMeters
	var db [32]byte
	// The distance keeps its four-decimal formatting under both protocols, framed
	// as a RESP2 bulk or a RESP3 double, the addReplyDoubleDistance path.
	r.DoubleBytes(strconv.AppendFloat(db[:0], d, 'f', 4, 64))
}

// appendGeoDouble appends a formatted GEO coordinate or distance in the
// negotiated protocol: a RESP3 double (,digits) when resp3 is set, or a RESP2
// bulk string of the same digits. GEO values keep their own strconv formatting
// (17-significant-figure coordinates, four-decimal distances), so this reuses the
// caller's digits rather than reformatting, the addReplyHumanLongDouble path.
func appendGeoDouble(out, digits []byte, resp3 bool) []byte {
	if resp3 {
		return resp.AppendDoubleBytes(out, digits)
	}
	return resp.AppendBulk(out, digits)
}

// Geohash answers GEOHASH key member [member ...]: an array sized to the
// requested members, each the member's eleven-character standard geohash string,
// or a null in a position whose member is absent. A missing key returns an array
// of nils, never an error.
func Geohash(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	g := registry(cx)
	z, wrong := g.lookup(cx, args[0])
	if wrong {
		r.Err(wrongType)
		return
	}
	members := args[1:]
	out := resp.AppendArrayHeader(cx.Aux[:0], len(members))
	var hb [16]byte
	for _, m := range members {
		if z == nil {
			out = resp.AppendNull(out)
			continue
		}
		s, ok := z.score(m)
		if !ok {
			out = resp.AppendNull(out)
			continue
		}
		lon, lat := geoDecode(uint64(s))
		out = resp.AppendBulk(out, geoHashString(lon, lat, hb[:0]))
	}
	cx.Aux = out
	r.Raw(out)
}
