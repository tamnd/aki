package command

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/tamnd/aki/keyspace"
)

// Geo commands store points in an ordinary sorted set whose score is the 52-bit
// interleaved geohash of the point's longitude and latitude (doc 13 §6). Every
// zset command works on a geo key; these commands add the coordinate encoding,
// the haversine distance, and the radius and box searches on top.

// geoCommands returns the geospatial command group.
func geoCommands() []*CmdDesc {
	return []*CmdDesc{
		{Name: "geoadd", Group: GroupGeo, Since: "3.2.0",
			Arity: -5, Flags: FlagWrite | FlagDenyOOM, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleGeoAdd},
		{Name: "geopos", Group: GroupGeo, Since: "3.2.0",
			Arity: -2, Flags: FlagReadOnly | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleGeoPos},
		{Name: "geodist", Group: GroupGeo, Since: "3.2.0",
			Arity: -4, Flags: FlagReadOnly, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleGeoDist},
		{Name: "geohash", Group: GroupGeo, Since: "3.2.0",
			Arity: -2, Flags: FlagReadOnly | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleGeoHash},
		{Name: "geosearch", Group: GroupGeo, Since: "6.2.0",
			Arity: -7, Flags: FlagReadOnly, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleGeoSearch},
		{Name: "geosearchstore", Group: GroupGeo, Since: "6.2.0",
			Arity: -8, Flags: FlagWrite | FlagDenyOOM, FirstKey: 1, LastKey: 2, Step: 1,
			Handler: handleGeoSearchStore},
		{Name: "georadius", Group: GroupGeo, Since: "3.2.0",
			Arity: -6, Flags: FlagWrite | FlagDenyOOM, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleGeoRadius},
		{Name: "georadius_ro", Group: GroupGeo, Since: "3.2.10",
			Arity: -6, Flags: FlagReadOnly, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleGeoRadiusRO},
		{Name: "georadiusbymember", Group: GroupGeo, Since: "3.2.0",
			Arity: -5, Flags: FlagWrite | FlagDenyOOM, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleGeoRadiusByMember},
		{Name: "georadiusbymember_ro", Group: GroupGeo, Since: "3.2.10",
			Arity: -5, Flags: FlagReadOnly, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleGeoRadiusByMemberRO},
	}
}

// Coordinate bounds. Latitude is clipped to the Web Mercator range Redis uses,
// not the full pole-to-pole range.
const (
	geoLonMin         = -180.0
	geoLonMax         = 180.0
	geoLatMin         = -85.05112878
	geoLatMax         = 85.05112878
	geoLatRange       = geoLatMax - geoLatMin // 170.10225756
	geoLonRange       = geoLonMax - geoLonMin // 360
	geoStepBits       = 26
	earthRadiusMeters = 6372797.560856
)

func geoValidLon(lon float64) bool {
	return !math.IsNaN(lon) && lon >= geoLonMin && lon <= geoLonMax
}

func geoValidLat(lat float64) bool {
	return !math.IsNaN(lat) && lat >= geoLatMin && lat <= geoLatMax
}

// spread26 inserts a zero bit between each of the low 26 bits of x, so a 26-bit
// value spreads into the even positions of a 52-bit value.
func spread26(x uint64) uint64 {
	x &= 0x0000000003FFFFFF
	x = (x | (x << 16)) & 0x0000FFFF0000FFFF
	x = (x | (x << 8)) & 0x00FF00FF00FF00FF
	x = (x | (x << 4)) & 0x0F0F0F0F0F0F0F0F
	x = (x | (x << 2)) & 0x3333333333333333
	x = (x | (x << 1)) & 0x5555555555555555
	return x
}

// squash is the inverse of spread26: it gathers the even bits of x back into a
// contiguous low field.
func squash(x uint64) uint32 {
	x &= 0x5555555555555555
	x = (x | (x >> 1)) & 0x3333333333333333
	x = (x | (x >> 2)) & 0x0F0F0F0F0F0F0F0F
	x = (x | (x >> 4)) & 0x00FF00FF00FF00FF
	x = (x | (x >> 8)) & 0x0000FFFF0000FFFF
	x = (x | (x >> 16)) & 0x00000000FFFFFFFF
	return uint32(x)
}

// geoCell quantizes a value within a range to its 26-bit cell index.
func geoCell(v, vmin, span float64) uint64 {
	off := math.Floor((v - vmin) / span * float64(uint64(1)<<geoStepBits))
	return uint64(min(max(off, 0), float64((uint64(1)<<geoStepBits)-1)))
}

// interleaveLatLon packs a latitude cell into the even bits and a longitude cell
// into the odd bits, the Morton ordering Redis uses for its geohash score.
func interleaveLatLon(lat26, lon26 uint64) uint64 {
	return spread26(lat26) | (spread26(lon26) << 1)
}

// geoEncode encodes a coordinate into its 52-bit geohash score using the Redis
// internal latitude range, so the score matches what real Redis stores.
func geoEncode(lon, lat float64) float64 {
	return float64(interleaveLatLon(geoCell(lat, geoLatMin, geoLatRange), geoCell(lon, geoLonMin, geoLonRange)))
}

// geoDecode decodes a 52-bit geohash score back to the center of its cell.
func geoDecode(score float64) (lon, lat float64) {
	hash := uint64(score)
	lat26 := squash(hash)
	lon26 := squash(hash >> 1)
	stepLon := geoLonRange / float64(uint64(1)<<geoStepBits)
	stepLat := geoLatRange / float64(uint64(1)<<geoStepBits)
	lon = float64(lon26)*stepLon + geoLonMin + stepLon/2
	lat = float64(lat26)*stepLat + geoLatMin + stepLat/2
	return
}

// geohashAlphabet is the standard base32 geohash alphabet.
const geohashAlphabet = "0123456789bcdefghjkmnpqrstuvwxyz"

// geoHashString renders the 11-character base32 geohash Redis returns. The
// stored score uses the internal latitude range, so the point is decoded and
// re-encoded against the standard -90..90 range before rendering, exactly as
// Redis does. The 52-bit hash is read most significant bits first, five at a
// time, with a zero pad for the final character.
func geoHashString(score float64) string {
	lon, lat := geoDecode(score)
	lat26 := geoCell(lat, -90.0, 180.0)
	lon26 := geoCell(lon, -180.0, 360.0)
	bits := interleaveLatLon(lat26, lon26)
	var buf [11]byte
	for i := range 11 {
		var idx uint64
		if i < 10 {
			idx = (bits >> (52 - uint((i+1)*5))) & 0x1F
		}
		buf[i] = geohashAlphabet[idx]
	}
	return string(buf[:])
}

// haversineMeters is the great-circle distance between two coordinates, using
// the Earth radius Redis hardcodes so distances match its output.
func haversineMeters(lon1, lat1, lon2, lat2 float64) float64 {
	lon1r := lon1 * math.Pi / 180.0
	lat1r := lat1 * math.Pi / 180.0
	lon2r := lon2 * math.Pi / 180.0
	lat2r := lat2 * math.Pi / 180.0
	u := math.Sin((lat2r - lat1r) / 2)
	v := math.Sin((lon2r - lon1r) / 2)
	a := u*u + math.Cos(lat1r)*math.Cos(lat2r)*v*v
	return earthRadiusMeters * 2.0 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
}

// geoUnitToMeters returns the meters-per-unit factor for a unit string, or false
// for an unknown unit. The miles factor matches the Redis source.
func geoUnitToMeters(unit string) (float64, bool) {
	switch strings.ToUpper(unit) {
	case "M":
		return 1.0, true
	case "KM":
		return 1000.0, true
	case "MI":
		return 1609.3440, true
	case "FT":
		return 0.3048, true
	}
	return 0, false
}

const geoUnitError = "ERR unsupported unit provided. please use M, KM, FT, MI"

// formatGeoFloat renders a coordinate the way GEOPOS does, with 17 digits after
// the decimal point, matching Redis's %.17g output for these values.
func formatGeoFloat(v float64) string {
	return strconv.FormatFloat(v, 'f', 17, 64)
}

// handleGeoAdd implements GEOADD key [NX|XX] [CH] lon lat member [lon lat member ...].
func handleGeoAdd(ctx *Ctx) {
	args := ctx.Argv[2:]
	var nx, xx, ch bool
	i := 0
	for ; i < len(args); i++ {
		switch strings.ToUpper(string(args[i])) {
		case "NX":
			nx = true
		case "XX":
			xx = true
		case "CH":
			ch = true
		default:
			goto parsed
		}
	}
parsed:
	if nx && xx {
		ctx.enc().WriteError("ERR XX and NX options at the same time are not compatible")
		return
	}
	rest := args[i:]
	if len(rest) == 0 || len(rest)%3 != 0 {
		ctx.enc().WriteError("ERR syntax error")
		return
	}
	type geoPoint struct {
		score  float64
		member []byte
	}
	points := make([]geoPoint, 0, len(rest)/3)
	for j := 0; j < len(rest); j += 3 {
		lon, lonOK := parseFloatArg(rest[j])
		lat, latOK := parseFloatArg(rest[j+1])
		if !lonOK || !latOK || !geoValidLon(lon) || !geoValidLat(lat) {
			ctx.enc().WriteError(fmt.Sprintf("ERR invalid longitude,latitude pair %g,%g", lon, lat))
			return
		}
		points = append(points, geoPoint{score: geoEncode(lon, lat), member: rest[j+2]})
	}

	var (
		wrongTyp bool
		added    int64
		changed  int64
	)
	key := ctx.Argv[1]
	lim := ctx.encLimits()
	done := ctx.updateShard(key, func(db *keyspace.DB) error {
		added, changed = 0, 0
		var (
			hdr   keyspace.ValueHeader
			found bool
		)
		// A geo set is a sorted set of geohash scores, so it takes the same storage
		// path as ZADD. A btree-backed (coll-form) geo set updates each member's two
		// rows in place through a point write; it is never decoded and rewritten as a
		// whole blob. This is what keeps a multi-million-member geo set from cloning
		// itself onto the heap on every GEOADD, and it is the precondition for the
		// GEODIST/GEOPOS/GEOHASH point reads to stay bounded: those can only do a point
		// lookup on the member-index row if the set is actually stored as a sub-tree.
		route, err := db.CollUpdateRouted(key, keyspace.TypeZSet, keyspace.EncSkiplist,
			func(rFound bool, h keyspace.ValueHeader, _ []byte) keyspace.CollRoute {
				hdr, found = h, rFound
				if rFound && h.Type != keyspace.TypeZSet {
					wrongTyp = true
					return keyspace.CollRouteSkip
				}
				if rFound && h.IsColl() {
					return keyspace.CollRouteColl
				}
				return keyspace.CollRouteBlob
			},
			func(w *keyspace.CollWriter) error {
				for _, p := range points {
					cur, mfound, e := zTreeScore(w, p.member)
					if e != nil {
						return e
					}
					if mfound {
						if nx {
							continue
						}
						if cur != p.score {
							if e := zTreeSet(w, p.member, p.score, true, cur); e != nil {
								return e
							}
							changed++
						}
						continue
					}
					if xx {
						continue
					}
					if e := zTreeSet(w, p.member, p.score, false, 0); e != nil {
						return e
					}
					added++
				}
				return nil
			})
		if err != nil {
			return err
		}
		if route != keyspace.CollRouteBlob {
			return nil
		}
		// Blob path: a small geo set decodes once (bounded by the listpack threshold),
		// applies the points, then rewrites the blob or promotes to the sub-tree.
		members, _, _, err := getZSet(db, key)
		if err != nil {
			return err
		}
		floor := keyspace.EncListpack
		if found {
			floor = hdr.Encoding
		}
		for _, p := range points {
			idx := zsetFind(members, p.member)
			if idx >= 0 {
				if nx {
					continue
				}
				if members[idx].score != p.score {
					members[idx].score = p.score
					changed++
				}
				continue
			}
			if xx {
				continue
			}
			members = append(members, zmember{member: p.member, score: p.score})
			added++
		}
		if added == 0 && changed == 0 {
			return nil
		}
		zsetSort(members)
		if zsetWantsTree(lim, members, floor) {
			return zsetPromote(db, key, members)
		}
		return db.Set(key, zsetEncode(members), keyspace.TypeZSet, zsetEncoding(lim, members, floor), keepTTL(hdr, found))
	})
	if !done {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	if ch {
		ctx.enc().WriteInteger(added + changed)
		return
	}
	ctx.enc().WriteInteger(added)
}

// parseFloatArg parses a float and rejects NaN, like the coordinate parser geo
// commands need.
func parseFloatArg(b []byte) (float64, bool) {
	f, err := strconv.ParseFloat(string(b), 64)
	if err != nil {
		return 0, false
	}
	return f, true
}

// handleGeoPos implements GEOPOS key member [member ...]. Each member yields a
// two-element [lon, lat] array, or a nil for a member not in the key.
func handleGeoPos(ctx *Ctx) {
	members := ctx.Argv[2:]
	var (
		wrongTyp bool
		scores   []float64
		present  []bool
	)
	ok := ctx.view(func(db *keyspace.DB) error {
		sc, pr, hdr, found, err := zsetMemberScores(db, ctx.Argv[1], members)
		if err != nil {
			return err
		}
		if found && hdr.Type != keyspace.TypeZSet {
			wrongTyp = true
			return nil
		}
		scores, present = sc, pr
		return nil
	})
	if !ok {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	enc := ctx.enc()
	enc.WriteArrayLen(len(members))
	for i := range members {
		if !present[i] {
			enc.WriteNullArray()
			continue
		}
		lon, lat := geoDecode(scores[i])
		enc.WriteArrayLen(2)
		enc.WriteBulkStringStr(formatGeoFloat(lon))
		enc.WriteBulkStringStr(formatGeoFloat(lat))
	}
}

// handleGeoDist implements GEODIST key member1 member2 [unit]. It returns the
// distance in the requested unit, or nil when either member is missing.
func handleGeoDist(ctx *Ctx) {
	if len(ctx.Argv) > 5 {
		ctx.enc().WriteError("ERR syntax error")
		return
	}
	factor := 1.0
	if len(ctx.Argv) == 5 {
		f, ok := geoUnitToMeters(string(ctx.Argv[4]))
		if !ok {
			ctx.enc().WriteError(geoUnitError)
			return
		}
		factor = f
	}
	var (
		wrongTyp       bool
		s1, s2         float64
		found1, found2 bool
	)
	ok := ctx.view(func(db *keyspace.DB) error {
		sc, pr, hdr, found, err := zsetMemberScores(db, ctx.Argv[1], [][]byte{ctx.Argv[2], ctx.Argv[3]})
		if err != nil {
			return err
		}
		if found && hdr.Type != keyspace.TypeZSet {
			wrongTyp = true
			return nil
		}
		s1, found1 = sc[0], pr[0]
		s2, found2 = sc[1], pr[1]
		return nil
	})
	if !ok {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	if !found1 || !found2 {
		ctx.enc().WriteNull()
		return
	}
	lon1, lat1 := geoDecode(s1)
	lon2, lat2 := geoDecode(s2)
	d := haversineMeters(lon1, lat1, lon2, lat2) / factor
	ctx.enc().WriteBulkStringStr(strconv.FormatFloat(d, 'f', 4, 64))
}

// handleGeoHash implements GEOHASH key member [member ...]. Each member yields
// its 11-character base32 geohash, or nil when absent.
func handleGeoHash(ctx *Ctx) {
	members := ctx.Argv[2:]
	var (
		wrongTyp bool
		out      []string
		present  []bool
	)
	ok := ctx.view(func(db *keyspace.DB) error {
		sc, pr, hdr, found, err := zsetMemberScores(db, ctx.Argv[1], members)
		if err != nil {
			return err
		}
		if found && hdr.Type != keyspace.TypeZSet {
			wrongTyp = true
			return nil
		}
		out = make([]string, len(members))
		present = pr
		for i := range members {
			if pr[i] {
				out[i] = geoHashString(sc[i])
			}
		}
		return nil
	})
	if !ok {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	enc := ctx.enc()
	enc.WriteArrayLen(len(members))
	for i := range members {
		if present[i] {
			enc.WriteBulkStringStr(out[i])
		} else {
			enc.WriteNull()
		}
	}
}
