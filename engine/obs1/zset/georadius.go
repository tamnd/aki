package zset

// GEORADIUS and GEORADIUSBYMEMBER are the deprecated proximity commands (spec
// 2064/f3/15 section 12). They are argument-shape wrappers over the section 11
// covering-range engine: the center and radius are positional rather than the
// FROMLONLAT/BYRADIUS keywords GEOSEARCH uses, and a trailing STORE or STOREDIST
// turns the read into a write with a destination key, on the same F17 hop plan
// GEOSEARCHSTORE rides. GEORADIUS_RO and GEORADIUSBYMEMBER_RO are the read-only
// forms that refuse STORE, so a replica can route them safely.
//
// The compat corners the differential suite pins: STORE and STOREDIST are
// rejected together with any WITH flag, a COUNT without ANY still sorts, a
// BYMEMBER center that names a missing member is an error not an empty result,
// and the deprecated forms sort whenever COUNT is given while GEOSEARCH sorts
// only when asked (both fall out of the shared geoOrderCut).

import (
	"strconv"

	"github.com/tamnd/aki/engine/obs1/shard"
	"github.com/tamnd/aki/obs1srv/resp"
)

// radiusOpts is the parsed GEORADIUS option tail: the search options GEOSEARCH
// shares, plus the optional STORE/STOREDIST destination that makes the command a
// write. storeDist (inherited from geoSearchOpts) selects the distance score over
// the geohash score, exactly as it does for GEOSEARCHSTORE.
type radiusOpts struct {
	geoSearchOpts
	storeKey  []byte
	haveStore bool
}

// parseRadiusTail parses the option tail shared by GEORADIUS and
// GEORADIUSBYMEMBER, everything after the positional center and radius. ro, the
// read-only form, refuses STORE and STOREDIST. STORE combined with any WITH flag
// is the documented incompatibility error, and STORE and STOREDIST are last one
// wins the way Redis resolves a doubled destination.
func parseRadiusTail(tail [][]byte, ro bool) (radiusOpts, string) {
	var (
		opts      radiusOpts
		haveCount bool
		withAny   bool
	)
	i := 0
	for i < len(tail) {
		switch {
		case eqFold(tail[i], "WITHCOORD"):
			opts.withCord = true
			withAny = true
			i++
		case eqFold(tail[i], "WITHDIST"):
			opts.withDist = true
			withAny = true
			i++
		case eqFold(tail[i], "WITHHASH"):
			opts.withHash = true
			withAny = true
			i++
		case eqFold(tail[i], "ASC"):
			opts.sortAsc = true
			i++
		case eqFold(tail[i], "DESC"):
			opts.sortDesc = true
			i++
		case eqFold(tail[i], "COUNT"):
			if i+1 >= len(tail) {
				return opts, "ERR syntax error"
			}
			n, ok := parseIndex(tail[i+1])
			if !ok || n <= 0 {
				return opts, "ERR COUNT must be > 0"
			}
			opts.count = n
			haveCount = true
			i += 2
			if i < len(tail) && eqFold(tail[i], "ANY") {
				opts.any = true
				i++
			}
		case eqFold(tail[i], "STORE"):
			if ro || i+1 >= len(tail) {
				return opts, "ERR syntax error"
			}
			opts.storeKey = tail[i+1]
			opts.haveStore = true
			opts.storeDist = false
			i += 2
		case eqFold(tail[i], "STOREDIST"):
			if ro || i+1 >= len(tail) {
				return opts, "ERR syntax error"
			}
			opts.storeKey = tail[i+1]
			opts.haveStore = true
			opts.storeDist = true
			i += 2
		default:
			return opts, "ERR syntax error"
		}
	}
	if opts.any && !haveCount {
		return opts, "ERR syntax error"
	}
	if opts.sortAsc && opts.sortDesc {
		return opts, "ERR syntax error"
	}
	if opts.haveStore && withAny {
		return opts, "ERR STORE option in GEORADIUS is not compatible with WITHDIST, WITHHASH and WITHCOORD options"
	}
	return opts, ""
}

// geoRadiusShape builds a radius shape from the positional radius and unit,
// leaving the center for the caller to fill.
func geoRadiusShape(radiusArg, unitArg []byte) (geoShape, string) {
	var sh geoShape
	radius, ok := parseScore(radiusArg)
	if !ok {
		return sh, "ERR value is not a valid float"
	}
	unit, ok := geoUnit(unitArg)
	if !ok {
		return sh, errGeoUnit
	}
	sh.radiusM = radius * unit
	sh.toMeters = unit
	return sh, ""
}

// parseRadius resolves a GEORADIUS or GEORADIUSBYMEMBER command against the geo
// set z (needed to resolve a BYMEMBER center) into a shape and options, or a
// non-empty Redis error. It parses the option tail before resolving the center,
// so a malformed tail reports its own error ahead of a missing-member lookup.
func parseRadius(z *zset, args [][]byte, byMember, ro bool) (geoShape, radiusOpts, string) {
	var (
		sh         geoShape
		opts       radiusOpts
		tail       [][]byte
		member     []byte
		lon, lat   float64
		fromLonLat bool
	)
	if byMember {
		member = args[1]
		s, errMsg := geoRadiusShape(args[2], args[3])
		if errMsg != "" {
			return sh, opts, errMsg
		}
		sh = s
		tail = args[4:]
	} else {
		var ok1, ok2 bool
		lon, ok1 = parseScore(args[1])
		lat, ok2 = parseScore(args[2])
		if !ok1 || !ok2 {
			return sh, opts, "ERR value is not a valid float"
		}
		fromLonLat = true
		s, errMsg := geoRadiusShape(args[3], args[4])
		if errMsg != "" {
			return sh, opts, errMsg
		}
		sh = s
		tail = args[5:]
	}
	o, errMsg := parseRadiusTail(tail, ro)
	if errMsg != "" {
		return sh, opts, errMsg
	}
	opts = o
	if byMember {
		if z == nil {
			return sh, opts, errGeoNoMember
		}
		s, ok := z.score(member)
		if !ok {
			return sh, opts, errGeoNoMember
		}
		sh.lon, sh.lat = geoDecode(uint64(s))
	} else if fromLonLat {
		if lon < geoLonMin || lon > geoLonMax || lat < geoLatMin || lat > geoLatMax {
			var buf [64]byte
			msg := append(buf[:0], "ERR invalid longitude,latitude pair "...)
			msg = strconv.AppendFloat(msg, lon, 'f', 6, 64)
			msg = append(msg, ',')
			msg = strconv.AppendFloat(msg, lat, 'f', 6, 64)
			return sh, opts, string(msg)
		}
		sh.lon, sh.lat = lon, lat
	}
	return sh, opts, ""
}

// runRadius drives the shared read-or-store path for all four GEORADIUS forms on
// a single owner: resolve, search, and either emit the annotated reply or
// materialize the survivors into the STORE destination and reply the stored
// cardinality. The store branch runs only when the destination is co-located
// with the source; a shard-spanning destination diverts to radiusCross upstream.
func runRadius(cx *shard.Ctx, args [][]byte, r shard.Reply, byMember, ro bool) {
	g := registry(cx)
	z, wrong := g.lookup(cx, args[0])
	if wrong {
		r.Err(wrongType)
		return
	}
	sh, opts, errMsg := parseRadius(z, args, byMember, ro)
	if errMsg != "" {
		r.Err(errMsg)
		return
	}
	var hits []geoHit
	if z != nil {
		hits = geoSearchHits(z, sh)
	}
	if !opts.haveStore {
		geoReply(cx, r, hits, opts.geoSearchOpts, sh.toMeters)
		return
	}
	hits = geoOrderCut(hits, opts.geoSearchOpts)
	pairs := geoStorePairs(hits, opts.geoSearchOpts, sh.toMeters)
	n, err := place(cx, g, opts.storeKey, buildDest(pairs))
	if err != nil {
		r.Err(err.Error())
		return
	}
	r.Int(int64(n))
}

// Georadius answers GEORADIUS key lon lat radius unit [WITHCOORD] [WITHDIST]
// [WITHHASH] [COUNT n [ANY]] [ASC|DESC] [STORE key] [STOREDIST key].
func Georadius(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	runRadius(cx, args, r, false, false)
}

// GeoradiusRo answers GEORADIUS_RO, the read-only form that refuses STORE.
func GeoradiusRo(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	runRadius(cx, args, r, false, true)
}

// Georadiusbymember answers GEORADIUSBYMEMBER key member radius unit with the
// same option tail, centering on a stored member.
func Georadiusbymember(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	runRadius(cx, args, r, true, false)
}

// GeoradiusbymemberRo answers GEORADIUSBYMEMBER_RO, the read-only member form.
func GeoradiusbymemberRo(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	runRadius(cx, args, r, true, true)
}

// radiusStoreDest returns the STORE/STOREDIST destination key of a GEORADIUS
// command, or nil when the command has none. It reuses the option-tail parser so
// the token scan is positional, never fooled by a value that spells STORE, and
// treats a malformed tail as no destination so the handler reports the error.
func radiusStoreDest(args [][]byte, byMember bool) []byte {
	off := 5
	if byMember {
		off = 4
	}
	if len(args) < off {
		return nil
	}
	opts, errMsg := parseRadiusTail(args[off:], false)
	if errMsg != "" || !opts.haveStore {
		return nil
	}
	return opts.storeKey
}

// GeoradiusKeys and GeoradiusbymemberKeys route a GEORADIUS command: the source
// alone when it is a pure read, the source and the STORE destination when it
// writes, so the co-location check and the F17 intent span both keys.
func GeoradiusKeys(args [][]byte) [][]byte { return radiusKeys(args, false) }

// GeoradiusbymemberKeys is the BYMEMBER routing twin.
func GeoradiusbymemberKeys(args [][]byte) [][]byte { return radiusKeys(args, true) }

func radiusKeys(args [][]byte, byMember bool) [][]byte {
	dest := radiusStoreDest(args, byMember)
	if dest == nil {
		return args[:1]
	}
	return [][]byte{args[0], dest}
}

// radiusCross is the F17 route for a GEORADIUS ... STORE whose destination and
// source span shards: run the search on the source owner, place the result on
// the destination owner, both inside the intent that holds the two keys.
func radiusCross(t *shard.Txn, args [][]byte, byMember bool) []byte {
	src := args[0]
	dest := radiusStoreDest(args, byMember)
	var (
		pairs  []scoredMember
		errMsg string
	)
	t.Do(src, func(cx *shard.Ctx) {
		g := registry(cx)
		z, wrong := g.lookup(cx, src)
		if wrong {
			errMsg = wrongType
			return
		}
		sh, opts, e := parseRadius(z, args, byMember, false)
		if e != "" {
			errMsg = e
			return
		}
		var hits []geoHit
		if z != nil {
			hits = geoSearchHits(z, sh)
		}
		hits = geoOrderCut(hits, opts.geoSearchOpts)
		pairs = geoStorePairs(hits, opts.geoSearchOpts, sh.toMeters)
	})
	if errMsg != "" {
		return resp.AppendError(nil, errMsg)
	}
	var (
		n      int
		logErr error
	)
	t.Do(dest, func(cx *shard.Ctx) {
		n, logErr = place(cx, registry(cx), dest, buildDest(pairs))
	})
	if logErr != nil {
		return resp.AppendError(nil, logErr.Error())
	}
	return resp.AppendInt(nil, int64(n))
}

// GeoradiusCross and GeoradiusbymemberCross are the cross-shard STORE routes.
func GeoradiusCross(t *shard.Txn, args [][]byte) []byte { return radiusCross(t, args, false) }

// GeoradiusbymemberCross is the BYMEMBER cross-shard STORE route.
func GeoradiusbymemberCross(t *shard.Txn, args [][]byte) []byte {
	return radiusCross(t, args, true)
}
