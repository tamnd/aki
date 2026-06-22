package command

import (
	"sort"
	"strconv"
	"strings"

	"github.com/tamnd/aki/keyspace"
)

// geoShape is the search area: a circle (BYRADIUS) or a rectangle (BYBOX).
type geoShape struct {
	byBox  bool
	radius float64 // meters, for a circle
	width  float64 // meters, for a box
	height float64 // meters, for a box
}

// geoQuery holds the parsed form common to GEOSEARCH, GEOSEARCHSTORE and the
// GEORADIUS family.
type geoQuery struct {
	fromMember []byte // set when the center is a member
	fromLon    float64
	fromLat    float64
	haveShape  bool
	haveCenter bool
	shape      geoShape

	withCoord bool
	withDist  bool
	withHash  bool

	hasCount bool
	count    int
	any      bool

	sortDir int // 0 none, 1 asc, -1 desc
	unitDiv float64

	store     []byte // GEOSEARCHSTORE dest / GEORADIUS STORE key
	storeDist bool
}

// geoHit is one matching member with everything a reply or a store needs.
type geoHit struct {
	member []byte
	score  float64
	dist   float64 // meters
	lon    float64
	lat    float64
}

// geoInRadius reports whether a point is within the circle and its distance.
func geoInRadius(centerLon, centerLat, lon, lat, radius float64) (float64, bool) {
	d := haversineMeters(centerLon, centerLat, lon, lat)
	return d, d <= radius
}

// geoInBox reports whether a point is within the rectangle and its distance from
// the center. It follows the per-axis test Redis uses so the boundary matches.
func geoInBox(centerLon, centerLat, lon, lat, width, height float64) (float64, bool) {
	latDist := haversineMeters(lon, centerLat, lon, lat)
	if latDist > height/2 {
		return 0, false
	}
	lonDist := haversineMeters(centerLon, lat, lon, lat)
	if lonDist > width/2 {
		return 0, false
	}
	return haversineMeters(centerLon, centerLat, lon, lat), true
}

// runGeoSearch scans every member of the source set and returns the matches. The
// caller has already resolved the center coordinate.
func runGeoSearch(members []zmember, q *geoQuery) []geoHit {
	hits := make([]geoHit, 0)
	for _, m := range members {
		lon, lat := geoDecode(m.score)
		var dist float64
		var ok bool
		if q.shape.byBox {
			dist, ok = geoInBox(q.fromLon, q.fromLat, lon, lat, q.shape.width, q.shape.height)
		} else {
			dist, ok = geoInRadius(q.fromLon, q.fromLat, lon, lat, q.shape.radius)
		}
		if !ok {
			continue
		}
		hits = append(hits, geoHit{member: m.member, score: m.score, dist: dist, lon: lon, lat: lat})
	}

	dir := q.sortDir
	// COUNT without ANY and without an explicit order still sorts nearest first,
	// so the truncation keeps the closest matches.
	if dir == 0 && q.hasCount && !q.any {
		dir = 1
	}
	if dir != 0 {
		sort.SliceStable(hits, func(i, j int) bool {
			if dir == 1 {
				return hits[i].dist < hits[j].dist
			}
			return hits[i].dist > hits[j].dist
		})
	}
	if q.hasCount && q.count < len(hits) {
		hits = hits[:q.count]
	}
	return hits
}

// parseGeoSearchOpts parses the option tail shared by the search commands,
// starting at args[start]. center and shape parsing is controlled by the flags,
// because GEORADIUS fixes them positionally while GEOSEARCH names them.
func parseGeoSearchOpts(q *geoQuery, args [][]byte) bool {
	for i := 0; i < len(args); i++ {
		switch strings.ToUpper(string(args[i])) {
		case "FROMMEMBER":
			if i+1 >= len(args) {
				return false
			}
			q.fromMember = args[i+1]
			q.haveCenter = true
			i++
		case "FROMLONLAT":
			if i+2 >= len(args) {
				return false
			}
			lon, lonOK := parseFloatArg(args[i+1])
			lat, latOK := parseFloatArg(args[i+2])
			if !lonOK || !latOK {
				return false
			}
			q.fromLon, q.fromLat = lon, lat
			q.haveCenter = true
			i += 2
		case "BYRADIUS":
			if i+2 >= len(args) {
				return false
			}
			r, rOK := parseFloatArg(args[i+1])
			div, unitOK := geoUnitToMeters(string(args[i+2]))
			if !rOK || !unitOK {
				return false
			}
			q.shape = geoShape{radius: r * div}
			q.unitDiv = div
			q.haveShape = true
			i += 2
		case "BYBOX":
			if i+3 >= len(args) {
				return false
			}
			w, wOK := parseFloatArg(args[i+1])
			h, hOK := parseFloatArg(args[i+2])
			div, unitOK := geoUnitToMeters(string(args[i+3]))
			if !wOK || !hOK || !unitOK {
				return false
			}
			q.shape = geoShape{byBox: true, width: w * div, height: h * div}
			q.unitDiv = div
			q.haveShape = true
			i += 3
		case "ASC":
			q.sortDir = 1
		case "DESC":
			q.sortDir = -1
		case "COUNT":
			if i+1 >= len(args) {
				return false
			}
			n, err := strconv.Atoi(string(args[i+1]))
			if err != nil || n <= 0 {
				return false
			}
			q.count = n
			q.hasCount = true
			i++
			if i+1 < len(args) && strings.EqualFold(string(args[i+1]), "ANY") {
				q.any = true
				i++
			}
		case "WITHCOORD":
			q.withCoord = true
		case "WITHDIST":
			q.withDist = true
		case "WITHHASH":
			q.withHash = true
		case "STOREDIST":
			q.storeDist = true
		default:
			return false
		}
	}
	return true
}

// resolveCenter fills q.fromLon/q.fromLat when the center was given as a member.
// It returns ok=false when that member is missing.
func resolveCenter(members []zmember, q *geoQuery) bool {
	if q.fromMember == nil {
		return true
	}
	idx := zsetFind(members, q.fromMember)
	if idx < 0 {
		return false
	}
	q.fromLon, q.fromLat = geoDecode(members[idx].score)
	return true
}

// writeGeoReply renders the GEOSEARCH/GEORADIUS reply for the given hits.
func writeGeoReply(ctx *Ctx, q *geoQuery, hits []geoHit) {
	enc := ctx.enc()
	withAny := q.withCoord || q.withDist || q.withHash
	enc.WriteArrayLen(len(hits))
	for _, h := range hits {
		if !withAny {
			enc.WriteBulkString(h.member)
			continue
		}
		n := 1
		if q.withDist {
			n++
		}
		if q.withHash {
			n++
		}
		if q.withCoord {
			n++
		}
		enc.WriteArrayLen(n)
		enc.WriteBulkString(h.member)
		if q.withDist {
			enc.WriteBulkStringStr(strconv.FormatFloat(h.dist/q.unitDiv, 'f', 4, 64))
		}
		if q.withHash {
			enc.WriteInteger(int64(h.score))
		}
		if q.withCoord {
			enc.WriteArrayLen(2)
			enc.WriteBulkStringStr(formatGeoFloat(h.lon))
			enc.WriteBulkStringStr(formatGeoFloat(h.lat))
		}
	}
}

// storeGeoResult writes the hits to the destination key as a sorted set and
// returns the count. STOREDIST stores the distance as the score, otherwise the
// geohash score is kept.
func storeGeoResult(lim encLimits, db *keyspace.DB, dest []byte, q *geoQuery, hits []geoHit) error {
	if len(hits) == 0 {
		_, err := db.Delete(dest)
		return err
	}
	out := make([]zmember, 0, len(hits))
	for _, h := range hits {
		score := h.score
		if q.storeDist {
			score = h.dist / q.unitDiv
		}
		out = append(out, zmember{member: h.member, score: score})
	}
	zsetSort(out)
	return db.Set(dest, zsetEncode(out), keyspace.TypeZSet, zsetEncoding(lim, out, keyspace.EncListpack), -1)
}

// handleGeoSearch implements the read-only GEOSEARCH.
func handleGeoSearch(ctx *Ctx) {
	q := &geoQuery{}
	if !parseGeoSearchOpts(q, ctx.Argv[2:]) || !q.haveCenter || !q.haveShape {
		ctx.enc().WriteError("ERR syntax error")
		return
	}
	var (
		wrongTyp bool
		noMember bool
		hits     []geoHit
	)
	ok := ctx.view(func(db *keyspace.DB) error {
		set, hdr, found, err := getZSet(db, ctx.Argv[1])
		if err != nil {
			return err
		}
		if found && hdr.Type != keyspace.TypeZSet {
			wrongTyp = true
			return nil
		}
		if !resolveCenter(set, q) {
			noMember = true
			return nil
		}
		hits = runGeoSearch(set, q)
		return nil
	})
	if !ok {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	if noMember {
		ctx.enc().WriteError("ERR could not decode requested zset member")
		return
	}
	writeGeoReply(ctx, q, hits)
}

// handleGeoSearchStore implements GEOSEARCHSTORE dest src ...
func handleGeoSearchStore(ctx *Ctx) {
	q := &geoQuery{}
	if !parseGeoSearchOpts(q, ctx.Argv[3:]) || !q.haveCenter || !q.haveShape {
		ctx.enc().WriteError("ERR syntax error")
		return
	}
	dest := ctx.Argv[1]
	src := ctx.Argv[2]
	var (
		wrongTyp bool
		noMember bool
		stored   int
	)
	done := ctx.update(func(db *keyspace.DB) error {
		set, hdr, found, err := getZSet(db, src)
		if err != nil {
			return err
		}
		if found && hdr.Type != keyspace.TypeZSet {
			wrongTyp = true
			return nil
		}
		if !resolveCenter(set, q) {
			noMember = true
			return nil
		}
		hits := runGeoSearch(set, q)
		stored = len(hits)
		return storeGeoResult(ctx.encLimits(), db, dest, q, hits)
	})
	if !done {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	if noMember {
		ctx.enc().WriteError("ERR could not decode requested zset member")
		return
	}
	ctx.enc().WriteInteger(int64(stored))
}

// parseGeoRadius parses the positional GEORADIUS family. byMember controls
// whether the center is read as a member name or as a lon/lat pair. store is
// returned through the query so the write variants can persist results.
func parseGeoRadius(q *geoQuery, args [][]byte, byMember bool, allowStore bool) bool {
	i := 0
	if byMember {
		if len(args) < 1 {
			return false
		}
		q.fromMember = args[0]
		q.haveCenter = true
		i = 1
	} else {
		if len(args) < 2 {
			return false
		}
		lon, lonOK := parseFloatArg(args[0])
		lat, latOK := parseFloatArg(args[1])
		if !lonOK || !latOK {
			return false
		}
		q.fromLon, q.fromLat = lon, lat
		q.haveCenter = true
		i = 2
	}
	if i+1 >= len(args) {
		return false
	}
	r, rOK := parseFloatArg(args[i])
	div, unitOK := geoUnitToMeters(string(args[i+1]))
	if !rOK || !unitOK {
		return false
	}
	q.shape = geoShape{radius: r * div}
	q.unitDiv = div
	q.haveShape = true
	i += 2

	for ; i < len(args); i++ {
		switch strings.ToUpper(string(args[i])) {
		case "WITHCOORD":
			q.withCoord = true
		case "WITHDIST":
			q.withDist = true
		case "WITHHASH":
			q.withHash = true
		case "ASC":
			q.sortDir = 1
		case "DESC":
			q.sortDir = -1
		case "COUNT":
			if i+1 >= len(args) {
				return false
			}
			n, err := strconv.Atoi(string(args[i+1]))
			if err != nil || n <= 0 {
				return false
			}
			q.count = n
			q.hasCount = true
			i++
			if i+1 < len(args) && strings.EqualFold(string(args[i+1]), "ANY") {
				q.any = true
				i++
			}
		case "STORE":
			if !allowStore || i+1 >= len(args) {
				return false
			}
			q.store = args[i+1]
			i++
		case "STOREDIST":
			if !allowStore || i+1 >= len(args) {
				return false
			}
			q.store = args[i+1]
			q.storeDist = true
			i++
		default:
			return false
		}
	}
	return true
}

// runGeoRadius is the shared body of the four GEORADIUS variants.
func runGeoRadius(ctx *Ctx, args [][]byte, byMember, allowStore bool) {
	q := &geoQuery{}
	if !parseGeoRadius(q, args, byMember, allowStore) {
		ctx.enc().WriteError("ERR syntax error")
		return
	}
	if q.store != nil && (q.withCoord || q.withDist || q.withHash) {
		ctx.enc().WriteError("ERR STORE option in GEORADIUS is not compatible with WITHCOORD, WITHDIST and WITHHASH options")
		return
	}

	var (
		wrongTyp bool
		noMember bool
		hits     []geoHit
		stored   int
	)
	run := func(db *keyspace.DB) error {
		set, hdr, found, err := getZSet(db, ctx.Argv[1])
		if err != nil {
			return err
		}
		if found && hdr.Type != keyspace.TypeZSet {
			wrongTyp = true
			return nil
		}
		if !resolveCenter(set, q) {
			noMember = true
			return nil
		}
		hits = runGeoSearch(set, q)
		stored = len(hits)
		if q.store != nil {
			return storeGeoResult(ctx.encLimits(), db, q.store, q, hits)
		}
		return nil
	}

	var ok bool
	if q.store != nil {
		ok = ctx.update(run)
	} else {
		ok = ctx.view(run)
	}
	if !ok {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	if noMember {
		ctx.enc().WriteError("ERR could not decode requested zset member")
		return
	}
	if q.store != nil {
		ctx.enc().WriteInteger(int64(stored))
		return
	}
	writeGeoReply(ctx, q, hits)
}

func handleGeoRadius(ctx *Ctx)           { runGeoRadius(ctx, ctx.Argv[2:], false, true) }
func handleGeoRadiusRO(ctx *Ctx)         { runGeoRadius(ctx, ctx.Argv[2:], false, false) }
func handleGeoRadiusByMember(ctx *Ctx)   { runGeoRadius(ctx, ctx.Argv[2:], true, true) }
func handleGeoRadiusByMemberRO(ctx *Ctx) { runGeoRadius(ctx, ctx.Argv[2:], true, false) }
