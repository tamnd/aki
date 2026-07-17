package sqlo1

// The geo family's command half: GEOADD, GEOPOS, GEODIST, GEOHASH,
// GEOSEARCH, GEOSEARCHSTORE, and the GEORADIUS compat forms. Every
// door here was probed live against Redis 8.8.0 rather than
// recalled: errors surface in token order (a FROMMEMBER decode
// failure outranks a later bad unit), an absent search key parses
// its options fully but answers empty, a FROMMEMBER lookup on an
// absent key is the empty answer while the same lookup on a live key
// is the decode error, ASC and DESC are last-wins, ANY is legal
// before its COUNT, and when GEORADIUS carries both STORE and
// STOREDIST only the dist arm lands. Distances print at four
// decimals in the search's unit; coordinates print at Go's shortest
// round-trip, matching Redis 8's dtoa. COUNT without a sort still
// sorts ascending, Redis's nearest-first trim.

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
)

// geoHit is one collected search match: the member's span in the geo
// arena (copied, because walk bytes die as the cells advance), the
// raw cell bits, the decoded position, and the center distance in
// meters.
type geoHit struct {
	off, end int
	dist     float64
	bits     uint64
	lon, lat float64
}

const geoUnitErrMsg = "ERR unsupported unit provided. please use M, KM, FT, MI"

func appendGeoCoord(dst []byte, f float64) []byte {
	return strconv.AppendFloat(dst, f, 'g', -1, 64)
}

func appendGeoDist(dst []byte, f float64) []byte {
	return strconv.AppendFloat(dst, f, 'f', 4, 64)
}

// parseGeoUnit answers the meters per unit.
func parseGeoUnit(arg []byte) (float64, bool) {
	switch strings.ToLower(string(arg)) {
	case "m":
		return 1, true
	case "km":
		return 1000, true
	case "ft":
		return 0.3048, true
	case "mi":
		return 1609.34, true
	}
	return 0, false
}

// parseGeoLonLat parses and range-checks a coordinate pair; a
// non-empty errMsg is the wire error.
func parseGeoLonLat(lonArg, latArg []byte) (lon, lat float64, errMsg string) {
	lon, e1 := strconv.ParseFloat(string(lonArg), 64)
	lat, e2 := strconv.ParseFloat(string(latArg), 64)
	if e1 != nil || e2 != nil || math.IsNaN(lon) || math.IsNaN(lat) {
		return 0, 0, "ERR value is not a valid float"
	}
	if lon < geoLonMin || lon > geoLonMax || lat < geoLatMin || lat > geoLatMax {
		return 0, 0, fmt.Sprintf("ERR invalid longitude,latitude pair %.6f,%.6f", lon, lat)
	}
	return lon, lat, ""
}

// geoaddCmd is GEOADD: NX, XX, CH, then lon lat member triples. All
// triples parse and encode before any write, the ZADD discipline, and
// the flag doors are GEOADD's own: NX with XX is a plain syntax
// error, as is a triple count that does not divide by three.
func (s *Server) geoaddCmd(ctx context.Context, reply []byte, args [][]byte) []byte {
	if len(args) < 5 {
		return arityErr(reply, "GEOADD")
	}
	var f ZAddFlags
	ch := false
	i := 2
flags:
	for ; i < len(args); i++ {
		switch strings.ToUpper(string(args[i])) {
		case "NX":
			f.NX = true
		case "XX":
			f.XX = true
		case "CH":
			ch = true
		default:
			break flags
		}
	}
	rest := args[i:]
	if len(rest) == 0 || len(rest)%3 != 0 || (f.NX && f.XX) {
		return syntaxErr(reply)
	}
	s.zscores = s.zscores[:0]
	for j := 0; j < len(rest); j += 3 {
		lon, lat, msg := parseGeoLonLat(rest[j], rest[j+1])
		if msg != "" {
			return AppendError(reply, msg)
		}
		s.zscores = append(s.zscores, float64(geoEncode(lon, lat)))
	}
	added, touched := int64(0), int64(0)
	for j := 0; j < len(rest); j += 3 {
		a, c, _, _, err := s.z.ZAdd(ctx, args[1], rest[j+2], s.zscores[j/3], f)
		if err != nil {
			return storeErr(reply, err)
		}
		if a {
			added++
			touched++
		} else if c {
			touched++
		}
	}
	if ch {
		return AppendInt(reply, touched)
	}
	return AppendInt(reply, added)
}

// geoposCmd is GEOPOS: a coordinate pair per member, a null array
// for a miss, and legal with no members at all.
func (s *Server) geoposCmd(ctx context.Context, reply []byte, args [][]byte) []byte {
	if len(args) < 2 {
		return arityErr(reply, "GEOPOS")
	}
	mark := len(reply)
	reply = AppendArray(reply, len(args)-2)
	var cb [32]byte
	for _, m := range args[2:] {
		sc, ok, err := s.z.ZScore(ctx, args[1], m)
		if err != nil {
			return storeErr(reply[:mark], err)
		}
		if !ok {
			reply = AppendNullArray(reply)
			continue
		}
		lon, lat := geoDecode(uint64(sc) & geoBits)
		reply = AppendArray(reply, 2)
		reply = AppendBulk(reply, appendGeoCoord(cb[:0], lon))
		reply = AppendBulk(reply, appendGeoCoord(cb[:0], lat))
	}
	return reply
}

// geodistCmd is GEODIST: two decodes and the haversine, nil when
// either member is missing, four decimals in the requested unit.
func (s *Server) geodistCmd(ctx context.Context, reply []byte, args [][]byte) []byte {
	if len(args) < 4 {
		return arityErr(reply, "GEODIST")
	}
	if len(args) > 5 {
		return syntaxErr(reply)
	}
	conv := 1.0
	if len(args) == 5 {
		var ok bool
		if conv, ok = parseGeoUnit(args[4]); !ok {
			return AppendError(reply, geoUnitErrMsg)
		}
	}
	s1, ok1, err := s.z.ZScore(ctx, args[1], args[2])
	if err != nil {
		return storeErr(reply, err)
	}
	s2, ok2, err := s.z.ZScore(ctx, args[1], args[3])
	if err != nil {
		return storeErr(reply, err)
	}
	if !ok1 || !ok2 {
		return AppendNullBulk(reply)
	}
	lon1, lat1 := geoDecode(uint64(s1) & geoBits)
	lon2, lat2 := geoDecode(uint64(s2) & geoBits)
	var fb [32]byte
	return AppendBulk(reply, appendGeoDist(fb[:0], geoDist(lon1, lat1, lon2, lat2)/conv))
}

// geohashCmd is GEOHASH: decode the score, re-encode at step 26 over
// the world ranges, and print eleven base32 chars, the last always
// the alphabet's zero because 52 bits only fill ten.
func (s *Server) geohashCmd(ctx context.Context, reply []byte, args [][]byte) []byte {
	if len(args) < 2 {
		return arityErr(reply, "GEOHASH")
	}
	mark := len(reply)
	reply = AppendArray(reply, len(args)-2)
	for _, m := range args[2:] {
		sc, ok, err := s.z.ZScore(ctx, args[1], m)
		if err != nil {
			return storeErr(reply[:mark], err)
		}
		if !ok {
			reply = AppendNullBulk(reply)
			continue
		}
		lon, lat := geoDecode(uint64(sc) & geoBits)
		wb := geoEncodeRanges(lon, lat, -180, 180, -90, 90)
		var hb [11]byte
		for i := range 10 {
			hb[i] = geoAlpha[(wb>>(52-uint(i+1)*5))&0x1f]
		}
		hb[10] = geoAlpha[0]
		reply = AppendBulk(reply, hb[:])
	}
	return reply
}

// geoSearchState is one search command's resolved arguments: the
// shape in meters, the output unit divisor, the reply flags, and the
// store target when a STORE form runs.
type geoSearchState struct {
	key      []byte
	sh       geoShape
	haveFrom bool
	haveBy   bool

	// emptyKey short-circuits execution to the empty answer: a
	// FROMMEMBER lookup on an absent key, per the probed door.
	emptyKey bool

	conv      float64
	sortDir   int // 0 none, 1 asc, 2 desc; last token wins
	count     int64
	any       bool
	withCoord bool
	withDist  bool
	withHash  bool
	storeKey  []byte
	storeDist bool
}

// geoFromMember resolves a FROMMEMBER (or GEORADIUSBYMEMBER) center:
// the decode error only fires on a live key, an absent key is the
// empty answer with the rest of the grammar still validated.
func (s *Server) geoFromMember(ctx context.Context, st *geoSearchState, member []byte) (errMsg string, err error) {
	sc, ok, err := s.z.ZScore(ctx, st.key, member)
	if err != nil {
		return "", err
	}
	if !ok {
		card, err := s.z.ZCard(ctx, st.key)
		if err != nil {
			return "", err
		}
		if card > 0 {
			return "ERR could not decode requested zset member", nil
		}
		st.emptyKey = true
		return "", nil
	}
	st.sh.lon, st.sh.lat = geoDecode(uint64(sc) & geoBits)
	return "", nil
}

// geoParseOpts parses the option tokens from args[i:] in token
// order, resolving FROMMEMBER inline the way Redis does so error
// precedence matches the probed doors. fromBy admits the GEOSEARCH
// tokens, storeKeyed the GEORADIUS STORE key forms, storeFlag the
// bare GEOSEARCHSTORE STOREDIST.
func (s *Server) geoParseOpts(ctx context.Context, st *geoSearchState, args [][]byte, i int, fromBy, storeKeyed, storeFlag bool) (errMsg string, err error) {
	for ; i < len(args); i++ {
		switch strings.ToUpper(string(args[i])) {
		case "FROMMEMBER":
			if !fromBy || st.haveFrom || i+1 >= len(args) {
				return "ERR syntax error", nil
			}
			if errMsg, err = s.geoFromMember(ctx, st, args[i+1]); errMsg != "" || err != nil {
				return errMsg, err
			}
			st.haveFrom = true
			i++
		case "FROMLONLAT":
			if !fromBy || st.haveFrom || i+2 >= len(args) {
				return "ERR syntax error", nil
			}
			lon, lat, msg := parseGeoLonLat(args[i+1], args[i+2])
			if msg != "" {
				return msg, nil
			}
			st.sh.lon, st.sh.lat = lon, lat
			st.haveFrom = true
			i += 2
		case "BYRADIUS":
			if !fromBy || st.haveBy || i+2 >= len(args) {
				return "ERR syntax error", nil
			}
			r, e := strconv.ParseFloat(string(args[i+1]), 64)
			if e != nil || math.IsNaN(r) {
				return "ERR need numeric radius", nil
			}
			if r < 0 {
				return "ERR radius cannot be negative", nil
			}
			conv, ok := parseGeoUnit(args[i+2])
			if !ok {
				return geoUnitErrMsg, nil
			}
			st.sh.radius, st.conv = r*conv, conv
			st.haveBy = true
			i += 2
		case "BYBOX":
			if !fromBy || st.haveBy || i+3 >= len(args) {
				return "ERR syntax error", nil
			}
			w, e1 := strconv.ParseFloat(string(args[i+1]), 64)
			if e1 != nil || math.IsNaN(w) {
				return "ERR need numeric width", nil
			}
			h, e2 := strconv.ParseFloat(string(args[i+2]), 64)
			if e2 != nil || math.IsNaN(h) {
				return "ERR need numeric height", nil
			}
			if w < 0 || h < 0 {
				return "ERR height or width cannot be negative", nil
			}
			conv, ok := parseGeoUnit(args[i+3])
			if !ok {
				return geoUnitErrMsg, nil
			}
			st.sh.byBox, st.sh.w, st.sh.h, st.conv = true, w*conv, h*conv, conv
			st.haveBy = true
			i += 3
		case "ASC":
			st.sortDir = 1
		case "DESC":
			st.sortDir = 2
		case "COUNT":
			if i+1 >= len(args) {
				return "ERR syntax error", nil
			}
			n, e := strconv.ParseInt(string(args[i+1]), 10, 64)
			if e != nil {
				return "ERR value is not an integer or out of range", nil
			}
			if n <= 0 {
				return "ERR COUNT must be > 0", nil
			}
			st.count = n
			i++
		case "ANY":
			st.any = true
		case "WITHCOORD":
			st.withCoord = true
		case "WITHDIST":
			st.withDist = true
		case "WITHHASH":
			st.withHash = true
		case "STORE":
			if !storeKeyed || i+1 >= len(args) {
				return "ERR syntax error", nil
			}
			st.storeKey = args[i+1]
			i++
		case "STOREDIST":
			if storeFlag {
				st.storeDist = true
			} else if !storeKeyed || i+1 >= len(args) {
				return "ERR syntax error", nil
			} else {
				st.storeKey, st.storeDist = args[i+1], true
				i++
			}
		default:
			return "ERR syntax error", nil
		}
	}
	return "", nil
}

// geosearchCmd is GEOSEARCH and GEOSEARCHSTORE: the FROM and BY
// grammar with the exactly-one checks, the ANY-needs-COUNT check,
// and the STORE form's WITH rejection, in Redis's order.
func (s *Server) geosearchCmd(ctx context.Context, reply []byte, args [][]byte, store bool) []byte {
	cmd, base, minA := "GEOSEARCH", 2, 7
	if store {
		cmd, base, minA = "GEOSEARCHSTORE", 3, 8
	}
	if len(args) < minA {
		return arityErr(reply, cmd)
	}
	st := geoSearchState{key: args[base-1], conv: 1}
	errMsg, err := s.geoParseOpts(ctx, &st, args, base, true, false, store)
	if err != nil {
		return storeErr(reply, err)
	}
	if errMsg != "" {
		return AppendError(reply, errMsg)
	}
	lc := strings.ToLower(cmd)
	if !st.haveFrom {
		return AppendError(reply, "ERR exactly one of FROMMEMBER or FROMLONLAT can be specified for "+lc)
	}
	if !st.haveBy {
		return AppendError(reply, "ERR exactly one of BYRADIUS and BYBOX can be specified for "+lc)
	}
	if st.any && st.count == 0 {
		return AppendError(reply, "ERR the ANY argument requires COUNT argument")
	}
	if store {
		if st.withCoord || st.withDist || st.withHash {
			return AppendError(reply, "ERR GEOSEARCHSTORE is not compatible with WITHDIST, WITHHASH and WITHCOORD options")
		}
		st.storeKey = args[1]
	}
	return s.geoExec(ctx, reply, &st)
}

// georadiusCmd is the GEORADIUS compat family: positional center and
// radius, then the shared option grammar, with STORE and STOREDIST
// keyed and barred from the _RO forms.
func (s *Server) georadiusCmd(ctx context.Context, reply []byte, args [][]byte, cmd string, byMember, ro bool) []byte {
	minA, ri := 6, 4
	if byMember {
		minA, ri = 5, 3
	}
	if len(args) < minA {
		return arityErr(reply, cmd)
	}
	st := geoSearchState{key: args[1], conv: 1}
	if byMember {
		errMsg, err := s.geoFromMember(ctx, &st, args[2])
		if err != nil {
			return storeErr(reply, err)
		}
		if errMsg != "" {
			return AppendError(reply, errMsg)
		}
	} else {
		lon, lat, msg := parseGeoLonLat(args[2], args[3])
		if msg != "" {
			return AppendError(reply, msg)
		}
		st.sh.lon, st.sh.lat = lon, lat
	}
	r, e := strconv.ParseFloat(string(args[ri]), 64)
	if e != nil || math.IsNaN(r) {
		return AppendError(reply, "ERR need numeric radius")
	}
	if r < 0 {
		return AppendError(reply, "ERR radius cannot be negative")
	}
	conv, ok := parseGeoUnit(args[ri+1])
	if !ok {
		return AppendError(reply, geoUnitErrMsg)
	}
	st.sh.radius, st.conv = r*conv, conv
	errMsg, err := s.geoParseOpts(ctx, &st, args, ri+2, false, !ro, false)
	if err != nil {
		return storeErr(reply, err)
	}
	if errMsg != "" {
		return AppendError(reply, errMsg)
	}
	if st.storeKey != nil && (st.withCoord || st.withDist || st.withHash) {
		return AppendError(reply, "ERR STORE option in GEORADIUS is not compatible with WITHDIST, WITHHASH and WITHCOORD options")
	}
	if st.any && st.count == 0 {
		return AppendError(reply, "ERR the ANY argument requires COUNT argument")
	}
	return s.geoExec(ctx, reply, &st)
}

// geoExec runs the resolved search: collect (short-circuiting at
// COUNT under ANY), sort when asked or when COUNT trims without ANY
// (Redis's nearest-first trim), then reply or land on the store
// target through the shared bulk build, dest deleted on an empty
// result.
func (s *Server) geoExec(ctx context.Context, reply []byte, st *geoSearchState) []byte {
	s.geoArena = s.geoArena[:0]
	s.geoHits = s.geoHits[:0]
	if !st.emptyKey {
		err := s.z.GeoSearch(ctx, st.key, st.sh, func(m []byte, bits uint64, lon, lat, dist float64) bool {
			off := len(s.geoArena)
			s.geoArena = append(s.geoArena, m...)
			s.geoHits = append(s.geoHits, geoHit{off: off, end: len(s.geoArena), dist: dist, bits: bits, lon: lon, lat: lat})
			return !st.any || int64(len(s.geoHits)) < st.count
		})
		if err != nil {
			return storeErr(reply, err)
		}
	}
	hits := s.geoHits
	if st.sortDir != 0 || (st.count > 0 && !st.any) {
		desc := st.sortDir == 2
		sort.Slice(hits, func(i, j int) bool {
			if hits[i].dist != hits[j].dist {
				if desc {
					return hits[i].dist > hits[j].dist
				}
				return hits[i].dist < hits[j].dist
			}
			return bytes.Compare(s.geoArena[hits[i].off:hits[i].end], s.geoArena[hits[j].off:hits[j].end]) < 0
		})
	}
	if st.count > 0 && int64(len(hits)) > st.count {
		hits = hits[:st.count]
	}
	if st.storeKey != nil {
		s.zrpairs = s.zrpairs[:0]
		for _, h := range hits {
			sc := float64(h.bits)
			if st.storeDist {
				sc = h.dist / st.conv
			}
			s.zrpairs = append(s.zrpairs, zbuildPair{s: zScoreSortable(sc), off: h.off, end: h.end})
		}
		sort.Sort(&zpairSorter{pairs: s.zrpairs, arena: s.geoArena})
		n, err := s.z.zalgStore(ctx, st.storeKey, s.zrpairs, s.geoArena)
		if err != nil {
			return storeErr(reply, err)
		}
		return AppendInt(reply, n)
	}
	if !st.withCoord && !st.withDist && !st.withHash {
		reply = AppendArray(reply, len(hits))
		for _, h := range hits {
			reply = AppendBulk(reply, s.geoArena[h.off:h.end])
		}
		return reply
	}
	inner := 1
	if st.withDist {
		inner++
	}
	if st.withHash {
		inner++
	}
	if st.withCoord {
		inner++
	}
	reply = AppendArray(reply, len(hits))
	var fb [32]byte
	for _, h := range hits {
		reply = AppendArray(reply, inner)
		reply = AppendBulk(reply, s.geoArena[h.off:h.end])
		if st.withDist {
			reply = AppendBulk(reply, appendGeoDist(fb[:0], h.dist/st.conv))
		}
		if st.withHash {
			reply = AppendInt(reply, int64(h.bits))
		}
		if st.withCoord {
			reply = AppendArray(reply, 2)
			reply = AppendBulk(reply, appendGeoCoord(fb[:0], h.lon))
			reply = AppendBulk(reply, appendGeoCoord(fb[:0], h.lat))
		}
	}
	return reply
}
