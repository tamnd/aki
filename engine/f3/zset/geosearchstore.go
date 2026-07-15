package zset

// GEOSEARCHSTORE writes a GEOSEARCH result into a destination geo set (spec
// 2064/f3/15 section 11). It runs the same covering-range engine the read path
// does, then instead of formatting an annotated reply it materializes the
// survivors as a fresh sorted set: each member keeps its original geohash score
// under STORE, or takes its distance from the center under STOREDIST, so the
// destination is a geo set again in the first case and a plain distance-ranked
// set in the second.
//
// Destination and source are two keys. The co-located case, the {tag}-hashed and
// single-shard norm, reads the source and writes the destination on one owner
// through the registry, the way ZDIFFSTORE does. A destination and source that
// span shards take the F17 intent route: the search runs on the source's owner
// and the placement on the destination's, both inside the transaction that holds
// both keys.

import (
	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/f3srv/resp"
)

// geoStorePairs turns the ordered survivors into the score-sorted member pairs
// buildDest installs. The score is the original geohash under STORE, or the
// distance from the center in the requested unit under STOREDIST. It returns nil
// for an empty result, which place turns into a destination delete.
func geoStorePairs(hits []geoHit, opts geoSearchOpts, toMeters float64) []scoredMember {
	if len(hits) == 0 {
		return nil
	}
	pairs := make([]scoredMember, len(hits))
	for i, h := range hits {
		s := float64(h.score)
		if opts.storeDist {
			s = h.distM / toMeters
		}
		pairs[i] = scoredMember{member: h.member, score: s}
	}
	sortByScore(pairs)
	return pairs
}

// geoStoreResult runs the parse, search, order, and pair build shared by the
// co-located and cross store paths against the source registry g. It returns the
// pairs to place and a non-empty Redis error on a malformed request or a
// wrong-type source.
func geoStoreResult(g *reg, cx *shard.Ctx, src []byte, tail [][]byte) ([]scoredMember, string) {
	z, wrong := g.lookup(cx, src)
	if wrong {
		return nil, wrongType
	}
	sh, opts, errMsg := parseGeoSearch(z, tail, true)
	if errMsg != "" {
		return nil, errMsg
	}
	var hits []geoHit
	if z != nil {
		hits = geoSearchHits(z, sh)
	}
	hits = geoOrderCut(hits, opts)
	return geoStorePairs(hits, opts, sh.toMeters), ""
}

// Geosearchstore answers GEOSEARCHSTORE destination source <FROMMEMBER m |
// FROMLONLAT lon lat> <BYRADIUS r unit | BYBOX w h unit> [ASC|DESC] [COUNT n
// [ANY]] [STOREDIST] on co-located keys: search the source, materialize the
// survivors into destination, and reply the stored cardinality. An empty result
// deletes the destination and replies zero.
func Geosearchstore(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	g := registry(cx)
	pairs, errMsg := geoStoreResult(g, cx, args[1], args[2:])
	if errMsg != "" {
		r.Err(errMsg)
		return
	}
	r.Int(int64(place(cx, g, args[0], buildDest(pairs))))
}

// GeosearchstoreCross is the F17 route for a destination and source on different
// shards: run the search on the source's owner, then place the result on the
// destination's owner, both under the intent transaction that holds the two
// keys. It returns the stored cardinality, or the same error the co-located path
// would have replied.
func GeosearchstoreCross(t *shard.Txn, args [][]byte) []byte {
	dest, src := args[0], args[1]
	var (
		pairs  []scoredMember
		errMsg string
	)
	t.Do(src, func(cx *shard.Ctx) {
		pairs, errMsg = geoStoreResult(registry(cx), cx, src, args[2:])
	})
	if errMsg != "" {
		return resp.AppendError(nil, errMsg)
	}
	var n int
	t.Do(dest, func(cx *shard.Ctx) {
		n = place(cx, registry(cx), dest, buildDest(pairs))
	})
	return resp.AppendInt(nil, int64(n))
}
