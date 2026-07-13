package drivers

import "testing"

// The Sicily corpus over the wire (spec 2064/f3/15 section 10): the same golden
// coordinates the codec unit tests pin, driven through the dispatch and the zset
// substrate so GEOADD lands real members and the read commands decode them.

// TestGeoAddAndRead walks GEOADD, GEOPOS, GEODIST, and GEOHASH end to end.
func TestGeoAddAndRead(t *testing.T) {
	_, nc, br := startServer(t)

	// Two members land; the key is a zset underneath.
	send(t, nc, "GEOADD", "Sicily", "13.361389", "38.115556", "Palermo",
		"15.087269", "37.502669", "Catania")
	expect(t, br, ":2\r\n")
	send(t, nc, "ZCARD", "Sicily")
	expect(t, br, ":2\r\n")

	// GEOPOS decodes a stored member to its cell center. The digits are the
	// deterministic float64 render of the decode.
	send(t, nc, "GEOPOS", "Sicily", "Palermo")
	expect(t, br, "*1\r\n*2\r\n$18\r\n13.361389338970184\r\n$18\r\n38.115556395496299\r\n")

	// A missing member is a null array element, not an error.
	send(t, nc, "GEOPOS", "Sicily", "Nowhere")
	expect(t, br, "*1\r\n*-1\r\n")

	// GEODIST is the haversine on Redis's sphere; the documented Palermo-Catania
	// distance is 166274.1516 meters, 166.2742 km.
	send(t, nc, "GEODIST", "Sicily", "Palermo", "Catania")
	expect(t, br, "$11\r\n166274.1516\r\n")
	send(t, nc, "GEODIST", "Sicily", "Palermo", "Catania", "km")
	expect(t, br, "$8\r\n166.2742\r\n")

	// A missing member in GEODIST is a null, not an error.
	send(t, nc, "GEODIST", "Sicily", "Palermo", "Nowhere")
	expect(t, br, "$-1\r\n")

	// GEOHASH renders the standard eleven-character interop strings.
	send(t, nc, "GEOHASH", "Sicily", "Palermo", "Catania")
	expect(t, br, "*2\r\n$11\r\nsqc8b49rny0\r\n$11\r\nsqdtr74hyu0\r\n")
}

// TestGeoAddFlags checks the NX/XX/CH matrix and the argument validation.
func TestGeoAddFlags(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "GEOADD", "k", "13.361389", "38.115556", "Palermo")
	expect(t, br, ":1\r\n")

	// NX will not move an existing member.
	send(t, nc, "GEOADD", "k", "NX", "0", "0", "Palermo")
	expect(t, br, ":0\r\n")
	send(t, nc, "GEOHASH", "k", "Palermo")
	expect(t, br, "*1\r\n$11\r\nsqc8b49rny0\r\n")

	// XX moves it, and CH counts the change.
	send(t, nc, "GEOADD", "k", "XX", "CH", "15.087269", "37.502669", "Palermo")
	expect(t, br, ":1\r\n")

	// NX and XX together are a syntax error.
	send(t, nc, "GEOADD", "k", "NX", "XX", "0", "0", "m")
	expect(t, br, "-ERR XX and NX options at the same time are not compatible\r\n")

	// A tail that is not a multiple of three is a syntax error (the arity gate
	// upstream only enforces the four-argument floor).
	send(t, nc, "GEOADD", "k", "1", "2", "a", "3")
	expect(t, br, "-ERR syntax error\r\n")

	// An out-of-range latitude errors with the offending pair, and nothing lands.
	send(t, nc, "GEOADD", "k", "0", "91", "bad")
	expect(t, br, "-ERR invalid longitude,latitude pair 0.000000,91.000000\r\n")
	send(t, nc, "ZSCORE", "k", "bad")
	expect(t, br, "$-1\r\n")
}

// TestGeoSearch drives GEOSEARCH over the corpus: a radius from a coordinate,
// the same radius annotated, a member-relative box, and the degenerate cases.
func TestGeoSearch(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "GEOADD", "Sicily", "13.361389", "38.115556", "Palermo",
		"15.087269", "37.502669", "Catania")
	expect(t, br, ":2\r\n")

	// A 200 km radius from (15,37) covers both members; ASC orders them by
	// distance, Catania at 56 km before Palermo at 190 km.
	send(t, nc, "GEOSEARCH", "Sicily", "FROMLONLAT", "15", "37",
		"BYRADIUS", "200", "km", "ASC")
	expect(t, br, "*2\r\n$7\r\nCatania\r\n$7\r\nPalermo\r\n")

	// A 100 km radius drops Palermo, keeping only Catania.
	send(t, nc, "GEOSEARCH", "Sicily", "FROMLONLAT", "15", "37",
		"BYRADIUS", "100", "km", "ASC")
	expect(t, br, "*1\r\n$7\r\nCatania\r\n")

	// The annotated form carries, in Redis order, the member, the distance in the
	// requested unit, the raw 52-bit hash, then the decoded [lon, lat].
	send(t, nc, "GEOSEARCH", "Sicily", "FROMLONLAT", "15", "37",
		"BYRADIUS", "200", "km", "ASC", "WITHCOORD", "WITHDIST", "WITHHASH")
	expect(t, br, "*2\r\n"+
		"*4\r\n$7\r\nCatania\r\n$7\r\n56.4413\r\n:3479447370796909\r\n"+
		"*2\r\n$18\r\n15.087267458438873\r\n$17\r\n37.50266842333162\r\n"+
		"*4\r\n$7\r\nPalermo\r\n$8\r\n190.4424\r\n:3479099956230698\r\n"+
		"*2\r\n$18\r\n13.361389338970184\r\n$18\r\n38.115556395496299\r\n")

	// FROMMEMBER centers on a stored member; Palermo within 300 km of Catania
	// returns both, Catania first at zero distance.
	send(t, nc, "GEOSEARCH", "Sicily", "FROMMEMBER", "Catania",
		"BYRADIUS", "300", "km", "ASC")
	expect(t, br, "*2\r\n$7\r\nCatania\r\n$7\r\nPalermo\r\n")

	// COUNT without ANY sorts and cuts to the nearest.
	send(t, nc, "GEOSEARCH", "Sicily", "FROMLONLAT", "15", "37",
		"BYRADIUS", "200", "km", "COUNT", "1")
	expect(t, br, "*1\r\n$7\r\nCatania\r\n")

	// A box wide and tall enough for both keeps both; DESC returns the far one
	// first.
	send(t, nc, "GEOSEARCH", "Sicily", "FROMLONLAT", "15", "37",
		"BYBOX", "400", "400", "km", "DESC")
	expect(t, br, "*2\r\n$7\r\nPalermo\r\n$7\r\nCatania\r\n")
}

// TestGeoSearchErrors checks the argument validation: a missing shape, a bad
// unit, both center sources, and an unknown member center.
func TestGeoSearchErrors(t *testing.T) {
	_, nc, br := startServer(t)
	send(t, nc, "GEOADD", "Sicily", "15.087269", "37.502669", "Catania")
	expect(t, br, ":1\r\n")

	// A center with no shape is a syntax error (padded past the arity floor with
	// annotation flags so it reaches the handler's own validation).
	send(t, nc, "GEOSEARCH", "Sicily", "FROMLONLAT", "15", "37", "WITHCOORD", "WITHDIST")
	expect(t, br, "-ERR exactly one of FROMMEMBER or FROMLONLAT can be specified for GEOSEARCH\r\n")

	// An unknown unit is rejected.
	send(t, nc, "GEOSEARCH", "Sicily", "FROMLONLAT", "15", "37", "BYRADIUS", "1", "parsecs")
	expect(t, br, "-ERR unsupported unit provided. please use M, KM, FT, MI\r\n")

	// A member center that is not present cannot be decoded.
	send(t, nc, "GEOSEARCH", "Sicily", "FROMMEMBER", "Nowhere", "BYRADIUS", "1", "km")
	expect(t, br, "-ERR could not decode requested zset member\r\n")

	// A missing key is an empty array, not an error.
	send(t, nc, "GEOSEARCH", "nokey", "FROMLONLAT", "15", "37", "BYRADIUS", "1", "km")
	expect(t, br, "*0\r\n")
}

// TestGeoMissingKey checks the read commands answer a missing key without an
// error: nils sized to the requested members.
func TestGeoMissingKey(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "GEOPOS", "nokey", "a", "b")
	expect(t, br, "*2\r\n*-1\r\n*-1\r\n")
	send(t, nc, "GEOHASH", "nokey", "a", "b")
	expect(t, br, "*2\r\n$-1\r\n$-1\r\n")
	send(t, nc, "GEODIST", "nokey", "a", "b")
	expect(t, br, "$-1\r\n")

	// An unknown unit is the unsupported-unit error.
	send(t, nc, "GEODIST", "nokey", "a", "b", "parsecs")
	expect(t, br, "-ERR unsupported unit provided. please use M, KM, FT, MI\r\n")
}
