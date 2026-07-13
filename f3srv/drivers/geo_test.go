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
