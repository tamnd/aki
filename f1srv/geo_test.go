package f1srv

import "testing"

// The reference values in these tests come from Redis 8.8.0 run against the same commands, so a
// passing run means the geohash encode/decode, distance, and reply formatting are byte-identical to
// Redis on this dataset. The classic Sicily dataset (Palermo, Catania) is the one the Redis GEO
// documentation and test suite use, which makes the expected scores and distances easy to check.

// TestGeoAddScore checks GEOADD stores the same 52-bit geohash score Redis stores, via a ZSCORE
// readback. Palermo -> 3479099956230698, Catania -> 3479447370796909.
func TestGeoAddScore(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "GEOADD", "Sicily", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania")
	expect(t, rw, ":2")

	cmd(t, rw, "ZSCORE", "Sicily", "Palermo")
	expect(t, rw, "$3479099956230698")
	cmd(t, rw, "ZSCORE", "Sicily", "Catania")
	expect(t, rw, "$3479447370796909")

	// Re-adding the same members with the same coordinates changes nothing and counts 0 new.
	cmd(t, rw, "GEOADD", "Sicily", "13.361389", "38.115556", "Palermo")
	expect(t, rw, ":0")
}

// TestGeoPos checks the decoded coordinates round-trip to the Redis 8.8 form (shortest round-trip
// double), and that a missing member yields a null array.
func TestGeoPos(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "GEOADD", "Sicily", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania")
	expect(t, rw, ":2")

	cmd(t, rw, "GEOPOS", "Sicily", "Palermo", "NonExisting", "Catania")
	expect(t, rw, "*3")
	// Palermo
	expect(t, rw, "*2")
	expect(t, rw, "$13.361389338970184")
	expect(t, rw, "$38.1155563954963")
	// NonExisting
	expect(t, rw, "*-1")
	// Catania
	expect(t, rw, "*2")
	expect(t, rw, "$15.087267458438873")
	expect(t, rw, "$37.50266842333162")
}

// TestGeoDist checks the great-circle distance between Palermo and Catania in meters (default) and
// kilometers, matching Redis 8.8's %.4f output.
func TestGeoDist(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "GEOADD", "Sicily", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania")
	expect(t, rw, ":2")

	cmd(t, rw, "GEODIST", "Sicily", "Palermo", "Catania")
	expect(t, rw, "$166274.1516")
	cmd(t, rw, "GEODIST", "Sicily", "Palermo", "Catania", "km")
	expect(t, rw, "$166.2742")

	// A missing member yields a null bulk reply.
	cmd(t, rw, "GEODIST", "Sicily", "Palermo", "Missing")
	expect(t, rw, "$-1")
}

// TestGeoHash checks the 11-character standard geohash strings for the Sicily dataset, matching
// Redis 8.8 (Palermo sqc8b49rny0, Catania sqdtr74hyu0).
func TestGeoHash(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "GEOADD", "Sicily", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania")
	expect(t, rw, ":2")

	cmd(t, rw, "GEOHASH", "Sicily", "Palermo", "Catania")
	expect(t, rw, "*2")
	expect(t, rw, "$sqc8b49rny0")
	expect(t, rw, "$sqdtr74hyu0")
}

// TestGeoSearch checks a BYRADIUS FROMLONLAT search returns the members inside the circle sorted by
// distance ascending, matching the Redis GEOSEARCH example.
func TestGeoSearch(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "GEOADD", "Sicily", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania")
	expect(t, rw, ":2")
	cmd(t, rw, "GEOADD", "Sicily", "2.349014", "48.864716", "Paris")
	expect(t, rw, ":1")

	// A 200 km circle around a point between Palermo and Catania holds both, Paris is far away.
	cmd(t, rw, "GEOSEARCH", "Sicily", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km", "ASC")
	expect(t, rw, "*2")
	expect(t, rw, "$Catania")
	expect(t, rw, "$Palermo")
}

// TestGeoSearchWithOptions checks WITHCOORD/WITHDIST/WITHHASH nest a per-member array and format
// each annotation the Redis 8.8 way.
func TestGeoSearchWithOptions(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "GEOADD", "Sicily", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania")
	expect(t, rw, ":2")

	cmd(t, rw, "GEOSEARCH", "Sicily", "FROMLONLAT", "15", "37", "BYBOX", "400", "400", "km", "ASC", "WITHCOORD", "WITHDIST", "WITHHASH")
	expect(t, rw, "*2")

	// Catania is closest.
	expect(t, rw, "*4")
	expect(t, rw, "$Catania")
	expect(t, rw, "$56.4413")
	expect(t, rw, ":3479447370796909")
	expect(t, rw, "*2")
	expect(t, rw, "$15.087267458438873")
	expect(t, rw, "$37.50266842333162")

	// Palermo is farther.
	expect(t, rw, "*4")
	expect(t, rw, "$Palermo")
	expect(t, rw, "$190.4424")
	expect(t, rw, ":3479099956230698")
	expect(t, rw, "*2")
	expect(t, rw, "$13.361389338970184")
	expect(t, rw, "$38.1155563954963")
}

// TestGeoSearchStore checks GEOSEARCHSTORE writes the matching members into a destination geoset
// carrying their original scores, and STOREDIST writes the distance instead.
func TestGeoSearchStore(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "GEOADD", "Sicily", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania")
	expect(t, rw, ":2")

	cmd(t, rw, "GEOSEARCHSTORE", "dest", "Sicily", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km", "ASC")
	expect(t, rw, ":2")
	cmd(t, rw, "ZSCORE", "dest", "Catania")
	expect(t, rw, "$3479447370796909")

	// STOREDIST stores the distance in the requested unit as the score.
	cmd(t, rw, "GEOSEARCHSTORE", "distdest", "Sicily", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km", "ASC", "STOREDIST")
	expect(t, rw, ":2")
	cmd(t, rw, "ZCARD", "distdest")
	expect(t, rw, ":2")
}

// TestGeoRadius checks the deprecated GEORADIUS still works and honors COUNT.
func TestGeoRadius(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "GEOADD", "Sicily", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania")
	expect(t, rw, ":2")

	cmd(t, rw, "GEORADIUS", "Sicily", "15", "37", "200", "km", "ASC")
	expect(t, rw, "*2")
	expect(t, rw, "$Catania")
	expect(t, rw, "$Palermo")

	cmd(t, rw, "GEORADIUS", "Sicily", "15", "37", "200", "km", "ASC", "COUNT", "1")
	expect(t, rw, "*1")
	expect(t, rw, "$Catania")

	// GEORADIUSBYMEMBER around Palermo finds both.
	cmd(t, rw, "GEORADIUSBYMEMBER", "Sicily", "Palermo", "200", "km", "ASC")
	expect(t, rw, "*2")
	expect(t, rw, "$Palermo")
	expect(t, rw, "$Catania")
}

// TestGeoMissingKey checks a search on an absent key replies with an empty array, and a store form
// on an absent key replies 0.
func TestGeoMissingKey(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "GEOSEARCH", "nope", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km")
	expect(t, rw, "*0")

	cmd(t, rw, "GEOSEARCHSTORE", "dest", "nope", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km")
	expect(t, rw, ":0")
}

// TestGeoWrongType checks a geo read against a plain string key returns WRONGTYPE.
func TestGeoWrongType(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SET", "plain", "value")
	expect(t, rw, "+OK")
	cmd(t, rw, "GEOPOS", "plain", "member")
	expect(t, rw, "-WRONGTYPE Operation against a key holding the wrong kind of value")
}
