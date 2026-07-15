package drivers

import (
	"strconv"
	"testing"
)

// The deprecated GEORADIUS family over the wire (spec 2064/f3/15 section 12):
// the same Sicily corpus GEOSEARCH drives, reached through the positional
// argument shapes, with STORE routing over both the co-located and F17 paths.

// TestGeoRadius walks the read forms: a coordinate center, the annotated reply,
// a member center, COUNT, and the read-only twin.
func TestGeoRadius(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "GEOADD", "Sicily", "13.361389", "38.115556", "Palermo",
		"15.087269", "37.502669", "Catania")
	expect(t, br, ":2\r\n")

	// A 200 km radius from (15,37) covers both, ASC nearest first.
	send(t, nc, "GEORADIUS", "Sicily", "15", "37", "200", "km", "ASC")
	expect(t, br, "*2\r\n$7\r\nCatania\r\n$7\r\nPalermo\r\n")

	// A tight 100 km drops Palermo.
	send(t, nc, "GEORADIUS", "Sicily", "15", "37", "100", "km", "ASC")
	expect(t, br, "*1\r\n$7\r\nCatania\r\n")

	// The annotated form carries the same member, distance, hash, and coord the
	// GEOSEARCH engine produces.
	send(t, nc, "GEORADIUS", "Sicily", "15", "37", "200", "km", "ASC",
		"WITHCOORD", "WITHDIST", "WITHHASH")
	expect(t, br, "*2\r\n"+
		"*4\r\n$7\r\nCatania\r\n$7\r\n56.4413\r\n:3479447370796909\r\n"+
		"*2\r\n$18\r\n15.087267458438873\r\n$17\r\n37.50266842333162\r\n"+
		"*4\r\n$7\r\nPalermo\r\n$8\r\n190.4424\r\n:3479099956230698\r\n"+
		"*2\r\n$18\r\n13.361389338970184\r\n$18\r\n38.115556395496299\r\n")

	// GEORADIUSBYMEMBER centers on a stored member.
	send(t, nc, "GEORADIUSBYMEMBER", "Sicily", "Catania", "300", "km", "ASC")
	expect(t, br, "*2\r\n$7\r\nCatania\r\n$7\r\nPalermo\r\n")

	// COUNT keeps the nearest even without ASC, the deprecated sort-when-COUNT.
	send(t, nc, "GEORADIUS", "Sicily", "15", "37", "200", "km", "COUNT", "1")
	expect(t, br, "*1\r\n$7\r\nCatania\r\n")

	// The read-only twin answers the same read.
	send(t, nc, "GEORADIUS_RO", "Sicily", "15", "37", "200", "km", "ASC")
	expect(t, br, "*2\r\n$7\r\nCatania\r\n$7\r\nPalermo\r\n")
	send(t, nc, "GEORADIUSBYMEMBER_RO", "Sicily", "Catania", "300", "km", "ASC")
	expect(t, br, "*2\r\n$7\r\nCatania\r\n$7\r\nPalermo\r\n")
}

// TestGeoRadiusErrors checks the deprecated compat corners: STORE with a WITH
// flag, STORE on a read-only form, and a missing BYMEMBER center.
func TestGeoRadiusErrors(t *testing.T) {
	_, nc, br := startServer(t)
	send(t, nc, "GEOADD", "Sicily", "15.087269", "37.502669", "Catania")
	expect(t, br, ":1\r\n")

	// STORE is incompatible with the annotations.
	send(t, nc, "GEORADIUS", "Sicily", "15", "37", "200", "km", "WITHDIST", "STORE", "d")
	expect(t, br, "-ERR STORE option in GEORADIUS is not compatible with WITHDIST, WITHHASH and WITHCOORD options\r\n")

	// The read-only form refuses STORE.
	send(t, nc, "GEORADIUS_RO", "Sicily", "15", "37", "200", "km", "STORE", "d")
	expect(t, br, "-ERR syntax error\r\n")

	// A missing member center is an error, not an empty array.
	send(t, nc, "GEORADIUSBYMEMBER", "Sicily", "Nowhere", "1", "km")
	expect(t, br, "-ERR could not decode requested zset member\r\n")

	// An unknown unit is rejected.
	send(t, nc, "GEORADIUS", "Sicily", "15", "37", "1", "parsecs")
	expect(t, br, "-ERR unsupported unit provided. please use M, KM, FT, MI\r\n")
}

// TestGeoRadiusStore drives GEORADIUS STORE and STOREDIST on co-located keys:
// STORE keeps the geohash score so the destination is a geo set again, STOREDIST
// ranks by distance, and an empty result deletes the destination.
func TestGeoRadiusStore(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "GEOADD", "{s}Sicily", "13.361389", "38.115556", "Palermo",
		"15.087269", "37.502669", "Catania")
	expect(t, br, ":2\r\n")

	// STORE writes both and the destination decodes back to the members.
	send(t, nc, "GEORADIUS", "{s}Sicily", "15", "37", "200", "km", "ASC", "STORE", "{s}dest")
	expect(t, br, ":2\r\n")
	send(t, nc, "GEOSEARCH", "{s}dest", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km", "ASC")
	expect(t, br, "*2\r\n$7\r\nCatania\r\n$7\r\nPalermo\r\n")

	// STOREDIST ranks by distance, so ZRANGE returns Catania before Palermo.
	send(t, nc, "GEORADIUS", "{s}Sicily", "15", "37", "200", "km", "STOREDIST", "{s}dist")
	expect(t, br, ":2\r\n")
	send(t, nc, "ZRANGE", "{s}dist", "0", "-1")
	expect(t, br, "*2\r\n$7\r\nCatania\r\n$7\r\nPalermo\r\n")

	// The member form stores too.
	send(t, nc, "GEORADIUSBYMEMBER", "{s}Sicily", "Catania", "300", "km", "COUNT", "1", "STORE", "{s}one")
	expect(t, br, ":1\r\n")
	send(t, nc, "ZRANGE", "{s}one", "0", "-1")
	expect(t, br, "*1\r\n$7\r\nCatania\r\n")

	// An empty result deletes the destination (ZCARD probes the zset keyspace).
	send(t, nc, "GEOADD", "{s}gone", "15.087269", "37.502669", "Catania")
	expect(t, br, ":1\r\n")
	send(t, nc, "GEORADIUS", "{s}Sicily", "15", "37", "1", "m", "STORE", "{s}gone")
	expect(t, br, ":0\r\n")
	send(t, nc, "ZCARD", "{s}gone")
	expect(t, br, ":0\r\n")
}

// TestGeoRadiusStoreCrossShard drives GEORADIUS STORE with the destination and
// source on different shards, so the store routes through the F17 coordinator.
func TestGeoRadiusStoreCrossShard(t *testing.T) {
	srv, nc, br := startServer(t)

	keyOn := func(sh int, prefix string) string {
		for i := 0; ; i++ {
			k := prefix + strconv.Itoa(i)
			if srv.rt.ShardOf([]byte(k)) == sh {
				return k
			}
		}
	}
	src := keyOn(0, "rsrc")
	dst := keyOn(1, "rdst")

	send(t, nc, "GEOADD", src, "13.361389", "38.115556", "Palermo",
		"15.087269", "37.502669", "Catania")
	expect(t, br, ":2\r\n")

	// Cross-shard STORE: search on the source owner, place on the destination
	// owner, both inside the intent.
	send(t, nc, "GEORADIUS", src, "15", "37", "200", "km", "ASC", "STORE", dst)
	expect(t, br, ":2\r\n")
	send(t, nc, "GEOSEARCH", dst, "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km", "ASC")
	expect(t, br, "*2\r\n$7\r\nCatania\r\n$7\r\nPalermo\r\n")

	// BYMEMBER STOREDIST over the cross route ranks by distance.
	send(t, nc, "GEORADIUSBYMEMBER", src, "Catania", "300", "km", "STOREDIST", dst)
	expect(t, br, ":2\r\n")
	send(t, nc, "ZRANGE", dst, "0", "-1")
	expect(t, br, "*2\r\n$7\r\nCatania\r\n$7\r\nPalermo\r\n")

	// A wrong-type source is caught before the destination is touched.
	strKey := keyOn(0, "rstr")
	send(t, nc, "SET", strKey, "x")
	expect(t, br, "+OK\r\n")
	send(t, nc, "GEORADIUS", strKey, "15", "37", "1", "km", "STORE", dst)
	expect(t, br, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
	send(t, nc, "ZCARD", dst)
	expect(t, br, ":2\r\n")
}
