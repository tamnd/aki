package sqlo1

import (
	"net"
	"testing"
)

// The geo wire tests below pin Redis 8.8.0's probed doors and the
// captured fixture replies byte for byte: the Palermo and Catania
// pair from the GEOADD documentation, the (1, 41) box discriminator
// that proves the longitude leg is measured at the point's latitude,
// and the antimeridian pair that proves the cell cover wraps.

func geoSend(t *testing.T, c net.Conn) func(args ...string) {
	return func(args ...string) {
		t.Helper()
		if _, err := c.Write([]byte(respCmd(args...))); err != nil {
			t.Fatal(err)
		}
	}
}

// TestServerGeoAdd pins GEOADD's own doors: NX with XX is a plain
// syntax error, triples validate wholly before any write, and the
// flag forms ride ZADD's semantics over encoded scores.
func TestServerGeoAdd(t *testing.T) {
	c, r := startServer(t)
	send := geoSend(t, c)

	send("GEOADD", "k", "1", "2")
	expect(t, r, "-ERR wrong number of arguments for 'geoadd' command\r\n")
	send("GEOADD", "k", "NX", "XX", "0", "0", "a")
	expect(t, r, "-ERR syntax error\r\n")
	send("GEOADD", "k", "0", "0", "a", "b")
	expect(t, r, "-ERR syntax error\r\n")
	send("GEOADD", "k", "notafloat", "0", "a")
	expect(t, r, "-ERR value is not a valid float\r\n")
	send("GEOADD", "k", "nan", "0", "a")
	expect(t, r, "-ERR value is not a valid float\r\n")
	send("GEOADD", "k", "200", "100", "a")
	expect(t, r, "-ERR invalid longitude,latitude pair 200.000000,100.000000\r\n")

	// All triples validate before any write lands.
	send("GEOADD", "k", "0", "0", "good", "200", "100", "bad")
	expect(t, r, "-ERR invalid longitude,latitude pair 200.000000,100.000000\r\n")
	send("ZCARD", "k")
	expect(t, r, ":0\r\n")

	send("SET", "str", "v")
	expect(t, r, "+OK\r\n")
	send("GEOADD", "str", "0", "0", "a")
	expect(t, r, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")

	// The fixture pair, scores bit-identical to Redis (Z-I6).
	send("GEOADD", "sic", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania")
	expect(t, r, ":2\r\n")
	send("ZSCORE", "sic", "Palermo")
	expect(t, r, "$16\r\n3479099956230698\r\n")
	send("ZSCORE", "sic", "Catania")
	expect(t, r, "$16\r\n3479447370796909\r\n")
	send("GEOADD", "sic", "13.361389", "38.115556", "Palermo")
	expect(t, r, ":0\r\n")

	// NX leaves an existing member alone, CH counts the move.
	send("GEOADD", "sic", "NX", "13.5", "38.2", "Palermo")
	expect(t, r, ":0\r\n")
	send("ZSCORE", "sic", "Palermo")
	expect(t, r, "$16\r\n3479099956230698\r\n")
	send("GEOADD", "sic", "CH", "13.5", "38.2", "Palermo")
	expect(t, r, ":1\r\n")
	send("ZSCORE", "sic", "Palermo")
	expect(t, r, "$16\r\n3479101704338477\r\n")
	send("GEOPOS", "sic", "Palermo")
	expect(t, r, "*1\r\n*2\r\n$18\r\n13.500000536441803\r\n$18\r\n38.200000630919675\r\n")
	send("GEOADD", "sic", "XX", "CH", "13.361389", "38.115556", "Palermo")
	expect(t, r, ":1\r\n")
	send("ZSCORE", "sic", "Palermo")
	expect(t, r, "$16\r\n3479099956230698\r\n")
}

// TestServerGeoPosDistHash pins the read trio's fixture replies:
// GEOPOS midpoint coordinates at shortest round-trip, GEODIST at four
// decimals in each unit, GEOHASH's eleven base32 chars ending in the
// alphabet's zero.
func TestServerGeoPosDistHash(t *testing.T) {
	c, r := startServer(t)
	send := geoSend(t, c)

	send("GEOADD", "sic", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania")
	expect(t, r, ":2\r\n")

	send("GEOPOS", "sic", "Palermo", "Catania", "ghost")
	expect(t, r, "*3\r\n*2\r\n$18\r\n13.361389338970184\r\n$16\r\n38.1155563954963\r\n*2\r\n$18\r\n15.087267458438873\r\n$17\r\n37.50266842333162\r\n*-1\r\n")
	send("GEOPOS", "sic")
	expect(t, r, "*0\r\n")
	send("GEOPOS", "nosuchkey", "Palermo")
	expect(t, r, "*1\r\n*-1\r\n")

	send("GEODIST", "sic", "Palermo")
	expect(t, r, "-ERR wrong number of arguments for 'geodist' command\r\n")
	send("GEODIST", "sic", "Palermo", "Catania")
	expect(t, r, "$11\r\n166274.1516\r\n")
	send("GEODIST", "sic", "Palermo", "Catania", "km")
	expect(t, r, "$8\r\n166.2742\r\n")
	send("GEODIST", "sic", "Palermo", "Catania", "ft")
	expect(t, r, "$11\r\n545518.8700\r\n")
	send("GEODIST", "sic", "Palermo", "Catania", "mi")
	expect(t, r, "$8\r\n103.3182\r\n")
	send("GEODIST", "sic", "Palermo", "Palermo")
	expect(t, r, "$6\r\n0.0000\r\n")
	send("GEODIST", "sic", "Palermo", "ghost")
	expect(t, r, "$-1\r\n")
	send("GEODIST", "sic", "Palermo", "Catania", "yd")
	expect(t, r, "-ERR unsupported unit provided. please use M, KM, FT, MI\r\n")
	send("GEODIST", "sic", "Palermo", "Catania", "km", "extra")
	expect(t, r, "-ERR syntax error\r\n")

	send("GEOHASH", "sic", "Palermo", "Catania", "ghost")
	expect(t, r, "*3\r\n$11\r\nsqc8b49rny0\r\n$11\r\nsqdtr74hyu0\r\n$-1\r\n")
	send("GEOHASH", "sic")
	expect(t, r, "*0\r\n")

	send("SET", "str", "v")
	expect(t, r, "+OK\r\n")
	send("GEOPOS", "str", "a")
	expect(t, r, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
	send("GEODIST", "str", "a", "b")
	expect(t, r, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
	send("GEOHASH", "str", "a")
	expect(t, r, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
}

// TestServerGeoSearchGrammar pins the option grammar's probed doors:
// errors surface in token order, the exactly-one checks fire after
// parsing, an absent key still validates everything, and a FROMMEMBER
// miss only errors on a live key.
func TestServerGeoSearchGrammar(t *testing.T) {
	c, r := startServer(t)
	send := geoSend(t, c)

	send("GEOSEARCH", "g", "FROMLONLAT", "0", "0", "BYRADIUS")
	expect(t, r, "-ERR wrong number of arguments for 'geosearch' command\r\n")
	send("GEOSEARCHSTORE", "d", "s", "FROMLONLAT", "0", "0", "BYRADIUS")
	expect(t, r, "-ERR wrong number of arguments for 'geosearchstore' command\r\n")

	// Token order: the bad pair beats the later bad unit.
	send("GEOSEARCH", "nosuchkey", "FROMLONLAT", "200", "100", "BYRADIUS", "1", "yd")
	expect(t, r, "-ERR invalid longitude,latitude pair 200.000000,100.000000\r\n")

	// FROMMEMBER on a live key errors, on an absent key it does not,
	// and the absent key still validates the tokens after it.
	send("ZADD", "zk", "1", "m")
	expect(t, r, ":1\r\n")
	send("GEOSEARCH", "zk", "FROMMEMBER", "ghost", "BYRADIUS", "1", "yd")
	expect(t, r, "-ERR could not decode requested zset member\r\n")
	send("GEOSEARCH", "nosuchkey", "FROMMEMBER", "m", "BYRADIUS", "1", "km")
	expect(t, r, "*0\r\n")
	send("GEOSEARCH", "nosuchkey", "FROMMEMBER", "m", "BYRADIUS", "1", "yd")
	expect(t, r, "-ERR unsupported unit provided. please use M, KM, FT, MI\r\n")
	send("GEOSEARCH", "nosuchkey", "BOGUS", "0", "0", "1", "km")
	expect(t, r, "-ERR syntax error\r\n")

	// Duplicate FROM or BY, and the exactly-one texts.
	send("GEOSEARCH", "zk", "FROMLONLAT", "0", "0", "FROMMEMBER", "m", "BYRADIUS", "1", "km")
	expect(t, r, "-ERR syntax error\r\n")
	send("GEOSEARCH", "zk", "FROMLONLAT", "0", "0", "BYRADIUS", "1", "km", "BYBOX", "1", "1", "km")
	expect(t, r, "-ERR syntax error\r\n")
	send("GEOSEARCH", "zk", "BYRADIUS", "1", "km", "ASC", "ASC")
	expect(t, r, "-ERR exactly one of FROMMEMBER or FROMLONLAT can be specified for geosearch\r\n")
	send("GEOSEARCH", "zk", "FROMLONLAT", "0", "0", "ASC", "ASC")
	expect(t, r, "-ERR exactly one of BYRADIUS and BYBOX can be specified for geosearch\r\n")

	// Radius and box numeric doors, negatives before units.
	send("GEOSEARCH", "zk", "FROMLONLAT", "0", "0", "BYRADIUS", "abc", "km")
	expect(t, r, "-ERR need numeric radius\r\n")
	send("GEOSEARCH", "zk", "FROMLONLAT", "0", "0", "BYRADIUS", "-1", "yd")
	expect(t, r, "-ERR radius cannot be negative\r\n")
	send("GEOSEARCH", "zk", "FROMLONLAT", "0", "0", "BYBOX", "abc", "1", "km")
	expect(t, r, "-ERR need numeric width\r\n")
	send("GEOSEARCH", "zk", "FROMLONLAT", "0", "0", "BYBOX", "1", "abc", "km")
	expect(t, r, "-ERR need numeric height\r\n")
	send("GEOSEARCH", "zk", "FROMLONLAT", "0", "0", "BYBOX", "-1", "1", "yd")
	expect(t, r, "-ERR height or width cannot be negative\r\n")
	send("GEOSEARCH", "zk", "FROMLONLAT", "0", "0", "BYBOX", "1", "1", "yd")
	expect(t, r, "-ERR unsupported unit provided. please use M, KM, FT, MI\r\n")

	// COUNT and ANY.
	send("GEOSEARCH", "zk", "FROMLONLAT", "0", "0", "BYRADIUS", "1", "km", "COUNT", "abc")
	expect(t, r, "-ERR value is not an integer or out of range\r\n")
	send("GEOSEARCH", "zk", "FROMLONLAT", "0", "0", "BYRADIUS", "1", "km", "COUNT", "0")
	expect(t, r, "-ERR COUNT must be > 0\r\n")
	send("GEOSEARCH", "zk", "FROMLONLAT", "0", "0", "BYRADIUS", "1", "km", "ANY")
	expect(t, r, "-ERR the ANY argument requires COUNT argument\r\n")

	// GEOSEARCH owns no STORE tokens.
	send("GEOSEARCH", "zk", "FROMLONLAT", "0", "0", "BYRADIUS", "1", "km", "STORE", "x")
	expect(t, r, "-ERR syntax error\r\n")
	send("GEOSEARCH", "zk", "FROMLONLAT", "0", "0", "BYRADIUS", "1", "km", "STOREDIST", "x")
	expect(t, r, "-ERR syntax error\r\n")

	send("SET", "str", "v")
	expect(t, r, "+OK\r\n")
	send("GEOSEARCH", "str", "FROMLONLAT", "0", "0", "BYRADIUS", "1", "km")
	expect(t, r, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
}

// TestServerGeoSearchFixture pins the fixture searches byte for byte:
// the WITH reply shape in dist, hash, coord order, last-wins sorting,
// the nearest-first implicit trim under COUNT, ANY before COUNT, and
// the (1, 41) box discriminator.
func TestServerGeoSearchFixture(t *testing.T) {
	c, r := startServer(t)
	send := geoSend(t, c)

	send("GEOADD", "sic", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania")
	expect(t, r, ":2\r\n")

	send("GEOSEARCH", "sic", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km", "ASC")
	expect(t, r, "*2\r\n$7\r\nCatania\r\n$7\r\nPalermo\r\n")
	send("GEOSEARCH", "sic", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km", "ASC", "WITHCOORD", "WITHDIST", "WITHHASH")
	expect(t, r, "*2\r\n"+
		"*4\r\n$7\r\nCatania\r\n$7\r\n56.4413\r\n:3479447370796909\r\n*2\r\n$18\r\n15.087267458438873\r\n$17\r\n37.50266842333162\r\n"+
		"*4\r\n$7\r\nPalermo\r\n$8\r\n190.4424\r\n:3479099956230698\r\n*2\r\n$18\r\n13.361389338970184\r\n$16\r\n38.1155563954963\r\n")

	// ASC and DESC are last-wins.
	send("GEOSEARCH", "sic", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km", "ASC", "DESC")
	expect(t, r, "*2\r\n$7\r\nPalermo\r\n$7\r\nCatania\r\n")
	send("GEOSEARCH", "sic", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km", "DESC", "ASC")
	expect(t, r, "*2\r\n$7\r\nCatania\r\n$7\r\nPalermo\r\n")

	// COUNT without a sort trims nearest-first; ANY is legal before
	// its COUNT and returns whatever surfaced.
	send("GEOSEARCH", "sic", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km", "COUNT", "1")
	expect(t, r, "*1\r\n$7\r\nCatania\r\n")
	send("GEOSEARCH", "sic", "FROMLONLAT", "15", "37", "BYRADIUS", "60", "km", "ANY", "COUNT", "5")
	expect(t, r, "*1\r\n$7\r\nCatania\r\n")

	send("GEOSEARCH", "sic", "FROMMEMBER", "Palermo", "BYRADIUS", "200", "km", "ASC", "WITHDIST")
	expect(t, r, "*2\r\n*2\r\n$7\r\nPalermo\r\n$6\r\n0.0000\r\n*2\r\n$7\r\nCatania\r\n$8\r\n166.2742\r\n")
	send("GEOSEARCH", "sic", "FROMMEMBER", "Palermo", "BYBOX", "400", "400", "km", "ASC", "WITHDIST")
	expect(t, r, "*2\r\n*2\r\n$7\r\nPalermo\r\n$6\r\n0.0000\r\n*2\r\n$7\r\nCatania\r\n$8\r\n166.2742\r\n")

	// The box discriminator: the longitude leg is measured at the
	// point's latitude, so 169 km of width includes (1, 41) from
	// (0, 40) and 167 km excludes it.
	send("GEOADD", "b", "1", "41", "p")
	expect(t, r, ":1\r\n")
	send("GEOSEARCH", "b", "FROMLONLAT", "0", "40", "BYBOX", "169", "250", "km", "ASC", "WITHDIST")
	expect(t, r, "*1\r\n*2\r\n$1\r\np\r\n$8\r\n139.7281\r\n")
	send("GEOSEARCH", "b", "FROMLONLAT", "0", "40", "BYBOX", "167", "250", "km", "ASC", "WITHDIST")
	expect(t, r, "*0\r\n")
}

// TestServerGeoSearchMeridian pins the antimeridian wrap: a search
// whose box crosses lon 180 returns the far side, radius and box
// forms both, matching the live probe.
func TestServerGeoSearchMeridian(t *testing.T) {
	c, r := startServer(t)
	send := geoSend(t, c)

	send("GEOADD", "w", "179.9", "0", "east", "-179.9", "0", "west")
	expect(t, r, ":2\r\n")
	send("GEOSEARCH", "w", "FROMLONLAT", "179.9", "0", "BYRADIUS", "50", "km", "ASC", "WITHDIST")
	expect(t, r, "*2\r\n*2\r\n$4\r\neast\r\n$6\r\n0.0002\r\n*2\r\n$4\r\nwest\r\n$7\r\n22.2453\r\n")
	send("GEOSEARCH", "w", "FROMLONLAT", "-179.95", "0.1", "BYBOX", "60", "60", "km", "ASC", "WITHDIST")
	expect(t, r, "*2\r\n*2\r\n$4\r\nwest\r\n$7\r\n12.4354\r\n*2\r\n$4\r\neast\r\n$7\r\n20.0516\r\n")
}

// TestServerGeoStore pins the store forms: STOREDIST lands the
// distance in the search's unit at full precision, the plain form
// lands the raw cell bits, an empty result deletes the dest, and the
// dest is overwritten whatever its prior type.
func TestServerGeoStore(t *testing.T) {
	c, r := startServer(t)
	send := geoSend(t, c)

	send("GEOADD", "sic", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania")
	expect(t, r, ":2\r\n")

	// The stored distances are full precision in the search's unit.
	// The last ulp is the libm's, not the algorithm's: sin, cos, and
	// asin are not correctly rounded and Go's kernels differ from a
	// given platform's libm there, so these bytes pin our own
	// deterministic value (live Redis on this box printed
	// 56.4412578701582 and 190.44242984775784, agreeing to 14
	// significant digits).
	send("GEOSEARCHSTORE", "dst", "sic", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km", "ASC", "STOREDIST")
	expect(t, r, ":2\r\n")
	send("ZSCORE", "dst", "Catania")
	expect(t, r, "$18\r\n56.441257870158246\r\n")
	send("ZSCORE", "dst", "Palermo")
	expect(t, r, "$18\r\n190.44242984775798\r\n")

	send("GEOSEARCHSTORE", "dst2", "sic", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km")
	expect(t, r, ":2\r\n")
	send("ZSCORE", "dst2", "Palermo")
	expect(t, r, "$16\r\n3479099956230698\r\n")

	// An empty result deletes the dest, even a pre-existing one.
	send("ZADD", "dst3", "1", "x")
	expect(t, r, ":1\r\n")
	send("GEOSEARCHSTORE", "dst3", "sic", "FROMLONLAT", "15", "37", "BYRADIUS", "1", "m")
	expect(t, r, ":0\r\n")
	send("ZCARD", "dst3")
	expect(t, r, ":0\r\n")
	send("TYPE", "dst3")
	expect(t, r, "+none\r\n")

	// The dest is overwritten whatever it held before.
	send("SET", "sdst", "v")
	expect(t, r, "+OK\r\n")
	send("GEOSEARCHSTORE", "sdst", "sic", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km")
	expect(t, r, ":2\r\n")
	send("TYPE", "sdst")
	expect(t, r, "+zset\r\n")

	// A missing source is the empty store.
	send("GEOSEARCHSTORE", "dsx", "nosrc", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km")
	expect(t, r, ":0\r\n")

	send("GEOSEARCHSTORE", "dst4", "sic", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km", "WITHDIST")
	expect(t, r, "-ERR GEOSEARCHSTORE is not compatible with WITHDIST, WITHHASH and WITHCOORD options\r\n")
}

// TestServerGeoRadius pins the compat family: the positional grammar,
// the byMember and _RO variants, and the probed STORE resolution
// where STORE plus STOREDIST lands only the dist arm.
func TestServerGeoRadius(t *testing.T) {
	c, r := startServer(t)
	send := geoSend(t, c)

	send("GEOADD", "sic", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania")
	expect(t, r, ":2\r\n")

	send("GEORADIUS", "sic", "15", "37", "200")
	expect(t, r, "-ERR wrong number of arguments for 'georadius' command\r\n")
	send("GEORADIUSBYMEMBER", "sic", "m", "1")
	expect(t, r, "-ERR wrong number of arguments for 'georadiusbymember' command\r\n")

	send("GEORADIUS", "sic", "15", "37", "200", "km", "ASC")
	expect(t, r, "*2\r\n$7\r\nCatania\r\n$7\r\nPalermo\r\n")
	send("GEORADIUS_RO", "sic", "15", "37", "200", "km", "ASC")
	expect(t, r, "*2\r\n$7\r\nCatania\r\n$7\r\nPalermo\r\n")
	send("GEORADIUSBYMEMBER", "sic", "Palermo", "200", "km", "ASC", "WITHDIST")
	expect(t, r, "*2\r\n*2\r\n$7\r\nPalermo\r\n$6\r\n0.0000\r\n*2\r\n$7\r\nCatania\r\n$8\r\n166.2742\r\n")

	// A missing key answers empty before the member can fail; a live
	// key with a missing member is the decode error.
	send("GEORADIUSBYMEMBER", "nokey", "m", "100", "km")
	expect(t, r, "*0\r\n")
	send("GEORADIUSBYMEMBER", "sic", "ghost", "100", "km")
	expect(t, r, "-ERR could not decode requested zset member\r\n")

	send("GEORADIUS", "sic", "200", "100", "1", "km")
	expect(t, r, "-ERR invalid longitude,latitude pair 200.000000,100.000000\r\n")
	send("GEORADIUS", "sic", "15", "37", "abc", "km")
	expect(t, r, "-ERR need numeric radius\r\n")
	send("GEORADIUS", "sic", "15", "37", "-1", "km")
	expect(t, r, "-ERR radius cannot be negative\r\n")
	send("GEORADIUS", "sic", "15", "37", "1", "yd")
	expect(t, r, "-ERR unsupported unit provided. please use M, KM, FT, MI\r\n")

	// STORE plus STOREDIST: only the dist target is written.
	send("GEORADIUS", "sic", "15", "37", "200", "km", "STORE", "k1", "STOREDIST", "k2")
	expect(t, r, ":2\r\n")
	send("ZCARD", "k1")
	expect(t, r, ":0\r\n")
	send("ZSCORE", "k2", "Catania")
	expect(t, r, "$18\r\n56.441257870158246\r\n")

	send("GEORADIUS", "sic", "15", "37", "200", "km", "STORE", "k3", "WITHDIST")
	expect(t, r, "-ERR STORE option in GEORADIUS is not compatible with WITHDIST, WITHHASH and WITHCOORD options\r\n")
	send("GEORADIUS_RO", "sic", "15", "37", "200", "km", "STORE", "k4")
	expect(t, r, "-ERR syntax error\r\n")
	send("GEORADIUS_RO", "sic", "15", "37", "200", "km", "STOREDIST", "k4")
	expect(t, r, "-ERR syntax error\r\n")
	send("GEORADIUS", "sic", "15", "37", "200", "km", "ANY")
	expect(t, r, "-ERR the ANY argument requires COUNT argument\r\n")
}
