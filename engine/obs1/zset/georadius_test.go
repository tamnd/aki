package zset

import (
	"sort"
	"testing"
)

// The GEORADIUS parser over the Sicily corpus (spec 2064/f3/15 section 12): the
// positional center and radius resolve to the same shape GEOSEARCH builds from
// its keywords, and the deprecated compat corners hold.

func bs(ss ...string) [][]byte {
	out := make([][]byte, len(ss))
	for i, s := range ss {
		out[i] = []byte(s)
	}
	return out
}

// TestParseRadiusLonLat checks GEORADIUS resolves a positional lon/lat center and
// radius into the shape the covering engine searches.
func TestParseRadiusLonLat(t *testing.T) {
	z := geoSetSicily()
	sh, opts, errMsg := parseRadius(z, bs("Sicily", "15", "37", "200", "km"), false, false)
	if errMsg != "" {
		t.Fatalf("parse error: %s", errMsg)
	}
	if opts.haveStore {
		t.Fatal("a plain GEORADIUS is not a store")
	}
	if sh.lon != 15 || sh.lat != 37 || sh.radiusM != 200000 || sh.toMeters != 1000 {
		t.Fatalf("shape = %+v, want lon 15 lat 37 radius 200000 unit 1000", sh)
	}
	got := hitNames(geoSearchHits(z, sh))
	sort.Strings(got)
	if len(got) != 2 || got[0] != "Catania" || got[1] != "Palermo" {
		t.Fatalf("200 km survivors = %v, want [Catania Palermo]", got)
	}
}

// TestParseRadiusByMember checks the BYMEMBER center resolves off a stored member
// and that a missing member is an error, not an empty result.
func TestParseRadiusByMember(t *testing.T) {
	z := geoSetSicily()
	sh, _, errMsg := parseRadius(z, bs("Sicily", "Catania", "300", "km"), true, false)
	if errMsg != "" {
		t.Fatalf("parse error: %s", errMsg)
	}
	got := hitNames(geoSearchHits(z, sh))
	sort.Strings(got)
	if len(got) != 2 || got[0] != "Catania" || got[1] != "Palermo" {
		t.Fatalf("300 km from Catania = %v, want [Catania Palermo]", got)
	}

	if _, _, errMsg := parseRadius(z, bs("Sicily", "Nowhere", "1", "km"), true, false); errMsg != errGeoNoMember {
		t.Fatalf("missing member error = %q, want %q", errMsg, errGeoNoMember)
	}
}

// TestParseRadiusStore checks STORE and STOREDIST capture a destination and set
// the score mode, and that the read-only forms refuse them.
func TestParseRadiusStore(t *testing.T) {
	z := geoSetSicily()

	_, opts, errMsg := parseRadius(z, bs("Sicily", "15", "37", "200", "km", "STORE", "dst"), false, false)
	if errMsg != "" || !opts.haveStore || opts.storeDist || string(opts.storeKey) != "dst" {
		t.Fatalf("STORE parse = (%q, %+v)", errMsg, opts)
	}

	_, opts, errMsg = parseRadius(z, bs("Sicily", "15", "37", "200", "km", "STOREDIST", "dd"), false, false)
	if errMsg != "" || !opts.haveStore || !opts.storeDist || string(opts.storeKey) != "dd" {
		t.Fatalf("STOREDIST parse = (%q, %+v)", errMsg, opts)
	}

	// The read-only form refuses a store.
	if _, _, errMsg := parseRadius(z, bs("Sicily", "15", "37", "200", "km", "STORE", "dst"), false, true); errMsg != "ERR syntax error" {
		t.Fatalf("_RO STORE error = %q, want syntax error", errMsg)
	}
}

// TestParseRadiusStoreWithFlag checks STORE combined with a WITH annotation is
// the documented incompatibility error.
func TestParseRadiusStoreWithFlag(t *testing.T) {
	z := geoSetSicily()
	want := "ERR STORE option in GEORADIUS is not compatible with WITHDIST, WITHHASH and WITHCOORD options"
	if _, _, errMsg := parseRadius(z, bs("Sicily", "15", "37", "200", "km", "WITHDIST", "STORE", "dst"), false, false); errMsg != want {
		t.Fatalf("STORE+WITHDIST error = %q, want %q", errMsg, want)
	}
}

// TestParseRadiusCountSorts checks a COUNT keeps the nearest, the deprecated
// sort-when-COUNT behavior the shared geoOrderCut provides.
func TestParseRadiusCountSorts(t *testing.T) {
	z := geoSetSicily()
	sh, opts, errMsg := parseRadius(z, bs("Sicily", "15", "37", "200", "km", "COUNT", "1"), false, false)
	if errMsg != "" {
		t.Fatalf("parse error: %s", errMsg)
	}
	hits := geoOrderCut(geoSearchHits(z, sh), opts.geoSearchOpts)
	if len(hits) != 1 || string(hits[0].member) != "Catania" {
		t.Fatalf("COUNT 1 kept = %v, want [Catania]", hitNames(hits))
	}
}
