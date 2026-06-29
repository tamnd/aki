package command

import (
	"bufio"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"
)

// readArray reads a RESP array header and returns its element lines as raw
// strings. Bulk elements are returned as their payload, nested arrays as their
// header line, so a test can walk a known shape.
func readArray(t *testing.T, r *bufio.Reader, c net.Conn, cmd string) []string {
	t.Helper()
	head := sendLine(t, r, c, cmd)
	if len(head) == 0 || head[0] != '*' {
		t.Fatalf("expected array header after %q, got %q", cmd, head)
	}
	n, err := strconv.Atoi(head[1:])
	if err != nil {
		t.Fatalf("bad array len %q", head)
	}
	out := make([]string, 0, n)
	for range n {
		// Refresh the deadline per element. A drain reply (ZPOPMAX over a whole
		// coll-form zset returns thousands of bulk strings) reads far more than the
		// one deadline sendLine set at command time would allow under the -race
		// build on a loaded runner, where the server is also slow to produce them.
		_ = c.SetReadDeadline(time.Now().Add(testReadDeadline))
		out = append(out, readElem(t, r))
	}
	return out
}

// readElem reads one already-pending reply element. A bulk string returns its
// payload, a nil bulk returns "<nil>", anything else returns the raw line.
func readElem(t *testing.T, r *bufio.Reader) string {
	t.Helper()
	line, err := r.ReadString('\n')
	if err != nil {
		t.Fatalf("read elem: %v", err)
	}
	line = strings.TrimRight(line, "\r\n")
	if line == "$-1" || line == "_" || line == "*-1" {
		return "<nil>"
	}
	if len(line) > 0 && line[0] == '$' {
		payload, err := r.ReadString('\n')
		if err != nil {
			t.Fatalf("read bulk payload: %v", err)
		}
		return strings.TrimRight(payload, "\r\n")
	}
	return line
}

// drainElem reads and discards one element, descending into a nested array by
// its declared length. Used to skip the parts of a WITH reply a test does not
// assert on.
func drainElem(t *testing.T, r *bufio.Reader) {
	t.Helper()
	line, err := r.ReadString('\n')
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	line = strings.TrimRight(line, "\r\n")
	switch line[0] {
	case '$':
		_, _ = r.ReadString('\n')
	case '*':
		n, _ := strconv.Atoi(line[1:])
		for range n {
			drainElem(t, r)
		}
	}
}

func addSicily(t *testing.T, r *bufio.Reader, c net.Conn) {
	t.Helper()
	got := sendLine(t, r, c, "GEOADD Sicily 13.361389 38.115556 Palermo 15.087269 37.502669 Catania")
	if got != ":2" {
		t.Fatalf("GEOADD = %q want :2", got)
	}
}

func TestGeoPosValues(t *testing.T) {
	r, c := startData(t)
	addSicily(t, r, c)

	head := sendLine(t, r, c, "GEOPOS Sicily Palermo")
	if head != "*1" {
		t.Fatalf("GEOPOS header = %q", head)
	}
	// One coordinate array of two bulk strings.
	if got := readElemArrayLen(t, r); got != 2 {
		t.Fatalf("coord array len = %d", got)
	}
	lon := mustFloat(t, readElem(t, r))
	lat := mustFloat(t, readElem(t, r))
	if !approx(lon, 13.361389, 1e-4) {
		t.Fatalf("lon = %v", lon)
	}
	if !approx(lat, 38.115556, 1e-4) {
		t.Fatalf("lat = %v", lat)
	}
}

func TestGeoPosMissingMember(t *testing.T) {
	r, c := startData(t)
	addSicily(t, r, c)
	head := sendLine(t, r, c, "GEOPOS Sicily NonExisting")
	if head != "*1" {
		t.Fatalf("header = %q", head)
	}
	if got := readElem(t, r); got != "<nil>" {
		t.Fatalf("missing member = %q want nil", got)
	}
}

func TestGeoDist(t *testing.T) {
	r, c := startData(t)
	addSicily(t, r, c)
	if got := bulk(t, r, c, "GEODIST Sicily Palermo Catania"); got != "166274.1516" {
		t.Fatalf("GEODIST m = %q want 166274.1516", got)
	}
	if got := bulk(t, r, c, "GEODIST Sicily Palermo Catania km"); got != "166.2742" {
		t.Fatalf("GEODIST km = %q want 166.2742", got)
	}
	if got := sendLine(t, r, c, "GEODIST Sicily Palermo NonExisting"); got != "$-1" {
		t.Fatalf("GEODIST missing = %q want nil", got)
	}
}

func TestGeoHash(t *testing.T) {
	r, c := startData(t)
	addSicily(t, r, c)
	got := readArray(t, r, c, "GEOHASH Sicily Palermo Catania")
	want := []string{"sqc8b49rny0", "sqdtr74hyu0"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("GEOHASH[%d] = %q want %q", i, got[i], want[i])
		}
	}
}

func TestGeoSearchRadius(t *testing.T) {
	r, c := startData(t)
	addSicily(t, r, c)
	got := readArray(t, r, c, "GEOSEARCH Sicily FROMLONLAT 15 37 BYRADIUS 200 km ASC")
	if len(got) != 2 || got[0] != "Catania" || got[1] != "Palermo" {
		t.Fatalf("GEOSEARCH = %v want [Catania Palermo]", got)
	}
}

func TestGeoSearchBox(t *testing.T) {
	r, c := startData(t)
	addSicily(t, r, c)
	got := readArray(t, r, c, "GEOSEARCH Sicily FROMLONLAT 15 37 BYBOX 400 400 km ASC")
	if len(got) != 2 {
		t.Fatalf("GEOSEARCH box = %v want 2 members", got)
	}
}

func TestGeoSearchFromMember(t *testing.T) {
	r, c := startData(t)
	addSicily(t, r, c)
	got := readArray(t, r, c, "GEOSEARCH Sicily FROMMEMBER Palermo BYRADIUS 200 km ASC")
	if len(got) != 2 || got[0] != "Palermo" {
		t.Fatalf("GEOSEARCH FROMMEMBER = %v want Palermo first", got)
	}
}

func TestGeoSearchCount(t *testing.T) {
	r, c := startData(t)
	addSicily(t, r, c)
	got := readArray(t, r, c, "GEOSEARCH Sicily FROMLONLAT 15 37 BYRADIUS 200 km ASC COUNT 1")
	if len(got) != 1 || got[0] != "Catania" {
		t.Fatalf("GEOSEARCH COUNT 1 = %v want [Catania]", got)
	}
}

func TestGeoSearchWithDist(t *testing.T) {
	r, c := startData(t)
	addSicily(t, r, c)
	head := sendLine(t, r, c, "GEOSEARCH Sicily FROMLONLAT 15 37 BYRADIUS 200 km ASC WITHDIST")
	if head != "*2" {
		t.Fatalf("header = %q", head)
	}
	// Each element is [member, dist]. First is Catania.
	if got := readElemArrayLen(t, r); got != 2 {
		t.Fatalf("elem array len = %d", got)
	}
	if got := readElem(t, r); got != "Catania" {
		t.Fatalf("member = %q", got)
	}
	d := mustFloat(t, readElem(t, r))
	if !approx(d, 56.4413, 0.01) {
		t.Fatalf("dist = %v want ~56.44", d)
	}
	drainElem(t, r) // second element [Palermo, dist]
}

func TestGeoSearchStore(t *testing.T) {
	r, c := startData(t)
	addSicily(t, r, c)
	if got := sendLine(t, r, c, "GEOSEARCHSTORE dst Sicily FROMLONLAT 15 37 BYRADIUS 200 km ASC"); got != ":2" {
		t.Fatalf("GEOSEARCHSTORE = %q want :2", got)
	}
	if got := sendLine(t, r, c, "ZCARD dst"); got != ":2" {
		t.Fatalf("ZCARD dst = %q want :2", got)
	}
	// Stored with the geohash score, so GEOPOS works on the copy.
	if got := bulk(t, r, c, "GEODIST dst Palermo Catania"); got != "166274.1516" {
		t.Fatalf("GEODIST on stored = %q", got)
	}
}

func TestGeoRadiusDeprecated(t *testing.T) {
	r, c := startData(t)
	addSicily(t, r, c)
	got := readArray(t, r, c, "GEORADIUS Sicily 15 37 200 km ASC")
	if len(got) != 2 || got[0] != "Catania" {
		t.Fatalf("GEORADIUS = %v", got)
	}
}

func TestGeoRadiusByMember(t *testing.T) {
	r, c := startData(t)
	addSicily(t, r, c)
	got := readArray(t, r, c, "GEORADIUSBYMEMBER Sicily Palermo 200 km ASC")
	if len(got) != 2 || got[0] != "Palermo" {
		t.Fatalf("GEORADIUSBYMEMBER = %v", got)
	}
}

func TestGeoAddInvalidCoord(t *testing.T) {
	r, c := startData(t)
	got := sendLine(t, r, c, "GEOADD Sicily 13.36 92 BadLat")
	if !strings.HasPrefix(got, "-ERR invalid longitude,latitude pair") {
		t.Fatalf("bad coord = %q", got)
	}
}

func TestGeoWrongType(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET plain v")
	if got := sendLine(t, r, c, "GEOADD plain 13 38 x"); !strings.HasPrefix(got, "-WRONGTYPE") {
		t.Fatalf("GEOADD wrongtype = %q", got)
	}
	if got := sendLine(t, r, c, "GEOPOS plain x"); !strings.HasPrefix(got, "-WRONGTYPE") {
		t.Fatalf("GEOPOS wrongtype = %q", got)
	}
}

func TestGeoAddNX(t *testing.T) {
	r, c := startData(t)
	addSicily(t, r, c)
	// NX keeps the existing point, so re-adding Palermo at a new spot is a no-op.
	if got := sendLine(t, r, c, "GEOADD Sicily NX 0 0 Palermo"); got != ":0" {
		t.Fatalf("GEOADD NX = %q want :0", got)
	}
	if got := bulk(t, r, c, "GEODIST Sicily Palermo Catania"); got != "166274.1516" {
		t.Fatalf("Palermo moved under NX: %q", got)
	}
}

// readElemArrayLen reads an array header element and returns its length.
func readElemArrayLen(t *testing.T, r *bufio.Reader) int {
	t.Helper()
	line, err := r.ReadString('\n')
	if err != nil {
		t.Fatalf("read array len: %v", err)
	}
	line = strings.TrimRight(line, "\r\n")
	if len(line) == 0 || line[0] != '*' {
		t.Fatalf("expected array header, got %q", line)
	}
	n, _ := strconv.Atoi(line[1:])
	return n
}

func mustFloat(t *testing.T, s string) float64 {
	t.Helper()
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		t.Fatalf("parse float %q: %v", s, err)
	}
	return f
}

func approx(a, b, tol float64) bool {
	d := a - b
	if d < 0 {
		d = -d
	}
	return d <= tol
}
