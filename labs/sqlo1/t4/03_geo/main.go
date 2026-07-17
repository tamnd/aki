// Lab: geo search cell cover (spec 2064/sqlo1 doc 09 section 7,
// milestone T4 lab 03).
//
// Geo search computes a cell cover of the search area, scans each
// cell as a sortable-score run range, and passes candidates through
// the exact distance filter. Redis picks the cell precision from the
// radius (geohashEstimateStepsByRadius) and covers with the center
// cell plus neighbors, which bounds the cover at nine cells but
// admits candidates well outside the circle. A finer precision cuts
// that over-read, but every extra cell is another fence-guided range
// scan, and our unit of cost is runs read: a run is a ~2.7 KB record
// that may be a cold pread. This lab prices the trade so slice 11
// bakes the cover precision on numbers: candidates examined per
// result (the over-read ratio doc 09 section 10 names), distinct
// runs read per search, and the hot walk latency, for Redis's step,
// one step finer, and one step coarser, across radii 100 m to 500 km
// and latitude bands 0, 45, and 70.
//
// The model is engine-faithful and resident (the salgebra pattern):
// points live as a sorted array of 52-bit scores only, coordinates
// are decoded from the score on every probe exactly as the engine
// will decode them, cell scans are binary searches on the array, and
// the filter is Redis's own haversine on Redis's earth radius, since
// parity on boundary points needs the same floating point path. The
// codec, the step estimator, and the filter are checked against a
// live Redis 8.8.0 by the -parity mode, which GEOADDs a fixture and
// demands bit-identical ZSCOREs, matching GEOPOS decodes, matching
// GEODIST values, and set-identical GEOSEARCH results.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Redis's geo constants, geohash.c and geohash_helper.c verbatim.
// The score is 26 interleaved steps per axis, latitude clamped to the
// mercator range, and distances use Redis's earth radius.
const (
	geoStep     = 26
	geoLatMin   = -85.05112878
	geoLatMax   = 85.05112878
	geoLonMin   = -180.0
	geoLonMax   = 180.0
	earthR      = 6372797.560856
	mercatorMax = 20037726.37
)

func deg2rad(d float64) float64 { return d * math.Pi / 180 }

// interleave64 spreads x into the even bits and y into the odd bits,
// Redis's magic-number ladder. The encoder passes latitude as x.
func interleave64(x, y uint32) uint64 {
	xx, yy := uint64(x), uint64(y)
	xx = (xx | (xx << 16)) & 0x0000FFFF0000FFFF
	xx = (xx | (xx << 8)) & 0x00FF00FF00FF00FF
	xx = (xx | (xx << 4)) & 0x0F0F0F0F0F0F0F0F
	xx = (xx | (xx << 2)) & 0x3333333333333333
	xx = (xx | (xx << 1)) & 0x5555555555555555
	yy = (yy | (yy << 16)) & 0x0000FFFF0000FFFF
	yy = (yy | (yy << 8)) & 0x00FF00FF00FF00FF
	yy = (yy | (yy << 4)) & 0x0F0F0F0F0F0F0F0F
	yy = (yy | (yy << 2)) & 0x3333333333333333
	yy = (yy | (yy << 1)) & 0x5555555555555555
	return xx | (yy << 1)
}

// compressEven collapses the even bits of v back into a 32-bit int,
// the inverse half of interleave64.
func compressEven(v uint64) uint32 {
	v &= 0x5555555555555555
	v = (v | (v >> 1)) & 0x3333333333333333
	v = (v | (v >> 2)) & 0x0F0F0F0F0F0F0F0F
	v = (v | (v >> 4)) & 0x00FF00FF00FF00FF
	v = (v | (v >> 8)) & 0x0000FFFF0000FFFF
	v = (v | (v >> 16)) & 0x00000000FFFFFFFF
	return uint32(v)
}

func geoEncode(lon, lat float64) uint64 {
	latOff := (lat - geoLatMin) / (geoLatMax - geoLatMin)
	lonOff := (lon - geoLonMin) / (geoLonMax - geoLonMin)
	ilat := uint32(latOff * float64(uint64(1)<<geoStep))
	ilon := uint32(lonOff * float64(uint64(1)<<geoStep))
	return interleave64(ilat, ilon)
}

// geoDecode returns the cell midpoint, mirroring Redis's decode
// arithmetic term for term so the midpoints agree to the last bit.
func geoDecode(bits uint64) (lon, lat float64) {
	ilat := compressEven(bits)
	ilon := compressEven(bits >> 1)
	latScale := geoLatMax - geoLatMin
	lonScale := geoLonMax - geoLonMin
	latMinC := geoLatMin + (float64(ilat)*1.0/float64(uint64(1)<<geoStep))*latScale
	latMaxC := geoLatMin + (float64(ilat+1)*1.0/float64(uint64(1)<<geoStep))*latScale
	lonMinC := geoLonMin + (float64(ilon)*1.0/float64(uint64(1)<<geoStep))*lonScale
	lonMaxC := geoLonMin + (float64(ilon+1)*1.0/float64(uint64(1)<<geoStep))*lonScale
	return (lonMinC + lonMaxC) / 2, (latMinC + latMaxC) / 2
}

// estimateStep is geohashEstimateStepsByRadius: precision from the
// radius, widened toward the poles where mercator cells narrow.
func estimateStep(r, lat float64) int {
	if r == 0 {
		return geoStep
	}
	step := 1
	for r < mercatorMax {
		r *= 2
		step++
	}
	step -= 2
	if lat > 66 || lat < -66 {
		step--
		if lat > 80 || lat < -80 {
			step--
		}
	}
	return min(max(step, 1), geoStep)
}

// geoDist is geohashGetDistance: haversine on Redis's earth radius,
// with the same longitude-only shortcut.
func geoDist(lon1, lat1, lon2, lat2 float64) float64 {
	lon1r, lon2r := deg2rad(lon1), deg2rad(lon2)
	v := math.Sin((lon2r - lon1r) / 2)
	if v == 0 {
		return earthR * math.Abs(deg2rad(lat2)-deg2rad(lat1))
	}
	lat1r, lat2r := deg2rad(lat1), deg2rad(lat2)
	u := math.Sin((lat2r - lat1r) / 2)
	a := u*u + math.Cos(lat1r)*math.Cos(lat2r)*v*v
	return 2 * earthR * math.Asin(math.Sqrt(a))
}

// cellIdx maps a coordinate to its cell index on one axis at step,
// clamped to the axis, which stands in for longitude wrap handling
// (the sweep's data sits away from the antimeridian).
func cellIdx(v, min, max float64, step int) uint32 {
	if v < min {
		v = min
	}
	if v > max {
		v = max
	}
	i := int64((v - min) / (max - min) * float64(uint64(1)<<step))
	if i < 0 {
		i = 0
	}
	if i >= int64(1)<<step {
		i = int64(1)<<step - 1
	}
	return uint32(i)
}

type model struct {
	scores  []uint64
	runSize int
}

type searchRes struct {
	step, cells, cands, results, runs int
}

// search covers the circle's bounding box with cells at the
// estimated step plus stepDelta, scans each cell as a score range on
// the sorted array (the fence-guided run range scan), decodes every
// candidate, and filters by exact distance. hits, when set, receives
// the index of every passing point.
func (m *model) search(lon, lat, r float64, stepDelta int, hits func(i int)) searchRes {
	step := min(max(estimateStep(r, lat)+stepDelta, 1), geoStep)
	dlat := r / earthR * 180 / math.Pi
	dlon := dlat / math.Cos(deg2rad(lat))
	iLat0 := cellIdx(lat-dlat, geoLatMin, geoLatMax, step)
	iLat1 := cellIdx(lat+dlat, geoLatMin, geoLatMax, step)
	iLon0 := cellIdx(lon-dlon, geoLonMin, geoLonMax, step)
	iLon1 := cellIdx(lon+dlon, geoLonMin, geoLonMax, step)
	res := searchRes{step: step}
	shift := uint(2 * (geoStep - step))
	runSeen := map[int]struct{}{}
	for ila := iLat0; ila <= iLat1; ila++ {
		for ilo := iLon0; ilo <= iLon1; ilo++ {
			res.cells++
			cell := interleave64(ila, ilo)
			lo := cell << shift
			hi := (cell + 1) << shift
			i0 := sort.Search(len(m.scores), func(i int) bool { return m.scores[i] >= lo })
			i1 := sort.Search(len(m.scores), func(i int) bool { return m.scores[i] >= hi })
			for i := i0; i < i1; i++ {
				res.cands++
				plon, plat := geoDecode(m.scores[i])
				if geoDist(lon, lat, plon, plat) <= r {
					res.results++
					if hits != nil {
						hits(i)
					}
				}
			}
			// Run accounting routes the way the fence does: the scan
			// starts at the run whose separator owns lo, which is the
			// run of the last entry below the range, and an empty
			// range still reads that run because separators cannot
			// prove interior emptiness. Only a range below the first
			// separator is provably empty and reads nothing.
			if i1 > i0 {
				start := 0
				if i0 > 0 {
					start = (i0 - 1) / m.runSize
				}
				for k := start; k <= (i1-1)/m.runSize; k++ {
					runSeen[k] = struct{}{}
				}
			} else if i0 > 0 {
				runSeen[(i0-1)/m.runSize] = struct{}{}
			}
		}
	}
	res.runs = len(runSeen)
	return res
}

type pt struct{ lon, lat float64 }

// genPoints fills a 1200 km square around (0, lat): uniform spreads
// evenly, cluster drops 200 gaussian towns at 2 km sigma, the two
// densities a metro workload swings between.
func genPoints(ds string, n int, lat float64, rng *rand.Rand) []pt {
	const halfKm = 600.0
	dlat := halfKm * 1000 / earthR * 180 / math.Pi
	dlon := dlat / math.Cos(deg2rad(lat))
	clampLat := func(v float64) float64 {
		return math.Min(geoLatMax-1e-9, math.Max(geoLatMin+1e-9, v))
	}
	pts := make([]pt, 0, n)
	switch ds {
	case "uniform":
		for range n {
			pts = append(pts, pt{
				lon: (rng.Float64()*2 - 1) * dlon,
				lat: clampLat(lat + (rng.Float64()*2-1)*dlat),
			})
		}
	case "cluster":
		const k = 200
		sigLat := 2.0 * 1000 / earthR * 180 / math.Pi
		sigLon := sigLat / math.Cos(deg2rad(lat))
		centers := make([]pt, k)
		for i := range centers {
			centers[i] = pt{
				lon: (rng.Float64()*2 - 1) * dlon,
				lat: lat + (rng.Float64()*2-1)*dlat,
			}
		}
		for range n {
			c := centers[rng.Intn(k)]
			pts = append(pts, pt{
				lon: c.lon + rng.NormFloat64()*sigLon,
				lat: clampLat(c.lat + rng.NormFloat64()*sigLat),
			})
		}
	default:
		fmt.Fprintf(os.Stderr, "unknown dataset %q\n", ds)
		os.Exit(1)
	}
	return pts
}

func buildModel(pts []pt, runSize int) *model {
	scores := make([]uint64, len(pts))
	for i, p := range pts {
		scores[i] = geoEncode(p.lon, p.lat)
	}
	slices.Sort(scores)
	return &model{scores: scores, runSize: runSize}
}

type quant []float64

func (q quant) at(p float64) float64 {
	s := append(quant(nil), q...)
	sort.Float64s(s)
	if len(s) == 0 {
		return 0
	}
	i := int(p * float64(len(s)-1))
	return s[i]
}

var armDelta = map[string]int{"coarse": -1, "redis": 0, "fine": 1}

func main() {
	dataset := flag.String("dataset", "uniform", "uniform or cluster")
	n := flag.Int("n", 1000000, "points")
	lat := flag.Float64("lat", 45, "latitude band of the dataset")
	radius := flag.Float64("radius", 1000, "search radius, meters")
	arm := flag.String("arm", "redis", "coarse, redis, or fine")
	searches := flag.Int("searches", 64, "searches per cell")
	runSize := flag.Int("runsize", 104, "entries per score run, the hsegz occupancy")
	seed := flag.Int64("seed", 1, "rng seed")
	quick := flag.Bool("quick", false, "smoke sweep")
	parity := flag.Bool("parity", false, "check codec, filter, and search results against a live Redis")
	port := flag.Int("port", 7799, "redis port for -parity")
	flag.Parse()

	if *parity {
		runParity(*port)
		return
	}
	if *quick {
		rng := rand.New(rand.NewSource(*seed))
		m := buildModel(genPoints("uniform", 50000, 45, rng), *runSize)
		for _, r := range []float64{1000, 100000} {
			for _, a := range []string{"coarse", "redis", "fine"} {
				sweepCell(m, "uniform", 50000, 45, r, a, 16, rng)
			}
		}
		return
	}

	if _, ok := armDelta[*arm]; !ok {
		fmt.Fprintf(os.Stderr, "unknown arm %q\n", *arm)
		os.Exit(1)
	}
	rng := rand.New(rand.NewSource(*seed))
	m := buildModel(genPoints(*dataset, *n, *lat, rng), *runSize)
	sweepCell(m, *dataset, *n, *lat, *radius, *arm, *searches, rng)
}

// sweepCell times searches at random centers in the dataset core and
// prints one CSV row. Over-read is the ratio of candidate sums, the
// counts are means, latency is per-search quantiles.
func sweepCell(m *model, dataset string, n int, lat, radius float64, arm string, searches int, rng *rand.Rand) {
	const halfKm = 600.0
	dlat := halfKm * 1000 / earthR * 180 / math.Pi / 2
	dlon := dlat / math.Cos(deg2rad(lat))
	delta := armDelta[arm]
	var cells, cands, results, runs, step float64
	var lats quant
	for range searches {
		clon := (rng.Float64()*2 - 1) * dlon
		clat := lat + (rng.Float64()*2-1)*dlat
		t0 := time.Now()
		r := m.search(clon, clat, radius, delta, nil)
		el := time.Since(t0)
		lats = append(lats, float64(el.Nanoseconds())/1e3)
		cells += float64(r.cells)
		cands += float64(r.cands)
		results += float64(r.results)
		runs += float64(r.runs)
		step = float64(r.step)
	}
	sf := float64(searches)
	over := 0.0
	if results > 0 {
		over = cands / results
	}
	fmt.Printf("%s,%d,%.0f,%.0f,%s,%.0f,%.1f,%.1f,%.1f,%.2f,%.1f,%.1f,%.1f\n",
		dataset, n, lat, radius, arm, step,
		cells/sf, cands/sf, results/sf, over, runs/sf,
		lats.at(0.50), lats.at(0.99))
}

// --- parity against a live Redis ---

type rcli struct {
	c  net.Conn
	br *bufio.Reader
}

func dialRedis(port int) *rcli {
	c, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		fmt.Fprintf(os.Stderr, "dial redis: %v\n", err)
		os.Exit(1)
	}
	return &rcli{c: c, br: bufio.NewReader(c)}
}

func (r *rcli) cmd(args ...string) any {
	var wb []byte
	wb = append(wb, fmt.Sprintf("*%d\r\n", len(args))...)
	for _, a := range args {
		wb = append(wb, fmt.Sprintf("$%d\r\n%s\r\n", len(a), a)...)
	}
	if _, err := r.c.Write(wb); err != nil {
		fmt.Fprintf(os.Stderr, "write: %v\n", err)
		os.Exit(1)
	}
	v, err := r.read()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", args[0], err)
		os.Exit(1)
	}
	return v
}

func (r *rcli) read() (any, error) {
	line, err := r.br.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimRight(line, "\r\n")
	switch line[0] {
	case '+':
		return line[1:], nil
	case '-':
		return nil, fmt.Errorf("redis: %s", line[1:])
	case ':':
		return strconv.ParseInt(line[1:], 10, 64)
	case '$':
		n, _ := strconv.Atoi(line[1:])
		if n < 0 {
			return nil, nil
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(r.br, buf); err != nil {
			return nil, err
		}
		return string(buf[:n]), nil
	case '*':
		n, _ := strconv.Atoi(line[1:])
		if n < 0 {
			return nil, nil
		}
		out := make([]any, n)
		for i := range out {
			v, err := r.read()
			if err != nil {
				return nil, err
			}
			out[i] = v
		}
		return out, nil
	}
	return nil, fmt.Errorf("bad reply %q", line)
}

func fmtF(v float64) string { return strconv.FormatFloat(v, 'g', 17, 64) }

// runParity GEOADDs a mixed fixture and demands: ZSCORE bit-identical
// to our encoder on every point, GEOPOS matching our decode, GEODIST
// matching our distance, and GEOSEARCH results set-identical both to
// a brute filter over decoded scores and to the redis-arm cover walk.
func runParity(port int) {
	cli := dialRedis(port)
	cli.cmd("DEL", "geolab")
	rng := rand.New(rand.NewSource(7))
	pts := genPoints("uniform", 3000, 45, rng)
	pts = append(pts, genPoints("cluster", 3000, 45, rng)...)

	for i := 0; i < len(pts); i += 500 {
		args := []string{"GEOADD", "geolab"}
		for j := i; j < i+500 && j < len(pts); j++ {
			args = append(args, fmtF(pts[j].lon), fmtF(pts[j].lat), "p"+strconv.Itoa(j))
		}
		cli.cmd(args...)
	}

	scoreMiss := 0
	scoreByName := map[string]uint64{}
	for i, p := range pts {
		name := "p" + strconv.Itoa(i)
		v := cli.cmd("ZSCORE", "geolab", name).(string)
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			fmt.Fprintf(os.Stderr, "ZSCORE parse %q: %v\n", v, err)
			os.Exit(1)
		}
		got := uint64(f)
		want := geoEncode(p.lon, p.lat)
		scoreByName[name] = got
		if got != want {
			scoreMiss++
			if scoreMiss <= 3 {
				fmt.Fprintf(os.Stderr, "score mismatch %s: redis %d ours %d\n", name, got, want)
			}
		}
	}

	posMiss := 0
	for range 200 {
		i := rng.Intn(len(pts))
		name := "p" + strconv.Itoa(i)
		v := cli.cmd("GEOPOS", "geolab", name).([]any)[0].([]any)
		rlon, _ := strconv.ParseFloat(v[0].(string), 64)
		rlat, _ := strconv.ParseFloat(v[1].(string), 64)
		olon, olat := geoDecode(scoreByName[name])
		if math.Abs(rlon-olon) > 1e-9 || math.Abs(rlat-olat) > 1e-9 {
			posMiss++
			if posMiss <= 3 {
				fmt.Fprintf(os.Stderr, "GEOPOS mismatch %s: redis (%v,%v) ours (%v,%v)\n", name, rlon, rlat, olon, olat)
			}
		}
	}

	distMiss := 0
	for range 200 {
		a, b := rng.Intn(len(pts)), rng.Intn(len(pts))
		na, nb := "p"+strconv.Itoa(a), "p"+strconv.Itoa(b)
		v := cli.cmd("GEODIST", "geolab", na, nb).(string)
		rd, _ := strconv.ParseFloat(v, 64)
		alon, alat := geoDecode(scoreByName[na])
		blon, blat := geoDecode(scoreByName[nb])
		od := geoDist(alon, alat, blon, blat)
		// GEODIST prints at 4 decimals, so compare at that grain.
		if math.Abs(rd-od) > 5e-4 {
			distMiss++
			if distMiss <= 3 {
				fmt.Fprintf(os.Stderr, "GEODIST mismatch %s %s: redis %v ours %v\n", na, nb, rd, od)
			}
		}
	}

	// The model over the same fixture, plus name lookup by score
	// position so cover hits map back to members. Scores can collide
	// across points, so positions map to name lists.
	m := buildModel(pts, 104)
	namesAt := make([][]string, len(m.scores))
	used := map[int]bool{}
	for i, p := range pts {
		sc := geoEncode(p.lon, p.lat)
		j := sort.Search(len(m.scores), func(k int) bool { return m.scores[k] >= sc })
		for m.scores[j] == sc && used[j] {
			j++
		}
		if m.scores[j] != sc {
			fmt.Fprintln(os.Stderr, "internal: score placement lost")
			os.Exit(1)
		}
		used[j] = true
		namesAt[j] = append(namesAt[j], "p"+strconv.Itoa(i))
	}

	searchMiss := 0
	radii := []float64{100, 500, 1000, 5000, 10000, 50000, 100000, 500000}
	for k := range 200 {
		r := radii[k%len(radii)] * (0.5 + rng.Float64())
		clon := (rng.Float64()*2 - 1) * 3
		clat := 45 + (rng.Float64()*2-1)*3
		v := cli.cmd("GEOSEARCH", "geolab", "FROMLONLAT", fmtF(clon), fmtF(clat), "BYRADIUS", fmtF(r), "m").([]any)
		redisSet := map[string]bool{}
		for _, e := range v {
			redisSet[e.(string)] = true
		}
		bruteSet := map[string]bool{}
		for i, sc := range m.scores {
			plon, plat := geoDecode(sc)
			if geoDist(clon, clat, plon, plat) <= r {
				for _, nm := range namesAt[i] {
					bruteSet[nm] = true
				}
			}
		}
		coverSet := map[string]bool{}
		m.search(clon, clat, r, 0, func(i int) {
			for _, nm := range namesAt[i] {
				coverSet[nm] = true
			}
		})
		if !sameSet(redisSet, bruteSet) || !sameSet(redisSet, coverSet) {
			searchMiss++
			if searchMiss <= 3 {
				fmt.Fprintf(os.Stderr, "GEOSEARCH mismatch at (%.5f,%.5f) r=%.1f: redis %d brute %d cover %d\n",
					clon, clat, r, len(redisSet), len(bruteSet), len(coverSet))
			}
		}
	}

	cli.cmd("DEL", "geolab")
	fmt.Printf("parity: %d points, score mismatches %d, geopos %d, geodist %d, search %d/200\n",
		len(pts), scoreMiss, posMiss, distMiss, searchMiss)
	if scoreMiss+posMiss+distMiss+searchMiss > 0 {
		os.Exit(1)
	}
}

func sameSet(a, b map[string]bool) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if !b[k] {
			return false
		}
	}
	return true
}
