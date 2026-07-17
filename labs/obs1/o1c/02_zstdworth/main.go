// Command zstdworth prices compression for obs1 (spec 2064/obs1 doc 03
// sections 5 and 12): what zstd buys in storage dollars and cold-read
// bytes against what it costs in fold CPU and decode time, at the
// segment-block and WAL-section units, on value corpora spanning the
// compressibility range. The obs1 import boundary keeps codec modules
// out of the module graph, so the codec under test is the system zstd
// CLI in its in-memory benchmark mode (-b), with -B cutting the input
// into independent blocks of exactly our unit size.
package main

import (
	"flag"
	"fmt"
	"hash/crc32"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/tamnd/aki/engine/obs1/sim"
)

const (
	segBlockBytes = 128 << 10
	// Modeled doc 04 frame cost per WAL entry: header (seq, kind,
	// lengths, crc share) plus a key. The frame-overhead lab pinned the
	// exact wire sizes; these stand in at the same order.
	frameHdrBytes = 24
	frameKeyBytes = 16
)

// prng is splitmix64: deterministic, seeded, and free of module imports.
type prng struct{ s uint64 }

func (p *prng) next() uint64 {
	p.s += 0x9E3779B97F4A7C15
	z := p.s
	z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
	z = (z ^ (z >> 27)) * 0x94D049BB133111EB
	return z ^ (z >> 31)
}

func (p *prng) intn(n int) int { return int(p.next() % uint64(n)) }

func (p *prng) fill(b []byte) {
	for i := range b {
		b[i] = byte(p.next() >> 56)
	}
}

// jsonSess is the doc 09 example A value shape: a ~200 B session object
// whose field names repeat across values while ids and counters do not.
func jsonSess(p *prng) []byte {
	return fmt.Appendf(nil,
		`{"session_id":"%016x","user_id":%d,"created_ms":%d,"last_seen_ms":%d,"flags":["active","verified"],"cart_items":%d,"locale":"en-US","tier":"standard","checksum":"%08x"}`,
		p.next(), p.intn(10000000), 1700000000000+int64(p.intn(1<<31)),
		1700000000000+int64(p.intn(1<<31)), p.intn(40), uint32(p.next()))
}

var textWords = strings.Fields(`the of and to in a is that for it as was
with be by on not he this are or his from at which but have an they you
were her all she there would their we him been has when who will no more
if out so said what up its about into than them can only other new some
could time these two may then do first any my now such like our over man
me even most made after also did many before must through back years
where much your way well down should because each just those people mr
how too little state good very make world still own see men work long
here get both between life being under never day same another know while
last might us great old year off come since against go came right used
take three`)

// text2k builds a ~2 KiB english-like value from a fixed word list.
func text2k(p *prng) []byte {
	var b []byte
	for len(b) < 2048 {
		b = append(b, textWords[p.intn(len(textWords))]...)
		b = append(b, ' ')
	}
	return b[:2048]
}

// numSer is the short numeric shape of counters, scores, and serials.
func numSer(p *prng) []byte {
	return fmt.Appendf(nil, "%d:%d:%d", p.intn(1<<30), p.intn(1<<20), p.next()>>24)
}

// randBin is the incompressible shape of tokens, hashes, and ciphertext.
func randBin(p *prng) []byte {
	b := make([]byte, 200)
	p.fill(b)
	return b
}

type corpus struct {
	name string
	vals [][]byte
	size int
}

func buildCorpus(name string, target int, seed uint64, gen func(*prng) []byte) corpus {
	p := &prng{s: seed}
	c := corpus{name: name}
	for c.size < target {
		v := gen(p)
		c.vals = append(c.vals, v)
		c.size += len(v)
	}
	return c
}

// buildMixed lands bytes near 60/20/10/10 across the four families by
// always feeding the first family still under its per-mille budget,
// which keeps the mix honest regardless of value-size differences.
func buildMixed(target int, seed uint64) corpus {
	p := &prng{s: seed}
	c := corpus{name: "mixed"}
	gens := []func(*prng) []byte{jsonSess, text2k, numSer, randBin}
	want := []int{600, 200, 100, 100}
	spent := make([]int, 4)
	for c.size < target {
		fam := 3
		for i := range gens {
			if spent[i]*1000 < want[i]*(c.size+1) {
				fam = i
				break
			}
		}
		v := gens[fam](p)
		c.vals = append(c.vals, v)
		c.size += len(v)
		spent[fam] += len(v)
	}
	return c
}

func concat(c corpus) []byte {
	b := make([]byte, 0, c.size)
	for _, v := range c.vals {
		b = append(b, v...)
	}
	return b
}

// walStream wraps every value in the modeled frame: a 24 B header whose
// bytes carry sequence, kind, lengths, and a real CRC (so the header is
// neither padding nor noise), then a random-looking 16 B key.
func walStream(c corpus) []byte {
	p := &prng{s: 0x77A1}
	b := make([]byte, 0, c.size+len(c.vals)*(frameHdrBytes+frameKeyBytes))
	var seq uint64
	for _, v := range c.vals {
		seq++
		var h [frameHdrBytes]byte
		putU64(h[0:], seq)
		h[8] = 1
		putU32(h[9:], uint32(frameKeyBytes))
		putU32(h[13:], uint32(len(v)))
		putU32(h[17:], crc32.ChecksumIEEE(v))
		b = append(b, h[:]...)
		var k [frameKeyBytes]byte
		p.fill(k[:])
		b = append(b, k[:]...)
		b = append(b, v...)
	}
	return b
}

func putU64(b []byte, v uint64) {
	for i := range 8 {
		b[i] = byte(v >> (8 * i))
	}
}

func putU32(b []byte, v uint32) {
	for i := range 4 {
		b[i] = byte(v >> (8 * i))
	}
}

type benchRow struct {
	level     int
	compBytes int64
	compMBps  float64
	decMBps   float64
}

// parseBenchLine reads one quiet-mode result line, shaped like
// "-1      1048672 (1.000) 13897.14 MB/s 70571.9 MB/s  zb.bin".
func parseBenchLine(s string) (benchRow, bool) {
	f := strings.Fields(s)
	if len(f) < 7 || len(f[0]) < 2 || f[0][0] != '-' || f[4] != "MB/s" || f[6] != "MB/s" {
		return benchRow{}, false
	}
	lvl, err1 := strconv.Atoi(f[0][1:])
	comp, err2 := strconv.ParseInt(f[1], 10, 64)
	cs, err3 := strconv.ParseFloat(f[3], 64)
	ds, err4 := strconv.ParseFloat(f[5], 64)
	if err1 != nil || err2 != nil || err3 != nil || err4 != nil {
		return benchRow{}, false
	}
	return benchRow{level: lvl, compBytes: comp, compMBps: cs, decMBps: ds}, true
}

func zbench(bin, path string, blockBytes, iters int, levelArgs ...string) ([]benchRow, error) {
	args := append([]string{}, levelArgs...)
	args = append(args, fmt.Sprintf("-B%d", blockBytes), fmt.Sprintf("-i%d", iters), "-q", path)
	out, err := exec.Command(bin, args...).CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("zstd %v: %v\n%s", args, err, out)
	}
	var rows []benchRow
	for line := range strings.SplitSeq(string(out), "\n") {
		if r, ok := parseBenchLine(line); ok {
			rows = append(rows, r)
		}
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("zstd %v: no result lines in %q", args, out)
	}
	return rows, nil
}

// storageUSDPerRawGBMonth prices one raw GB held one month when it is
// stored at the given fraction of its size, through the sim table so the
// O5 E-cloud refit moves this lab automatically.
func storageUSDPerRawGBMonth(storedFrac float64) float64 {
	u := sim.Usage{BytesStoredPeak: int64(storedFrac * (1 << 30))}
	return sim.S3StandardPrices.Bill(u, 730*time.Hour).Storage
}

func decodeUsPerUnit(unitBytes int, decMBps float64) float64 {
	return float64(unitBytes) / (decMBps * 1e6) * 1e6
}

func cpuSecPerGiB(compMBps float64) float64 {
	return float64(1<<30) / (compMBps * 1e6)
}

func main() {
	quick := flag.Bool("quick", false, "small corpora and one-second reps, smoke only")
	bin := flag.String("zstd", "zstd", "zstd binary to drive")
	flag.Parse()

	target, iters := 32<<20, 2
	if *quick {
		target, iters = 2<<20, 1
	}

	ver, err := exec.Command(*bin, "--version").CombinedOutput()
	if err != nil {
		fmt.Fprintf(os.Stderr, "zstd binary not runnable: %v\n", err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "%s", ver)

	tmp, err := os.MkdirTemp("", "zstdworth")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer func() { _ = os.RemoveAll(tmp) }()

	corpora := []corpus{
		buildCorpus("jsonsess", target, 1, jsonSess),
		buildCorpus("text2k", target, 2, text2k),
		buildCorpus("numser", target, 3, numSer),
		buildCorpus("randbin", target, 4, randBin),
		buildMixed(target, 5),
	}

	fmt.Println("kind,corpus,unitKiB,level,rawMiB,ratio,storedFrac,compMBps,decompMBps,usdPerRawGBMonth,decodeUsPerUnit,cpuSecPerGiB")

	cell := func(kind string, c corpus, data []byte, unit int) {
		path := filepath.Join(tmp, fmt.Sprintf("%s-%s-%d.bin", kind, c.name, unit))
		if err := os.WriteFile(path, data, 0o600); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		var rows []benchRow
		for _, levelArgs := range [][]string{{"-b1", "-e3"}, {"-b9"}} {
			r, err := zbench(*bin, path, unit, iters, levelArgs...)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			rows = append(rows, r...)
		}
		raw := float64(len(data))
		for _, r := range rows {
			frac := float64(r.compBytes) / raw
			fmt.Printf("%s,%s,%d,%d,%.1f,%.3f,%.4f,%.1f,%.1f,%.5f,%.1f,%.2f\n",
				kind, c.name, unit>>10, r.level, raw/(1<<20), raw/float64(r.compBytes),
				frac, r.compMBps, r.decMBps, storageUSDPerRawGBMonth(frac),
				decodeUsPerUnit(unit, r.decMBps), cpuSecPerGiB(r.compMBps))
		}
	}

	for _, c := range corpora {
		seg := concat(c)
		cell("seg", c, seg, segBlockBytes)
		if c.name == "mixed" {
			cell("seg", c, seg, 32<<10)
			cell("seg", c, seg, 512<<10)
		}
		wal := walStream(c)
		for _, unit := range []int{4 << 10, 64 << 10, 1 << 20} {
			cell("wal", c, wal, unit)
		}
	}
}
