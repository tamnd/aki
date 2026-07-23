// Lab: ring depth and batch-submit threshold against the iopool
// backend (spec 2064/sqlo1 doc 04 section 12, milestone B5 lab 01).
//
// B5 bakes three constants: the ring depth, the batch-submit
// threshold (the spec carries 16 as the amortization point and bans
// 128 for its tail cost), and the registered-buffer pool size. The
// first two are priced here by driving both backends through the
// Backend seam with the same request stream: batches of group reads
// against a filled file for the cold-read shape, sequential group
// writes with an fsync barrier per extent for the drain shape.
//
// What a row means: every request in a batch is stamped with the
// batch's submit time, so per-op latency includes the queueing a
// too-deep batch inflicts on its own tail. That is exactly the
// spec's argument against batch 128, and the p99 column is where it
// has to show.
//
// Two caveats the verdict must respect. Reads here are page-cache
// warm unless -direct is set, so warm read rows price the submission
// path (syscall count, wakeups, copy discipline), not device latency;
// the gate box reruns this sweep with -direct for the cold shape. The
// -regbuf flag (slice 2) switches the ring arm's batch slots to the
// registered pool so the fixed opcodes carry the IO, and O_DIRECT
// requires it since the pool is the aligned memory.
//
// The ring arm needs a Linux kernel with io_uring; elsewhere the
// binary exits 3 so run.sh can skip those rows.
package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"slices"
	"time"

	"github.com/tamnd/aki/engine/sqlo1b"
)

const (
	extSize      = 1 << 20 // the format's fixed extent size
	groupsPerExt = extSize / sqlo1b.GroupSize
)

func main() {
	backend := flag.String("backend", "iopool", "iopool or ring")
	workload := flag.String("workload", "coldread", "coldread or drain")
	depth := flag.Int("depth", 8, "ring depth or iopool workers")
	batch := flag.Int("batch", 16, "requests per Submit call")
	n := flag.Int("n", 20000, "requests per run")
	exts := flag.Int("exts", 64, "extents in the coldread working file")
	regbuf := flag.Int("regbuf", 0, "registered pool size in buffers, ring only; 0 uses heap buffers")
	direct := flag.Bool("direct", false, "reopen the file O_DIRECT, ring only")
	probe := flag.Bool("probe", false, "exit 0 if the backend can run here, 3 if not")
	flag.Parse()

	if *probe {
		if *backend == "ring" {
			if err := sqlo1b.RingProbe(); err != nil {
				fmt.Fprintf(os.Stderr, "ring unavailable: %v\n", err)
				os.Exit(3)
			}
		}
		return
	}

	f, err := os.CreateTemp("", "ringpool-*.dat")
	if err != nil {
		fatal(err)
	}
	defer os.Remove(f.Name())
	defer f.Close()

	fileExts := *exts
	if *workload == "drain" {
		fileExts = (*n + groupsPerExt - 1) / groupsPerExt
	}
	if err := fill(f, fileExts, *workload == "coldread"); err != nil {
		fatal(err)
	}

	if *direct {
		// O_DIRECT is the ring arm's own-caching mode; the fill above
		// went through the buffered fd, this swap makes the measured
		// IO bypass the page cache.
		if *backend != "ring" {
			fatal(fmt.Errorf("-direct is ring only"))
		}
		f.Close()
		df, err := sqlo1b.OpenDirect(f.Name())
		if err != nil {
			fmt.Fprintf(os.Stderr, "O_DIRECT unavailable: %v\n", err)
			os.Exit(3)
		}
		f = df
	}

	comp := make(chan sqlo1b.IOResult, *batch+1)
	var b sqlo1b.Backend
	bufAt := func(i int) []byte { return make([]byte, sqlo1b.GroupSize) }
	switch *backend {
	case "iopool":
		b = sqlo1b.NewIOPool(f, extSize, *depth, comp)
	case "ring":
		r, err := sqlo1b.NewIORing(f, extSize, *depth, *regbuf, comp)
		if err != nil {
			fmt.Fprintf(os.Stderr, "ring unavailable: %v\n", err)
			os.Exit(3)
		}
		if *regbuf > 0 {
			if *regbuf < *batch {
				fatal(fmt.Errorf("-regbuf %d smaller than -batch %d", *regbuf, *batch))
			}
			bufAt = r.RegBuf
		} else if *direct {
			fatal(fmt.Errorf("-direct needs -regbuf for aligned buffers"))
		}
		b = r
	default:
		fatal(fmt.Errorf("backend %q", *backend))
	}

	var lat []time.Duration
	start := time.Now()
	switch *workload {
	case "coldread":
		lat = coldread(b, comp, *n, *batch, fileExts, bufAt)
	case "drain":
		lat = drain(b, comp, *n, *batch, bufAt)
	default:
		fatal(fmt.Errorf("workload %q", *workload))
	}
	secs := time.Since(start).Seconds()
	b.Close()

	slices.Sort(lat)
	bytes := float64(*n) * sqlo1b.GroupSize
	fmt.Printf("%s,%s,%d,%d,%d,%d,%d,%.3f,%.0f,%.1f,%.1f,%.1f\n",
		*backend, *workload, *depth, *batch, *regbuf, boolInt(*direct), *n, secs,
		float64(*n)/secs, bytes/secs/1e6,
		us(lat[len(lat)/2]), us(lat[len(lat)*99/100]))
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func us(d time.Duration) float64 { return float64(d.Nanoseconds()) / 1e3 }

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "ringpool:", err)
	os.Exit(1)
}

// fill sizes the file and, for the read shape, writes real bytes so
// no read lands on a hole.
func fill(f *os.File, exts int, data bool) error {
	if err := f.Truncate(int64(exts) * extSize); err != nil {
		return err
	}
	if !data {
		return nil
	}
	buf := make([]byte, extSize)
	for i := range buf {
		buf[i] = byte(i * 7)
	}
	for e := range exts {
		if _, err := f.WriteAt(buf, int64(e)*extSize); err != nil {
			return err
		}
	}
	return f.Sync()
}

// collect drains one batch's completions, stamping each against the
// batch submit time so queueing inside the batch counts.
func collect(comp chan sqlo1b.IOResult, k int, t0 time.Time, lat []time.Duration) []time.Duration {
	for range k {
		res := <-comp
		if res.Err != nil {
			fatal(res.Err)
		}
		lat = append(lat, time.Since(t0))
	}
	return lat
}

// coldread issues batches of random group reads across the filled
// file, one buffer per batch slot, waiting out each batch before the
// next so in-flight depth equals the batch size under test.
func coldread(b sqlo1b.Backend, comp chan sqlo1b.IOResult, n, batch, exts int, bufAt func(int) []byte) []time.Duration {
	rng := rand.New(rand.NewSource(1))
	bufs := make([][]byte, batch)
	for i := range bufs {
		bufs[i] = bufAt(i)
	}
	lat := make([]time.Duration, 0, n)
	for done := 0; done < n; {
		k := min(batch, n-done)
		reqs := make([]sqlo1b.IOReq, k)
		for i := range reqs {
			reqs[i] = sqlo1b.IOReq{
				Op:  sqlo1b.OpRead,
				Ext: uint64(rng.Intn(exts)),
				Off: uint32(rng.Intn(groupsPerExt)) * sqlo1b.GroupSize,
				Buf: bufs[i],
				Tag: uint64(done + i),
			}
		}
		t0 := time.Now()
		b.Submit(reqs)
		lat = collect(comp, k, t0, lat)
		done += k
	}
	return lat
}

// drain writes groups sequentially and puts an fsync barrier at
// every extent boundary, the checkpoint shape the store's drain
// path pays.
func drain(b sqlo1b.Backend, comp chan sqlo1b.IOResult, n, batch int, bufAt func(int) []byte) []time.Duration {
	bufs := make([][]byte, batch)
	for i := range bufs {
		bufs[i] = bufAt(i)
		for j := range bufs[i] {
			bufs[i][j] = byte(i + j)
		}
	}
	lat := make([]time.Duration, 0, n)
	synced := 0
	for done := 0; done < n; {
		k := min(batch, n-done)
		reqs := make([]sqlo1b.IOReq, k)
		for i := range reqs {
			g := done + i
			reqs[i] = sqlo1b.IOReq{
				Op:  sqlo1b.OpWrite,
				Ext: uint64(g / groupsPerExt),
				Off: uint32(g%groupsPerExt) * sqlo1b.GroupSize,
				Buf: bufs[i],
				Tag: uint64(g),
			}
		}
		t0 := time.Now()
		b.Submit(reqs)
		lat = collect(comp, k, t0, lat)
		done += k
		if done/groupsPerExt > synced || done == n {
			synced = done / groupsPerExt
			b.Sync(uint64(1 << 40))
			if res := <-comp; res.Err != nil {
				fatal(res.Err)
			}
		}
	}
	return lat
}
