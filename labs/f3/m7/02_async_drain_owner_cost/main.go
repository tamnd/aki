// Lab: owner critical-path time the two-phase cold drain saves by moving the
// pwrite off the owner (spec 2064/f3/06 sections 3.1 and 3.4, M7 slice 1 lab 02).
//
// The question: the whole-record migrator has two forms. The synchronous form
// (migrate.go) frames a run of cold-bound records and pwrites them to the cold
// region inside the owner goroutine, then flips their index slots. Correct, but
// the owner sits in the pwrite: while a shard is blocked in that syscall it serves
// no commands. The two-phase form (coldstage.go) frames the run and hands the
// buffer to the shard's one off-owner I/O worker, which pwrites it while the owner
// goes back to serving; a completion event later runs the slot flips on the owner
// in program order. Same bytes moved, same records demoted; the difference is
// where the pwrite's wall-clock lands.
//
// This lab prices that difference and finds the crossover. The owner's saving per
// drain is the pwrite it no longer sits in, net of the channel hand-off the async
// form adds, so the async form wins exactly when the pwrite exceeds the hand-off.
// The hand-off is a single fixed cost per drain, tens of nanoseconds, while the
// pwrite scales with the drain: even a warm buffered write to the page cache, a
// few nanoseconds per record, outweighs the one hand-off across a full drain, so
// async wins for any drain past a handful of records. A write that actually blocks
// (cold cache, dirty-page writeback throttling, the fsync M8 adds) is microseconds
// to milliseconds, orders of magnitude past the hand-off, and there the owner
// reclaims almost the whole pwrite. The warm floor is a marginal win; the blocking
// write the owner must not sit inside is the design case and a decisive one.
//
// Method: in-process, no server, no wire, no engine import, the lab-local model
// the other f3 labs use. The record and cold-frame geometry match the store, so
// the framed bytes are the store's bytes; the writes go to a real temp file, both
// a warm buffered WriteAt (the cheap floor) and a WriteAt plus fsync (a real
// blocking cost). Every rounding is against aki's win: the hand-off is charged a
// full channel round trip, more than the single send the real submit does, and
// the flip is charged to both forms, so the async owner cost is modelled high.
package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"hash/maphash"
	"os"
	"path/filepath"
	"time"
)

const (
	coldHdr = 12 // the cold frame header: total, kind, flags, klen, vlen
	klen    = 16 // a 16-byte key
	vlen    = 8  // an 8-byte int value cell
)

// frameBytes is one whole-record cold frame's size, the store's coldframe.go
// layout: header, key, value.
func frameBytes() int { return coldHdr + klen + vlen }

// buildDrain frames recs records into a buffer exactly as coldstage.go stages a
// drain: an append per record of the frame header, the key, and the value. It is
// the CPU half of phase 1, the part that stays on the owner in both forms.
func buildDrain(recs int) []byte {
	buf := make([]byte, 0, recs*frameBytes())
	var key [klen]byte
	var val [vlen]byte
	for i := 0; i < recs; i++ {
		binary.LittleEndian.PutUint64(key[:8], uint64(i))
		binary.LittleEndian.PutUint64(val[:], uint64(i))
		total := coldHdr + klen + vlen
		var h [coldHdr]byte
		binary.LittleEndian.PutUint32(h[0:], uint32(total))
		h[4] = 1      // kindString
		h[5] = 1 << 4 // flagInt
		binary.LittleEndian.PutUint16(h[6:], uint16(klen))
		binary.LittleEndian.PutUint32(h[8:], uint32(vlen))
		buf = append(buf, h[:]...)
		buf = append(buf, key[:]...)
		buf = append(buf, val[:]...)
	}
	return buf
}

// costs is the per-piece timing the model runs on, all in nanoseconds. pwrite is
// the whole-drain figure (the model prices a specific drain size), the others are
// per-record CPU or per-drain hand-off.
type costs struct {
	frameRec float64 // CPU to frame one record (owner, both forms)
	pwrite   float64 // wall-clock to write one drain buffer (owner in sync, off-owner in async)
	submit   float64 // the async hand-off round trip the owner pays per drain
	flipRec  float64 // index flip per record (owner, both forms)
}

// ownerSyncNs is the owner's critical-path time for one drain of recs records in
// the synchronous form: frame the run, write it inline, flip the slots. The owner
// is busy for all of it.
func (c costs) ownerSyncNs(recs int) float64 {
	return c.frameRec*float64(recs) + c.pwrite + c.flipRec*float64(recs)
}

// ownerAsyncNs is the owner's critical-path time for the same drain in the
// two-phase form: frame the run, hand it off, and later flip the slots. The write
// is off the owner, so it is not here; the hand-off is.
func (c costs) ownerAsyncNs(recs int) float64 {
	return c.frameRec*float64(recs) + c.submit + c.flipRec*float64(recs)
}

// reclaimedFrac is the share of the synchronous owner time the async form takes
// off the critical path: the write, net of the hand-off it adds. Negative below
// the crossover (a write cheaper than the hand-off), rising toward 1 as the write
// blocks longer.
func (c costs) reclaimedFrac(recs int) float64 {
	return (c.ownerSyncNs(recs) - c.ownerAsyncNs(recs)) / c.ownerSyncNs(recs)
}

// uplift is the owner-serving throughput ratio async/sync under a duty cycle
// where the owner serves serveCmds commands (each serveNs) per drain. Throughput
// is inversely proportional to owner time for the same work, so the ratio of
// owner times is the throughput uplift. As the drain intensity rises (fewer
// commands per drain) the uplift climbs toward the pure per-drain ratio.
func (c costs) uplift(recs, serveCmds int, serveNs float64) float64 {
	work := float64(serveCmds) * serveNs
	return (work + c.ownerSyncNs(recs)) / (work + c.ownerAsyncNs(recs))
}

// withWrite returns the costs with a specific whole-drain write latency, the knob
// the pwrite-latency sweep turns.
func (c costs) withWrite(pwrite float64) costs {
	return costs{frameRec: c.frameRec, pwrite: pwrite, submit: c.submit, flipRec: c.flipRec}
}

// measured is the box's per-piece timings, the raw the sweeps price against.
type measured struct {
	frameRec     float64 // ns per record framed
	warmPerRec   float64 // ns per record for a warm buffered WriteAt
	fsyncPerCall float64 // ns for a WriteAt plus fsync, the blocking-write floor
	submit       float64 // ns per hand-off round trip
	flipRec      float64 // ns per index flip
}

// measure times each piece on this box.
func measure(dir string) measured {
	const frameRecs = 200_000

	// Frame CPU: build a large drain twice (warm the allocator) and take the
	// per-record cost of the second.
	buildDrain(frameRecs)
	start := time.Now()
	buf := buildDrain(frameRecs)
	frameRec := float64(time.Since(start).Nanoseconds()) / float64(frameRecs)
	sink(buf)

	f, err := os.OpenFile(filepath.Join(dir, "drain"), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	// Warm buffered write: the cheap floor, the page-cache copy with no writeback
	// wait. Best of several, so the floor is a floor.
	var warm float64 = 1e18
	for r := 0; r < 8; r++ {
		start = time.Now()
		if _, err := f.WriteAt(buf, int64(r)*int64(len(buf))); err != nil {
			panic(err)
		}
		if ns := float64(time.Since(start).Nanoseconds()); ns < warm {
			warm = ns
		}
	}
	warmPerRec := warm / float64(frameRecs)

	// Blocking write: WriteAt plus fsync, a real durable-write cost, the kind of
	// stall the owner must not sit inside. Median of several.
	small := buildDrain(1024)
	var fs float64 = 1e18
	for r := 0; r < 8; r++ {
		start = time.Now()
		if _, err := f.WriteAt(small, int64(r)*int64(len(small))); err != nil {
			panic(err)
		}
		if err := f.Sync(); err != nil {
			panic(err)
		}
		if ns := float64(time.Since(start).Nanoseconds()); ns < fs {
			fs = ns
		}
	}

	// Hand-off: a buffered-channel round trip per drain, overstating the single
	// send submit really does.
	ch := make(chan int, 1)
	const hops = 200_000
	start = time.Now()
	for i := 0; i < hops; i++ {
		ch <- i
		<-ch
	}
	submit := float64(time.Since(start).Nanoseconds()) / float64(hops)

	// Flip: a maphash of a key plus an 8-byte store, the index flip's floor.
	seed := maphash.MakeSeed()
	var key [klen]byte
	slots := make([]uint64, 1024)
	const flips = 2_000_000
	start = time.Now()
	var acc uint64
	for i := 0; i < flips; i++ {
		binary.LittleEndian.PutUint64(key[:8], uint64(i))
		h := maphash.Bytes(seed, key[:])
		slots[h&1023] = h
		acc += h
	}
	flipRec := float64(time.Since(start).Nanoseconds()) / float64(flips)
	sinkU(acc)

	return measured{frameRec: frameRec, warmPerRec: warmPerRec, fsyncPerCall: fs, submit: submit, flipRec: flipRec}
}

func main() {
	// The sweep counts are already fast; the flag stays for parity with the other
	// labs' invocation so the runner's --quick call is valid.
	_ = flag.Bool("quick", false, "smaller counts for a fast check")
	flag.Parse()

	dir, err := os.MkdirTemp("", "asyncdrain")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	m := measure(dir)

	base := costs{frameRec: m.frameRec, submit: m.submit, flipRec: m.flipRec}

	fmt.Printf("async cold drain, owner critical-path time saved by moving the pwrite off-owner, %s\n", time.Now().Format("2006-01-02"))
	fmt.Printf("per-record framing %.1f ns, warm write %.2f ns/rec, blocking write (fsync) %s/call, hand-off %.1f ns, flip %.1f ns/rec\n",
		m.frameRec, m.warmPerRec, dur(m.fsyncPerCall), m.submit, m.flipRec)
	fmt.Printf("record frame %d B; a drain of R records writes R*%d B; async wins once the write exceeds the %.1f ns hand-off\n",
		frameBytes(), frameBytes(), m.submit)

	// Sweep A: a fixed 1024-record drain, rising write latency from the warm floor
	// through blocking-disk figures. The reclaimed share is negative at the floor
	// (the write is cheaper than the hand-off), crosses zero just past it, and
	// approaches the whole owner time as the write blocks longer.
	const recs = 1024
	warmDrain := m.warmPerRec * float64(recs)
	fmt.Println()
	fmt.Println("Sweep A: 1024-record drain, rising write latency")
	fmt.Printf("%-18s %12s %12s %12s %14s\n", "write", "ownerSync", "ownerAsync", "reclaimed", "uplift@1k")
	writes := []struct {
		label string
		ns    float64
	}{
		{"warm floor", warmDrain},
		{"1us", 1_000},
		{"10us", 10_000},
		{"fsync (measured)", m.fsyncPerCall},
		{"100us", 100_000},
		{"1ms", 1_000_000},
	}
	for _, w := range writes {
		c := base.withWrite(w.ns)
		fmt.Printf("%-18s %12s %12s %13.1f%% %14.2f\n",
			w.label, dur(c.ownerSyncNs(recs)), dur(c.ownerAsyncNs(recs)), c.reclaimedFrac(recs)*100, c.uplift(recs, 1000, serveNs))
	}

	// Sweep B: the measured blocking write, rising commands served per drain. The
	// heavier the drain intensity (fewer commands between drains), the more of the
	// owner's time is the write, so the async uplift climbs toward the per-drain
	// ceiling.
	block := base.withWrite(m.fsyncPerCall)
	fmt.Println()
	fmt.Println("Sweep B: blocking write, 1024-record drain, rising commands served per drain")
	fmt.Printf("%-14s %14s\n", "cmds/drain", "async/sync")
	for _, cmds := range []int{100, 1000, 10000, 100000} {
		fmt.Printf("%-14d %14.4f\n", cmds, block.uplift(recs, cmds, serveNs))
	}

	fmt.Println()
	fmt.Printf("Verdict: the hand-off is one fixed cost per drain, the write scales with it, so even a warm write over a full drain reclaims %.1f%% of the owner's per-drain time; a blocking write reclaims %.1f%%, and a drain-bound shard then serves up to %.2fx the commands.\n",
		base.withWrite(warmDrain).reclaimedFrac(recs)*100, block.reclaimedFrac(recs)*100, block.ownerSyncNs(recs)/block.ownerAsyncNs(recs))
}

const serveNs = 80.0 // a served command's owner time, a plain point op

// sink and sinkU keep the compiler from folding the measured work away.
var sinkBuf []byte
var sinkAcc uint64

func sink(b []byte)  { sinkBuf = b }
func sinkU(u uint64) { sinkAcc = u }

func dur(ns float64) string {
	switch {
	case ns >= 1e9:
		return fmt.Sprintf("%.2fs", ns/1e9)
	case ns >= 1e6:
		return fmt.Sprintf("%.2fms", ns/1e6)
	case ns >= 1e3:
		return fmt.Sprintf("%.2fus", ns/1e3)
	default:
		return fmt.Sprintf("%.0fns", ns)
	}
}
