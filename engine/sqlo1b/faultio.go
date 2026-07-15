package sqlo1b

import (
	"fmt"
	"io"
	"math/rand/v2"
	"sync"
)

// Torn-write injection at the iopool layer (milestone B1). FaultFile
// models the disk the format must survive: a volatile write cache in
// front of durable media. WriteAt lands in the cache, reads see the
// cache (the OS page cache view live code observes), Sync moves the
// cache to durable media, and Crash cuts power. At a power cut the
// disk has applied some sectors of some cached writes in whatever
// order it liked, so the legal outcomes are exactly "any subset of
// unsynced sectors landed": partial group writes, reordered flushes,
// and sector-level tears are all points in that space, chosen by the
// keep function.
//
// FaultFile implements FileIO, so it plugs under a real IOPool or
// under the superblock and extent writers directly. It is harness
// code: single-goroutine like everything else the owner drives, and
// the crash matrix in cmd/sqlo1crash is its production consumer.

// FaultSectorSize is the tear granularity: the classic 512-byte
// sector, the largest unit a device is assumed to write atomically.
const FaultSectorSize = 512

type faultWrite struct {
	off  int64
	data []byte
}

// FaultFile wraps durable media with a volatile write cache. The
// mutex is there so it can sit under a real IOPool, whose workers
// hit the file concurrently.
type FaultFile struct {
	Base FileIO

	mu      sync.Mutex
	pending []faultWrite
}

// NewFaultFile wraps base; base holds the durable image.
func NewFaultFile(base FileIO) *FaultFile { return &FaultFile{Base: base} }

// Pending reports how many unsynced writes the cache holds.
func (f *FaultFile) Pending() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.pending)
}

// WriteAt caches the write; nothing reaches durable media until Sync
// or the kept sectors of a Crash.
func (f *FaultFile) WriteAt(p []byte, off int64) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	buf := make([]byte, len(p))
	copy(buf, p)
	f.mu.Lock()
	f.pending = append(f.pending, faultWrite{off: off, data: buf})
	f.mu.Unlock()
	return len(p), nil
}

// ReadAt reads the durable image patched with every cached write in
// order, which is what live code sees through the page cache.
func (f *FaultFile) ReadAt(p []byte, off int64) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	n, err := f.Base.ReadAt(p, off)
	if err != nil && err != io.EOF {
		return n, err
	}
	for i := n; i < len(p); i++ {
		p[i] = 0
	}
	end := off + int64(n)
	for _, w := range f.pending {
		lo := max(off, w.off)
		hi := min(off+int64(len(p)), w.off+int64(len(w.data)))
		if lo >= hi {
			continue
		}
		copy(p[lo-off:hi-off], w.data[lo-w.off:hi-w.off])
		if hi > end {
			end = hi
		}
	}
	n = int(end - off)
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

// Sync applies the cache to durable media in write order and syncs
// it; after Sync those bytes survive any Crash.
func (f *FaultFile) Sync() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, w := range f.pending {
		if _, err := f.Base.WriteAt(w.data, w.off); err != nil {
			return err
		}
	}
	f.pending = nil
	return f.Base.Sync()
}

// sectorRange reports the absolute file sectors a write touches.
func (w faultWrite) sectorRange() (first, count int64) {
	first = w.off / FaultSectorSize
	last := (w.off + int64(len(w.data)) - 1) / FaultSectorSize
	return first, last - first + 1
}

// Crash cuts power. For each unsynced write, in cache order, keep
// decides which of its sectors reached the media (write index, then
// per-sector, absolute sector numbers derivable from the write's
// offset); everything else is lost. The durable image then holds
// only what survived, and the cache is empty, exactly the state a
// reopen recovers from.
func (f *FaultFile) Crash(keep func(write int, sectors int64) []bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i, w := range f.pending {
		first, count := w.sectorRange()
		mask := keep(i, count)
		if int64(len(mask)) != count {
			return fmt.Errorf("sqlo1b: crash mask for write %d has %d sectors, want %d", i, len(mask), count)
		}
		for s := range count {
			if !mask[s] {
				continue
			}
			lo := max(w.off, (first+s)*FaultSectorSize)
			hi := min(w.off+int64(len(w.data)), (first+s+1)*FaultSectorSize)
			if _, err := f.Base.WriteAt(w.data[lo-w.off:hi-w.off], lo); err != nil {
				return err
			}
		}
	}
	f.pending = nil
	return f.Base.Sync()
}

// KeepAll lands every unsynced sector: a crash right after the disk
// drained its cache.
func KeepAll(_ int, sectors int64) []bool {
	m := make([]bool, sectors)
	for i := range m {
		m[i] = true
	}
	return m
}

// KeepNone loses the whole cache: a crash before the disk wrote
// anything back.
func KeepNone(_ int, sectors int64) []bool { return make([]bool, sectors) }

// KeepRandom derives a deterministic coin-flip crash from seed: every
// unsynced sector independently lands or not, which subsumes partial
// writes, reordering, and tears. The same seed replays the same
// crash, so a failing matrix iteration is reproducible from its seed
// alone.
func KeepRandom(seed uint64) func(int, int64) []bool {
	rng := rand.New(rand.NewPCG(seed, seed^0x9E3779B97F4A7C15))
	return func(_ int, sectors int64) []bool {
		m := make([]bool, sectors)
		for i := range m {
			m[i] = rng.IntN(2) == 1
		}
		return m
	}
}
