//go:build unix

package store

import "syscall"

// The arena's backing lives outside the Go heap, mapped anonymously, because
// heap accounting is GC policy: a make([]byte) arena counts its full
// reservation as live heap, which multiplies the collector's pacing goal and
// the scavenger's retention target by gigabytes of mostly-untouched
// reservation. On the M0 gate config (4 shards x 512MiB) that put the GC goal
// past 4GiB, so transient garbage (per-connection buffers, rebuilt index
// tables) was never collected and never returned, and the 64B gate cells read
// 3-4x rival RSS on pages that were pure garbage (labs/f3/m0/20_arena_rss).
// Doc 04 section 6.2 already states the posture: the arena is accounted by
// our ledger, not by the Go heap. Mapping it keeps the heap at the substrate
// objects the discipline promises, so the default GC pacing works unmodified.
//
// The mapping is plain anonymous private memory: zero-filled on first touch,
// untouched pages cost nothing resident, and MADV_DONTNEED (advise_linux.go)
// works on it exactly as it did on heap pages. On a mapping failure the heap
// slice is the fallback; the store still runs, only the pacing benefit is
// lost.

// arenaMap maps n bytes and reports whether the buffer is a mapping (true)
// or the heap fallback (false); only a mapping may be handed to arenaUnmap.
func arenaMap(n int) ([]byte, bool) {
	b, err := syscall.Mmap(-1, 0, n,
		syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_ANON|syscall.MAP_PRIVATE)
	if err != nil {
		return make([]byte, n), false
	}
	return b, true
}

// arenaUnmap releases a mapped arena buffer. The caller must pass only a
// buffer arenaMap reported mapped: munmap does not distinguish heap addresses
// from mapped ones, and unmapping heap pages would corrupt the runtime.
func arenaUnmap(b []byte) {
	if len(b) > 0 {
		_ = syscall.Munmap(b)
	}
}
