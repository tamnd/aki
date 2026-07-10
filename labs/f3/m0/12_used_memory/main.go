// Lab: used_memory vs allocator-held bytes vs redis (spec 2064/f3, M0 gate
// follow-up, lab 12, issue #542).
//
// The question: the gate follow-up on the native-Linux boxes caught the INFO
// used_memory ledger undercounting on bursty SET churn, 52MB reported where
// redis 8.8 said 220MB under identical load, which makes every SET-cell
// memory column in the gate table unreliable. used_memory was defined as
// index tables plus the arena's live charge, so the dead-but-uncompacted
// bytes republish churn strands behind live neighbors, real resident pages,
// never showed. redis's used_memory is what its allocator holds for the
// dataset, dead-space slack included, so the comparable figure is the
// touched-segment fill: live plus dead plus the reuse slack behind the bump
// cursors. This lab measures both definitions and redis on the same
// workload and reads them against maxrss, the ground truth.
//
// Method: the workload is lab 10's pass two, the shape that produced the
// undercount: 1M keys, value size a coin flip between 512B (embedded) and
// 4KiB (separated) so about half the overwrites change band and republish, a
// pinned eighth written at fill and never again, churn to 2x arena turnover.
// `go run .` runs it in process over engine/f3/store with the emulated
// worker boundaries (tightness check per 1024-op batch, idle reclaim trigger
// every 64 batches at the 1MiB floor) and prints both used_memory
// definitions plus maxrss at fill and after churn. `go run . -engine redis
// -addr 127.0.0.1:6390` drives the identical op stream over RESP against a
// running redis-server and reads INFO used_memory and used_memory_rss at the
// same two points.
//
// See README.md for the numbers and the verdict.
package main

import (
	"bufio"
	"encoding/binary"
	"flag"
	"fmt"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"syscall"

	"github.com/tamnd/aki/engine/f3/store"
)

const (
	keys       = 1 << 20 // 1M
	batchOps   = 1024    // drainPassCap * batchCap, one worker drain pass
	idleMod    = 64      // batches between emulated idle-boundary checks
	idleMin    = 1 << 20 // arenaCompactMinDead, the shard's idle floor
	arenaBytes = 4096 << 20
	turnover   = 2 // churn until written bytes reach this many arena fills
)

// xorshift is the key picker, identical across engines so both see the same
// op stream.
type xorshift uint64

func (x *xorshift) next() uint64 {
	v := *x
	v ^= v << 13
	v ^= v >> 7
	v ^= v << 17
	*x = v
	return uint64(v)
}

func makeKey(buf []byte, n uint64) []byte {
	binary.LittleEndian.PutUint64(buf[0:8], n)
	binary.LittleEndian.PutUint64(buf[8:16], n*0x9e3779b97f4a7c15)
	return buf[:16]
}

func pickSize(r *xorshift) int {
	if r.next()&1 == 0 {
		return 512
	}
	return 4096
}

func maxrss() uint64 {
	var ru syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	rss := uint64(ru.Maxrss)
	if runtime.GOOS == "linux" {
		rss *= 1024
	}
	return rss
}

func main() {
	engine := flag.String("engine", "store", "store (in process) or redis (over RESP)")
	addr := flag.String("addr", "127.0.0.1:6390", "redis address for -engine redis")
	flag.Parse()
	switch *engine {
	case "store":
		runStore()
	case "redis":
		runRedis(*addr)
	default:
		fmt.Fprintln(os.Stderr, "unknown engine", *engine)
		os.Exit(2)
	}
}

func runStore() {
	s := store.New(arenaBytes, 0)
	val := make([]byte, 4096)
	for i := range val {
		val[i] = 'a' + byte(i%26)
	}
	var kb [16]byte
	rng := xorshift(0x9e3779b97f4a7c15)
	set := func(k uint64) uint64 {
		v := val[:pickSize(&rng)]
		if err := s.SetString(makeKey(kb[:], k), v, 0, 0, false); err != nil {
			fmt.Fprintf(os.Stderr, "set: %v\n", err)
			os.Exit(1)
		}
		return uint64(len(v))
	}
	for i := 0; i < keys; i++ {
		set(uint64(i))
	}
	report := func(phase string) {
		m := s.Mem()
		fmt.Printf("%-10s live-only %7.1fMB  allocator-held %7.1fMB  (index %.1fMB arena live %.1fMB fill %.1fMB)  maxrss %7.1fMB\n",
			phase, mb(m.IndexBytes+m.ArenaLiveBytes), mb(m.UsedMemory()),
			mb(m.IndexBytes), mb(m.ArenaLiveBytes), mb(m.ArenaAllocBytes), mb(maxrss()))
	}
	report("fill")
	_, total := s.ArenaBytes()
	var written uint64
	for batch := 0; written < turnover*total; batch++ {
		for i := 0; i < batchOps; i++ {
			k := rng.next() & (keys - 1)
			if k&7 == 0 {
				k |= 1 // the pinned eighth never rewrites
			}
			written += set(k)
		}
		if s.ArenaTight() {
			s.CompactArena()
		}
		if batch%idleMod == idleMod-1 && s.ArenaReclaimable() >= idleMin {
			s.CompactArena()
		}
	}
	report("churned")
}

func mb(n uint64) float64 { return float64(n) / (1 << 20) }

// resp is a minimal pipelined RESP client, enough for SET and INFO.
type resp struct {
	c net.Conn
	r *bufio.Reader
	w *bufio.Writer
}

func dial(addr string) *resp {
	c, err := net.Dial("tcp", addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "dial %s: %v\n", addr, err)
		os.Exit(1)
	}
	return &resp{c: c, r: bufio.NewReaderSize(c, 64<<10), w: bufio.NewWriterSize(c, 64<<10)}
}

// cmd buffers one command; a write error is sticky in the bufio.Writer and
// surfaces at the next flush, which is where it is checked.
func (r *resp) cmd(args ...[]byte) {
	_, _ = fmt.Fprintf(r.w, "*%d\r\n", len(args))
	for _, a := range args {
		_, _ = fmt.Fprintf(r.w, "$%d\r\n", len(a))
		_, _ = r.w.Write(a)
		_, _ = r.w.WriteString("\r\n")
	}
}

func (r *resp) flush() {
	if err := r.w.Flush(); err != nil {
		fmt.Fprintf(os.Stderr, "flush: %v\n", err)
		os.Exit(1)
	}
}

// reply consumes one reply and returns a bulk's payload (nil for anything
// else).
func (r *resp) reply() []byte {
	line, err := r.r.ReadString('\n')
	if err != nil {
		fmt.Fprintf(os.Stderr, "read: %v\n", err)
		os.Exit(1)
	}
	line = strings.TrimRight(line, "\r\n")
	if len(line) == 0 {
		return nil
	}
	switch line[0] {
	case '+', ':':
		return nil
	case '-':
		fmt.Fprintf(os.Stderr, "server error: %s\n", line[1:])
		os.Exit(1)
	case '$':
		n, _ := strconv.Atoi(line[1:])
		if n < 0 {
			return nil
		}
		b := make([]byte, n+2)
		if _, err := readFull(r.r, b); err != nil {
			fmt.Fprintf(os.Stderr, "read bulk: %v\n", err)
			os.Exit(1)
		}
		return b[:n]
	}
	fmt.Fprintf(os.Stderr, "unexpected reply %q\n", line)
	os.Exit(1)
	return nil
}

func readFull(r *bufio.Reader, b []byte) (int, error) {
	n := 0
	for n < len(b) {
		m, err := r.Read(b[n:])
		n += m
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

func runRedis(addr string) {
	r := dial(addr)
	val := make([]byte, 4096)
	for i := range val {
		val[i] = 'a' + byte(i%26)
	}
	var kb [16]byte
	rng := xorshift(0x9e3779b97f4a7c15)
	pending := 0
	set := func(k uint64) uint64 {
		v := val[:pickSize(&rng)]
		r.cmd([]byte("SET"), makeKey(kb[:], k), v)
		pending++
		if pending == 128 {
			r.flush()
			for ; pending > 0; pending-- {
				r.reply()
			}
		}
		return uint64(len(v))
	}
	drain := func() {
		r.flush()
		for ; pending > 0; pending-- {
			r.reply()
		}
	}
	report := func(phase string) {
		drain()
		r.cmd([]byte("INFO"), []byte("memory"))
		r.flush()
		info := string(r.reply())
		used, rss := infoField(info, "used_memory:"), infoField(info, "used_memory_rss:")
		fmt.Printf("%-10s redis used_memory %7.1fMB  used_memory_rss %7.1fMB\n", phase, mb(used), mb(rss))
	}
	for i := 0; i < keys; i++ {
		set(uint64(i))
	}
	report("fill")
	var written uint64
	for written < turnover*arenaBytes {
		k := rng.next() & (keys - 1)
		if k&7 == 0 {
			k |= 1
		}
		written += set(k)
	}
	report("churned")
}

func infoField(info, name string) uint64 {
	for _, line := range strings.Split(info, "\r\n") {
		if v, ok := strings.CutPrefix(line, name); ok {
			n, _ := strconv.ParseUint(strings.TrimSpace(v), 10, 64)
			return n
		}
	}
	return 0
}
