package f1srv

import (
	"fmt"
	"sync/atomic"
	"testing"
)

// Slice 6c settles the two adaptive-partitioning knobs from measurement rather than decree (spec
// 2064/f1_rewrite_ltm/19 section 3): SetPartitionThreshold (T_up, the cardinality a set must reach
// before it engages at all) and SetPartitionTarget (the members-per-partition a grow aims for). The
// two benchmarks here sweep the write path and the read path over a grid of (cardinality, P), each
// run at -cpu 1 and at the box's data-core count, so one grid exposes both knobs at once:
//
//   - The -cpu 1 column is the P1-gate proxy: with no contention, more partitions can only add
//     overhead (the extra composite-key byte and the routing on the write, the weighted pick across
//     P on the read). Where that column is flat across P, partitioning is free to engage; where it
//     rises, engaging early would tax the uncontended path. This bounds how low T_up and target may go.
//   - The -cpu <data-cores> column is the P16-gate proxy: splitting one stripe lock into P partition
//     locks lets single-key write throughput climb. The smallest cardinality at which P=2 first beats
//     P=1 in this column is the floor for T_up; the members-per-partition at which the column stops
//     improving is the target (splitting finer buys nothing past the core count).
//
// Both probes are deliberately allocation-free so the timed loop measures lock contention and not the
// f1raw arena, which is grow-only and never frees: a churning SREM+re-SADD or SPOP+re-SADD leaks a
// fresh arena record every iteration, and at high op-counts that unbounded growth, multiplied by P
// growing structures, swamps the lock signal (an earlier churn sweep showed a 7x regression at -cpu 1,
// where there is no contention at all, purely from arena growth). So the write probe re-adds a member
// already in the set (an idempotent SADD takes the exclusive partition lock and does the membership
// check but never inserts, so zero arena growth) and the read probe draws non-destructively. The grid
// is read off the box, not asserted here, so both benchmarks stay pure measurement. They size the
// arena and index for the largest cardinality up front because the f1raw arena is grow-only.

// sweepPartServer builds a forceP=p server sized for a set of card members. The arena is grow-only, so
// it is provisioned for the whole prefill in one shot: ~128 bytes per member covers the composite key,
// its hash record, and the ordered-index slot with headroom, and the index gets at least two buckets
// per member to keep the primary chains short. forceP pins P for the whole run so the benchmark
// measures a fixed layout, exactly as the adaptive engage will hold P steady between growth steps.
func sweepPartServer(b testing.TB, p, card int) *Server {
	b.Helper()
	buckets := 1 << 12
	for buckets < card*2 {
		buckets <<= 1
	}
	arena := card*128 + (16 << 20)
	srv := New(Config{
		Addr: "127.0.0.1:0", IndexBuckets: buckets, ArenaBytes: arena, ReadBufSize: 4 << 10, IncrStripes: 256,
	})
	srv.forceP.Store(int64(p))
	return srv
}

// sweepMembers returns card distinct member names of a fixed 12-byte width, so every (card, P) cell
// prefills the same shaped members and only the count and partition layout differ between cells.
func sweepMembers(card int) [][]byte {
	ms := make([][]byte, card)
	for i := range ms {
		ms[i] = []byte(fmt.Sprintf("m%011d", i))
	}
	return ms
}

// prefillSet loads every member of ms into skey through the routed SADD path, so the set carries the
// forced partition layout before the timed loop starts. It adds in blocks to keep the argv small.
func prefillSet(c *connState, skey string, ms [][]byte) {
	const block = 512
	for i := 0; i < len(ms); i += block {
		end := i + block
		if end > len(ms) {
			end = len(ms)
		}
		argv := make([][]byte, 0, end-i+2)
		argv = append(argv, []byte("SADD"), []byte(skey))
		argv = append(argv, ms[i:end]...)
		c.out = c.out[:0]
		c.cmdSAdd(argv)
	}
}

// sweepCards is the cardinality axis: it brackets the proposed T_up window (10k-100k) below and the
// proposed per-partition target window (64k-256k) at and above, so the crossover where P>1 starts to
// pay and the knee where finer splitting stops paying both fall inside the grid.
var sweepCards = []int{4096, 16384, 65536, 262144, 1048576}

// sweepPs is the partition axis, every power of two from unpartitioned up to the proposed cap of 16.
var sweepPs = []int{1, 2, 4, 8, 16}

// BenchmarkSetPartitionWriteSweep hammers one hot key with an idempotent SADD per iteration over a
// prefilled set, so the cardinality stays fixed at card and no arena grows while the timed work is
// pure partition-lock write contention. Re-adding a member that is already present still takes the
// member's exclusive partition lock and runs the membership check, but the store reports it as not
// new, so nothing is inserted and no arena record is allocated. Each goroutine owns a disjoint stride
// of the prefilled members, so goroutines never collide on the same member but do share partitions
// (hash mod P), which is exactly the write contention P splits. Run it as
//
//	go test -run x -bench BenchmarkSetPartitionWriteSweep -benchmem -cpu 1,14 ./f1srv/
//
// and read the P1-gate tax from the cpu=1 rows and the P16-gate benefit from the cpu=14 rows.
func BenchmarkSetPartitionWriteSweep(b *testing.B) {
	for _, card := range sweepCards {
		ms := sweepMembers(card)
		for _, p := range sweepPs {
			b.Run(fmt.Sprintf("card=%d/P=%d", card, p), func(b *testing.B) {
				srv := sweepPartServer(b, p, card)
				defer srv.Close()
				prefillSet(bareConn(srv), "hot", ms)
				var gid int64
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					// Stride the member space by goroutine id so each goroutine re-adds a disjoint set of
					// already-present members, taking exclusive partition locks that are spread across
					// the P partitions without two goroutines contending on the very same member.
					g := int(atomic.AddInt64(&gid, 1)) - 1
					c := bareConn(srv)
					add := [][]byte{[]byte("SADD"), []byte("hot"), nil}
					i := g
					for pb.Next() {
						if i >= card {
							i = g
						}
						add[2] = ms[i]
						c.out = c.out[:0]
						c.cmdSAdd(add)
						i += 64
					}
				})
			})
		}
	}
}

// BenchmarkSetPartitionReadSweep hammers one hot key with a no-count SRANDMEMBER per iteration over the
// same (card, P) grid. The draw is non-destructive and allocation-free, and its weighted pick across
// the P partitions resolves through the slice-4b partition-descriptor cache (O(P) atomic loads, no
// per-partition map lookup). Reads hold the partition stripes shared, so readers never block readers:
// this probe is not a lock-split-benefit measurement but its complement, a guard that routing an
// uncontended draw through P partitions stays cheap (the cpu=1 rows) and that the read path never
// regresses as concurrency climbs (the cpu=14 rows). Same invocation and same two-column reading.
func BenchmarkSetPartitionReadSweep(b *testing.B) {
	for _, card := range sweepCards {
		ms := sweepMembers(card)
		for _, p := range sweepPs {
			b.Run(fmt.Sprintf("card=%d/P=%d", card, p), func(b *testing.B) {
				srv := sweepPartServer(b, p, card)
				defer srv.Close()
				prefillSet(bareConn(srv), "hot", ms)
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					c := bareConn(srv)
					srand := [][]byte{[]byte("SRANDMEMBER"), []byte("hot")}
					for pb.Next() {
						c.out = c.out[:0]
						c.cmdSRandMember(srand)
					}
				})
			})
		}
	}
}
