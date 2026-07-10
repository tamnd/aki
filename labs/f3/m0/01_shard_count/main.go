// Lab: shard count per box (spec 2064/f3/03 section 2.2, M0 lab 1).
//
// The question: how many single-owner shards should one box run per core?
// Doc 03 fixes S at startup and starts from "data plane gets ~60 percent of
// cores"; this lab measures the raw engine side of that split by running the
// ported engine/f3/store partitioned N ways across N pinned worker
// goroutines, with no network and no queues in the way, and sweeping N over
// 1, 2, 4, 8, cores, and 2x cores.
//
// Method: totalKeys keys (16B keys, 64B values) are dealt round-robin across
// N stores so every shard holds totalKeys/N keys. Each worker locks its OS
// thread, fills its own store, then executes a pre-shuffled 90/10 GET/SET op
// stream against its own keys. All workers start on a barrier and the run is
// timed to the slowest worker's finish, so the reported figure is aggregate
// throughput at full fan-out, the shape the shard runtime will see when every
// inbound queue is hot.
//
// See README.md for the numbers and the verdict.
package main

import (
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/tamnd/aki/engine/f3/store"
)

const (
	totalKeys = 1 << 20 // 1M keys across all shards
	totalOps  = 1 << 24 // 16M ops across all shards
	valBytes  = 64
	setEvery  = 10 // 1 SET per 10 ops, the rest GETs
)

func keyBytes(dst []byte, i uint64) []byte {
	// 16-byte key from a splitmix64 of the index, so keys are dense in count
	// but not in byte order.
	x := i + 0x9e3779b97f4a7c15
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	dst = dst[:0]
	for j := 0; j < 16; j++ {
		dst = append(dst, "0123456789abcdef"[(x>>(j*4))&15])
	}
	return dst
}

// runN measures aggregate ops/sec with the keyspace partitioned N ways.
func runN(n int) float64 {
	keysPerShard := totalKeys / n
	opsPerShard := totalOps / n
	// Arena sized for the fill plus SET churn headroom.
	arenaBytes := keysPerShard*160 + 32<<20

	stores := make([]*store.Store, n)
	orders := make([][]uint32, n)
	for i := range stores {
		stores[i] = store.New(arenaBytes, 0)
		rng := rand.New(rand.NewSource(int64(i) + 1))
		ord := make([]uint32, opsPerShard)
		for j := range ord {
			ord[j] = uint32(rng.Intn(keysPerShard))
		}
		orders[i] = ord
	}

	var fill sync.WaitGroup
	start := make(chan struct{})
	var done sync.WaitGroup
	val := make([]byte, valBytes)
	for i := range val {
		val[i] = byte('a' + i%26)
	}

	for i := 0; i < n; i++ {
		fill.Add(1)
		done.Add(1)
		go func(shard int) {
			defer done.Done()
			runtime.LockOSThread()
			defer runtime.UnlockOSThread()
			s := stores[shard]
			key := make([]byte, 0, 16)
			for k := 0; k < keysPerShard; k++ {
				// Global key id: dealt round-robin, shard gets k*n+shard.
				if err := s.Set(keyBytes(key, uint64(k*n+shard)), val); err != nil {
					panic(err)
				}
			}
			fill.Done()
			<-start
			dst := make([]byte, 0, valBytes)
			ord := orders[shard]
			for j, k := range ord {
				kb := keyBytes(key, uint64(int(k)*n+shard))
				if j%setEvery == 0 {
					if err := s.Set(kb, val); err != nil {
						panic(err)
					}
					continue
				}
				if _, ok := s.Get(kb, dst); !ok {
					panic("miss")
				}
			}
		}(i)
	}

	fill.Wait()
	t0 := time.Now()
	close(start)
	done.Wait()
	el := time.Since(t0)
	return float64(totalOps) / el.Seconds() / 1e6
}

func main() {
	cores := runtime.NumCPU()
	sweep := []int{1, 2, 4, 8, cores, 2 * cores}
	fmt.Printf("cores=%d GOMAXPROCS=%d keys=%d ops=%d val=%dB mix=90/10 GET/SET\n\n",
		cores, runtime.GOMAXPROCS(0), totalKeys, totalOps, valBytes)
	fmt.Println("| shards | Mops/s | speedup vs 1 | per-shard Mops/s |")
	fmt.Println("|---|---|---|---|")
	var base float64
	for _, n := range sweep {
		m := runN(n)
		if base == 0 {
			base = m
		}
		fmt.Printf("| %d | %.1f | %.2fx | %.2f |\n", n, m, m/base, m/float64(n))
		_ = os.Stdout.Sync()
	}
}
