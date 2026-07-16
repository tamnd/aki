package obs1_test

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/tamnd/aki/engine/obs1"
	"github.com/tamnd/aki/engine/obs1/sim"
)

func testRoot(ckpt uint64) obs1.Root {
	r := obs1.Root{CreatedMS: 1_752_600_000_000, G: 64, D: 1, CkptSeq: ckpt}
	copy(r.DBID[:], "test-db-00000000")
	return r
}

func TestRootLifecycleOnSim(t *testing.T) {
	for _, fallback := range []bool{false, true} {
		t.Run(fmt.Sprintf("fallback=%v", fallback), func(t *testing.T) {
			s := sim.New(sim.Config{Seed: 1})
			ctx := t.Context()

			if _, err := obs1.LoadRoot(ctx, s, "db/a", fallback); !errors.Is(err, obs1.ErrNotFound) {
				t.Fatalf("load before create: %v", err)
			}
			if err := obs1.CreateRoot(ctx, s, "db/a", fallback, testRoot(0)); err != nil {
				t.Fatalf("create: %v", err)
			}
			if err := obs1.CreateRoot(ctx, s, "db/a", fallback, testRoot(0)); !errors.Is(err, obs1.ErrPrecondition) {
				t.Fatalf("second create: %v", err)
			}
			if err := obs1.AdvanceRoot(ctx, s, "db/a", fallback, 3, testRoot(5)); err != nil {
				t.Fatalf("advance: %v", err)
			}
			if err := obs1.AdvanceRoot(ctx, s, "db/a", fallback, 4, testRoot(2)); err != nil {
				t.Fatalf("stale advance: %v", err)
			}
			got, err := obs1.LoadRoot(ctx, s, "db/a", fallback)
			if err != nil {
				t.Fatalf("load: %v", err)
			}
			if got.CkptSeq != 5 {
				t.Fatalf("ckpt %d after stale advance, want 5", got.CkptSeq)
			}
		})
	}
}

func TestRootRacingWriters(t *testing.T) {
	const writers = 8
	for _, fallback := range []bool{false, true} {
		t.Run(fmt.Sprintf("fallback=%v", fallback), func(t *testing.T) {
			s := sim.New(sim.Config{Seed: 2})
			ctx := t.Context()
			if err := obs1.CreateRoot(ctx, s, "", fallback, testRoot(0)); err != nil {
				t.Fatal(err)
			}
			var wg sync.WaitGroup
			errs := make([]error, writers)
			for i := range writers {
				wg.Go(func() {
					errs[i] = obs1.AdvanceRoot(ctx, s, "", fallback, uint64(i+1), testRoot(uint64(i+1)))
				})
			}
			wg.Wait()
			for i, err := range errs {
				if err != nil {
					t.Fatalf("writer %d: %v", i+1, err)
				}
			}
			got, err := obs1.LoadRoot(ctx, s, "", fallback)
			if err != nil {
				t.Fatal(err)
			}
			if got.CkptSeq != writers {
				t.Fatalf("final ckpt %d, want %d", got.CkptSeq, writers)
			}
		})
	}
}

// TestRootAdvanceThroughAmbiguity injects an ambiguous verdict on every
// first conditional write per key and proves both advance protocols
// converge: the CAS path by re-reading and comparing checkpoints, the
// fallback path by Recheck self-recognition.
func TestRootAdvanceThroughAmbiguity(t *testing.T) {
	for _, applied := range []bool{false, true} {
		for _, fallback := range []bool{false, true} {
			t.Run(fmt.Sprintf("applied=%v_fallback=%v", applied, fallback), func(t *testing.T) {
				hit := map[string]bool{}
				var mu sync.Mutex
				s := sim.New(sim.Config{Seed: 3, Fault: func(op sim.Op, key string) *sim.Fault {
					if op != sim.OpPutIfAbsent && op != sim.OpPutIfMatch {
						return nil
					}
					mu.Lock()
					defer mu.Unlock()
					if hit[key] {
						return nil
					}
					hit[key] = true
					return &sim.Fault{Err: obs1.ErrAmbiguous, Applied: applied}
				}})
				ctx := t.Context()
				// The create itself rides through one ambiguity too: an
				// applied fault means the object landed, so the caller
				// treats ambiguous create as retryable-by-recheck at the
				// layer above. Here we sidestep by seeding directly.
				if err := obs1.CreateRoot(ctx, s, "", fallback, testRoot(0)); err != nil {
					if !errors.Is(err, obs1.ErrAmbiguous) {
						t.Fatalf("create: %v", err)
					}
					if !applied {
						if err := obs1.CreateRoot(ctx, s, "", fallback, testRoot(0)); err != nil {
							t.Fatalf("create retry: %v", err)
						}
					}
				}
				if err := obs1.AdvanceRoot(ctx, s, "", fallback, 7, testRoot(5)); err != nil {
					t.Fatalf("advance through ambiguity: %v", err)
				}
				got, err := obs1.LoadRoot(ctx, s, "", fallback)
				if err != nil {
					t.Fatal(err)
				}
				if got.CkptSeq != 5 {
					t.Fatalf("ckpt %d, want 5", got.CkptSeq)
				}
			})
		}
	}
}
