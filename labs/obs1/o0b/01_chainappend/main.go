// chainappend measures the doc 02 section 2.4 append protocol under
// contention (spec 2064/obs1 milestone O0b): append latency, 412 rate,
// and record commit latency versus contender count and offered record
// rate, sweeping the retry backoff policy the protocol leaves as a
// constant. The winning policy is what the append-loop slice bakes.
//
// The load model is the doc 04 flush shape, not raw closed-loop appends:
// each node generates records on a fixed schedule, keeps at most one
// append in flight, and folds everything pending into the next batch, so
// under contention batches grow instead of the queue. That coalescing is
// the mechanism the doc 02 rate budget stands on, and the lab reports
// records per batch so its effect is visible, not assumed.
//
// One configuration per run, one CSV row to stdout. The sim arm needs
// nothing running; the minio arm needs AKI_OBS1_S3 and an existing
// bucket (run.sh creates one).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/tamnd/aki/engine/obs1"
	"github.com/tamnd/aki/engine/obs1/sim"
)

func main() {
	store := flag.String("store", "sim", "sim | minio")
	policy := flag.String("policy", "spec", "none | spec | fixed | probe")
	contenders := flag.Int("contenders", 16, "nodes appending to one chain")
	rate := flag.Float64("rate", 20, "records per second per node")
	dur := flag.Duration("dur", 10*time.Second, "run length")
	seed := flag.Uint64("seed", 1, "simulator seed")
	bucket := flag.String("bucket", "obs1-chainappend", "bucket for the minio arm")
	quick := flag.Bool("quick", false, "one short sim run")
	header := flag.Bool("header", false, "print the CSV header and exit")
	flag.Parse()

	if *header {
		fmt.Println(csvHeader)
		return
	}
	if *quick {
		s := sim.New(sim.Config{Seed: *seed, Latency: sim.S3Standard})
		fmt.Println(sweep(s, "sim", "spec", 4, 20, 2*time.Second))
		return
	}
	var s obs1.Store
	switch *store {
	case "sim":
		s = sim.New(sim.Config{Seed: *seed, Latency: sim.S3Standard})
	case "minio":
		endpoint := os.Getenv("AKI_OBS1_S3")
		if endpoint == "" {
			fmt.Fprintln(os.Stderr, "the minio arm needs AKI_OBS1_S3")
			os.Exit(1)
		}
		c, err := obs1.NewClient(obs1.ClientConfig{
			Endpoint:  endpoint,
			Region:    "us-east-1",
			Bucket:    *bucket,
			AccessKey: envOr("AKI_OBS1_S3_USER", "minioadmin"),
			SecretKey: envOr("AKI_OBS1_S3_PASS", "minioadmin"),
			PathStyle: true,
		})
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		s = c
	default:
		fmt.Fprintf(os.Stderr, "unknown store %q\n", *store)
		os.Exit(1)
	}
	fmt.Println(sweep(s, *store, *policy, *contenders, *rate, *dur))
}

func envOr(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

const csvHeader = "store,policy,contenders,rate_per_node,dur_s,records,records_per_s,appends,appends_per_s,recs_per_batch,puts,rate412_pct,gets_per_append,append_p50_ms,append_p99_ms,commit_p50_ms,commit_p99_ms,commit_max_ms"

// prefix namespaces each run so sweeps against a shared live bucket never
// collide; the caller varies it, the sim starts empty every time.
func chainSeqKey(prefix string, seq uint64) string {
	return fmt.Sprintf("%schain/00/%016d", prefix, seq)
}

type metrics struct {
	records, appends, puts, p412, gets int64
	appendLat, commitLat               []time.Duration
}

// backoff sleeps per policy. Attempt 1 is the first PUT; the doc 02
// suggestion is nothing on the first retry (the loser just caught up and
// should go again immediately) and jittered 5 to 25ms after that. The
// probe policy never sleeps; its lever is in appendOnce instead.
func backoff(policy string, attempt int, rng *rand.Rand) {
	if attempt < 3 {
		return
	}
	switch policy {
	case "none", "probe":
	case "spec":
		time.Sleep(5*time.Millisecond + time.Duration(rng.Int63n(int64(20*time.Millisecond))))
	case "fixed":
		time.Sleep(15 * time.Millisecond)
	}
}

// buildBatch renders the real doc 03 bytes for one append: a commit
// record standing in for the flush that triggered it, plus one heartbeat
// per additional coalesced record, which keeps object sizes honest.
func buildBatch(writer, batchID uint64, nrec int) []byte {
	records := []obs1.ChainRecord{obs1.CommitRecord{
		WALNode: writer, WALSeq: batchID, WALSize: 1 << 20,
		Sections: []obs1.CommitSection{{Group: 1, Epoch: 1, Offset: 32, StoredLen: 4096, NFrames: 8, FirstSeq: 1, LastSeq: 8}},
	}}
	for range nrec - 1 {
		records = append(records, obs1.HeartbeatRecord{})
	}
	b, err := obs1.AppendChainBatch(nil, writer, obs1.ChainBatch{BatchID: batchID, Incarnation: 1, Records: records})
	if err != nil {
		panic(err)
	}
	return b
}

// appendOnce is the doc 02 section 2.4 loop for one batch: PUT at
// tail+1, 412 means apply the winner and retry one up, 409 means re-read
// and follow what the key shows, anything ambiguous means re-read and
// recognize our own object by writer id and batch id.
func appendOnce(ctx context.Context, s obs1.Store, prefix string, tail *uint64, writer, batchID uint64, nrec int, policy string, rng *rand.Rand, m *metrics) error {
	body := buildBatch(writer, batchID, nrec)
	tag := obs1.WriteTag{Writer: fmt.Sprintf("%016x", writer), Batch: fmt.Sprintf("%016d", batchID)}
	for attempt := 1; ; attempt++ {
		seq := *tail + 1
		// The probe policy pays a GET before every contended PUT: once one
		// 412 proves a race, checking the target first turns most further
		// losses into cheap reads (a GET is 12.5x cheaper than a PUT on S3
		// and faster on the latency model). The blind first PUT keeps the
		// uncontended fast path at one round trip.
		if policy == "probe" && attempt > 1 {
			m.gets++
			_, _, gerr := s.Get(ctx, chainSeqKey(prefix, seq))
			if gerr == nil {
				*tail = seq
				continue
			}
			if !errors.Is(gerr, obs1.ErrNotFound) {
				return gerr
			}
		}
		m.puts++
		_, err := s.PutIfAbsent(ctx, chainSeqKey(prefix, seq), body, tag)
		switch {
		case err == nil:
			*tail = seq
			m.appends++
			return nil
		case errors.Is(err, obs1.ErrPrecondition):
			m.p412++
			m.gets++
			if _, _, gerr := s.Get(ctx, chainSeqKey(prefix, seq)); gerr != nil {
				return gerr
			}
			*tail = seq
			backoff(policy, attempt, rng)
		case errors.Is(err, obs1.ErrConflict), errors.Is(err, obs1.ErrAmbiguous):
			m.gets++
			b, _, gerr := s.Get(ctx, chainSeqKey(prefix, seq))
			if errors.Is(gerr, obs1.ErrNotFound) {
				continue // nothing landed; retry the PUT at the same seq
			}
			if gerr != nil {
				return gerr
			}
			batch, h, perr := obs1.ParseChainBatch(b)
			if perr != nil {
				return perr
			}
			*tail = seq
			if h.Writer == writer && batch.BatchID == batchID {
				m.appends++
				return nil // the timed-out PUT had won
			}
			backoff(policy, attempt, rng)
		default:
			return err
		}
	}
}

// node runs one contender: records arrive every 1/rate seconds, at most
// one append is in flight, and everything pending rides the next batch.
func node(ctx context.Context, s obs1.Store, prefix string, id int, rate float64, dur time.Duration, policy string, m *metrics) {
	rng := rand.New(rand.NewSource(int64(id) + 1))
	interval := time.Duration(float64(time.Second) / rate)
	start := time.Now()
	next := start.Add(time.Duration(rng.Int63n(int64(interval)))) // desynchronize the fleet
	tail := uint64(0)
	var pending []time.Time
	batchID := uint64(id)<<32 + 1
	for {
		now := time.Now()
		for !next.After(now) && next.Sub(start) < dur {
			pending = append(pending, next)
			next = next.Add(interval)
		}
		if len(pending) == 0 {
			if next.Sub(start) >= dur {
				return
			}
			time.Sleep(time.Until(next))
			continue
		}
		nrec := len(pending)
		t0 := time.Now()
		if err := appendOnce(ctx, s, prefix, &tail, uint64(id)+1, batchID, nrec, policy, rng, m); err != nil {
			fmt.Fprintf(os.Stderr, "node %d: %v\n", id, err)
			return
		}
		batchID++
		done := time.Now()
		m.appendLat = append(m.appendLat, done.Sub(t0))
		for _, ready := range pending[:nrec] {
			m.commitLat = append(m.commitLat, done.Sub(ready))
		}
		m.records += int64(nrec)
		pending = pending[nrec:]
	}
}

func sweep(s obs1.Store, store, policy string, contenders int, rate float64, dur time.Duration) string {
	ctx := context.Background()
	prefix := fmt.Sprintf("run-%s-%d-%.0f/", policy, contenders, rate)
	per := make([]metrics, contenders)
	start := time.Now()
	var wg sync.WaitGroup
	for i := range contenders {
		wg.Go(func() { node(ctx, s, prefix, i, rate, dur, policy, &per[i]) })
	}
	wg.Wait()
	elapsed := time.Since(start).Seconds()

	var m metrics
	for i := range per {
		m.records += per[i].records
		m.appends += per[i].appends
		m.puts += per[i].puts
		m.p412 += per[i].p412
		m.gets += per[i].gets
		m.appendLat = append(m.appendLat, per[i].appendLat...)
		m.commitLat = append(m.commitLat, per[i].commitLat...)
	}
	div := func(a, b float64) float64 {
		if b == 0 {
			return 0
		}
		return a / b
	}
	return fmt.Sprintf("%s,%s,%d,%.0f,%.1f,%d,%.1f,%d,%.1f,%.2f,%d,%.2f,%.2f,%.1f,%.1f,%.1f,%.1f,%.1f",
		store, policy, contenders, rate, elapsed,
		m.records, div(float64(m.records), elapsed),
		m.appends, div(float64(m.appends), elapsed),
		div(float64(m.records), float64(m.appends)),
		m.puts, 100*div(float64(m.p412), float64(m.puts)),
		div(float64(m.gets), float64(m.appends)),
		ms(pct(m.appendLat, 50)), ms(pct(m.appendLat, 99)),
		ms(pct(m.commitLat, 50)), ms(pct(m.commitLat, 99)), ms(pct(m.commitLat, 100)))
}

func pct(d []time.Duration, p int) time.Duration {
	if len(d) == 0 {
		return 0
	}
	slices.Sort(d)
	i := len(d) * p / 100
	if i >= len(d) {
		i = len(d) - 1
	}
	return d[i]
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }
