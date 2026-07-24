// K2 chain contention (spec 2064/obs1 doc 11 section 4): 16 as-built
// write-path stacks, Flusher plus Committer plus ChainAppender, share
// one chain on the S3Standard latency model, and the lab measures
// whether the flush cadence and the lease-safety append gap survive
// the contention. The K2 decision, log domains deferred or pulled into
// O4, follows these numbers.
package main

import (
	"context"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/tamnd/aki/engine/obs1"
	"github.com/tamnd/aki/engine/obs1/sim"
)

const (
	nNodes  = 16
	opsPerS = 100
	opBytes = 100
	warmup  = 5 * time.Second
	measure = 60 * time.Second
	prefix  = "k2"
)

// timedChain wraps the shared-chain appender to clock every committer
// append, the lease-safety proxy.
type timedChain struct {
	inner *obs1.ChainAppender
	mu    sync.Mutex
	lats  []time.Duration
}

func (t *timedChain) Append(ctx context.Context, records []obs1.ChainRecord) (obs1.ChainPos, error) {
	start := time.Now()
	pos, err := t.inner.Append(ctx, records)
	if err == nil {
		t.mu.Lock()
		t.lats = append(t.lats, time.Since(start))
		t.mu.Unlock()
	}
	return pos, err
}

// timedSink stamps each WAL delivery on its way into the committer so
// OnCommitted can compute commit lag.
type timedSink struct {
	inner obs1.FlushSink
	mu    sync.Mutex
	born  map[uint64]time.Time
}

func (t *timedSink) WALFlushed(walSeq uint64, size int64, index []obs1.WALIndexEntry) error {
	t.mu.Lock()
	t.born[walSeq] = time.Now()
	t.mu.Unlock()
	return t.inner.WALFlushed(walSeq, size, index)
}

func (t *timedSink) lag(walSeq uint64) (time.Duration, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	b, ok := t.born[walSeq]
	if !ok {
		return 0, false
	}
	delete(t.born, walSeq)
	return time.Since(b), true
}

type node struct {
	id    uint64
	chain *timedChain
	sink  *timedSink
	fl    *obs1.Flusher
	cm    *obs1.Committer

	mu   sync.Mutex
	lags []time.Duration
}

func newNode(store obs1.Store, id uint64, age time.Duration) (*node, error) {
	n := &node{id: id}
	fold := obs1.NewLeaseFold()
	app, err := obs1.NewChainAppender(store, prefix, 0, id, 1, obs1.ChainPos{}, fold)
	if err != nil {
		return nil, err
	}
	n.chain = &timedChain{inner: app}
	cm, err := obs1.NewCommitter(obs1.CommitterConfig{
		Chain: n.chain, Node: id,
		OnCommitted: func(walSeq uint64, pos obs1.ChainPos) {
			if d, ok := n.sink.lag(walSeq); ok {
				n.mu.Lock()
				n.lags = append(n.lags, d)
				n.mu.Unlock()
			}
		},
	})
	if err != nil {
		return nil, err
	}
	n.cm = cm
	n.sink = &timedSink{inner: cm, born: make(map[uint64]time.Time)}
	fl, err := obs1.NewFlusher(obs1.FlusherConfig{
		Store: store, Sink: n.sink, Prefix: prefix, Node: id, FlushAge: age,
	})
	if err != nil {
		return nil, err
	}
	n.fl = fl
	return n, nil
}

func quantile(d []time.Duration, q float64) time.Duration {
	if len(d) == 0 {
		return 0
	}
	sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
	i := int(q * float64(len(d)-1))
	return d[i]
}

func runArm(seed uint64, age time.Duration) error {
	store := sim.New(sim.Config{Seed: seed, Latency: sim.S3Standard})
	ctx := context.Background()

	nodes := make([]*node, nNodes)
	for i := range nodes {
		n, err := newNode(store, uint64(i+1), age)
		if err != nil {
			return err
		}
		nodes[i] = n
	}
	// Each node grants itself one group at epoch 1, on the chain, so
	// the flushed sections carry a real lease.
	for _, n := range nodes {
		rec := obs1.GrantRecord{Group: uint16(n.id - 1), Node: n.id, Epoch: 1}
		if _, err := n.chain.inner.Append(ctx, []obs1.ChainRecord{rec}); err != nil {
			return err
		}
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for _, n := range nodes {
		wg.Add(1)
		go func(n *node) {
			defer wg.Done()
			tick := time.NewTicker(time.Second / opsPerS)
			defer tick.Stop()
			payload := make([]byte, opBytes)
			seq := uint64(0)
			for {
				select {
				case <-stop:
					return
				case <-tick.C:
					seq++
					err := n.fl.AppendOp(uint16(n.id-1), 1, obs1.WALFrame{
						Kind: 0x01, Slot: 1, Seq: seq,
						Key:     fmt.Appendf(nil, "k%06d", seq),
						Payload: payload,
					})
					if err != nil {
						return
					}
				}
			}
		}(n)
	}

	time.Sleep(warmup)
	type snap struct{ flushes, batches, records uint64 }
	before := make([]snap, nNodes)
	for i, n := range nodes {
		before[i] = snap{n.fl.Stats().Flushes, n.cm.Stats().Batches, n.cm.Stats().Records}
		n.chain.mu.Lock()
		n.chain.lats = nil
		n.chain.mu.Unlock()
		n.mu.Lock()
		n.lags = nil
		n.mu.Unlock()
	}
	time.Sleep(measure)

	var flushLo, flushHi float64
	var appends, records uint64
	var allLats, allLags []time.Duration
	var appendMax time.Duration
	for i, n := range nodes {
		fr := float64(n.fl.Stats().Flushes-before[i].flushes) / measure.Seconds()
		if i == 0 || fr < flushLo {
			flushLo = fr
		}
		if fr > flushHi {
			flushHi = fr
		}
		appends += n.cm.Stats().Batches - before[i].batches
		records += n.cm.Stats().Records - before[i].records
		n.chain.mu.Lock()
		for _, d := range n.chain.lats {
			allLats = append(allLats, d)
			if d > appendMax {
				appendMax = d
			}
		}
		n.chain.mu.Unlock()
		n.mu.Lock()
		allLags = append(allLags, n.lags...)
		n.mu.Unlock()
	}

	close(stop)
	wg.Wait()
	for _, n := range nodes {
		if err := n.fl.Close(); err != nil {
			return fmt.Errorf("age %v node %d flusher: %w", age, n.id, err)
		}
		if err := n.cm.Close(); err != nil {
			return fmt.Errorf("age %v node %d committer: %w", age, n.id, err)
		}
	}

	perBatch := 0.0
	if appends > 0 {
		perBatch = float64(records) / float64(appends)
	}
	fmt.Printf("%d,%d,%.1f,%.1f,%.1f,%.1f,%d,%d,%d,%d,%d,%d\n",
		age.Milliseconds(), nNodes,
		flushLo, flushHi,
		float64(appends)/measure.Seconds(), perBatch,
		quantile(allLats, 0.5).Milliseconds(), quantile(allLats, 0.99).Milliseconds(),
		appendMax.Milliseconds(),
		quantile(allLags, 0.5).Milliseconds(), quantile(allLags, 0.99).Milliseconds(),
		quantile(allLags, 1.0).Milliseconds(),
	)
	return nil
}

func main() {
	fmt.Println("age_ms,nodes,flush_rate_min,flush_rate_max,appends_per_s,records_per_batch,append_p50_ms,append_p99_ms,append_max_ms,lag_p50_ms,lag_p99_ms,lag_max_ms")
	for i, age := range []time.Duration{50 * time.Millisecond, 250 * time.Millisecond, time.Second} {
		if err := runArm(uint64(7000+i), age); err != nil {
			fmt.Fprintf(os.Stderr, "k2chain: %v\n", err)
			os.Exit(1)
		}
	}
}
