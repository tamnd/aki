package obs1_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/tamnd/aki/engine/obs1"
	"github.com/tamnd/aki/engine/obs1/sim"
)

// recorder is a ChainApplier that checks the exactly-once dense-order
// contract as it goes and keeps what it saw for the test to assert on.
type recorder struct {
	t    *testing.T
	mu   sync.Mutex
	next uint64
	fail error
	seen []applied
}

type applied struct {
	pos    obs1.ChainPos
	writer uint64
	batch  uint64
	nrec   int
}

func newRecorder(t *testing.T, start uint64) *recorder {
	return &recorder{t: t, next: start + 1}
}

func (r *recorder) ApplyChain(pos obs1.ChainPos, h obs1.Header, b obs1.ChainBatch) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.fail != nil {
		return r.fail
	}
	if pos.Seq != r.next {
		r.t.Errorf("apply got seq %d, want %d", pos.Seq, r.next)
	}
	r.next = pos.Seq + 1
	r.seen = append(r.seen, applied{pos, h.Writer, b.BatchID, len(b.Records)})
	return nil
}

func hb() []obs1.ChainRecord {
	return []obs1.ChainRecord{obs1.HeartbeatRecord{}}
}

func TestAppendAndFollow(t *testing.T) {
	ctx := context.Background()
	s := sim.New(sim.Config{Seed: 1})

	ra := newRecorder(t, 0)
	a, err := obs1.NewChainAppender(s, "db/t", 0, 1, 1, obs1.ChainPos{}, ra)
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(1); i <= 3; i++ {
		pos, err := a.Append(ctx, hb())
		if err != nil {
			t.Fatal(err)
		}
		if pos != (obs1.ChainPos{DD: 0, Seq: i}) {
			t.Fatalf("append %d landed at %+v", i, pos)
		}
	}

	// A second node starting from nothing follows the whole chain.
	rb := newRecorder(t, 0)
	b, err := obs1.NewChainAppender(s, "db/t", 0, 2, 1, obs1.ChainPos{}, rb)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Follow(ctx); err != nil {
		t.Fatal(err)
	}
	if len(rb.seen) != 3 || rb.seen[2].writer != 1 {
		t.Fatalf("follow saw %+v", rb.seen)
	}
	if b.Tail() != (obs1.ChainPos{DD: 0, Seq: 3}) {
		t.Fatalf("tail after follow: %+v", b.Tail())
	}

	// B takes seq 4; A's next append starts blind at 4, eats the 412,
	// applies B's batch on the way, and lands at 5 through the probe path.
	if pos, err := b.Append(ctx, hb()); err != nil || pos.Seq != 4 {
		t.Fatalf("b append: %+v %v", pos, err)
	}
	pos, err := a.Append(ctx, hb())
	if err != nil {
		t.Fatal(err)
	}
	if pos.Seq != 5 {
		t.Fatalf("contended append landed at %+v", pos)
	}
	if len(ra.seen) != 5 || ra.seen[3].writer != 2 || ra.seen[4].writer != 1 {
		t.Fatalf("a saw %+v", ra.seen)
	}
}

func TestAppendContention(t *testing.T) {
	ctx := context.Background()
	s := sim.New(sim.Config{Seed: 2})
	const nodes, each = 3, 20

	var wg sync.WaitGroup
	for n := range nodes {
		wg.Go(func() {
			r := newRecorder(t, 0)
			a, err := obs1.NewChainAppender(s, "db/t", 0, uint64(n)+1, 1, obs1.ChainPos{}, r)
			if err != nil {
				t.Error(err)
				return
			}
			for range each {
				if _, err := a.Append(ctx, hb()); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	wg.Wait()

	// A follower proves the chain dense and every batch on it exactly once.
	r := newRecorder(t, 0)
	f, err := obs1.NewChainAppender(s, "db/t", 0, 9, 1, obs1.ChainPos{}, r)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Follow(ctx); err != nil {
		t.Fatal(err)
	}
	if len(r.seen) != nodes*each {
		t.Fatalf("chain holds %d batches, want %d", len(r.seen), nodes*each)
	}
	once := make(map[[2]uint64]bool)
	for _, got := range r.seen {
		id := [2]uint64{got.writer, got.batch}
		if once[id] {
			t.Fatalf("batch %v committed twice", id)
		}
		once[id] = true
	}
}

// ambientFault fires one injected fault on the first conditional PUT.
func ambientFault(f sim.Fault) sim.FaultFn {
	fired := false
	return func(op sim.Op, key string) *sim.Fault {
		if op == sim.OpPutIfAbsent && !fired {
			fired = true
			return &f
		}
		return nil
	}
}

func TestAppendAmbiguousLanded(t *testing.T) {
	ctx := context.Background()
	s := sim.New(sim.Config{Seed: 3, Fault: ambientFault(sim.Fault{
		Err:     fmt.Errorf("wire died: %w", obs1.ErrAmbiguous),
		Applied: true,
	})})
	r := newRecorder(t, 0)
	a, err := obs1.NewChainAppender(s, "db/t", 0, 1, 1, obs1.ChainPos{}, r)
	if err != nil {
		t.Fatal(err)
	}
	pos, err := a.Append(ctx, hb())
	if err != nil {
		t.Fatal(err)
	}
	if pos.Seq != 1 || len(r.seen) != 1 || r.seen[0].writer != 1 {
		t.Fatalf("ambiguous landed: pos %+v seen %+v", pos, r.seen)
	}
	// The loop recognized its own object; the next append must not collide.
	if pos, err := a.Append(ctx, hb()); err != nil || pos.Seq != 2 {
		t.Fatalf("append after ambiguity: %+v %v", pos, err)
	}
}

func TestAppendAmbiguousNotLanded(t *testing.T) {
	ctx := context.Background()
	s := sim.New(sim.Config{Seed: 4, Fault: ambientFault(sim.Fault{
		Err: fmt.Errorf("wire died: %w", obs1.ErrAmbiguous),
	})})
	r := newRecorder(t, 0)
	a, err := obs1.NewChainAppender(s, "db/t", 0, 1, 1, obs1.ChainPos{}, r)
	if err != nil {
		t.Fatal(err)
	}
	pos, err := a.Append(ctx, hb())
	if err != nil {
		t.Fatal(err)
	}
	if pos.Seq != 1 || len(r.seen) != 1 {
		t.Fatalf("ambiguous not landed: pos %+v seen %+v", pos, r.seen)
	}
}

// putLiar lands the first conditional PUT and then reports 412 anyway:
// the shape of a delayed duplicate landing after its ambiguity was
// resolved as absent, which the 412 branch must recognize as ours.
type putLiar struct {
	obs1.Store
	fired bool
}

func (p *putLiar) PutIfAbsent(ctx context.Context, key string, body []byte, tag obs1.WriteTag) (obs1.ObjectInfo, error) {
	info, err := p.Store.PutIfAbsent(ctx, key, body, tag)
	if !p.fired && err == nil {
		p.fired = true
		return info, fmt.Errorf("liar: %w", obs1.ErrPrecondition)
	}
	return info, err
}

func TestAppend412AgainstOwnObject(t *testing.T) {
	ctx := context.Background()
	s := &putLiar{Store: sim.New(sim.Config{Seed: 5})}
	r := newRecorder(t, 0)
	a, err := obs1.NewChainAppender(s, "db/t", 0, 1, 1, obs1.ChainPos{}, r)
	if err != nil {
		t.Fatal(err)
	}
	pos, err := a.Append(ctx, hb())
	if err != nil {
		t.Fatal(err)
	}
	if pos.Seq != 1 || len(r.seen) != 1 {
		t.Fatalf("412 against own object: pos %+v seen %+v", pos, r.seen)
	}
}

func TestBootChain(t *testing.T) {
	ctx := context.Background()
	s := sim.New(sim.Config{Seed: 6})
	r := newRecorder(t, 0)
	a, err := obs1.NewChainAppender(s, "db/t", 0, 1, 1, obs1.ChainPos{}, r)
	if err != nil {
		t.Fatal(err)
	}
	for range 7 {
		if _, err := a.Append(ctx, hb()); err != nil {
			t.Fatal(err)
		}
	}
	ck, err := obs1.AppendCheckpoint(nil, 1, obs1.Checkpoint{Through: obs1.ChainPos{DD: 0, Seq: 5}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Put(ctx, "db/t/chain/00/ckpt/0000000000000005", ck); err != nil {
		t.Fatal(err)
	}
	if err := obs1.CreateRoot(ctx, s, "db/t", false, obs1.Root{CreatedMS: 1, G: 64, D: 1, CkptSeq: 5, CkptDD: 0}); err != nil {
		t.Fatal(err)
	}

	rb := newRecorder(t, 5)
	b, ckpt, err := obs1.BootChain(ctx, s, "db/t", false, 0, 2, 1, rb)
	if err != nil {
		t.Fatal(err)
	}
	if ckpt.Through != (obs1.ChainPos{DD: 0, Seq: 5}) {
		t.Fatalf("boot checkpoint through %+v", ckpt.Through)
	}
	if b.Tail() != (obs1.ChainPos{DD: 0, Seq: 5}) {
		t.Fatalf("boot tail %+v", b.Tail())
	}
	if err := b.Follow(ctx); err != nil {
		t.Fatal(err)
	}
	if len(rb.seen) != 2 || rb.seen[0].pos.Seq != 6 || rb.seen[1].pos.Seq != 7 {
		t.Fatalf("replay after fast-forward saw %+v", rb.seen)
	}
}

func TestBootChainYoungRoot(t *testing.T) {
	ctx := context.Background()
	s := sim.New(sim.Config{Seed: 7})
	if err := obs1.CreateRoot(ctx, s, "db/t", false, obs1.Root{CreatedMS: 1, G: 64, D: 1}); err != nil {
		t.Fatal(err)
	}
	r := newRecorder(t, 0)
	a, ckpt, err := obs1.BootChain(ctx, s, "db/t", false, 0, 1, 1, r)
	if err != nil {
		t.Fatal(err)
	}
	if ckpt.Through != (obs1.ChainPos{}) || a.Tail() != (obs1.ChainPos{}) {
		t.Fatalf("young boot: ckpt %+v tail %+v", ckpt.Through, a.Tail())
	}
	if pos, err := a.Append(ctx, hb()); err != nil || pos.Seq != 1 {
		t.Fatalf("first append: %+v %v", pos, err)
	}
}

func TestApplierErrorStopsTail(t *testing.T) {
	ctx := context.Background()
	s := sim.New(sim.Config{Seed: 8})
	r := newRecorder(t, 0)
	r.fail = errors.New("fold rejected the batch")
	a, err := obs1.NewChainAppender(s, "db/t", 0, 1, 1, obs1.ChainPos{}, r)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Append(ctx, hb()); !errors.Is(err, r.fail) {
		t.Fatalf("append swallowed the applier error: %v", err)
	}
	if a.Tail() != (obs1.ChainPos{}) {
		t.Fatalf("tail advanced past a failed apply: %+v", a.Tail())
	}
}

func TestFollowRejectsCorruptObject(t *testing.T) {
	ctx := context.Background()
	s := sim.New(sim.Config{Seed: 9})
	if _, err := s.Put(ctx, "db/t/chain/00/0000000000000001", []byte("junk")); err != nil {
		t.Fatal(err)
	}
	r := newRecorder(t, 0)
	a, err := obs1.NewChainAppender(s, "db/t", 0, 1, 1, obs1.ChainPos{}, r)
	if err != nil {
		t.Fatal(err)
	}
	err = a.Follow(ctx)
	if err == nil || !strings.Contains(err.Error(), "chain/00/0000000000000001") {
		t.Fatalf("follow on junk: %v", err)
	}
	if len(r.seen) != 0 {
		t.Fatalf("junk reached the applier: %+v", r.seen)
	}
}

func TestNewChainAppenderRejects(t *testing.T) {
	s := sim.New(sim.Config{Seed: 10})
	r := newRecorder(t, 0)
	if _, err := obs1.NewChainAppender(nil, "db/t", 0, 1, 1, obs1.ChainPos{}, r); err == nil {
		t.Fatal("nil store accepted")
	}
	if _, err := obs1.NewChainAppender(s, "db/t", 0, 1, 1, obs1.ChainPos{}, nil); err == nil {
		t.Fatal("nil applier accepted")
	}
	if _, err := obs1.NewChainAppender(s, "db/t", 0, 0, 1, obs1.ChainPos{}, r); err == nil {
		t.Fatal("zero writer accepted")
	}
	if _, err := obs1.NewChainAppender(s, "db/t", 0, 1, 1, obs1.ChainPos{DD: 1, Seq: 5}, r); err == nil {
		t.Fatal("cross-domain start accepted")
	}
}
