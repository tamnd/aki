// Fence torture (spec 2064/obs1 doc 02 section 8, milestone O0b): drive
// randomized adversarial schedules against the chain and the lease fold,
// then prove C-I2 and C-I3 the hard way. Every schedule mixes racing
// grants, expired-lease writers, zombie commits, voluntary releases,
// stale member records, ambiguous PUTs that did or did not land, and
// crash-restarts that reset the batch counter under a new incarnation.
// After each schedule three independent folders consume the chain through
// three different paths (a following appender, a raw object walk, and a
// checkpoint prime at mid-chain) and every folder, plus every surviving
// node's live fold, must agree bit for bit on the lease table and on
// every commit verdict. Any disagreement is a violation and the run
// fails.
//
// The torture must also have teeth: a run where no grant was ever
// rejected and no commit section ever died proves nothing, so toothless
// runs fail too.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"hash/crc32"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"

	"github.com/tamnd/aki/engine/obs1"
	"github.com/tamnd/aki/engine/obs1/sim"
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

const csvHeader = "store,schedules,steps,nodes,groups,fault_pct,seed,appends,grants_ok,grants_rej,sections_live,sections_dead,members_stale,crashes,violations"

type metrics struct {
	appends      uint64
	grantsOK     uint64
	grantsRej    uint64
	sectionsLive uint64
	sectionsDead uint64
	membersStale uint64
	crashes      uint64
	violations   uint64
}

// node is one schedule participant: an appender and its live fold. Its
// view of who holds what is always its own fold, which may be stale, and
// stale beliefs are the point: a commit under a stale epoch is a zombie
// commit, a grant computed from a stale table is an ambiguous grant.
type node struct {
	id   uint64
	inc  uint32
	app  *obs1.ChainAppender
	fold *obs1.LeaseFold
}

func newNode(s obs1.Store, prefix string, id uint64, inc uint32) (*node, error) {
	f := obs1.NewLeaseFold()
	a, err := obs1.NewChainAppender(s, prefix, 0, id, inc, obs1.ChainPos{}, f)
	if err != nil {
		return nil, err
	}
	return &node{id: id, inc: inc, app: a, fold: f}, nil
}

// believed is the node's view of a group: the current epoch (kept across
// releases) and whether the node believes itself the holder.
func (n *node) believed(g uint16) (epoch uint32, self bool) {
	for _, l := range n.fold.Leases() {
		if l.Group == g {
			return l.Epoch, l.Node == n.id
		}
	}
	return 0, false
}

// verdictSink counts sections and folds every verdict into a running
// digest, the C-I3 comparison across folders.
type verdictSink struct {
	live, dead uint64
	sum        uint32
}

func (v *verdictSink) attach(f *obs1.LeaseFold) {
	f.OnCommit = func(cv obs1.CommitVerdict) error {
		var b []byte
		b = append(b, byte(cv.Pos.DD))
		b = appendU64(b, cv.Pos.Seq)
		b = appendU64(b, cv.Writer)
		for _, ok := range cv.Live {
			if ok {
				v.live++
				b = append(b, 1)
			} else {
				v.dead++
				b = append(b, 0)
			}
		}
		v.sum = crc32.Update(v.sum, castagnoli, b)
		return nil
	}
}

func appendU64(b []byte, x uint64) []byte {
	for i := range 8 {
		b = append(b, byte(x>>(8*i)))
	}
	return b
}

// chainSeqKey renders the same key the appender's dbKey join produces
// for a nonempty prefix.
func chainSeqKey(prefix string, seq uint64) string {
	return fmt.Sprintf("%s/chain/00/%016d", prefix, seq)
}

func heartbeat() obs1.ChainRecord { return obs1.HeartbeatRecord{} }

func commitRec(writer uint64, g uint16, epoch uint32, rng *rand.Rand) obs1.ChainRecord {
	return obs1.CommitRecord{
		WALNode: writer, WALSeq: uint64(rng.Intn(1000) + 1), WALSize: 4096,
		Sections: []obs1.CommitSection{{
			Group: g, Epoch: epoch, Offset: 32, StoredLen: 512,
			NFrames: 1, FirstSeq: 1, LastSeq: 1,
		}},
	}
}

func memberRec(rng *rand.Rand, nodes int) obs1.ChainRecord {
	op := uint8(obs1.MemberJoin)
	if rng.Intn(3) == 0 {
		op = obs1.MemberLeave
	}
	return obs1.MemberRecord{Op: op, Member: obs1.Member{
		Node:        uint64(rng.Intn(nodes) + 1),
		Incarnation: uint32(rng.Intn(4) + 1),
		Resp:        "r:1", Mesh: "m:1", Weight: 1, Version: "v1",
	}}
}

type config struct {
	store    string
	prefix   string
	steps    int
	nodes    int
	groups   int
	faultPct int
	seed     int64
}

// schedule runs one adversarial script and then verifies it. Steps are
// sequential, so a sim schedule is deterministic given its seed; the
// opening burst is deliberately concurrent, because two simultaneous
// takers racing the same CAS is the doc 02 section 3.2 case the
// sequential part cannot produce, and the verification pass does not
// care whether the chain it checks was built deterministically.
func schedule(ctx context.Context, s obs1.Store, cfg config, rng *rand.Rand, m *metrics) error {
	nodes := make([]*node, cfg.nodes)
	for i := range nodes {
		n, err := newNode(s, cfg.prefix, uint64(i+1), 1)
		if err != nil {
			return err
		}
		nodes[i] = n
	}

	// The opening burst: every node tries to take group 0 at epoch 1 at
	// the same time. Exactly one grant folds; the rest are rejected by
	// every folder at the same chain position.
	var wg sync.WaitGroup
	var burst atomic.Uint64
	for _, n := range nodes {
		wg.Go(func() {
			rec := obs1.GrantRecord{Group: 0, Node: n.id, Epoch: 1}
			if _, err := n.app.Append(ctx, []obs1.ChainRecord{rec}); err == nil {
				burst.Add(1)
			}
		})
	}
	wg.Wait()
	m.appends += burst.Load()

	crashes := 0
	for step := 0; step < cfg.steps; step++ {
		n := nodes[rng.Intn(len(nodes))]
		g := uint16(rng.Intn(cfg.groups))
		var recs []obs1.ChainRecord
		switch roll := rng.Intn(100); {
		case roll < 25:
			recs = append(recs, heartbeat())
		case roll < 45:
			// Commit under whatever epoch this node believes, holder or
			// not. A stale belief is a zombie commit, a non-holder belief
			// is an expired-lease writer; both must die at fold.
			epoch, self := n.believed(g)
			if !self && rng.Intn(100) >= 30 {
				continue
			}
			if epoch == 0 {
				epoch = uint32(rng.Intn(3) + 1)
			}
			recs = append(recs, commitRec(n.id, g, epoch, rng))
			if rng.Intn(100) < 20 {
				recs = append(recs, heartbeat())
			}
		case roll < 65:
			// Grant computed from a possibly stale table: the epoch may
			// already be taken, which is the ambiguous-grant race.
			epoch, _ := n.believed(g)
			recs = append(recs, obs1.GrantRecord{Group: g, Node: n.id, Epoch: epoch + 1})
		case roll < 75:
			epoch, self := n.believed(g)
			if !self {
				continue
			}
			recs = append(recs, obs1.ReleaseRecord{Group: g, Epoch: epoch})
		case roll < 90:
			if err := n.app.Follow(ctx); err != nil {
				return fmt.Errorf("follow: %w", err)
			}
			continue
		case roll < 95:
			recs = append(recs, memberRec(rng, cfg.nodes))
		default:
			if crashes >= 2 {
				continue
			}
			crashes++
			m.crashes++
			// Crash-restart: same writer id, fresh batch counter, new
			// incarnation, fold rebuilt by replaying the whole chain. Its
			// next append with batch id 1 must not be claimed by any
			// object its previous life wrote.
			fresh, err := newNode(s, cfg.prefix, n.id, n.inc+1)
			if err != nil {
				return err
			}
			if err := fresh.app.Follow(ctx); err != nil {
				return fmt.Errorf("restart follow: %w", err)
			}
			for i := range nodes {
				if nodes[i].id == n.id {
					nodes[i] = fresh
				}
			}
			continue
		}
		if _, err := n.app.Append(ctx, recs); err != nil {
			return fmt.Errorf("append (node %d step %d): %w", n.id, step, err)
		}
		m.appends++
	}

	// Everyone catches up before the check, so every live fold has
	// consumed the identical chain.
	tail := uint64(0)
	for _, n := range nodes {
		if err := n.app.Follow(ctx); err != nil {
			return fmt.Errorf("final follow: %w", err)
		}
		if t := n.app.Tail().Seq; t > tail {
			tail = t
		}
	}
	return verify(ctx, s, cfg, tail, nodes, m)
}

// walk replays chain objects (from, to] into a fold by raw GETs, the
// appender-free consumption path.
func walk(ctx context.Context, s obs1.Store, prefix string, from, to uint64, f *obs1.LeaseFold) error {
	for seq := from + 1; seq <= to; seq++ {
		b, _, err := s.Get(ctx, chainSeqKey(prefix, seq))
		if err != nil {
			return fmt.Errorf("walk seq %d: %w", seq, err)
		}
		batch, h, err := obs1.ParseChainBatch(b)
		if err != nil {
			return fmt.Errorf("walk seq %d: %w", seq, err)
		}
		if err := f.ApplyChain(obs1.ChainPos{Seq: seq}, h, batch); err != nil {
			return err
		}
	}
	return nil
}

// verify is the actual gate: three independent folders over three
// different consumption paths, plus every node's live fold, must agree.
func verify(ctx context.Context, s obs1.Store, cfg config, tail uint64, nodes []*node, m *metrics) error {
	bad := func(format string, args ...any) {
		m.violations++
		fmt.Fprintf(os.Stderr, "VIOLATION seed %d: %s\n", cfg.seed, fmt.Sprintf(format, args...))
	}

	// Folder A: a fresh appender that only follows.
	foldA := obs1.NewLeaseFold()
	var sinkA verdictSink
	sinkA.attach(foldA)
	appA, err := obs1.NewChainAppender(s, cfg.prefix, 0, 0xA11CE, 1, obs1.ChainPos{}, foldA)
	if err != nil {
		return err
	}
	if err := appA.Follow(ctx); err != nil {
		return err
	}
	if got := appA.Tail().Seq; got != tail {
		bad("folder A reached seq %d, chain tail is %d", got, tail)
	}

	// Folder B: a raw object walk, no appender involved.
	foldB := obs1.NewLeaseFold()
	var sinkB verdictSink
	sinkB.attach(foldB)
	if err := walk(ctx, s, cfg.prefix, 0, tail, foldB); err != nil {
		return err
	}

	// Folder C: replay to mid-chain, summarize into a checkpoint, prime a
	// fresh fold from it, replay the rest. C-I7 says the summary must
	// land it on the same table.
	mid := tail / 2
	foldMid := obs1.NewLeaseFold()
	if err := walk(ctx, s, cfg.prefix, 0, mid, foldMid); err != nil {
		return err
	}
	foldC := obs1.NewLeaseFold()
	if err := foldC.Prime(obs1.Checkpoint{
		Through: obs1.ChainPos{Seq: mid},
		Members: foldMid.Members(),
		Leases:  foldMid.Leases(),
	}); err != nil {
		return err
	}
	if err := walk(ctx, s, cfg.prefix, mid, tail, foldC); err != nil {
		return err
	}

	// C-I2: A and B agree bit for bit, on the table and on every commit
	// verdict.
	if foldA.StateSum() != foldB.StateSum() {
		bad("folder A sum %08x != folder B sum %08x", foldA.StateSum(), foldB.StateSum())
	}
	if sinkA.sum != sinkB.sum || sinkA.live != sinkB.live || sinkA.dead != sinkB.dead {
		bad("verdict digests differ: A %08x (%d/%d) B %08x (%d/%d)",
			sinkA.sum, sinkA.live, sinkA.dead, sinkB.sum, sinkB.live, sinkB.dead)
	}
	// Every node's live fold, whatever mix of Append and Follow built it,
	// matches folder A.
	for _, n := range nodes {
		if n.fold.StateSum() != foldA.StateSum() {
			bad("node %d live fold sum %08x != folder A sum %08x", n.id, n.fold.StateSum(), foldA.StateSum())
		}
	}
	// C-I7: the checkpoint-primed folder lands on the same tables. Epoch
	// spans differ across a prime by design, so compare the tables, not
	// the sums.
	if fmt.Sprintf("%+v", foldC.Leases()) != fmt.Sprintf("%+v", foldA.Leases()) {
		bad("primed folder leases %+v != folder A leases %+v", foldC.Leases(), foldA.Leases())
	}
	if fmt.Sprintf("%+v", foldC.Members()) != fmt.Sprintf("%+v", foldA.Members()) {
		bad("primed folder members %+v != folder A members %+v", foldC.Members(), foldA.Members())
	}

	for _, l := range foldA.Leases() {
		m.grantsOK += uint64(l.Epoch) // every folded grant bumps exactly one epoch by one
	}
	m.grantsRej += foldA.Stats.GrantsRejected
	m.sectionsLive += sinkA.live
	m.sectionsDead += sinkA.dead
	m.membersStale += foldA.Stats.MembersStale
	return nil
}

func openStore(kind, bucket string, seed int64, faultPct int) (obs1.Store, error) {
	switch kind {
	case "sim":
		frng := rand.New(rand.NewSource(seed * 7919))
		var fault sim.FaultFn
		if faultPct > 0 {
			fault = func(op sim.Op, key string) *sim.Fault {
				if op != sim.OpPutIfAbsent || frng.Intn(100) >= faultPct {
					return nil
				}
				// An ambiguous PUT that did or did not land: the two
				// halves of the doc 02 section 2.4 re-check.
				return &sim.Fault{
					Err:     fmt.Errorf("torture: %w", obs1.ErrAmbiguous),
					Applied: frng.Intn(2) == 0,
				}
			}
		}
		return sim.New(sim.Config{Seed: uint64(seed), Fault: fault}), nil
	case "minio":
		endpoint := os.Getenv("AKI_OBS1_S3")
		if endpoint == "" {
			return nil, errors.New("minio store needs AKI_OBS1_S3")
		}
		return obs1.NewClient(obs1.ClientConfig{
			Endpoint:  endpoint,
			Region:    "us-east-1",
			Bucket:    bucket,
			AccessKey: envOr("AKI_OBS1_S3_USER", "minioadmin"),
			SecretKey: envOr("AKI_OBS1_S3_PASS", "minioadmin"),
			PathStyle: true,
		})
	}
	return nil, fmt.Errorf("unknown store %q", kind)
}

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func main() {
	store := flag.String("store", "sim", "sim or minio")
	schedules := flag.Int("schedules", 40, "schedules to run")
	steps := flag.Int("steps", 150, "steps per schedule")
	nodesN := flag.Int("nodes", 4, "nodes per schedule")
	groups := flag.Int("groups", 8, "groups")
	faultPct := flag.Int("faults", 15, "ambiguous-PUT percent (sim only)")
	bucket := flag.String("bucket", "obs1-fencetorture", "bucket for the minio arm")
	seed := flag.Int64("seed", 1, "base seed")
	header := flag.Bool("header", false, "print the CSV header")
	flag.Parse()
	if *header {
		fmt.Println(csvHeader)
		if *schedules == 0 {
			return
		}
	}

	ctx := context.Background()
	var m metrics
	for i := 0; i < *schedules; i++ {
		s := *seed + int64(i)
		st, err := openStore(*store, *bucket, s, *faultPct)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		cfg := config{
			store: *store, prefix: fmt.Sprintf("ft-%d", s),
			steps: *steps, nodes: *nodesN, groups: *groups,
			faultPct: *faultPct, seed: s,
		}
		rng := rand.New(rand.NewSource(s))
		if err := schedule(ctx, st, cfg, rng, &m); err != nil {
			fmt.Fprintf(os.Stderr, "schedule seed %d: %v\n", s, err)
			os.Exit(1)
		}
	}

	fmt.Printf("%s,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d\n",
		*store, *schedules, *steps, *nodesN, *groups, *faultPct, *seed,
		m.appends, m.grantsOK, m.grantsRej, m.sectionsLive, m.sectionsDead,
		m.membersStale, m.crashes, m.violations)

	if m.violations > 0 {
		fmt.Fprintf(os.Stderr, "%d violations\n", m.violations)
		os.Exit(1)
	}
	if m.grantsRej == 0 || m.sectionsDead == 0 {
		fmt.Fprintln(os.Stderr, "toothless torture: no rejected grants or no dead sections")
		os.Exit(1)
	}
}
