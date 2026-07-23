package drivers

// The cold arm of the conformance suite (doc 10, suite conformance: the
// corpus "against cold state through the full read path"; O2a's
// conformance-cold row). The hot corpus builds its state, the cold-arm
// subset stands up strings, a hash, a set, and a zset past the inline bands,
// resident pressure demotes and stage-drains the working set, a fold
// publishes it between build and verify, and the same reads must answer
// identically off the cold tiers. A restart tail reboots from the bucket
// and verifies once more, so folded string placements answer through the
// bucket cold read path where the hot tier does not hold them.

import (
	"testing"

	"github.com/tamnd/aki/engine/obs1/sim"
	"github.com/tamnd/aki/obs1srv/conformance"
)

// foldedKeys collects every key the folder has placed into a published
// segment, records and collections both, across the ledger so far.
func foldedKeys(b *Booted) map[string]bool {
	keys := map[string]bool{}
	for _, led := range b.Folder.Ledger() {
		for _, p := range led.Places {
			keys[string(p.Key)] = true
		}
	}
	return keys
}

// settleFold kicks the folder until the pipeline is idle: everything cut
// has PUT and published. Unlike forceFold it does not demand a new
// segment, because the age cadences usually fold the drains before the
// explicit kick lands.
func settleFold(t *testing.T, b *Booted) {
	t.Helper()
	pollFor(t, "the fold to settle", func() bool {
		b.Folder.Flush()
		fs := b.Folder.Stats()
		return fs.SegmentsPut == fs.SegmentsCut && fs.Published == fs.SegmentsPut
	})
}

// spinDemote drives command boundaries on the shards owning the cold-arm
// collections. The collection demoters shed one bounded quantum per
// boundary, so a pipelined ballast burst alone lands too few boundaries
// to empty a whole hash; EXISTS touches the registry without reading any
// value, so it cannot promote what it is trying to demote.
func spinDemote(t *testing.T, rc *conformance.Conn) {
	t.Helper()
	for i := 0; i < 50; i++ {
		doStep(t, rc, []string{"EXISTS", conformance.ColdHashKey})
		doStep(t, rc, []string{"EXISTS", conformance.ColdSetKey})
		doStep(t, rc, []string{"EXISTS", conformance.ColdZsetKey})
		doStep(t, rc, []string{"EXISTS", conformance.ColdListKey})
	}
}

// runSteps replays steps through the corpus client, failing on the first
// divergence. The label tells the arms apart in a failure.
func runSteps(t *testing.T, rc *conformance.Conn, label string, steps []conformance.Step) {
	t.Helper()
	for i, s := range steps {
		if msg := conformance.CheckDurable(s, doStep(t, rc, s.Cmd)); msg != "" {
			t.Fatalf("%s step %d %s", label, i, msg)
		}
	}
}

// TestConformanceCold is the fold-between-build-and-verify cold arm for
// the O2a types. Build: the hot corpus minus its wipe tail, then the
// cold-arm subset. Force cold: ballast rounds push the shard past its
// resident cap so the migrator stages string drains and the collection
// demoters shed the hash and the set, and the poll insists the exact
// cold-arm keys reach published segments, which only happens once their
// bytes really left the resident tier through the fold tap. Verify: every
// point read, miss, and count answers identically, and the whole surviving
// keyspace fingerprint is unchanged. Restart: a second incarnation reboots
// from the bucket and both checks run again.
func TestConformanceCold(t *testing.T) {
	bucket := sim.New(sim.Config{})
	b1, srv1, rc1, nc1 := bootConfServer(t, bucket, 1)
	steps := conformance.Hot[:len(conformance.Hot)-conformance.WipeTail]
	runSteps(t, rc1, "corpus", steps)
	build := conformance.ColdBuild()
	runSteps(t, rc1, "cold build", build)
	allSteps := append(append([]conformance.Step{}, steps...), build...)
	before := fingerprint(t, rc1, allSteps)

	// The keys whose fold placement the pressure loop waits for: the two
	// collections demote through their quantum triggers, the strings ride
	// the migrator's staged drains.
	want := []string{conformance.ColdHashKey, conformance.ColdSetKey, conformance.ColdZsetKey, conformance.ColdListKey}
	for i := 0; i < conformance.ColdStrings; i++ {
		want = append(want, conformance.ColdStringKey(i))
	}
	round := 0
	pollFor(t, "the cold-arm keys to fold", func() bool {
		folded := foldedKeys(b1)
		missing := 0
		for _, k := range want {
			if !folded[k] {
				missing++
			}
		}
		if missing == 0 {
			return true
		}
		ballast(t, rc1, 1000+round)
		round++
		spinDemote(t, rc1)
		settleFold(t, b1)
		return false
	})
	// One more pressure round settled through the fold, so the verify
	// runs over a bucket state the fold just rewrote, the T-I6 shape on
	// the cold arm.
	ballast(t, rc1, 999)
	settleFold(t, b1)

	runSteps(t, rc1, "cold verify", conformance.ColdVerify())
	after := fingerprint(t, rc1, allSteps)
	for key, w := range before {
		if after[key] != w {
			t.Errorf("key %q diverged after going cold: before %s, after %s", key, w, after[key])
		}
	}
	if fs := b1.Folder.Stats(); fs.Chunks == 0 || fs.BuildErrs != 0 || fs.WalkErrs != 0 {
		t.Fatalf("no collection chunks crossed the fold, or the fold erred: %+v", fs)
	}
	commitAndStop(t, b1, srv1, nc1)

	// The restart tail: the rebuilt keymap and directories answer the
	// same reads, with folded string placements served off the bucket
	// where replay left the hot tier without them.
	b2, srv2, rc2, nc2 := bootConfServer(t, bucket, 2)
	defer func() { commitAndStop(t, b2, srv2, nc2) }()
	runSteps(t, rc2, "restart verify", conformance.ColdVerify())
	rebooted := fingerprint(t, rc2, allSteps)
	for key, w := range before {
		if rebooted[key] != w {
			t.Errorf("key %q diverged across the reboot: before %s, after %s", key, w, rebooted[key])
		}
	}
	if st := b2.Cold.Stats(); st.Errs != 0 || st.Unresolved != 0 {
		t.Fatalf("bucket cold reader stats %+v, want a clean sweep", st)
	}
}
