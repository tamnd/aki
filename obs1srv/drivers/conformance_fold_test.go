// The fold and restart arms of the conformance suite (doc 10, suite
// conformance): the same per-command corpus the binary suite replays,
// run against a durability-booted server with folds forced between
// steps (T-I6's before and after identity), then replayed to its final
// state, fingerprinted, rebooted, and fingerprinted again. Fold moves
// bytes, never user-visible content; a reboot rebuilds the same state,
// with folded string placements answering through the cold read path
// when the hot tier does not hold them. Full-surface cold conformance
// is doc 11's O2 exit; at O1 only the string band serves cold.
package drivers

import (
	"context"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/tamnd/aki/engine/obs1/shard"
	"github.com/tamnd/aki/engine/obs1/sim"
	"github.com/tamnd/aki/obs1srv/conformance"
)

// bootConfServer is the corpus harness: four shards to match the binary
// suite's slot spread, the tight resident cap and fast cadences of
// bootColdServer so folds happen for real under the corpus.
func bootConfServer(t *testing.T, bucket *sim.Sim, inc uint32) (*Booted, *Server, *conformance.Conn, net.Conn) {
	t.Helper()
	var booted *Booted
	dir := t.TempDir()
	srv, err := Listen(Options{
		Addr: "127.0.0.1:0", Shards: 4, ArenaBytes: 16 << 20, SegBytes: 1 << 18,
		ConnShape: testConnShape(), NetDriver: testNetDriver(),
		VlogDir: dir, ColdDir: dir, ResidentCapBytes: 8 << 10,
		Boot: func(rt *shard.Runtime) error {
			b, err := BootDurability(context.Background(), BootConfig{
				Store: bucket, Prefix: "p", Node: 0xC0, Incarnation: inc,
				FlushAge: 5 * time.Millisecond, FoldAge: 20 * time.Millisecond,
			}, rt)
			if err != nil {
				return err
			}
			booted = b
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve()
	nc, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		srv.Close()
		t.Fatal(err)
	}
	return booted, srv, conformance.NewConn(nc), nc
}

// doStep runs one corpus command, failing the test on transport errors.
func doStep(t *testing.T, rc *conformance.Conn, cmd []string) string {
	t.Helper()
	v, err := rc.Do(cmd)
	if err != nil {
		t.Fatal(err)
	}
	return conformance.Render(v)
}

// ballast writes a working set of small embedded records past the
// resident cap so the migrator stages drains for real; the corpus alone
// is a few kilobytes and would never drain, so without this the folder
// has nothing to fold. Values stay tiny on purpose: separated values
// spill to the value log at write time, which is not a drain. The
// batches pipeline raw RESP to keep round trips off the clock.
func ballast(t *testing.T, rc *conformance.Conn, round int) {
	t.Helper()
	const keys, batch = 2000, 500
	_ = rc.C.SetDeadline(time.Now().Add(30 * time.Second))
	for base := 0; base < keys; base += batch {
		var b strings.Builder
		for i := base; i < base+batch; i++ {
			key := "ballast:" + strconv.Itoa(round) + ":" + strconv.Itoa(i)
			fmt.Fprintf(&b, "*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$3\r\nbal\r\n", len(key), key)
		}
		if _, err := rc.C.Write([]byte(b.String())); err != nil {
			t.Fatal(err)
		}
		for range batch {
			line, err := rc.R.ReadString('\n')
			if err != nil {
				t.Fatal(err)
			}
			if line != "+OK\r\n" {
				t.Fatalf("ballast reply %q", line)
			}
		}
	}
}

// forceFold kicks the folder until a new segment publishes and the
// pipeline goes idle, so the next corpus step reads over a bucket state
// the fold just rewrote. Drains arrive at the folder asynchronously, so
// the kick repeats inside the poll.
func forceFold(t *testing.T, b *Booted) {
	t.Helper()
	prev := b.Folder.Stats().SegmentsPut
	pollFor(t, "a fold to publish", func() bool {
		b.Folder.Flush()
		fs := b.Folder.Stats()
		return fs.SegmentsPut > prev && fs.SegmentsPut == fs.SegmentsCut && fs.Published == fs.SegmentsPut
	})
}

// TestConformanceFoldStorm replays the full corpus with a forced fold
// every few steps. The corpus's own expected replies are the identity
// oracle: it interleaves writes and reads across every type, so if a
// fold changed user-visible order or content, some later step would
// answer differently.
func TestConformanceFoldStorm(t *testing.T) {
	bucket := sim.New(sim.Config{})
	b, srv, rc, nc := bootConfServer(t, bucket, 1)
	defer func() {
		nc.Close()
		srv.Close()
		_ = b.Close()
	}()
	const every = 25
	for i, s := range conformance.Hot {
		if msg := conformance.CheckDurable(s, doStep(t, rc, s.Cmd)); msg != "" {
			t.Fatalf("step %d %s", i, msg)
		}
		if (i+1)%every == 0 {
			ballast(t, rc, i)
			forceFold(t, b)
		}
	}
	if fs := b.Folder.Stats(); fs.SegmentsPut == 0 || fs.Records == 0 || fs.BuildErrs != 0 || fs.WalkErrs != 0 {
		t.Fatalf("the storm never folded cleanly: %+v", fs)
	}
}

// fingerprint reads every key the corpus left behind through the serve
// path and renders it canonically: every distinct argument token is
// probed with TYPE, junk tokens answer none, and typed keys get their
// band's deterministic read. Set members and hash pairs sort, so the
// comparison survives iteration-order differences across a reboot.
func fingerprint(t *testing.T, rc *conformance.Conn, steps []conformance.Step) map[string]string {
	t.Helper()
	tokens := map[string]bool{}
	for _, s := range steps {
		for _, a := range s.Cmd[1:] {
			tokens[a] = true
		}
	}
	names := make([]string, 0, len(tokens))
	for tok := range tokens {
		names = append(names, tok)
	}
	sort.Strings(names)
	fp := map[string]string{}
	for _, key := range names {
		typ := doStep(t, rc, []string{"TYPE", key})
		switch typ {
		case "none":
		case "string":
			fp[key] = "string " + doStep(t, rc, []string{"GET", key})
		case "list":
			fp[key] = "list " + doStep(t, rc, []string{"LRANGE", key, "0", "-1"})
		case "set":
			members := strings.Split(strings.Trim(doStep(t, rc, []string{"SMEMBERS", key}), "[]"), " ")
			sort.Strings(members)
			fp[key] = "set [" + strings.Join(members, " ") + "]"
		case "zset":
			fp[key] = "zset " + doStep(t, rc, []string{"ZRANGE", key, "0", "-1", "WITHSCORES"})
		case "hash":
			flat := strings.Split(strings.Trim(doStep(t, rc, []string{"HGETALL", key}), "[]"), " ")
			pairs := make([]string, 0, len(flat)/2)
			for i := 0; i+1 < len(flat); i += 2 {
				pairs = append(pairs, flat[i]+"="+flat[i+1])
			}
			sort.Strings(pairs)
			fp[key] = "hash [" + strings.Join(pairs, " ") + "]"
		case "stream":
			fp[key] = "stream " + doStep(t, rc, []string{"XRANGE", key, "-", "+"})
		default:
			fp[key] = typ + " (unfingerprinted)"
		}
	}
	return fp
}

// TestConformanceRestartFingerprint replays the corpus minus its wipe
// tail, fingerprints the surviving keyspace, reboots from the bucket,
// and demands the identical fingerprint. Replay rebuilds the hot state
// and the rebuilt keymap answers folded string placements through the
// cold read path where the hot tier does not hold them.
func TestConformanceRestartFingerprint(t *testing.T) {
	bucket := sim.New(sim.Config{})
	b1, srv1, rc1, nc1 := bootConfServer(t, bucket, 1)
	steps := conformance.Hot[:len(conformance.Hot)-conformance.WipeTail]
	for i, s := range steps {
		if msg := conformance.CheckDurable(s, doStep(t, rc1, s.Cmd)); msg != "" {
			t.Fatalf("step %d %s", i, msg)
		}
	}
	ballast(t, rc1, 0)
	forceFold(t, b1)
	if fs := b1.Folder.Stats(); fs.Records == 0 {
		t.Fatalf("nothing folded before the reboot: %+v", fs)
	}
	before := fingerprint(t, rc1, steps)
	if len(before) == 0 {
		t.Fatal("the corpus left no fingerprintable keys")
	}
	commitAndStop(t, b1, srv1, nc1)

	b2, srv2, rc2, nc2 := bootConfServer(t, bucket, 2)
	defer func() { commitAndStop(t, b2, srv2, nc2) }()
	after := fingerprint(t, rc2, steps)
	for key, want := range before {
		if got, ok := after[key]; !ok {
			t.Errorf("key %q lost across the reboot (was %s)", key, want)
		} else if got != want {
			t.Errorf("key %q diverged: before %s, after %s", key, want, got)
		}
	}
	for key, got := range after {
		if _, ok := before[key]; !ok {
			t.Errorf("key %q appeared from nowhere after the reboot: %s", key, got)
		}
	}
}
