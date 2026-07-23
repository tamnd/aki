package drivers

// The write half of the O1c async cold read row (spec 2064/obs1 doc 05
// section 5): an incarnation writes past its resident cap so records
// stage-drain and fold into segments, and a restart brings every key back
// correct with deletes staying dead. Replay covers the full committed
// stream (the fold cursor over-claims until it is made sound, see
// recover.go), so the folded copies shadow behind hot replays here; the
// takeover test next door is where GETs actually serve off the bucket.

import (
	"bufio"
	"context"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/tamnd/aki/engine/obs1/shard"
	"github.com/tamnd/aki/engine/obs1/sim"
)

// bootColdServer is bootServer with the cold pipeline armed: a resident
// cap small enough that a working set stages drains, the local stage
// directories, and live flush and fold cadences so segments cut and
// publish without explicit kicks.
func bootColdServer(t *testing.T, bucket *sim.Sim, inc uint32) (*Booted, *Server, net.Conn, *bufio.Reader) {
	t.Helper()
	var booted *Booted
	dir := t.TempDir()
	srv, err := Listen(Options{
		Addr: "127.0.0.1:0", Shards: 2, ArenaBytes: 16 << 20, SegBytes: 1 << 18,
		ConnShape: testConnShape(), NetDriver: testNetDriver(),
		VlogDir: dir, ColdDir: dir, ResidentCapBytes: 64 << 10,
		Boot: func(rt *shard.Runtime) error {
			b, err := BootDurability(context.Background(), BootConfig{
				Store: bucket, Prefix: "p", Node: 0xE9, Incarnation: inc,
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
	return booted, srv, nc, bufio.NewReader(nc)
}

func TestColdServeAcrossRestart(t *testing.T) {
	bucket := sim.New(sim.Config{})
	const keys, batch = 6000, 500

	// Incarnation 1: write past the resident cap so drains stage and the
	// age cadences fold record-bearing segments onto the bucket.
	b1, srv1, nc1, r1 := bootColdServer(t, bucket, 1)
	for base := 0; base < keys; base += batch {
		for i := base; i < base+batch; i++ {
			send(t, nc1, "SET", "c:"+strconv.Itoa(i), "v-"+strconv.Itoa(i))
		}
		for range batch {
			expect(t, r1, "+OK\r\n")
		}
	}
	pollFor(t, "a record-bearing publish", func() bool {
		if b1.Pub.Stats().Published == 0 {
			return false
		}
		for _, led := range b1.Folder.Ledger() {
			if led.NRecords > 0 {
				return true
			}
		}
		return false
	})
	// A delete after folding has begun: whether c:0's tombstone folds
	// before the stop or rides the WAL tail into the next boot, the key
	// must stay dead across the restart.
	send(t, nc1, "DEL", "c:0")
	expect(t, r1, ":1\r\n")
	commitAndStop(t, b1, srv1, nc1)

	// Incarnation 2: the rebuild carries the folded records, the replay
	// carries the committed stream, and every key answers correctly.
	b2, srv2, nc2, r2 := bootColdServer(t, bucket, 2)
	if b2.Resident.Records == 0 || b2.Resident.Segments == 0 {
		t.Fatalf("resident rebuild %+v, want folded segments", b2.Resident)
	}
	for base := 0; base < keys; base += batch {
		for i := base; i < base+batch; i++ {
			send(t, nc2, "GET", "c:"+strconv.Itoa(i))
		}
		for i := base; i < base+batch; i++ {
			if i == 0 {
				expect(t, r2, "$-1\r\n")
				continue
			}
			v := "v-" + strconv.Itoa(i)
			expect(t, r2, "$"+strconv.Itoa(len(v))+"\r\n"+v+"\r\n")
		}
	}

	// A key the object tier never held is a definitive miss: null with
	// zero reader traffic.
	before := b2.Cold.Stats().Fetches
	send(t, nc2, "GET", "never:written")
	expect(t, r2, "$-1\r\n")
	if after := b2.Cold.Stats().Fetches; after != before {
		t.Fatalf("definitive miss reached the reader: %d -> %d fetches", before, after)
	}

	st := b2.Cold.Stats()
	if st.Errs != 0 || st.Unresolved != 0 || st.Misses != 0 {
		t.Fatalf("reader stats %+v, want a clean sweep", st)
	}
	t.Logf("restart sweep: replay %d frames, rebuild %+v, cold %+v", b2.Replay.Frames, b2.Resident, st)
	commitAndStop(t, b2, srv2, nc2)
}
