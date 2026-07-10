package drivers

import (
	"bufio"
	"fmt"
	"math/rand"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestBoundaryFlushStress hammers the writer's boundary-flush deferral from
// the client side: many connections, rounds of random pipeline depth (1 to
// 32, so plenty of lone commands and plenty of deep bursts), random think
// time between rounds so writers and workers park and get woken constantly,
// and a fan-out mixed in because a fan's sub-commands publish a watermark the
// reader has not advanced past, the one edge where Owes must read overtaken
// as not owed. Each round's replies are read to the exact expected bytes
// under a deadline, so full ordered delivery is asserted and a deferred flush
// that never comes shows up as a read timeout, not a hang. Run under -race;
// the CI f3 race pass covers it.
func TestBoundaryFlushStress(t *testing.T) {
	srv, err := Listen(Options{Addr: "127.0.0.1:0", Shards: 4, ArenaBytes: 32 << 20})
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve()
	t.Cleanup(func() { srv.Close() })

	const conns = 12
	rounds := 300
	if testing.Short() {
		rounds = 60
	}

	var wg sync.WaitGroup
	for cid := 0; cid < conns; cid++ {
		wg.Add(1)
		go func(cid int) {
			defer wg.Done()
			nc, err := net.Dial("tcp", srv.Addr().String())
			if err != nil {
				t.Error(err)
				return
			}
			defer nc.Close()
			br := bufio.NewReader(nc)
			rng := rand.New(rand.NewSource(int64(cid)*104729 + 7))
			last := map[string]string{}

			for r := 0; r < rounds; r++ {
				depth := 1 + rng.Intn(32)
				var req, want strings.Builder
				for j := 0; j < depth; j++ {
					switch rng.Intn(4) {
					case 0: // lone-friendly keyless
						p := fmt.Sprintf("c%02d-r%04d-%02d", cid, r, j)
						fmt.Fprintf(&req, "*2\r\n$4\r\nECHO\r\n$%d\r\n%s\r\n", len(p), p)
						fmt.Fprintf(&want, "$%d\r\n%s\r\n", len(p), p)
					case 1: // keyed write across shards
						k := fmt.Sprintf("c%02d-k%02d", cid, rng.Intn(40))
						v := fmt.Sprintf("v%06d", r*32+j)
						fmt.Fprintf(&req, "*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(k), k, len(v), v)
						want.WriteString("+OK\r\n")
						last[k] = v
					case 2: // keyed read of this connection's own writes
						k := fmt.Sprintf("c%02d-k%02d", cid, rng.Intn(40))
						fmt.Fprintf(&req, "*2\r\n$3\r\nGET\r\n$%d\r\n%s\r\n", len(k), k)
						if v, ok := last[k]; ok {
							fmt.Fprintf(&want, "$%d\r\n%s\r\n", len(v), v)
						} else {
							want.WriteString("$-1\r\n")
						}
					case 3: // fan-out: sub-commands share one sequence
						k1 := fmt.Sprintf("c%02d-k%02d", cid, rng.Intn(40))
						k2 := fmt.Sprintf("c%02d-k%02d", cid, rng.Intn(40))
						fmt.Fprintf(&req, "*3\r\n$4\r\nMGET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(k1), k1, len(k2), k2)
						want.WriteString("*2\r\n")
						for _, k := range []string{k1, k2} {
							if v, ok := last[k]; ok {
								fmt.Fprintf(&want, "$%d\r\n%s\r\n", len(v), v)
							} else {
								want.WriteString("$-1\r\n")
							}
						}
					}
				}
				if _, err := nc.Write([]byte(req.String())); err != nil {
					t.Errorf("conn %d round %d write: %v", cid, r, err)
					return
				}
				wantB := []byte(want.String())
				got := make([]byte, len(wantB))
				_ = nc.SetReadDeadline(time.Now().Add(20 * time.Second))
				n := 0
				for n < len(got) {
					m, err := br.Read(got[n:])
					if err != nil {
						t.Errorf("conn %d round %d (depth %d) stalled after %d of %d reply bytes: %v",
							cid, r, depth, n, len(got), err)
						return
					}
					n += m
				}
				if string(got) != string(wantB) {
					t.Errorf("conn %d round %d replies wrong or out of order:\n got %q\nwant %q", cid, r, got, wantB)
					return
				}
				if rng.Intn(4) == 0 {
					time.Sleep(time.Duration(rng.Intn(200)) * time.Microsecond)
				}
			}
		}(cid)
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(120 * time.Second):
		t.Fatal("stress run hung; a deferred flush or a missed wake stalled a connection")
	}
}

// TestLoneCommandFlushesPromptly is the P1 latency guard on the boundary
// flush: a lone command's owed count hits zero the moment its reply emits, so
// the flush must happen right there, with no further input to force it out.
// The deadline is generous for CI noise; the failure mode being guarded
// against is not slowness but a reply parked in the writer buffer forever.
func TestLoneCommandFlushesPromptly(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "SET", "k", "v")
	expect(t, br, "+OK\r\n")

	worst := time.Duration(0)
	for i := 0; i < 200; i++ {
		start := time.Now()
		send(t, nc, "GET", "k")
		expect(t, br, "$1\r\nv\r\n")
		if el := time.Since(start); el > worst {
			worst = el
		}
	}
	if worst > 2*time.Second {
		t.Fatalf("lone command round trip took %v; the boundary flush is deferring past the pipeline boundary", worst)
	}
}
