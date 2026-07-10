package shard

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// The owner-to-loop wake edge, driven the way the reactor loop drives it: one
// goroutine plays the event loop (dispatch, drain, ParkWriter, block on a
// stand-in for epoll_wait), the shard owners deliver wakes through the
// SetWriterNotify seam instead of the waker channel. A lost wake on this edge
// hangs the connection invisibly, so every test here runs under a watchdog
// deadline: a hang fails as a timeout, not as a stuck CI job.

// notifyLoop is the reactor-loop stand-in: the notify target is a one-slot
// channel standing in for the eventfd (a redundant delivery is dropped, like
// a second eventfd write folding into the counter), and service blocks on it
// like the loop blocks in epoll_wait.
type notifyLoop struct {
	ch    chan struct{}
	wakes atomic.Uint64
}

func newNotifyLoop() *notifyLoop { return &notifyLoop{ch: make(chan struct{}, 1)} }

func (nl *notifyLoop) notify() {
	nl.wakes.Add(1)
	select {
	case nl.ch <- struct{}{}:
	default:
	}
}

// wait blocks until a notify lands or the deadline passes; false is the
// starvation verdict.
func (nl *notifyLoop) wait(d time.Duration) bool {
	select {
	case <-nl.ch:
		return true
	case <-time.After(d):
		return false
	}
}

// TestNotifyWakeEdge is the starvation watchdog: many rounds of dispatch,
// drain what is there, park, block on the notify. The park's re-check and the
// owner's publish-then-load are the only things keeping this loop alive; a
// lost wake anywhere shows up as the deadline firing with replies still owed.
// P1-shaped on purpose (one command per round), because a lone reply is the
// wake edge with nothing else in flight to hide behind.
func TestNotifyWakeEdge(t *testing.T) {
	const rounds = 20000

	rt := testRuntime(4)
	rt.Start()
	defer rt.Stop()

	nl := newNotifyLoop()
	c := rt.NewConn()
	c.SetWriterNotify(nl.notify)
	defer c.Close()

	var got [][]byte
	emit := func(rep []byte) { got = append(got, append([]byte(nil), rep...)) }

	for i := 0; i < rounds; i++ {
		got = got[:0]
		k := fmt.Sprintf("k%03d", i%251)
		if err := c.Do(opSet, true, args(k, "v")); err != nil {
			t.Fatal(err)
		}
		c.Flush()
		for {
			c.DrainReplies(emit)
			if !c.Owes() {
				break
			}
			if c.ParkWriter() {
				if !nl.wait(10 * time.Second) {
					t.Fatalf("round %d: reply owed but no notify arrived; the wake edge starved", i)
				}
			}
		}
		if len(got) != 1 || string(got[0]) != "+OK\r\n" {
			t.Fatalf("round %d: replies %q, want one +OK", i, got)
		}
	}
	if nl.wakes.Load() == 0 {
		t.Fatal("notify never fired; the seam is not wired")
	}
}

// TestNotifyWakeEdgePipelined runs the same watchdog with multi-shard bursts,
// so wakes race in from several owners at once and the redundant-delivery
// folding (the eventfd counter shape) is exercised alongside the park
// re-check.
func TestNotifyWakeEdgePipelined(t *testing.T) {
	const rounds = 2000
	const burst = 32

	rt := testRuntime(4)
	rt.Start()
	defer rt.Stop()

	nl := newNotifyLoop()
	c := rt.NewConn()
	c.SetWriterNotify(nl.notify)
	defer c.Close()

	n := 0
	emit := func([]byte) { n++ }

	for i := 0; i < rounds; i++ {
		n = 0
		for j := 0; j < burst; j++ {
			if err := c.Do(opEcho, false, args(fmt.Sprintf("e%d-%d", i, j))); err != nil {
				t.Fatal(err)
			}
		}
		c.Flush()
		for {
			c.DrainReplies(emit)
			if !c.Owes() {
				break
			}
			if c.ParkWriter() {
				if !nl.wait(10 * time.Second) {
					t.Fatalf("round %d: %d of %d replies, then the wake edge starved", i, n, burst)
				}
			}
		}
		if n != burst {
			t.Fatalf("round %d: %d replies, want %d", i, n, burst)
		}
	}
}
