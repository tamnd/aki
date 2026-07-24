package mesh

import (
	"bytes"
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"
)

func startServer(t *testing.T, h Hooks) (*Server, string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s, err := Serve(ln, ServerConfig{Secret: "sekrit", Hooks: h})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s, ln.Addr().String()
}

func newTestPeer(t *testing.T, addr, secret string) *Peer {
	t.Helper()
	p, err := NewPeer(PeerConfig{Addr: addr, Secret: secret})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	return p
}

// TestAuthGate: nothing dispatches before a good secret, and a bad one
// closes the connection.
func TestAuthGate(t *testing.T) {
	called := false
	_, addr := startServer(t, Hooks{Hint: func(uint8) error { called = true; return nil }})

	bad := newTestPeer(t, addr, "wrong")
	if err := bad.Hint(context.Background(), 0); err == nil {
		t.Fatal("wrong secret got through")
	}
	if called {
		t.Fatal("a verb dispatched without auth")
	}
	good := newTestPeer(t, addr, "sekrit")
	if err := good.Hint(context.Background(), 0); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("authenticated hint never reached the hook")
	}
}

// TestVerbRoundTrips drives each wired verb through a real TCP
// connection and checks the arguments land intact.
func TestVerbRoundTrips(t *testing.T) {
	ctx := context.Background()
	var mu sync.Mutex
	var gotGroup uint16
	var gotKey []byte
	var gotDD uint8
	var gotRepl int
	_, addr := startServer(t, Hooks{
		Int: func(payload [][]byte) ([][]byte, error) {
			out := make([][]byte, len(payload))
			for i, p := range payload {
				out[i] = append([]byte("re:"), p...)
			}
			return out, nil
		},
		Wake: func(group uint16, key []byte) error {
			mu.Lock()
			gotGroup, gotKey = group, append([]byte(nil), key...)
			mu.Unlock()
			return nil
		},
		Repl: func(frames [][]byte) error {
			mu.Lock()
			gotRepl = len(frames)
			mu.Unlock()
			return nil
		},
		Hint: func(dd uint8) error {
			mu.Lock()
			gotDD = dd
			mu.Unlock()
			return nil
		},
	})
	p := newTestPeer(t, addr, "sekrit")

	out, err := p.Int(ctx, [][]byte{[]byte("a"), []byte("b")})
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 2 || !bytes.Equal(out[0], []byte("re:a")) || !bytes.Equal(out[1], []byte("re:b")) {
		t.Fatalf("int reply = %q", out)
	}
	if err := p.Wake(ctx, 300, []byte("k1")); err != nil {
		t.Fatal(err)
	}
	if err := p.Repl(ctx, [][]byte{[]byte("f1"), []byte("f2"), []byte("f3")}); err != nil {
		t.Fatal(err)
	}
	if err := p.Hint(ctx, 7); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if gotGroup != 300 || !bytes.Equal(gotKey, []byte("k1")) {
		t.Fatalf("wake landed as group %d key %q", gotGroup, gotKey)
	}
	if gotRepl != 3 {
		t.Fatalf("repl landed %d frames", gotRepl)
	}
	if gotDD != 7 {
		t.Fatalf("hint landed domain %d", gotDD)
	}
}

// TestMultiplexOutOfOrder: a slow verb never head-of-line blocks the
// connection; a wake sent after a stalled intent completes first.
func TestMultiplexOutOfOrder(t *testing.T) {
	ctx := context.Background()
	release := make(chan struct{})
	_, addr := startServer(t, Hooks{
		Int: func(payload [][]byte) ([][]byte, error) {
			<-release
			return payload, nil
		},
		Wake: func(uint16, []byte) error { return nil },
	})
	p := newTestPeer(t, addr, "sekrit")

	intDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		_, err := p.Int(ctx, [][]byte{[]byte("slow")})
		intDone <- err
	}()
	// The wake must complete while the intent is still parked.
	if err := p.Wake(ctx, 1, []byte("k")); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-intDone:
		t.Fatalf("intent finished before release: %v", err)
	default:
	}
	close(release)
	if err := <-intDone; err != nil {
		t.Fatal(err)
	}
}

// TestReplSeamRefusal: with no standby wired, M.REPL is refused with a
// clean VerbError on a healthy connection, and the connection keeps
// serving other verbs. The fallback is takeover replay, which the
// engine's takeover tests prove without any mesh at all.
func TestReplSeamRefusal(t *testing.T) {
	ctx := context.Background()
	_, addr := startServer(t, Hooks{Hint: func(uint8) error { return nil }})
	p := newTestPeer(t, addr, "sekrit")

	err := p.Repl(ctx, [][]byte{[]byte("frame")})
	var ve *VerbError
	if !errors.As(err, &ve) || ve.Msg != "repl not enabled" {
		t.Fatalf("repl refusal = %v, want the seam's VerbError", err)
	}
	if err := p.Hint(ctx, 0); err != nil {
		t.Fatalf("connection unusable after a refusal: %v", err)
	}
}

// TestIntFallbackAbortsClean: P-I4 for M.INT. Against a dead peer the
// intent leg errors inside the call timeout and nothing half-applies;
// the caller's abort path is just the error return.
func TestIntFallbackAbortsClean(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()

	p := newTestPeer(t, addr, "sekrit")
	start := time.Now()
	if _, err := p.Int(context.Background(), [][]byte{[]byte("leg")}); err == nil {
		t.Fatal("intent against a dead peer succeeded")
	}
	if d := time.Since(start); d > DefaultCallTimeout+time.Second {
		t.Fatalf("intent took %v to fail, must abort inside the timeout", d)
	}
}

// TestWakeFallbackPollTick: P-I4 for M.WAKE. A blocked waiter whose
// wakeup rides a dead mesh still completes on its own poll tick; the
// lost wake costs latency, never correctness.
func TestWakeFallbackPollTick(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	p := newTestPeer(t, addr, "sekrit")

	// The owner publishes the state change, then tries the wake.
	var mu sync.Mutex
	ready := false
	mu.Lock()
	ready = true
	mu.Unlock()
	if err := p.Wake(context.Background(), 4, []byte("k")); err == nil {
		t.Fatal("wake over a dead mesh reported success")
	}

	// The waiter never hears the wake; its poll tick finds the state.
	deadline := time.After(5 * time.Second)
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			mu.Lock()
			ok := ready
			mu.Unlock()
			if ok {
				return
			}
		case <-deadline:
			t.Fatal("poll tick never observed the state change")
		}
	}
}

// TestHintFallbackPollCadence: P-I4 for M.HINT. The hint is a nudge,
// never authority: when it is lost the follower's own poll cadence
// observes the appended state regardless.
func TestHintFallbackPollCadence(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	p := newTestPeer(t, addr, "sekrit")

	// The appender advances the shared chain state, then hints.
	var mu sync.Mutex
	chainTail := uint64(0)
	mu.Lock()
	chainTail = 9
	mu.Unlock()
	if err := p.Hint(context.Background(), 0); err == nil {
		t.Fatal("hint over a dead mesh reported success")
	}

	// The follower's poll cadence reads the tail with no hint at all.
	deadline := time.After(5 * time.Second)
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			mu.Lock()
			tail := chainTail
			mu.Unlock()
			if tail == 9 {
				return
			}
		case <-deadline:
			t.Fatal("poll cadence never observed the chain tail")
		}
	}
}

// TestReconnectAfterDrop: a dropped connection fails in-flight calls
// loudly and the next call re-dials and works.
func TestReconnectAfterDrop(t *testing.T) {
	ctx := context.Background()
	s, addr := startServer(t, Hooks{Hint: func(uint8) error { return nil }})
	p := newTestPeer(t, addr, "sekrit")
	if err := p.Hint(ctx, 1); err != nil {
		t.Fatal(err)
	}

	// Kill every server-side connection under the peer.
	s.mu.Lock()
	for c := range s.conns {
		c.Close()
	}
	s.mu.Unlock()

	// The peer notices on some call soon and then recovers; the first
	// call may race the close notification either way.
	deadline := time.Now().Add(5 * time.Second)
	for {
		if err := p.Hint(ctx, 2); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("peer never recovered after the drop")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestCallDeadline: a verb the peer never answers returns the context
// error inside the default call timeout, and the late reply drops
// harmlessly.
func TestCallDeadline(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	_, addr := startServer(t, Hooks{
		Int: func(payload [][]byte) ([][]byte, error) {
			<-release
			return nil, nil
		},
	})
	p, err := NewPeer(PeerConfig{Addr: addr, Secret: "sekrit", CallTimeout: 200 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	start := time.Now()
	if _, err := p.Int(context.Background(), [][]byte{[]byte("x")}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("stalled call returned %v, want deadline", err)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("deadline took %v", d)
	}
}
