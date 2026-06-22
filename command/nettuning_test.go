package command

import (
	"bufio"
	"net"
	"testing"
	"time"

	"github.com/tamnd/aki/networking"
)

// startWithServer brings up a dispatcher and a real server the test keeps a
// handle on, so it can watch the network knobs change. It mirrors start but
// returns the server too.
func startWithServer(t *testing.T) (*bufio.Reader, net.Conn, *networking.Server) {
	t.Helper()
	d := New(Config{})
	ncfg := networking.Config{Addr: "127.0.0.1:0"}
	srv := networking.New(ncfg, d)
	d.SetServer(srv)
	d.ApplyNetworkConfig()
	go func() { _ = srv.ListenAndServe(ncfg) }()
	t.Cleanup(func() { _ = srv.Close() })

	deadline := time.Now().Add(2 * time.Second)
	for srv.Addr() == nil {
		if time.Now().After(deadline) {
			t.Fatal("server did not bind")
		}
		time.Sleep(time.Millisecond)
	}

	conn, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	return bufio.NewReader(conn), conn, srv
}

// TestNetworkConfigStartupAndLive checks the startup wiring seeds the server from
// the config defaults and that CONFIG SET timeout and CONFIG SET tcp-keepalive
// push new values to the running server.
func TestNetworkConfigStartupAndLive(t *testing.T) {
	r, c, srv := startWithServer(t)

	// Startup applied the defaults: timeout 0 (off), tcp-keepalive 300s.
	if got := srv.IdleTimeout(); got != 0 {
		t.Fatalf("startup IdleTimeout = %v want 0", got)
	}
	if got := srv.TCPKeepAlive(); got != 300*time.Second {
		t.Fatalf("startup TCPKeepAlive = %v want 300s", got)
	}

	if got := sendLine(t, r, c, "CONFIG SET timeout 7"); got != "+OK" {
		t.Fatalf("CONFIG SET timeout = %q", got)
	}
	if got := srv.IdleTimeout(); got != 7*time.Second {
		t.Fatalf("IdleTimeout after set = %v want 7s", got)
	}

	if got := sendLine(t, r, c, "CONFIG SET tcp-keepalive 0"); got != "+OK" {
		t.Fatalf("CONFIG SET tcp-keepalive = %q", got)
	}
	if got := srv.TCPKeepAlive(); got != 0 {
		t.Fatalf("TCPKeepAlive after set = %v want 0", got)
	}
}
