package networking

import (
	"testing"
	"time"
)

// TestServerTimeoutAccessors checks that New seeds the idle timeout and keepalive
// from Config and that the setters change them live, which is what CONFIG SET
// timeout and CONFIG SET tcp-keepalive rely on.
func TestServerTimeoutAccessors(t *testing.T) {
	s := New(Config{IdleTimeout: 30 * time.Second, TCPKeepAlive: 300 * time.Second}, nil)

	if got := s.IdleTimeout(); got != 30*time.Second {
		t.Fatalf("IdleTimeout = %v want 30s", got)
	}
	if got := s.TCPKeepAlive(); got != 300*time.Second {
		t.Fatalf("TCPKeepAlive = %v want 300s", got)
	}

	s.SetIdleTimeout(5 * time.Second)
	if got := s.IdleTimeout(); got != 5*time.Second {
		t.Fatalf("IdleTimeout after set = %v want 5s", got)
	}

	s.SetTCPKeepAlive(0)
	if got := s.TCPKeepAlive(); got != 0 {
		t.Fatalf("TCPKeepAlive after set = %v want 0", got)
	}

	// The zero Config leaves both off.
	z := New(Config{}, nil)
	if z.IdleTimeout() != 0 || z.TCPKeepAlive() != 0 {
		t.Fatalf("zero config timeouts = %v / %v want 0 / 0", z.IdleTimeout(), z.TCPKeepAlive())
	}
}
