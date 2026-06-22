package networking

import (
	"testing"
	"time"

	"github.com/tamnd/aki/resp"
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

// TestServerMaxBulkLenAccessor checks that New seeds the bulk cap from Config,
// the setter changes it live, and a zero or negative value resets to the
// default, which is what CONFIG SET proto-max-bulk-len relies on.
func TestServerMaxBulkLenAccessor(t *testing.T) {
	s := New(Config{MaxBulkLen: 1024}, nil)
	if got := s.MaxBulkLen(); got != 1024 {
		t.Fatalf("MaxBulkLen = %d want 1024", got)
	}

	s.SetMaxBulkLen(64)
	if got := s.MaxBulkLen(); got != 64 {
		t.Fatalf("MaxBulkLen after set = %d want 64", got)
	}

	// Zero and negative reset to the default.
	s.SetMaxBulkLen(0)
	if got := s.MaxBulkLen(); got != resp.DefaultMaxBulkLen {
		t.Fatalf("MaxBulkLen after set 0 = %d want %d", got, resp.DefaultMaxBulkLen)
	}
	s.SetMaxBulkLen(-1)
	if got := s.MaxBulkLen(); got != resp.DefaultMaxBulkLen {
		t.Fatalf("MaxBulkLen after set -1 = %d want %d", got, resp.DefaultMaxBulkLen)
	}

	// The zero Config selects the default.
	z := New(Config{}, nil)
	if got := z.MaxBulkLen(); got != resp.DefaultMaxBulkLen {
		t.Fatalf("zero config MaxBulkLen = %d want %d", got, resp.DefaultMaxBulkLen)
	}
}
