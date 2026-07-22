package drivers

import (
	"path/filepath"
	"testing"
)

// TestPersistDurable exercises the persistence family against a real durable .aki
// server: SAVE forces the group-writer fsync barrier after a write and returns OK
// without hanging (the barrier round-trips through the writer goroutine), BGSAVE
// and BGREWRITEAOF give their redis start acks, and LASTSAVE reports a positive
// clock that a SAVE never moves backwards.
func TestPersistDurable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "persist.aki")
	srv, nc, br := startDurableServer(t, path)
	defer func() { nc.Close(); srv.Close() }()

	if got := sendCmd(t, br, nc, "SET", "k", "v"); got != "OK" {
		t.Fatalf("SET = %v, want OK", got)
	}

	before, ok := sendCmd(t, br, nc, "LASTSAVE").(int64)
	if !ok || before <= 0 {
		t.Fatalf("LASTSAVE = %v, want a positive integer", before)
	}
	if got := sendCmd(t, br, nc, "SAVE"); got != "OK" {
		t.Fatalf("SAVE = %v, want OK", got)
	}
	after, ok := sendCmd(t, br, nc, "LASTSAVE").(int64)
	if !ok || after < before {
		t.Fatalf("LASTSAVE after SAVE = %v, want >= %v", after, before)
	}

	if got := sendCmd(t, br, nc, "BGSAVE"); got != "Background saving started" {
		t.Fatalf("BGSAVE = %v, want the background-saving ack", got)
	}
	if got := sendCmd(t, br, nc, "BGSAVE", "SCHEDULE"); got != "Background saving started" {
		t.Fatalf("BGSAVE SCHEDULE = %v, want the background-saving ack", got)
	}
	if got := sendCmd(t, br, nc, "BGREWRITEAOF"); got != "Background append only file rewriting started" {
		t.Fatalf("BGREWRITEAOF = %v, want the rewrite ack", got)
	}

	// DEBUG RELOAD forces the same barrier and the data reads back unchanged, since
	// every write is already in the durable log.
	if got := sendCmd(t, br, nc, "DEBUG", "RELOAD"); got != "OK" {
		t.Fatalf("DEBUG RELOAD = %v, want OK", got)
	}
	if got := sendCmd(t, br, nc, "GET", "k"); got != "v" {
		t.Fatalf("GET after DEBUG RELOAD = %v, want v", got)
	}
}

// TestPersistNonDurable checks the same verbs on the default scratch server, where
// there is no shared .aki file: the barrier is a no-op and every verb still answers
// its normal reply rather than erroring, so a client that saves against a
// non-durable server is not surprised.
func TestPersistNonDurable(t *testing.T) {
	_, nc, br := startServer(t)

	if got := sendCmd(t, br, nc, "SAVE"); got != "OK" {
		t.Fatalf("SAVE = %v, want OK", got)
	}
	if got := sendCmd(t, br, nc, "BGSAVE"); got != "Background saving started" {
		t.Fatalf("BGSAVE = %v, want the background-saving ack", got)
	}
	if got, ok := sendCmd(t, br, nc, "LASTSAVE").(int64); !ok || got <= 0 {
		t.Fatalf("LASTSAVE = %v, want a positive integer", got)
	}
	if got := sendCmd(t, br, nc, "DEBUG", "RELOAD"); got != "OK" {
		t.Fatalf("DEBUG RELOAD = %v, want OK", got)
	}
}

// TestPersistArity checks the arity guards: SAVE/BGREWRITEAOF/LASTSAVE take no
// argument, and a stray one is the wrong-arity error.
func TestPersistArity(t *testing.T) {
	_, nc, br := startServer(t)

	if _, ok := sendCmd(t, br, nc, "SAVE", "now").(errorReply); !ok {
		t.Fatalf("SAVE now did not error")
	}
	if _, ok := sendCmd(t, br, nc, "LASTSAVE", "x").(errorReply); !ok {
		t.Fatalf("LASTSAVE x did not error")
	}
}
