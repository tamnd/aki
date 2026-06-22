package command

import (
	"net"
	"testing"
)

// TestInfoPubsubClients checks pubsub_clients in INFO clients counts a connection
// that holds a subscription. The subscriber sits on its own connection because a
// subscribed RESP2 client cannot run INFO.
func TestInfoPubsubClients(t *testing.T) {
	r, c, host, port := startDataAddr(t)
	addr := net.JoinHostPort(host, port)

	if got := infoField(t, r, c, "clients", "pubsub_clients"); got != "0" {
		t.Fatalf("pubsub_clients before subscribe = %q want 0", got)
	}

	sr, sc := dial(t, addr)
	sendArgs(t, sr, sc, "SUBSCRIBE", "ch1")

	if got := infoField(t, r, c, "clients", "pubsub_clients"); got != "1" {
		t.Fatalf("pubsub_clients after subscribe = %q want 1", got)
	}
}

// TestInfoWatchingClients checks watching_clients and total_watched_keys reflect
// the WATCH state of live connections.
func TestInfoWatchingClients(t *testing.T) {
	r, c := startData(t)

	if got := infoField(t, r, c, "clients", "watching_clients"); got != "0" {
		t.Fatalf("watching_clients before WATCH = %q want 0", got)
	}
	if got := infoField(t, r, c, "clients", "total_watched_keys"); got != "0" {
		t.Fatalf("total_watched_keys before WATCH = %q want 0", got)
	}

	if got := sendLine(t, r, c, "WATCH k1 k2"); got != "+OK" {
		t.Fatalf("WATCH = %q", got)
	}

	if got := infoField(t, r, c, "clients", "watching_clients"); got != "1" {
		t.Fatalf("watching_clients after WATCH = %q want 1", got)
	}
	if got := infoField(t, r, c, "clients", "total_watched_keys"); got != "2" {
		t.Fatalf("total_watched_keys after WATCH = %q want 2", got)
	}

	// UNWATCH clears the count again.
	if got := sendLine(t, r, c, "UNWATCH"); got != "+OK" {
		t.Fatalf("UNWATCH = %q", got)
	}
	if got := infoField(t, r, c, "clients", "watching_clients"); got != "0" {
		t.Fatalf("watching_clients after UNWATCH = %q want 0", got)
	}
}
