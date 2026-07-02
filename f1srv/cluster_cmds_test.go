package f1srv

import "testing"

// CLUSTER on a cluster-disabled standalone reports the feature is off for every recognized
// subcommand, including the ones clients probe on startup. HELP is not special-cased here: Redis
// reports it disabled too.
func TestClusterDisabled(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	for _, sub := range []string{"INFO", "SLOTS", "SHARDS", "NODES", "MYID", "RESET", "HELP"} {
		cmd(t, rw, "CLUSTER", sub)
		expect(t, rw, "-ERR This instance has cluster support disabled")
	}
	// KEYSLOT is a recognized subcommand, so aki reports it disabled like the rest. Valkey would
	// compute the slot, but aki follows Redis 8.8 here.
	cmd(t, rw, "CLUSTER", "KEYSLOT", "foo")
	expect(t, rw, "-ERR This instance has cluster support disabled")
	// The fold is case-insensitive, so a lowercase subcommand lands on the same reply.
	cmd(t, rw, "CLUSTER", "info")
	expect(t, rw, "-ERR This instance has cluster support disabled")
}

// An unrecognized CLUSTER subcommand gets the unknown-subcommand error, and a bare CLUSTER with no
// subcommand is the wrong-args error.
func TestClusterUnknownAndArity(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "CLUSTER", "BADSUB")
	expect(t, rw, "-ERR unknown subcommand 'BADSUB'. Try CLUSTER HELP.")
	cmd(t, rw, "CLUSTER")
	expect(t, rw, "-ERR wrong number of arguments for 'cluster' command")
}

// REPLICAOF and its SLAVEOF alias accept "NO ONE" and a host/port pair with OK, and reject a call
// that does not carry exactly the two arguments, naming the alias the client used.
func TestReplicaof(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "REPLICAOF", "NO", "ONE")
	expect(t, rw, "+OK")
	cmd(t, rw, "REPLICAOF", "localhost", "6390")
	expect(t, rw, "+OK")
	cmd(t, rw, "REPLICAOF")
	expect(t, rw, "-ERR wrong number of arguments for 'replicaof' command")
	cmd(t, rw, "REPLICAOF", "x")
	expect(t, rw, "-ERR wrong number of arguments for 'replicaof' command")

	cmd(t, rw, "SLAVEOF", "NO", "ONE")
	expect(t, rw, "+OK")
	cmd(t, rw, "SLAVEOF", "localhost", "6390")
	expect(t, rw, "+OK")
	cmd(t, rw, "SLAVEOF")
	expect(t, rw, "-ERR wrong number of arguments for 'slaveof' command")
}

// FAILOVER ABORT with no failover running is the "no failover in progress" error, and any other
// form is not valid on a node with no replicas.
func TestFailover(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "FAILOVER", "ABORT")
	expect(t, rw, "-ERR No failover in progress.")
	cmd(t, rw, "FAILOVER")
	expect(t, rw, "-ERR FAILOVER is not valid when server is a replica.")
	cmd(t, rw, "FAILOVER", "FORCE")
	expect(t, rw, "-ERR FAILOVER is not valid when server is a replica.")
}
