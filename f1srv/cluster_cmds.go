package f1srv

// This file holds the cluster and replication stance. f1srv is a single standalone node: it runs
// with cluster support disabled and keeps no replicas, so none of these commands change any state.
// They exist so a client library that probes them on startup sees the same replies Redis 8.8 gives
// a standalone master, rather than an unknown-command error. Where Redis 8.8 and Valkey 9.1 differ
// the choice follows Redis 8.8, aki's compatibility target: the one split here is CLUSTER KEYSLOT,
// which Valkey computes even with clustering off while Redis reports the feature disabled, so aki
// reports it disabled like every other cluster subcommand.

// clusterSubcommands is the set of CLUSTER subcommand names Redis 8.8 recognizes. On a
// cluster-disabled instance a recognized subcommand answers "support disabled" while an
// unrecognized one answers the unknown-subcommand error, so aki has to tell the two apart. The
// names are lowercase; the lookup folds the caller's bytes to match. Redis also runs a
// per-subcommand argument-count check before the disabled gate (so CLUSTER INFO with an extra
// argument reports wrong-args, not disabled), but that arity table describes a feature aki never
// executes, so aki does not reproduce it: a recognized subcommand always reports disabled here.
var clusterSubcommands = map[string]struct{}{
	"addslots":              {},
	"addslotsrange":         {},
	"bumpepoch":             {},
	"count-failure-reports": {},
	"countkeysinslot":       {},
	"delslots":              {},
	"delslotsrange":         {},
	"failover":              {},
	"flushslots":            {},
	"forget":                {},
	"getkeysinslot":         {},
	"info":                  {},
	"keyslot":               {},
	"links":                 {},
	"meet":                  {},
	"myid":                  {},
	"nodes":                 {},
	"replicas":              {},
	"replicate":             {},
	"reset":                 {},
	"set-config-epoch":      {},
	"setslot":               {},
	"shards":                {},
	"slaves":                {},
	"slots":                 {},
	"help":                  {},
}

// cmdCluster answers the CLUSTER command for a cluster-disabled standalone node. A recognized
// subcommand (including HELP, which Redis does not special-case here) reports that clustering is
// off; an unrecognized one gets the unknown-subcommand error a client would see from Redis; and a
// bare CLUSTER with no subcommand is the wrong-args error.
func (c *connState) cmdCluster(argv [][]byte) {
	if len(argv) < 2 {
		c.writeErr("ERR wrong number of arguments for 'cluster' command")
		return
	}
	sub := string(argv[1])
	if _, ok := clusterSubcommands[foldASCII(sub)]; ok {
		c.writeErr("ERR This instance has cluster support disabled")
		return
	}
	c.writeErr("ERR unknown subcommand '" + sub + "'. Try CLUSTER HELP.")
}

// cmdReplicaof answers REPLICAOF and its SLAVEOF alias. aki keeps no replication link, so it
// accepts both "NO ONE" (stay a master) and a "host port" pair (nominally start replicating) with
// the same OK Redis returns immediately, without actually connecting to any primary. Fewer than
// two arguments is the wrong-args error, named for whichever alias the client used.
func (c *connState) cmdReplicaof(argv [][]byte, name string) {
	if len(argv) != 3 {
		c.writeErr("ERR wrong number of arguments for '" + name + "' command")
		return
	}
	c.writeSimple("OK")
}

// cmdFailover answers FAILOVER for a standalone master. FAILOVER ABORT cancels a coordinated
// failover, and with none in progress that is the "no failover in progress" error. Any other form
// asks this node to hand its role to a replica, which a node with no replicas cannot do, so it
// reports that the command is not valid here, matching what Redis returns for a standalone master.
func (c *connState) cmdFailover(argv [][]byte) {
	if len(argv) == 2 && eqFold(argv[1], "ABORT") {
		c.writeErr("ERR No failover in progress.")
		return
	}
	c.writeErr("ERR FAILOVER is not valid when server is a replica.")
}

// foldASCII lowercases an ASCII string for the CLUSTER subcommand lookup. The subcommand names are
// ASCII, so a byte-wise fold is enough and avoids pulling in strings.ToLower for the hot dispatch
// path.
func foldASCII(s string) string {
	needsFold := false
	for i := 0; i < len(s); i++ {
		if s[i] >= 'A' && s[i] <= 'Z' {
			needsFold = true
			break
		}
	}
	if !needsFold {
		return s
	}
	b := []byte(s)
	for i := range b {
		if b[i] >= 'A' && b[i] <= 'Z' {
			b[i] += 'a' - 'A'
		}
	}
	return string(b)
}
