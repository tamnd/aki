package dispatch

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"strconv"

	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/f3srv/resp"
)

// The cluster and replication stub surface (spec 2064/f3/17 section 16). aki f3
// is a single standalone node: it runs no cluster bus and speaks no replication
// protocol. The stub policy is to answer truthfully as a healthy standalone
// instance, never pretend to be a cluster, and never accept an operation that
// implies state the node does not have. A client's cluster-aware driver probes
// these on connect (redis-py, lettuce, and go-redis all call CLUSTER INFO or
// CLUSTER SLOTS to decide their topology), so the honest empty answers here keep
// those drivers on their standalone path instead of hunting for shards.

// nodeID is this run's stable 40-hex-character node id, the value CLUSTER MYID
// and the CLUSTER NODES self line report. Redis mints one per process run and
// keeps it stable for the run's life; aki does the same, generating it once at
// startup from the system CSPRNG. It is identity, never a secret, so a random
// 20-byte value rendered as hex matches the redis shape exactly.
var nodeID = mintNodeID()

func mintNodeID() string {
	var b [20]byte
	if _, err := rand.Read(b[:]); err != nil {
		// The CSPRNG does not fail on any platform aki targets; if it somehow
		// did, a fixed id is still a valid stable answer for the run.
		return "0000000000000000000000000000000000000000"
	}
	return hex.EncodeToString(b[:])
}

// clusterInfoBlock is the CLUSTER INFO body for a node with cluster support off:
// the assignment counters are all zero, the single known node is this one, and
// the state reads ok because a standalone node is never in a degraded cluster.
// cluster_enabled:0 is the field every cluster-aware driver keys on.
const clusterInfoBlock = "cluster_enabled:0\r\n" +
	"cluster_state:ok\r\n" +
	"cluster_slots_assigned:0\r\n" +
	"cluster_slots_ok:0\r\n" +
	"cluster_slots_pfail:0\r\n" +
	"cluster_slots_fail:0\r\n" +
	"cluster_known_nodes:1\r\n" +
	"cluster_size:0\r\n" +
	"cluster_current_epoch:0\r\n" +
	"cluster_my_epoch:0\r\n" +
	"cluster_stats_messages_sent:0\r\n" +
	"cluster_stats_messages_received:0\r\n" +
	"total_cluster_links_buffer_limit_exceeded:0\r\n"

// clusterCmd answers the CLUSTER family. The read subcommands report the
// standalone topology truthfully; every mutating or bus subcommand (MEET,
// ADDSLOTS, SETSLOT, REPLICATE, FORGET, and the rest) gets the same
// cluster-disabled error redis gives when cluster support is off, because none
// of them can mean anything on a node with no cluster bus.
func clusterCmd(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	switch upperVerb(args[0]) {
	case "INFO":
		r.Bulk([]byte(clusterInfoBlock))
	case "MYID":
		r.Bulk([]byte(nodeID))
	case "SLOTS", "SHARDS", "LINKS":
		// No slots are assigned and no cluster links exist on a standalone node,
		// so each of these is the empty array, exactly as redis answers with
		// cluster support off.
		r.Raw(resp.AppendArrayHeader(cx.Aux[:0], 0))
	case "NODES":
		// The self line only: this node, master, connected, holding no slots.
		// The address is empty because no cluster bus port is announced, which
		// is the shape a standalone redis prints here.
		line := nodeID + " :0@0 myself,master - 0 0 0 connected\n"
		r.Bulk([]byte(line))
	case "COUNTKEYSINSLOT":
		// No slots hold keys on a standalone node.
		r.Int(0)
	case "RESET":
		// Nothing to reset: a standalone node has no cluster state. redis acks.
		r.Status("OK")
	default:
		r.Err("ERR This instance has cluster support disabled")
	}
}

// replicaofCmd answers REPLICAOF and its SLAVEOF alias. Promotion tooling issues
// REPLICAOF NO ONE to make a node a primary, and on a node that is already a
// standalone primary that is a no-op that must succeed. Any host/port form asks
// the node to start replicating, which f3 cannot do, so it is refused with a
// clear error rather than silently accepted.
func replicaofCmd(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	if bytes.EqualFold(args[0], []byte("NO")) && bytes.EqualFold(args[1], []byte("ONE")) {
		r.Status("OK")
		return
	}
	r.Err("ERR REPLICAOF is not supported: aki f3 is a standalone server and does not replicate")
}

// waitaofCmd answers WAITAOF numlocal numreplicas timeout. It reports how many
// local fsyncs and replica acknowledgements the preceding writes reached. f3 in
// this build runs with the doc 07 WAL disabled by default and has no replicas,
// so the honest answer is [0, 0]: zero local AOF fsyncs, zero replica acks. The
// three arguments are parsed so a non-integer is refused the way redis refuses
// it. When the WAL lands as an on-by-config path this row reports [1, 0] after
// the local group fsync; that refinement is deferred with the persistence arc.
func waitaofCmd(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	for i := 0; i < 3; i++ {
		if _, err := strconv.ParseInt(string(args[i]), 10, 64); err != nil {
			r.Err("ERR value is not an integer or out of range")
			return
		}
	}
	out := resp.AppendArrayHeader(cx.Aux[:0], 2)
	out = resp.AppendInt(out, 0)
	out = resp.AppendInt(out, 0)
	cx.Aux = out
	r.Raw(out)
}

// replUnsupportedCmd answers the replication-protocol verbs PSYNC, SYNC, and
// REPLCONF. These are spoken only between a primary and its replicas during the
// replication handshake; a normal client never sends them, and f3 speaks no
// replication protocol, so it refuses them plainly. Refusing rather than leaving
// them as unknown commands gives a replica that mistakenly dialed a standalone
// node a clear reason instead of a generic unknown-command reply.
func replUnsupportedCmd(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	r.Err("ERR aki f3 does not support replication")
}
