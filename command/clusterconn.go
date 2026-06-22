package command

import "github.com/tamnd/aki/resp"

// This file implements the cluster connection commands ASKING, READONLY and
// READWRITE from doc 18 section 24. They control per-connection cluster routing
// state. aki serves all 16384 slots from a single node and never emits MOVED or
// ASK redirects, so none of these change where a command runs. They are still
// recognized and reply OK, and the flags are tracked, so a cluster-aware client
// that sends them before retrying a command or before reading from a replica
// works against aki the same way it works against a real cluster node.

// clusterConnCommands returns ASKING, READONLY and READWRITE.
func clusterConnCommands() []*CmdDesc {
	return []*CmdDesc{
		{Name: "asking", Group: GroupCluster, Since: "3.0.0",
			Arity: 1, Flags: FlagFast, Handler: handleAsking},
		{Name: "readonly", Group: GroupCluster, Since: "3.0.0",
			Arity: 1, Flags: FlagFast | FlagLoading | FlagStale, Handler: handleReadOnly},
		{Name: "readwrite", Group: GroupCluster, Since: "3.0.0",
			Arity: 1, Flags: FlagFast | FlagLoading | FlagStale, Handler: handleReadWrite},
	}
}

// handleAsking sets the one-shot asking flag so the command right after it may
// touch a slot in the importing state. The dispatch loop clears the flag once
// that next command has run.
func handleAsking(ctx *Ctx) {
	ctx.sess.asking = true
	ctx.Conn.WriteRaw(resp.ReplyOK)
}

// handleReadOnly puts the connection in read-only mode, which in cluster mode
// lets it serve reads from a replica.
func handleReadOnly(ctx *Ctx) {
	ctx.sess.clusterReadonly = true
	ctx.Conn.WriteRaw(resp.ReplyOK)
}

// handleReadWrite returns the connection to read-write mode, the default.
func handleReadWrite(ctx *Ctx) {
	ctx.sess.clusterReadonly = false
	ctx.Conn.WriteRaw(resp.ReplyOK)
}
