package command

// This file registers the replication command surface: REPLICAOF and its alias
// SLAVEOF, the REPLCONF handshake command, and PSYNC/SYNC on the master side
// (spec 2064 doc 18 sections 2, 3 and 10). The mechanics live in replication.go.

func replicationCommands() []*CmdDesc {
	return []*CmdDesc{
		{Name: "replicaof", Group: GroupServer, Since: "5.0.0",
			Arity: 3, Flags: FlagAdmin | FlagNoScript | FlagStale | FlagNoMulti,
			Handler: func(ctx *Ctx) { ctx.d.handleReplicaOf(ctx) }},
		{Name: "slaveof", Group: GroupServer, Since: "1.0.0",
			Arity: 3, Flags: FlagAdmin | FlagNoScript | FlagStale | FlagNoMulti,
			Handler: func(ctx *Ctx) { ctx.d.handleReplicaOf(ctx) }},
		{Name: "replconf", Group: GroupServer, Since: "3.0.0",
			Arity: -1, Flags: FlagAdmin | FlagNoScript | FlagLoading | FlagStale,
			Handler: func(ctx *Ctx) { ctx.d.handleReplconf(ctx) }},
		{Name: "psync", Group: GroupServer, Since: "2.8.0",
			Arity: -3, Flags: FlagAdmin | FlagNoScript | FlagNoMulti,
			Handler: func(ctx *Ctx) { ctx.d.handlePsync(ctx, true) }},
		{Name: "sync", Group: GroupServer, Since: "1.0.0",
			Arity: 1, Flags: FlagAdmin | FlagNoScript | FlagNoMulti,
			Handler: func(ctx *Ctx) { ctx.d.handlePsync(ctx, false) }},
		{Name: "wait", Group: GroupConnection, Since: "3.0.0",
			Arity: 3, Flags: FlagNoScript,
			Handler: func(ctx *Ctx) { ctx.d.handleWait(ctx) }},
		{Name: "waitaof", Group: GroupConnection, Since: "7.2.0",
			Arity: 4, Flags: FlagNoScript,
			Handler: func(ctx *Ctx) { ctx.d.handleWaitAOF(ctx) }},
	}
}
