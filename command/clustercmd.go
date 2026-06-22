package command

// This file registers the CLUSTER container command and its subcommands (spec
// 2064 doc 18 section 20). The mechanics live in cluster.go. CLUSTER KEYSLOT,
// MYID, INFO, NODES, SLOTS, SHARDS, COUNTKEYSINSLOT, GETKEYSINSLOT and LINKS work
// in any mode; the slot-management and peer subcommands need cluster-enabled yes.

func clusterCommands() []*CmdDesc {
	cluster := &CmdDesc{
		Name: "cluster", Group: GroupServer, Since: "3.0.0",
		Arity: -2, Flags: FlagAdmin | FlagStale,
		Handler: handleClusterHelp,
		SubCmds: []*CmdDesc{
			{Name: "myid", SubName: "cluster|myid", Group: GroupServer, Since: "3.0.0",
				Arity: 2, Flags: FlagStale, Handler: handleClusterMyID},
			{Name: "info", SubName: "cluster|info", Group: GroupServer, Since: "3.0.0",
				Arity: 2, Flags: FlagStale, Handler: handleClusterInfo},
			{Name: "nodes", SubName: "cluster|nodes", Group: GroupServer, Since: "3.0.0",
				Arity: 2, Flags: FlagStale, Handler: handleClusterNodes},
			{Name: "slots", SubName: "cluster|slots", Group: GroupServer, Since: "3.0.0",
				Arity: 2, Flags: FlagStale, Handler: handleClusterSlots},
			{Name: "shards", SubName: "cluster|shards", Group: GroupServer, Since: "7.0.0",
				Arity: 2, Flags: FlagStale, Handler: handleClusterShards},
			{Name: "keyslot", SubName: "cluster|keyslot", Group: GroupServer, Since: "3.0.0",
				Arity: 3, Flags: FlagStale, Handler: handleClusterKeyslot},
			{Name: "countkeysinslot", SubName: "cluster|countkeysinslot", Group: GroupServer, Since: "3.0.0",
				Arity: 3, Flags: FlagStale, Handler: handleClusterCountKeysInSlot},
			{Name: "getkeysinslot", SubName: "cluster|getkeysinslot", Group: GroupServer, Since: "3.0.0",
				Arity: 4, Flags: FlagStale, Handler: handleClusterGetKeysInSlot},
			{Name: "links", SubName: "cluster|links", Group: GroupServer, Since: "7.0.0",
				Arity: 2, Flags: FlagStale, Handler: handleClusterLinks},
			{Name: "replicas", SubName: "cluster|replicas", Group: GroupServer, Since: "5.0.0",
				Arity: 3, Flags: FlagStale, Handler: handleClusterReplicas},
			{Name: "slaves", SubName: "cluster|slaves", Group: GroupServer, Since: "3.0.0",
				Arity: 3, Flags: FlagStale | FlagAdmin, Handler: handleClusterReplicas},
			{Name: "addslots", SubName: "cluster|addslots", Group: GroupServer, Since: "3.0.0",
				Arity: -3, Flags: FlagAdmin | FlagStale, Handler: handleClusterAddSlots},
			{Name: "delslots", SubName: "cluster|delslots", Group: GroupServer, Since: "3.0.0",
				Arity: -3, Flags: FlagAdmin | FlagStale, Handler: handleClusterDelSlots},
			{Name: "addslotsrange", SubName: "cluster|addslotsrange", Group: GroupServer, Since: "7.0.0",
				Arity: -4, Flags: FlagAdmin | FlagStale, Handler: handleClusterAddSlotsRange},
			{Name: "delslotsrange", SubName: "cluster|delslotsrange", Group: GroupServer, Since: "7.0.0",
				Arity: -4, Flags: FlagAdmin | FlagStale, Handler: handleClusterDelSlotsRange},
			{Name: "setslot", SubName: "cluster|setslot", Group: GroupServer, Since: "3.0.0",
				Arity: -4, Flags: FlagAdmin | FlagStale, Handler: handleClusterSetSlot},
			{Name: "flushslots", SubName: "cluster|flushslots", Group: GroupServer, Since: "3.0.0",
				Arity: 2, Flags: FlagAdmin | FlagStale, Handler: handleClusterFlushSlots},
			{Name: "bumpepoch", SubName: "cluster|bumpepoch", Group: GroupServer, Since: "3.0.0",
				Arity: 2, Flags: FlagAdmin | FlagStale, Handler: handleClusterBumpEpoch},
			{Name: "set-config-epoch", SubName: "cluster|set-config-epoch", Group: GroupServer, Since: "3.0.0",
				Arity: 3, Flags: FlagAdmin | FlagStale, Handler: handleClusterSetConfigEpoch},
			{Name: "reset", SubName: "cluster|reset", Group: GroupServer, Since: "3.0.0",
				Arity: -2, Flags: FlagAdmin | FlagStale, Handler: handleClusterReset},
			{Name: "meet", SubName: "cluster|meet", Group: GroupServer, Since: "3.0.0",
				Arity: -4, Flags: FlagAdmin | FlagStale, Handler: handleClusterPeerOp},
			{Name: "forget", SubName: "cluster|forget", Group: GroupServer, Since: "3.0.0",
				Arity: 3, Flags: FlagAdmin | FlagStale, Handler: handleClusterPeerOp},
			{Name: "replicate", SubName: "cluster|replicate", Group: GroupServer, Since: "3.0.0",
				Arity: 3, Flags: FlagAdmin | FlagStale, Handler: handleClusterPeerOp},
			{Name: "failover", SubName: "cluster|failover", Group: GroupServer, Since: "3.0.0",
				Arity: -2, Flags: FlagAdmin | FlagStale, Handler: handleClusterPeerOp},
			{Name: "help", SubName: "cluster|help", Group: GroupServer, Since: "5.0.0",
				Arity: 2, Flags: FlagStale, Handler: handleClusterHelp},
		},
	}
	return []*CmdDesc{cluster}
}

// handleClusterHelp lists the CLUSTER subcommands, the reply a bare CLUSTER or
// CLUSTER HELP gets.
func handleClusterHelp(ctx *Ctx) {
	lines := []string{
		"CLUSTER <subcommand> [<arg> ...]. Subcommands are:",
		"INFO",
		"    Return information about the cluster.",
		"MYID",
		"    Return the node id.",
		"NODES",
		"    Return cluster configuration seen by node.",
		"SLOTS",
		"    Return information about slots range mappings.",
		"SHARDS",
		"    Return information about slot range mappings and the nodes serving them.",
		"KEYSLOT <key>",
		"    Return the hash slot for <key>.",
		"COUNTKEYSINSLOT <slot>",
		"    Return the number of keys in <slot>.",
		"GETKEYSINSLOT <slot> <count>",
		"    Return up to <count> keys in <slot>.",
		"ADDSLOTS <slot> [<slot> ...]",
		"    Assign slots to this node.",
		"DELSLOTS <slot> [<slot> ...]",
		"    Remove slots from this node.",
		"SETSLOT <slot> (IMPORTING <node>|MIGRATING <node>|STABLE|NODE <node>)",
		"    Set slot state during resharding.",
		"RESET [HARD|SOFT]",
		"    Reset the cluster state of this node.",
		"HELP",
		"    Print this help.",
	}
	enc := ctx.enc()
	enc.WriteArrayLen(len(lines))
	for _, l := range lines {
		enc.WriteStatus(l)
	}
}
