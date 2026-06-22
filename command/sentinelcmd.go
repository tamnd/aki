package command

// This file registers the SENTINEL command (spec 2064 doc 18 section 29). The
// subcommands are dispatched by hand in sentinel.go rather than through the
// container framework, because their names carry hyphens, their argument counts
// vary, and the spec fixes the exact error strings. SENTINEL is left
// unauthenticated-admin so a discovery client can read the master address, and it
// answers while loading or stale so failover-aware clients can query at any time.

func sentinelCommands() []*CmdDesc {
	return []*CmdDesc{
		{Name: "sentinel", Group: GroupServer, Since: "2.8.4",
			Arity: -2, Flags: FlagStale | FlagLoading, Handler: handleSentinel},
	}
}
