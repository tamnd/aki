package command

// MODULE is the loadable-module management surface. aki is a single static binary
// with no dynamic module system, so there is nothing to load: MODULE LIST always
// reports an empty set and the load and unload subcommands report that the action
// is unavailable. Tools that probe MODULE LIST to enumerate extensions (exporters,
// dashboards) get the well-formed empty answer Redis gives a module-free server
// rather than an unknown-command error.

func moduleCommands() []*CmdDesc {
	module := &CmdDesc{
		Name: "module", Group: GroupServer, Since: "4.0.0",
		Arity: -2, Flags: FlagAdmin | FlagNoScript,
		Handler: handleModuleHelp,
		SubCmds: []*CmdDesc{
			{Name: "list", SubName: "module|list", Group: GroupServer, Since: "4.0.0",
				Arity: 2, Flags: FlagAdmin | FlagNoScript, Handler: handleModuleList},
			{Name: "load", SubName: "module|load", Group: GroupServer, Since: "4.0.0",
				Arity: -3, Flags: FlagAdmin | FlagNoScript, Handler: handleModuleLoad},
			{Name: "loadex", SubName: "module|loadex", Group: GroupServer, Since: "7.0.0",
				Arity: -3, Flags: FlagAdmin | FlagNoScript, Handler: handleModuleLoad},
			{Name: "unload", SubName: "module|unload", Group: GroupServer, Since: "4.0.0",
				Arity: 3, Flags: FlagAdmin | FlagNoScript, Handler: handleModuleUnload},
			{Name: "help", SubName: "module|help", Group: GroupServer, Since: "5.0.0",
				Arity: 2, Flags: FlagLoading | FlagStale, Handler: handleModuleHelp},
		},
	}
	return []*CmdDesc{module}
}

// handleModuleList reports the loaded modules: always the empty array, since aki
// loads none.
func handleModuleList(ctx *Ctx) {
	ctx.enc().WriteArrayLen(0)
}

// handleModuleLoad rejects MODULE LOAD and MODULE LOADEX: aki has no module host.
func handleModuleLoad(ctx *Ctx) {
	ctx.enc().WriteError("ERR Error loading the extension. Please check the server logs.")
}

// handleModuleUnload rejects MODULE UNLOAD: there is never a module to unload.
func handleModuleUnload(ctx *Ctx) {
	ctx.enc().WriteError("ERR Error unloading module: no such module with that name")
}

func handleModuleHelp(ctx *Ctx) {
	writeHelp(ctx, []string{
		"MODULE <subcommand> [<arg> ...]. Subcommands are:",
		"LIST",
		"    Return a list of loaded modules.",
		"LOAD <path> [<arg> ...]",
		"    Load a module library from <path>, passing to it any optional arguments.",
		"LOADEX <path> [[CONFIG NAME VALUE] [CONFIG NAME VALUE]] [ARGS ...]",
		"    Load a module library from <path>, while passing it module configurations and arguments.",
		"UNLOAD <name>",
		"    Unload a module.",
		"HELP",
		"    Print this help.",
	})
}
