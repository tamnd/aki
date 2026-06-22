package command

import "strings"

// This file implements SHUTDOWN from doc 20 section 9.6. aki shuts down through
// the server's main loop: the handler optionally writes a final RDB snapshot, then
// signals the loop, which drains clients, closes the data file, and exits 0. The
// command does not reply on success because the connection closes first, the same
// as Redis.

// shutdownCommands registers SHUTDOWN.
func shutdownCommands() []*CmdDesc {
	return []*CmdDesc{
		{Name: "shutdown", Group: GroupServer, Since: "1.0.0",
			Arity: -1, Flags: FlagAdmin | FlagLoading | FlagNoScript | FlagAllowBusy | FlagStale,
			Handler: handleShutdown},
	}
}

// SetShutdown installs the callback the handler fires to begin a graceful
// shutdown. The server command wires it to its main-loop signal.
func (d *Dispatcher) SetShutdown(fn func()) { d.shutdownFn = fn }

// handleShutdown parses the flags, runs the save policy, and signals the server to
// stop. SHUTDOWN ABORT cancels a pending shutdown, but aki shuts down at once so
// there is never one to cancel.
func handleShutdown(ctx *Ctx) {
	nosave, save, abort := false, false, false
	for _, a := range ctx.Argv[1:] {
		switch strings.ToUpper(string(a)) {
		case "NOSAVE":
			nosave = true
		case "SAVE":
			save = true
		case "NOW", "FORCE":
			// aki drains and exits in one pass, so these are accepted no-ops.
		case "ABORT":
			abort = true
		default:
			ctx.enc().WriteError("ERR syntax error")
			return
		}
	}
	if nosave && save {
		ctx.enc().WriteError("ERR syntax error")
		return
	}
	if abort {
		ctx.enc().WriteError("ERR No shutdown in progress")
		return
	}
	if ctx.d.shutdownFn == nil {
		ctx.enc().WriteError("ERR SHUTDOWN is not available in this context")
		return
	}

	// Decide whether to snapshot. NOSAVE always skips, SAVE always writes, and the
	// default writes only when save points are configured, the same as Redis.
	doSave := save || (!nosave && ctx.d.hasSavePoints())
	if doSave && ctx.d.engine != nil {
		if err := ctx.d.writeRDB(); err != nil {
			ctx.d.logWarning("SHUTDOWN save failed", lf("err", err.Error()))
			ctx.enc().WriteError("ERR Errors trying to SHUTDOWN. Check logs.")
			return
		}
	}

	ctx.d.logNotice("Server shutting down on SHUTDOWN command")
	ctx.d.shutdownFn()
	// No reply: the server closes the connection as it stops.
}

// hasSavePoints reports whether the save directive lists any save points. An empty
// value or the explicit "" form means none, so a default SHUTDOWN skips the
// snapshot.
func (d *Dispatcher) hasSavePoints() bool {
	v := strings.TrimSpace(d.confValue("save", ""))
	return v != "" && v != `""`
}
