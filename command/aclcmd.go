package command

import (
	"strconv"
	"strings"
	"time"

	"github.com/tamnd/aki/resp"
)

// This file implements the ACL command family and the dispatch-time enforcement
// entry point (spec 2064 doc 19 sections 12 and 13). The user model and the
// permission checks live in acl.go; this file wires them to the wire protocol.

// aclLogEntry is one entry in the ACL denial log. Consecutive identical denials
// coalesce into a single entry with a rising count.
type aclLogEntry struct {
	count      int64
	reason     string // auth, cmd, key, channel
	context    string // toplevel, multi, scripting
	object     string // the command, key or channel denied
	username   string
	clientInfo string
	addr       string
	at         time.Time
	entryID    int64
}

// aclCommands registers the ACL container command and its subcommands.
func aclCommands() []*CmdDesc {
	return []*CmdDesc{
		{
			Name: "acl", Group: GroupServer, Since: "6.0.0",
			Arity: -2, Flags: FlagAdmin | FlagNoScript | FlagLoading | FlagStale,
			Handler: handleACL,
			SubCmds: []*CmdDesc{
				{Name: "setuser", SubName: "acl|setuser", Group: GroupServer, Since: "6.0.0",
					Arity: -3, Flags: FlagAdmin | FlagNoScript | FlagLoading | FlagStale, Handler: handleACLSetUser},
				{Name: "getuser", SubName: "acl|getuser", Group: GroupServer, Since: "6.0.0",
					Arity: 3, Flags: FlagAdmin | FlagNoScript | FlagLoading | FlagStale, Handler: handleACLGetUser},
				{Name: "deluser", SubName: "acl|deluser", Group: GroupServer, Since: "6.0.0",
					Arity: -3, Flags: FlagAdmin | FlagNoScript | FlagLoading | FlagStale, Handler: handleACLDelUser},
				{Name: "list", SubName: "acl|list", Group: GroupServer, Since: "6.0.0",
					Arity: 2, Flags: FlagAdmin | FlagNoScript | FlagLoading | FlagStale, Handler: handleACLList},
				{Name: "users", SubName: "acl|users", Group: GroupServer, Since: "6.0.0",
					Arity: 2, Flags: FlagAdmin | FlagNoScript | FlagLoading | FlagStale, Handler: handleACLUsers},
				{Name: "whoami", SubName: "acl|whoami", Group: GroupServer, Since: "6.0.0",
					Arity: 2, Flags: FlagLoading | FlagStale, Handler: handleACLWhoami},
				{Name: "cat", SubName: "acl|cat", Group: GroupServer, Since: "6.0.0",
					Arity: -2, Flags: FlagLoading | FlagStale, Handler: handleACLCat},
				{Name: "genpass", SubName: "acl|genpass", Group: GroupServer, Since: "6.0.0",
					Arity: -2, Flags: FlagLoading | FlagStale, Handler: handleACLGenPass},
				{Name: "dryrun", SubName: "acl|dryrun", Group: GroupServer, Since: "7.0.0",
					Arity: -4, Flags: FlagAdmin | FlagNoScript | FlagLoading | FlagStale, Handler: handleACLDryRun},
				{Name: "log", SubName: "acl|log", Group: GroupServer, Since: "6.0.0",
					Arity: -2, Flags: FlagAdmin | FlagNoScript | FlagLoading | FlagStale, Handler: handleACLLog},
				{Name: "load", SubName: "acl|load", Group: GroupServer, Since: "6.0.0",
					Arity: 2, Flags: FlagAdmin | FlagNoScript | FlagLoading | FlagStale, Handler: handleACLLoad},
				{Name: "save", SubName: "acl|save", Group: GroupServer, Since: "6.0.0",
					Arity: 2, Flags: FlagAdmin | FlagNoScript | FlagLoading | FlagStale, Handler: handleACLSave},
				{Name: "help", SubName: "acl|help", Group: GroupServer, Since: "6.0.0",
					Arity: 2, Flags: FlagLoading | FlagStale, Handler: handleACLHelp},
			},
		},
	}
}

// handleACL with no usable subcommand is an error; the table routes the known
// subcommands to their own handlers.
func handleACL(ctx *Ctx) {
	ctx.enc().WriteError(unknownSubcmdError(ctx.Argv).Error())
}

// handleACLSetUser creates or updates a user from a list of rule tokens.
func handleACLSetUser(ctx *Ctx) {
	name := string(ctx.Argv[2])
	tokens := make([]string, 0, len(ctx.Argv)-3)
	for _, a := range ctx.Argv[3:] {
		tokens = append(tokens, string(a))
	}
	a := ctx.d.acl
	a.mu.Lock()
	existing := a.users[name]
	var u *aclUser
	if existing != nil {
		clone := *existing
		clone.cmdRules = append([]aclCmdRule(nil), existing.cmdRules...)
		clone.keyRules = append([]aclKeyRule(nil), existing.keyRules...)
		clone.chanRules = append([]aclChanRule(nil), existing.chanRules...)
		clone.selectors = append([]aclSelector(nil), existing.selectors...)
		clone.passwords = append([]string(nil), existing.passwords...)
		u = &clone
	} else {
		u = &aclUser{name: name, created: time.Now()}
	}
	if err := applyACLRules(u, tokens); err != nil {
		a.mu.Unlock()
		ctx.enc().WriteError("ERR " + err.Error())
		return
	}
	a.users[name] = u
	a.mu.Unlock()
	ctx.d.persistACL()
	ctx.Conn.WriteRaw(resp.ReplyOK)
}

// handleACLGetUser returns the full definition of a user as a map, or null if the
// user does not exist.
func handleACLGetUser(ctx *Ctx) {
	name := string(ctx.Argv[2])
	u := ctx.d.acl.get(name)
	enc := ctx.enc()
	if u == nil {
		enc.WriteNull()
		return
	}
	enc.WriteMapLen(6)

	enc.WriteBulkStringStr("flags")
	flags := []string{"off"}
	if u.on {
		flags[0] = "on"
	}
	if u.allKeys() {
		flags = append(flags, "allkeys")
	}
	if u.allChannels() {
		flags = append(flags, "allchannels")
	}
	if u.nopass {
		flags = append(flags, "nopass")
	}
	enc.WriteArrayLen(len(flags))
	for _, f := range flags {
		enc.WriteBulkStringStr(f)
	}

	enc.WriteBulkStringStr("passwords")
	enc.WriteArrayLen(len(u.passwords))
	for _, h := range u.passwords {
		enc.WriteBulkStringStr(h)
	}

	enc.WriteBulkStringStr("commands")
	enc.WriteBulkStringStr(describeCommands(u))

	enc.WriteBulkStringStr("keys")
	ks := keyTokens(u)
	enc.WriteArrayLen(len(ks))
	for _, t := range ks {
		enc.WriteBulkStringStr(t)
	}

	enc.WriteBulkStringStr("channels")
	cs := channelTokens(u)
	enc.WriteArrayLen(len(cs))
	for _, t := range cs {
		enc.WriteBulkStringStr(t)
	}

	enc.WriteBulkStringStr("selectors")
	enc.WriteArrayLen(len(u.selectors))
	for i := range u.selectors {
		enc.WriteMapLen(2)
		enc.WriteBulkStringStr("commands")
		sel := &u.selectors[i]
		if len(sel.cmdRules) == 0 {
			enc.WriteBulkStringStr("-@all")
		} else {
			var b strings.Builder
			for j, r := range sel.cmdRules {
				if j > 0 {
					b.WriteByte(' ')
				}
				b.WriteString(r.token())
			}
			enc.WriteBulkStringStr(b.String())
		}
		enc.WriteBulkStringStr("keys")
		var kb strings.Builder
		for j, r := range sel.keyRules {
			if j > 0 {
				kb.WriteByte(' ')
			}
			kb.WriteString(r.token())
		}
		enc.WriteBulkStringStr(kb.String())
	}
}

// handleACLDelUser deletes one or more users and returns the count deleted. The
// default user cannot be removed.
func handleACLDelUser(ctx *Ctx) {
	for _, a := range ctx.Argv[2:] {
		if string(a) == "default" {
			ctx.enc().WriteError("ERR The 'default' user cannot be removed")
			return
		}
	}
	n := 0
	for _, a := range ctx.Argv[2:] {
		if ctx.d.acl.del(string(a)) {
			n++
		}
	}
	if n > 0 {
		ctx.d.persistACL()
	}
	ctx.enc().WriteInteger(int64(n))
}

// handleACLList returns one ACL line per user, sorted by name.
func handleACLList(ctx *Ctx) {
	a := ctx.d.acl
	names := a.usernames()
	enc := ctx.enc()
	enc.WriteArrayLen(len(names))
	for _, n := range names {
		enc.WriteBulkStringStr(aclLine(a.get(n)))
	}
}

// handleACLUsers returns every username.
func handleACLUsers(ctx *Ctx) {
	names := ctx.d.acl.usernames()
	enc := ctx.enc()
	enc.WriteArrayLen(len(names))
	for _, n := range names {
		enc.WriteBulkStringStr(n)
	}
}

// handleACLWhoami returns the username this connection authenticated as.
func handleACLWhoami(ctx *Ctx) {
	name := ctx.sess.username
	if name == "" {
		name = "default"
	}
	ctx.enc().WriteBulkStringStr(name)
}

// handleACLCat lists category names, or the commands in one category.
func handleACLCat(ctx *Ctx) {
	enc := ctx.enc()
	if len(ctx.Argv) == 2 {
		enc.WriteArrayLen(len(aclCategoryNames))
		for _, c := range aclCategoryNames {
			enc.WriteBulkStringStr(c)
		}
		return
	}
	if len(ctx.Argv) != 3 {
		enc.WriteError("ERR Unknown ACL cat subcommand or wrong number of arguments")
		return
	}
	cat := normalizeCategory(string(ctx.Argv[2]))
	known := false
	for _, c := range aclCategoryNames {
		if "@"+c == cat {
			known = true
			break
		}
	}
	if !known {
		enc.WriteError("ERR Unknown ACL cat '" + string(ctx.Argv[2]) + "'")
		return
	}
	var names []string
	for _, cmd := range ctx.d.table.commands() {
		for _, c := range commandCategories(cmd) {
			if c == cat {
				names = append(names, cmd.Name)
				break
			}
		}
	}
	enc.WriteArrayLen(len(names))
	for _, n := range names {
		enc.WriteBulkStringStr(n)
	}
}

// handleACLGenPass returns a random password, 256 bits by default.
func handleACLGenPass(ctx *Ctx) {
	bits := 256
	if len(ctx.Argv) == 3 {
		n, err := strconv.Atoi(string(ctx.Argv[2]))
		if err != nil {
			ctx.enc().WriteError("ERR Invalid number of bits. It must be between 0 and 4096.")
			return
		}
		bits = n
	} else if len(ctx.Argv) > 3 {
		ctx.enc().WriteError(arityError(&CmdDesc{Name: "acl|genpass"}))
		return
	}
	pw, ok := genPass(bits)
	if !ok {
		ctx.enc().WriteError("ERR Invalid number of bits. It must be between 0 and 4096.")
		return
	}
	ctx.enc().WriteBulkStringStr(pw)
}

// handleACLDryRun reports whether a user could run a command without running it.
func handleACLDryRun(ctx *Ctx) {
	name := string(ctx.Argv[2])
	u := ctx.d.acl.get(name)
	if u == nil {
		ctx.enc().WriteError("ERR User '" + name + "' not found")
		return
	}
	cmdName := strings.ToLower(string(ctx.Argv[3]))
	target := ctx.Argv[3:]
	cmd, err := ctx.d.table.lookup(cmdName, target)
	if err != nil {
		ctx.enc().WriteError(err.Error())
		return
	}
	if msg := aclCheckUser(ctx.d, u, cmd, cmdName, target); msg != "" {
		ctx.enc().WriteError(msg)
		return
	}
	ctx.Conn.WriteRaw(resp.ReplyOK)
}

// handleACLLog returns the denial log, or resets it.
func handleACLLog(ctx *Ctx) {
	a := ctx.d.acl
	if len(ctx.Argv) == 3 && strings.EqualFold(string(ctx.Argv[2]), "RESET") {
		a.mu.Lock()
		a.log = nil
		a.mu.Unlock()
		ctx.Conn.WriteRaw(resp.ReplyOK)
		return
	}
	limit := -1
	if len(ctx.Argv) == 3 {
		n, err := strconv.Atoi(string(ctx.Argv[2]))
		if err != nil || n < 0 {
			ctx.enc().WriteError("ERR Got invalid ACL LOG count")
			return
		}
		limit = n
	} else if len(ctx.Argv) > 3 {
		ctx.enc().WriteError(arityError(&CmdDesc{Name: "acl|log"}))
		return
	}

	a.mu.RLock()
	entries := a.log
	if limit >= 0 && limit < len(entries) {
		entries = entries[:limit]
	}
	snapshot := append([]*aclLogEntry(nil), entries...)
	a.mu.RUnlock()

	enc := ctx.enc()
	enc.WriteArrayLen(len(snapshot))
	now := time.Now()
	for _, e := range snapshot {
		enc.WriteMapLen(8)
		enc.WriteBulkStringStr("count")
		enc.WriteInteger(e.count)
		enc.WriteBulkStringStr("reason")
		enc.WriteBulkStringStr(e.reason)
		enc.WriteBulkStringStr("context")
		enc.WriteBulkStringStr(e.context)
		enc.WriteBulkStringStr("object")
		enc.WriteBulkStringStr(e.object)
		enc.WriteBulkStringStr("username")
		enc.WriteBulkStringStr(e.username)
		enc.WriteBulkStringStr("age-seconds")
		enc.WriteBulkStringStr(strconv.FormatFloat(now.Sub(e.at).Seconds(), 'f', 3, 64))
		enc.WriteBulkStringStr("client-info")
		enc.WriteBulkStringStr(e.clientInfo)
		enc.WriteBulkStringStr("entry-id")
		enc.WriteInteger(e.entryID)
	}
}

// handleACLLoad reloads users from the aclfile.
func handleACLLoad(ctx *Ctx) {
	a := ctx.d.acl
	if a.aclFile == "" {
		ctx.enc().WriteError("ERR This Redis instance is not configured to use an ACL file. You may want to specify users via the ACL SETUSER command and then issue a CONFIG REWRITE (assuming you have a Redis configuration file set) in order to store users in the Redis configuration.")
		return
	}
	if err := a.loadFile(); err != nil {
		ctx.enc().WriteError("ERR " + err.Error())
		return
	}
	ctx.Conn.WriteRaw(resp.ReplyOK)
}

// handleACLSave writes the current users to the aclfile.
func handleACLSave(ctx *Ctx) {
	a := ctx.d.acl
	if a.aclFile == "" {
		ctx.enc().WriteError("ERR This Redis instance is not configured to use an ACL file. You may want to specify users via the ACL SETUSER command and then issue a CONFIG REWRITE (assuming you have a Redis configuration file set) in order to store users in the Redis configuration.")
		return
	}
	if err := a.saveFile(); err != nil {
		ctx.enc().WriteError("ERR " + err.Error())
		return
	}
	ctx.Conn.WriteRaw(resp.ReplyOK)
}

// handleACLHelp returns a short help array.
func handleACLHelp(ctx *Ctx) {
	lines := []string{
		"ACL <subcommand> [<arg> ...]. Subcommands are:",
		"CAT [<category>]",
		"DELUSER <username> [<username> ...]",
		"DRYRUN <username> <command> [<arg> ...]",
		"GETUSER <username>",
		"GENPASS [<bits>]",
		"LIST",
		"LOAD",
		"LOG [<count> | RESET]",
		"SAVE",
		"SETUSER <username> <attribute> [<attribute> ...]",
		"USERS",
		"WHOAMI",
		"HELP",
	}
	enc := ctx.enc()
	enc.WriteArrayLen(len(lines))
	for _, l := range lines {
		enc.WriteStatus(l)
	}
}
