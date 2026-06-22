package command

import (
	"strconv"
	"strings"

	"github.com/tamnd/aki/resp"
)

// This file implements the COMMAND introspection family (spec 2064 doc 07
// section 7): the bare COMMAND reply, COMMAND INFO, COMMAND LIST, COMMAND GETKEYS
// and COMMAND GETKEYSANDFLAGS. Clients use these for tab completion, validation,
// and cluster slot routing. COMMAND COUNT lives next to its sibling in
// connection.go. The descriptors in the table are the single source of truth, so
// the replies are built straight from CmdDesc.

// flagName maps each command flag bit to the Redis status string COMMAND INFO
// reports. The order here is the order the flags appear in the reply.
var flagOrder = []struct {
	bit  FlagSet
	name string
}{
	{FlagWrite, "write"},
	{FlagReadOnly, "readonly"},
	{FlagDenyOOM, "denyoom"},
	{FlagAdmin, "admin"},
	{FlagPubSub, "pubsub"},
	{FlagNoScript, "noscript"},
	{FlagBlocking, "blocking"},
	{FlagLoading, "loading"},
	{FlagStale, "stale"},
	{FlagNoAuth, "no_auth"},
	{FlagFast, "fast"},
	{FlagMovableKeys, "movablekeys"},
	{FlagNoMulti, "no_multi"},
	{FlagNoMandatoryKeys, "no_mandatory_keys"},
	{FlagAllowBusy, "allow_busy"},
}

// commandFlags returns the flag status strings for a command in reply order.
func commandFlags(cmd *CmdDesc) []string {
	var out []string
	for _, f := range flagOrder {
		if cmd.Flags.Has(f.bit) {
			out = append(out, f.name)
		}
	}
	return out
}

// groupCategory maps a command group to its ACL category. A few groups have no
// single category and return "".
var groupCategory = map[CmdGroup]string{
	GroupString:       "@string",
	GroupBitmap:       "@bitmap",
	GroupHyperLogLog:  "@hyperloglog",
	GroupGeo:          "@geo",
	GroupList:         "@list",
	GroupSet:          "@set",
	GroupSortedSet:    "@sortedset",
	GroupHash:         "@hash",
	GroupStream:       "@stream",
	GroupPubSub:       "@pubsub",
	GroupTransactions: "@transaction",
	GroupScripting:    "@scripting",
	GroupGeneric:      "@keyspace",
	GroupConnection:   "@connection",
}

// commandCategories derives the ACL categories for a command from its group and
// flags. aki does not carry a hand-maintained category list per command, so the
// set is built from what the descriptor already knows. The values match the
// categories Redis uses for the same group and flags.
func commandCategories(cmd *CmdDesc) []string {
	seen := map[string]bool{}
	var out []string
	add := func(cat string) {
		if cat == "" || seen[cat] {
			return
		}
		seen[cat] = true
		out = append(out, cat)
	}

	add(groupCategory[cmd.Group])
	if cmd.Flags.Has(FlagWrite) {
		add("@write")
	}
	if cmd.Flags.Has(FlagReadOnly) {
		add("@read")
	}
	if cmd.Flags.Has(FlagFast) {
		add("@fast")
	} else {
		add("@slow")
	}
	if cmd.Flags.Has(FlagAdmin) {
		add("@admin")
		add("@dangerous")
	}
	if cmd.Flags.Has(FlagPubSub) {
		add("@pubsub")
	}
	if cmd.Flags.Has(FlagBlocking) {
		add("@blocking")
	}
	return out
}

// writeCommandInfo writes the 10-element COMMAND INFO array for one command.
// Elements 7 to 10 (ACL categories, tips, key specs, subcommands) are always
// present; tips and key specs are empty because aki does not carry that metadata
// yet, and subcommands are filled for container commands.
func writeCommandInfo(enc *resp.Encoder, cmd *CmdDesc) {
	enc.WriteArrayLen(10)

	enc.WriteBulkStringStr(cmd.Name)
	enc.WriteInteger(int64(cmd.Arity))

	flags := commandFlags(cmd)
	enc.WriteArrayLen(len(flags))
	for _, f := range flags {
		enc.WriteStatus(f)
	}

	enc.WriteInteger(int64(cmd.FirstKey))
	enc.WriteInteger(int64(cmd.LastKey))
	enc.WriteInteger(int64(cmd.Step))

	cats := commandCategories(cmd)
	enc.WriteArrayLen(len(cats))
	for _, c := range cats {
		enc.WriteStatus(c)
	}

	enc.WriteArrayLen(0) // tips
	enc.WriteArrayLen(0) // key specs

	enc.WriteArrayLen(len(cmd.SubCmds))
	for _, sub := range cmd.SubCmds {
		writeCommandInfo(enc, sub)
	}
}

// commandLookupForInfo resolves a name as typed by COMMAND INFO. A bare name hits
// a top-level command; a "parent|sub" or "parent sub" form is not accepted by
// COMMAND INFO in Redis (only the container name is), so only the top level is
// resolved here, with the container's full subcommand list carried in element 10.
func (t *Table) commandLookupForInfo(name string) *CmdDesc {
	return t.get(strings.ToLower(name))
}

// handleCommandInfo returns the info array for each requested command, in order.
// With no names it returns info for every command, the same as bare COMMAND. An
// unknown name produces a null element in its position rather than an error.
func handleCommandInfo(ctx *Ctx) {
	enc := ctx.enc()
	if len(ctx.Argv) == 2 {
		cmds := ctx.d.table.commands()
		enc.WriteArrayLen(len(cmds))
		for _, c := range cmds {
			writeCommandInfo(enc, c)
		}
		return
	}
	names := ctx.Argv[2:]
	enc.WriteArrayLen(len(names))
	for _, n := range names {
		cmd := ctx.d.table.commandLookupForInfo(string(n))
		if cmd == nil {
			enc.WriteNullArray()
			continue
		}
		writeCommandInfo(enc, cmd)
	}
}

// handleCommandList returns command names. Without FILTERBY it lists every
// command. FILTERBY PATTERN keeps names matching a glob, FILTERBY ACLCAT keeps
// names in a category, and FILTERBY MODULE is always empty because aki has no
// modules.
func handleCommandList(ctx *Ctx) {
	enc := ctx.enc()
	cmds := ctx.d.table.commands()

	if len(ctx.Argv) == 2 {
		enc.WriteArrayLen(len(cmds))
		for _, c := range cmds {
			enc.WriteBulkStringStr(c.Name)
		}
		return
	}

	if len(ctx.Argv) < 4 || !strings.EqualFold(string(ctx.Argv[2]), "FILTERBY") {
		enc.WriteError("ERR syntax error")
		return
	}
	kind := strings.ToUpper(string(ctx.Argv[3]))
	switch kind {
	case "MODULE":
		if len(ctx.Argv) != 5 {
			enc.WriteError("ERR syntax error")
			return
		}
		enc.WriteArrayLen(0)
	case "ACLCAT":
		if len(ctx.Argv) != 5 {
			enc.WriteError("ERR syntax error")
			return
		}
		cat := normalizeCategory(string(ctx.Argv[4]))
		var names []string
		for _, c := range cmds {
			for _, cc := range commandCategories(c) {
				if cc == cat {
					names = append(names, c.Name)
					break
				}
			}
		}
		enc.WriteArrayLen(len(names))
		for _, n := range names {
			enc.WriteBulkStringStr(n)
		}
	case "PATTERN":
		if len(ctx.Argv) != 5 {
			enc.WriteError("ERR syntax error")
			return
		}
		pat := ctx.Argv[4]
		var names []string
		for _, c := range cmds {
			if stringMatch(pat, []byte(c.Name), false) {
				names = append(names, c.Name)
			}
		}
		enc.WriteArrayLen(len(names))
		for _, n := range names {
			enc.WriteBulkStringStr(n)
		}
	default:
		enc.WriteError("ERR syntax error")
	}
}

// normalizeCategory makes an ACLCAT filter value comparable to the category
// strings commandCategories returns, which all start with "@".
func normalizeCategory(cat string) string {
	cat = strings.ToLower(cat)
	if !strings.HasPrefix(cat, "@") {
		cat = "@" + cat
	}
	return cat
}

// handleCommandGetKeys returns the keys a command invocation would touch.
func handleCommandGetKeys(ctx *Ctx) {
	keys, errMsg := ctx.d.getKeysForGetKeys(ctx.Argv[2:])
	if errMsg != "" {
		ctx.enc().WriteError(errMsg)
		return
	}
	enc := ctx.enc()
	enc.WriteArrayLen(len(keys))
	for _, k := range keys {
		enc.WriteBulkString(k)
	}
}

// handleCommandGetKeysAndFlags returns each key paired with its access flags.
func handleCommandGetKeysAndFlags(ctx *Ctx) {
	target := ctx.Argv[2:]
	keys, errMsg := ctx.d.getKeysForGetKeys(target)
	if errMsg != "" {
		ctx.enc().WriteError(errMsg)
		return
	}
	cmd := ctx.d.table.get(strings.ToLower(string(target[0])))
	flags := keyAccessFlags(cmd)
	enc := ctx.enc()
	enc.WriteArrayLen(len(keys))
	for _, k := range keys {
		enc.WriteArrayLen(2)
		enc.WriteBulkString(k)
		enc.WriteArrayLen(len(flags))
		for _, f := range flags {
			enc.WriteStatus(f)
		}
	}
}

// keyAccessFlags returns the GETKEYSANDFLAGS access flags for a command's keys.
// aki does not carry per-key-spec flags, so it reports the command-level access:
// a write command reads and updates its keys, a read command only accesses them.
func keyAccessFlags(cmd *CmdDesc) []string {
	if cmd != nil && cmd.Flags.Has(FlagWrite) {
		return []string{"RW", "access", "update"}
	}
	return []string{"RO", "access"}
}

// getKeysForGetKeys resolves the key arguments of the command invocation in args
// (args[0] is the command name). It returns a RESP error string on failure.
func (d *Dispatcher) getKeysForGetKeys(args [][]byte) ([][]byte, string) {
	name := strings.ToLower(string(args[0]))
	cmd := d.table.get(name)
	if cmd == nil {
		return nil, "ERR Invalid command specified"
	}
	if !checkArity(cmd, len(args)) {
		return nil, "ERR Invalid number of arguments specified for command"
	}
	keys, ok := extractKeys(name, cmd, args)
	if !ok {
		return nil, "ERR Invalid arguments specified for command"
	}
	if len(keys) == 0 {
		return nil, "ERR The command has no key arguments"
	}
	return keys, ""
}

// extractKeys returns the keys a command invocation operates on. It handles the
// movable-key commands whose key positions depend on a numkeys argument or a
// keyword, and otherwise reads the fixed first/last/step positions from the
// descriptor. The bool is false when the arguments cannot be parsed.
func extractKeys(name string, cmd *CmdDesc, args [][]byte) ([][]byte, bool) {
	switch name {
	case "zunionstore", "zinterstore", "zdiffstore":
		// dest numkeys key [key ...]
		n, ok := numAt(args, 2)
		if !ok || 3+n > len(args) {
			return nil, false
		}
		keys := [][]byte{args[1]}
		keys = append(keys, args[3:3+n]...)
		return keys, true
	case "zunion", "zinter", "zdiff", "zmpop", "lmpop", "sintercard", "lcs":
		if name == "lcs" {
			break // lcs has fixed keys, fall through to the descriptor path
		}
		// numkeys key [key ...]
		n, ok := numAt(args, 1)
		if !ok || 2+n > len(args) {
			return nil, false
		}
		return cloneKeys(args[2 : 2+n]), true
	case "bzmpop", "blmpop":
		// timeout numkeys key [key ...]
		n, ok := numAt(args, 2)
		if !ok || 3+n > len(args) {
			return nil, false
		}
		return cloneKeys(args[3 : 3+n]), true
	case "eval", "evalsha", "eval_ro", "evalsha_ro", "fcall", "fcall_ro":
		// script-or-name numkeys key [key ...] arg [arg ...]
		n, ok := numAt(args, 2)
		if !ok || 3+n > len(args) {
			return nil, false
		}
		return cloneKeys(args[3 : 3+n]), true
	case "sort", "sort_ro":
		keys := [][]byte{args[1]}
		for i := 2; i < len(args); i++ {
			if strings.EqualFold(string(args[i]), "STORE") && i+1 < len(args) {
				keys = append(keys, args[i+1])
				i++
			}
		}
		return keys, true
	case "georadius", "georadiusbymember":
		keys := [][]byte{args[1]}
		for i := 2; i < len(args); i++ {
			tok := strings.ToUpper(string(args[i]))
			if (tok == "STORE" || tok == "STOREDIST") && i+1 < len(args) {
				keys = append(keys, args[i+1])
				i++
			}
		}
		return keys, true
	case "xread", "xreadgroup":
		return streamKeys(args)
	case "migrate":
		return migrateKeys(args)
	case "object":
		if len(args) >= 3 && !strings.EqualFold(string(args[1]), "HELP") {
			return [][]byte{args[2]}, true
		}
		return nil, true
	case "memory":
		if len(args) >= 3 && strings.EqualFold(string(args[1]), "USAGE") {
			return [][]byte{args[2]}, true
		}
		return nil, true
	}
	return fixedKeys(cmd, args), true
}

// fixedKeys reads the keys at the descriptor's first/last/step positions. A last
// position below zero counts back from the end, the Redis convention used by
// commands like DEL and MSET.
func fixedKeys(cmd *CmdDesc, args [][]byte) [][]byte {
	if cmd.FirstKey == 0 {
		return nil
	}
	last := cmd.LastKey
	if last < 0 {
		last = len(args) + last
	}
	step := cmd.Step
	if step <= 0 {
		step = 1
	}
	var keys [][]byte
	for i := cmd.FirstKey; i <= last && i < len(args); i += step {
		keys = append(keys, args[i])
	}
	return keys
}

// streamKeys pulls the key half out of an XREAD or XREADGROUP invocation: every
// argument after the STREAMS keyword splits in half, keys first then ids.
func streamKeys(args [][]byte) ([][]byte, bool) {
	idx := -1
	for i := 1; i < len(args); i++ {
		if strings.EqualFold(string(args[i]), "STREAMS") {
			idx = i
			break
		}
	}
	if idx < 0 {
		return nil, false
	}
	rest := args[idx+1:]
	if len(rest) == 0 || len(rest)%2 != 0 {
		return nil, false
	}
	return cloneKeys(rest[:len(rest)/2]), true
}

// migrateKeys returns the keys a MIGRATE invocation moves: the single key at
// position 3, or every key after a KEYS keyword when the position-3 key is empty.
func migrateKeys(args [][]byte) ([][]byte, bool) {
	for i := 6; i < len(args); i++ {
		if strings.EqualFold(string(args[i]), "KEYS") {
			if i+1 >= len(args) {
				return nil, false
			}
			return cloneKeys(args[i+1:]), true
		}
	}
	if len(args) > 3 && len(args[3]) > 0 {
		return [][]byte{args[3]}, true
	}
	return nil, false
}

// numAt parses the integer argument at position i as a non-negative count.
func numAt(args [][]byte, i int) (int, bool) {
	if i >= len(args) {
		return 0, false
	}
	n, err := strconv.Atoi(string(args[i]))
	if err != nil || n < 0 {
		return 0, false
	}
	return n, true
}

// cloneKeys copies a slice of key arguments so the reply does not alias the input
// buffer.
func cloneKeys(in [][]byte) [][]byte {
	out := make([][]byte, len(in))
	copy(out, in)
	return out
}
