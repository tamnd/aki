package command

import (
	"strconv"
	"strings"
	"sync"

	"github.com/tamnd/aki/resp"
)

// This file implements the scripting command family (spec 2064 doc 15 sections 2
// through 5): EVAL, EVALSHA, their _RO read-only forms, and the SCRIPT container
// with LOAD, EXISTS, FLUSH, KILL and HELP. The Lua execution and the redis.*
// bridge live in script.go; this file is the wire layer and the script cache.

// scriptCache holds loaded script bodies keyed by their lowercase SHA1 hex. It is
// shared across connections, so every method takes the lock.
type scriptCache struct {
	mu sync.RWMutex
	m  map[string]string
}

// put stores a body under its SHA1 and returns the SHA1. A repeated load is a
// no-op that returns the same digest.
func (s *scriptCache) put(body string) string {
	sum := sha1hex(body)
	s.mu.Lock()
	if s.m == nil {
		s.m = make(map[string]string)
	}
	s.m[sum] = body
	s.mu.Unlock()
	return sum
}

// get returns the body for a SHA1 and whether it was present. The lookup is
// case-insensitive on the digest, matching real Redis.
func (s *scriptCache) get(sum string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	body, ok := s.m[strings.ToLower(sum)]
	return body, ok
}

// flush drops every cached script.
func (s *scriptCache) flush() {
	s.mu.Lock()
	s.m = nil
	s.mu.Unlock()
}

// count reports how many scripts are cached. INFO's memory section reads it for
// number_of_cached_scripts.
func (s *scriptCache) count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.m)
}

// scriptCommands registers EVAL, EVALSHA, their _RO forms and the SCRIPT
// container command.
func scriptCommands() []*CmdDesc {
	return []*CmdDesc{
		{
			Name: "eval", Group: GroupScripting, Since: "2.6.0",
			Arity: -3, Flags: FlagNoScript | FlagStale | FlagMovableKeys | FlagNoMandatoryKeys,
			Handler: handleEval,
		},
		{
			Name: "eval_ro", Group: GroupScripting, Since: "7.0.0",
			Arity: -3, Flags: FlagReadOnly | FlagNoScript | FlagStale | FlagMovableKeys | FlagNoMandatoryKeys,
			Handler: handleEvalRO,
		},
		{
			Name: "evalsha", Group: GroupScripting, Since: "2.6.0",
			Arity: -3, Flags: FlagNoScript | FlagStale | FlagMovableKeys | FlagNoMandatoryKeys,
			Handler: handleEvalSha,
		},
		{
			Name: "evalsha_ro", Group: GroupScripting, Since: "7.0.0",
			Arity: -3, Flags: FlagReadOnly | FlagNoScript | FlagStale | FlagMovableKeys | FlagNoMandatoryKeys,
			Handler: handleEvalShaRO,
		},
		{
			Name: "script", Group: GroupScripting, Since: "2.6.0",
			Arity: -2, Flags: FlagNoScript,
			Handler: handleScript,
			SubCmds: []*CmdDesc{
				{Name: "load", SubName: "script|load", Group: GroupScripting, Since: "2.6.0",
					Arity: 3, Flags: FlagNoScript | FlagStale, Handler: handleScriptLoad},
				{Name: "exists", SubName: "script|exists", Group: GroupScripting, Since: "2.6.0",
					Arity: -3, Flags: FlagNoScript | FlagStale, Handler: handleScriptExists},
				{Name: "flush", SubName: "script|flush", Group: GroupScripting, Since: "2.6.0",
					Arity: -2, Flags: FlagNoScript | FlagStale, Handler: handleScriptFlush},
				{Name: "kill", SubName: "script|kill", Group: GroupScripting, Since: "2.6.0",
					Arity: 2, Flags: FlagNoScript | FlagStale | FlagAllowBusy, Handler: handleScriptKill},
				{Name: "help", SubName: "script|help", Group: GroupScripting, Since: "5.0.0",
					Arity: 2, Flags: FlagLoading | FlagStale, Handler: handleScriptHelp},
			},
		},
	}
}

// parseEvalArgs splits the shared EVAL/EVALSHA argument tail into keys and args.
// argv[2] is numkeys, argv[3:3+numkeys] are the keys, the rest are the args. It
// returns an error string ready for the wire on a bad numkeys.
func parseEvalArgs(argv [][]byte) (keys, args [][]byte, errMsg string) {
	n, err := strconv.Atoi(string(argv[2]))
	if err != nil {
		return nil, nil, "ERR value is not an integer or out of range"
	}
	if n < 0 {
		return nil, nil, "ERR Number of keys can't be negative"
	}
	if n > len(argv)-3 {
		return nil, nil, "ERR Number of keys can't be greater than number of args"
	}
	keys = cloneKeys(argv[3 : 3+n])
	args = cloneKeys(argv[3+n:])
	return keys, args, ""
}

// handleEval runs an inline script body.
func handleEval(ctx *Ctx) { evalInline(ctx, false) }

// handleEvalRO runs an inline script body in read-only mode.
func handleEvalRO(ctx *Ctx) { evalInline(ctx, true) }

func evalInline(ctx *Ctx, readonly bool) {
	body := string(ctx.Argv[1])
	keys, args, errMsg := parseEvalArgs(ctx.Argv)
	if errMsg != "" {
		ctx.enc().WriteError(errMsg)
		return
	}
	ctx.d.scripts.put(body)
	ctx.d.evalScript(ctx, body, keys, args, readonly)
}

// handleEvalSha runs a script already in the cache, addressed by its SHA1.
func handleEvalSha(ctx *Ctx) { evalSha(ctx, false) }

// handleEvalShaRO runs a cached script in read-only mode.
func handleEvalShaRO(ctx *Ctx) { evalSha(ctx, true) }

func evalSha(ctx *Ctx, readonly bool) {
	sum := strings.ToLower(string(ctx.Argv[1]))
	body, ok := ctx.d.scripts.get(sum)
	if !ok {
		ctx.enc().WriteError("NOSCRIPT No matching script. Please use EVAL.")
		return
	}
	keys, args, errMsg := parseEvalArgs(ctx.Argv)
	if errMsg != "" {
		ctx.enc().WriteError(errMsg)
		return
	}
	ctx.d.evalScript(ctx, body, keys, args, readonly)
}

// handleScript without a usable subcommand is an error.
func handleScript(ctx *Ctx) {
	ctx.enc().WriteError(unknownSubcmdError(ctx.Argv).Error())
}

// handleScriptLoad compiles and caches a script, returning its SHA1. A script
// that does not parse is rejected before it is cached.
func handleScriptLoad(ctx *Ctx) {
	body := string(ctx.Argv[2])
	if err := checkScriptCompiles(body); err != nil {
		ctx.enc().WriteError("ERR Error compiling script (new function): " + err.Error())
		return
	}
	sum := ctx.d.scripts.put(body)
	ctx.d.persistScript(sum, body)
	ctx.enc().WriteBulkStringStr(sum)
}

// handleScriptExists reports, for each requested SHA1, whether it is cached.
func handleScriptExists(ctx *Ctx) {
	enc := ctx.enc()
	enc.WriteArrayLen(len(ctx.Argv) - 2)
	for _, a := range ctx.Argv[2:] {
		if _, ok := ctx.d.scripts.get(strings.ToLower(string(a))); ok {
			enc.WriteInteger(1)
		} else {
			enc.WriteInteger(0)
		}
	}
}

// handleScriptFlush empties the script cache. The optional ASYNC or SYNC argument
// is accepted and treated the same, since aki flushes synchronously.
func handleScriptFlush(ctx *Ctx) {
	if len(ctx.Argv) > 3 {
		ctx.enc().WriteError("ERR " + strings.ToUpper(string(ctx.Argv[0])) + "|FLUSH only support SYNC|ASYNC option")
		return
	}
	if len(ctx.Argv) == 3 {
		mode := strings.ToUpper(string(ctx.Argv[2]))
		if mode != "ASYNC" && mode != "SYNC" {
			ctx.enc().WriteError("ERR SCRIPT FLUSH only support SYNC|ASYNC option")
			return
		}
	}
	ctx.d.scripts.flush()
	ctx.d.clearPersistedScripts()
	ctx.Conn.WriteRaw(resp.ReplyOK)
}

// handleScriptKill has no busy script to stop yet: aki runs each script to
// completion in the calling goroutine, so there is never a killable script. It
// returns the NOTBUSY reply real Redis gives when nothing is running.
func handleScriptKill(ctx *Ctx) {
	ctx.enc().WriteError("NOTBUSY No scripts in execution right now.")
}

// handleScriptHelp returns the subcommand help text.
func handleScriptHelp(ctx *Ctx) {
	lines := []string{
		"SCRIPT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
		"LOAD <script>",
		"    Load a script into the scripts cache without executing it.",
		"EXISTS <sha1> [<sha1> ...]",
		"    Return information about the existence of the scripts in the cache.",
		"FLUSH [ASYNC|SYNC]",
		"    Flush the Lua scripts cache. Very dangerous on replicas.",
		"KILL",
		"    Kill the currently executing Lua script.",
		"HELP",
		"    Print this help.",
	}
	enc := ctx.enc()
	enc.WriteArrayLen(len(lines))
	for _, l := range lines {
		enc.WriteStatus(l)
	}
}
