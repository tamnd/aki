package command

import (
	"encoding/binary"
	"hash/crc32"
	"sort"
	"strings"

	"github.com/tamnd/aki/resp"
)

// This file is the wire layer for the FUNCTION command family (spec 2064 doc 15
// section 12): FUNCTION LOAD, DELETE, FLUSH, LIST, DUMP, RESTORE, STATS, KILL and
// the FCALL/FCALL_RO callers. The library model and execution live in
// function.go.

// functionCommands registers FCALL, FCALL_RO and the FUNCTION container command.
func functionCommands() []*CmdDesc {
	return []*CmdDesc{
		{
			Name: "fcall", Group: GroupScripting, Since: "7.0.0",
			Arity: -3, Flags: FlagNoScript | FlagStale | FlagMovableKeys,
			Handler: handleFCall,
		},
		{
			Name: "fcall_ro", Group: GroupScripting, Since: "7.0.0",
			Arity: -3, Flags: FlagReadOnly | FlagNoScript | FlagStale | FlagMovableKeys,
			Handler: handleFCallRO,
		},
		{
			Name: "function", Group: GroupScripting, Since: "7.0.0",
			Arity: -2, Flags: FlagNoScript,
			Handler: handleFunction,
			SubCmds: []*CmdDesc{
				{Name: "load", SubName: "function|load", Group: GroupScripting, Since: "7.0.0",
					Arity: -3, Flags: FlagNoScript | FlagWrite | FlagDenyOOM, Handler: handleFunctionLoad},
				{Name: "delete", SubName: "function|delete", Group: GroupScripting, Since: "7.0.0",
					Arity: 3, Flags: FlagNoScript | FlagWrite, Handler: handleFunctionDelete},
				{Name: "flush", SubName: "function|flush", Group: GroupScripting, Since: "7.0.0",
					Arity: -2, Flags: FlagNoScript | FlagWrite, Handler: handleFunctionFlush},
				{Name: "list", SubName: "function|list", Group: GroupScripting, Since: "7.0.0",
					Arity: -2, Flags: FlagNoScript, Handler: handleFunctionList},
				{Name: "dump", SubName: "function|dump", Group: GroupScripting, Since: "7.0.0",
					Arity: 2, Flags: FlagNoScript, Handler: handleFunctionDump},
				{Name: "restore", SubName: "function|restore", Group: GroupScripting, Since: "7.0.0",
					Arity: -3, Flags: FlagNoScript | FlagWrite | FlagDenyOOM, Handler: handleFunctionRestore},
				{Name: "stats", SubName: "function|stats", Group: GroupScripting, Since: "7.0.0",
					Arity: 2, Flags: FlagNoScript | FlagStale, Handler: handleFunctionStats},
				{Name: "kill", SubName: "function|kill", Group: GroupScripting, Since: "7.0.0",
					Arity: 2, Flags: FlagNoScript | FlagStale | FlagAllowBusy, Handler: handleFunctionKill},
				{Name: "help", SubName: "function|help", Group: GroupScripting, Since: "7.0.0",
					Arity: 2, Flags: FlagLoading | FlagStale, Handler: handleFunctionHelp},
			},
		},
	}
}

// handleFCall calls a registered function.
func handleFCall(ctx *Ctx) { fcall(ctx, false) }

// handleFCallRO calls a registered function that must be flagged no-writes.
func handleFCallRO(ctx *Ctx) { fcall(ctx, true) }

func fcall(ctx *Ctx, readonly bool) {
	fname := string(ctx.Argv[1])
	keys, args, errMsg := parseEvalArgs(ctx.Argv)
	if errMsg != "" {
		ctx.enc().WriteError(errMsg)
		return
	}
	fr := &ctx.d.functions
	fr.mu.RLock()
	libName, ok := fr.fnIndex[fname]
	var lib *funcLib
	var noWrites bool
	if ok {
		lib = fr.libs[libName]
		for _, m := range lib.funcs {
			if m.name == fname {
				noWrites = m.noWrites
				break
			}
		}
	}
	fr.mu.RUnlock()
	if !ok {
		ctx.enc().WriteError("ERR Function not found")
		return
	}
	if readonly && !noWrites {
		ctx.enc().WriteError("ERR Can not execute a script with write flag using *_ro command")
		return
	}
	ctx.d.runFunction(ctx, lib, fname, keys, args, readonly)
}

// handleFunction without a usable subcommand is an error.
func handleFunction(ctx *Ctx) {
	ctx.enc().WriteError(unknownSubcmdError(ctx.Argv).Error())
}

// handleFunctionLoad loads a library, optionally replacing one of the same name.
func handleFunctionLoad(ctx *Ctx) {
	replace := false
	idx := 2
	if len(ctx.Argv) >= 4 && strings.EqualFold(string(ctx.Argv[2]), "REPLACE") {
		replace = true
		idx = 3
	}
	if idx >= len(ctx.Argv) {
		ctx.enc().WriteError("ERR missing function code")
		return
	}
	source := string(ctx.Argv[idx])

	name, freg, errMsg := ctx.d.buildLibrary(ctx, source)
	if errMsg != "" {
		ctx.enc().WriteError(errMsg)
		return
	}

	fr := &ctx.d.functions
	fr.mu.Lock()
	fr.ensure()
	if _, exists := fr.libs[name]; exists && !replace {
		fr.mu.Unlock()
		ctx.enc().WriteError("ERR Library '" + name + "' already exists")
		return
	}
	// A function name must be unique across every library, except the names in the
	// library being replaced.
	for _, fname := range freg.order {
		if owner, taken := fr.fnIndex[fname]; taken && owner != name {
			fr.mu.Unlock()
			ctx.enc().WriteError("ERR Function '" + fname + "' already exists")
			return
		}
	}
	if old, exists := fr.libs[name]; exists {
		for _, m := range old.funcs {
			delete(fr.fnIndex, m.name)
		}
	}
	lib := &funcLib{name: name, engine: "LUA", source: source}
	for _, fname := range freg.order {
		rf := freg.funcs[fname]
		lib.funcs = append(lib.funcs, &funcMeta{
			name: rf.name, description: rf.description, flags: rf.flags, noWrites: rf.noWrites,
		})
		fr.fnIndex[fname] = name
	}
	fr.libs[name] = lib
	fr.mu.Unlock()
	ctx.MarkPropagate()
	ctx.d.persistFunctions()
	ctx.enc().WriteBulkStringStr(name)
}

// handleFunctionDelete removes a library and its functions.
func handleFunctionDelete(ctx *Ctx) {
	name := string(ctx.Argv[2])
	fr := &ctx.d.functions
	fr.mu.Lock()
	lib, ok := fr.libs[name]
	if !ok {
		fr.mu.Unlock()
		ctx.enc().WriteError("ERR Library not found")
		return
	}
	for _, m := range lib.funcs {
		delete(fr.fnIndex, m.name)
	}
	delete(fr.libs, name)
	fr.mu.Unlock()
	ctx.MarkPropagate()
	ctx.d.persistFunctions()
	ctx.Conn.WriteRaw(resp.ReplyOK)
}

// handleFunctionFlush removes every library. The optional ASYNC or SYNC word is
// accepted and treated the same.
func handleFunctionFlush(ctx *Ctx) {
	if len(ctx.Argv) > 3 {
		ctx.enc().WriteError("ERR FUNCTION FLUSH only supports SYNC|ASYNC")
		return
	}
	if len(ctx.Argv) == 3 {
		mode := strings.ToUpper(string(ctx.Argv[2]))
		if mode != "ASYNC" && mode != "SYNC" {
			ctx.enc().WriteError("ERR FUNCTION FLUSH only supports SYNC|ASYNC")
			return
		}
	}
	fr := &ctx.d.functions
	fr.mu.Lock()
	fr.libs = map[string]*funcLib{}
	fr.fnIndex = map[string]string{}
	fr.mu.Unlock()
	ctx.MarkPropagate()
	ctx.d.persistFunctions()
	ctx.Conn.WriteRaw(resp.ReplyOK)
}

// handleFunctionList lists libraries, optionally filtered by name pattern and
// optionally including the source.
func handleFunctionList(ctx *Ctx) {
	var pattern []byte
	withCode := false
	for i := 2; i < len(ctx.Argv); i++ {
		switch strings.ToUpper(string(ctx.Argv[i])) {
		case "LIBRARYNAME":
			if i+1 >= len(ctx.Argv) {
				ctx.enc().WriteError("ERR syntax error")
				return
			}
			pattern = ctx.Argv[i+1]
			i++
		case "WITHCODE":
			withCode = true
		default:
			ctx.enc().WriteError("ERR syntax error")
			return
		}
	}

	fr := &ctx.d.functions
	fr.mu.RLock()
	names := make([]string, 0, len(fr.libs))
	for name := range fr.libs {
		if pattern != nil && !stringMatch(pattern, []byte(name), false) {
			continue
		}
		names = append(names, name)
	}
	sort.Strings(names)
	libs := make([]*funcLib, len(names))
	for i, name := range names {
		libs[i] = fr.libs[name]
	}
	fr.mu.RUnlock()

	enc := ctx.enc()
	enc.WriteArrayLen(len(libs))
	for _, lib := range libs {
		writeLibraryEntry(enc, lib, withCode, ctx.Conn.Proto())
	}
}

// writeLibraryEntry writes one library as the map (RESP3) or flat array (RESP2)
// FUNCTION LIST returns.
func writeLibraryEntry(enc *resp.Encoder, lib *funcLib, withCode bool, proto int) {
	fields := 3
	if withCode {
		fields = 4
	}
	if proto >= 3 {
		enc.WriteMapLen(fields)
	} else {
		enc.WriteArrayLen(fields * 2)
	}
	enc.WriteBulkStringStr("library_name")
	enc.WriteBulkStringStr(lib.name)
	enc.WriteBulkStringStr("engine")
	enc.WriteBulkStringStr(lib.engine)
	enc.WriteBulkStringStr("functions")
	enc.WriteArrayLen(len(lib.funcs))
	for _, m := range lib.funcs {
		if proto >= 3 {
			enc.WriteMapLen(3)
		} else {
			enc.WriteArrayLen(6)
		}
		enc.WriteBulkStringStr("name")
		enc.WriteBulkStringStr(m.name)
		enc.WriteBulkStringStr("description")
		if m.description == "" {
			enc.WriteNull()
		} else {
			enc.WriteBulkStringStr(m.description)
		}
		enc.WriteBulkStringStr("flags")
		if proto >= 3 {
			enc.WriteSetLen(len(m.flags))
		} else {
			enc.WriteArrayLen(len(m.flags))
		}
		for _, f := range m.flags {
			enc.WriteStatus(f)
		}
	}
	if withCode {
		enc.WriteBulkStringStr("library_code")
		enc.WriteBulkStringStr(lib.source)
	}
}

const (
	funcDumpMagic   = 0x46554e43 // "FUNC"
	funcDumpVersion = 1
)

// handleFunctionDump serializes every library to the binary payload FUNCTION
// RESTORE reads back.
func handleFunctionDump(ctx *Ctx) {
	fr := &ctx.d.functions
	fr.mu.RLock()
	names := make([]string, 0, len(fr.libs))
	for name := range fr.libs {
		names = append(names, name)
	}
	sort.Strings(names)
	body := make([]byte, 0, 256)
	var hdr [4]byte
	binary.LittleEndian.PutUint32(hdr[:], uint32(len(names)))
	body = append(body, hdr[:]...)
	for _, name := range names {
		lib := fr.libs[name]
		var nl [2]byte
		binary.LittleEndian.PutUint16(nl[:], uint16(len(lib.name)))
		body = append(body, nl[:]...)
		body = append(body, lib.name...)
		var sl [4]byte
		binary.LittleEndian.PutUint32(sl[:], uint32(len(lib.source)))
		body = append(body, sl[:]...)
		body = append(body, lib.source...)
	}
	fr.mu.RUnlock()

	out := make([]byte, 0, len(body)+9)
	var magic [5]byte
	binary.LittleEndian.PutUint32(magic[:4], funcDumpMagic)
	magic[4] = funcDumpVersion
	out = append(out, magic[:]...)
	out = append(out, body...)
	sum := crc32.Checksum(out, crc32.MakeTable(crc32.Castagnoli))
	var crc [4]byte
	binary.LittleEndian.PutUint32(crc[:], sum)
	out = append(out, crc[:]...)
	ctx.enc().WriteBulkString(out)
}

// parsedLib is one library decoded from a FUNCTION DUMP payload.
type parsedLib struct {
	name   string
	source string
}

// decodeFunctionDump validates and parses a FUNCTION DUMP payload.
func decodeFunctionDump(payload []byte) ([]parsedLib, bool) {
	if len(payload) < 9 {
		return nil, false
	}
	if binary.LittleEndian.Uint32(payload[:4]) != funcDumpMagic || payload[4] != funcDumpVersion {
		return nil, false
	}
	body := payload[:len(payload)-4]
	want := binary.LittleEndian.Uint32(payload[len(payload)-4:])
	if crc32.Checksum(body, crc32.MakeTable(crc32.Castagnoli)) != want {
		return nil, false
	}
	pos := 5
	if pos+4 > len(body) {
		return nil, false
	}
	count := binary.LittleEndian.Uint32(body[pos : pos+4])
	pos += 4
	out := make([]parsedLib, 0, count)
	for i := uint32(0); i < count; i++ {
		if pos+2 > len(body) {
			return nil, false
		}
		nl := int(binary.LittleEndian.Uint16(body[pos : pos+2]))
		pos += 2
		if pos+nl > len(body) {
			return nil, false
		}
		name := string(body[pos : pos+nl])
		pos += nl
		if pos+4 > len(body) {
			return nil, false
		}
		sl := int(binary.LittleEndian.Uint32(body[pos : pos+4]))
		pos += 4
		if pos+sl > len(body) {
			return nil, false
		}
		source := string(body[pos : pos+sl])
		pos += sl
		out = append(out, parsedLib{name: name, source: source})
	}
	return out, true
}

// handleFunctionRestore loads libraries from a FUNCTION DUMP payload under one of
// the FLUSH, APPEND or REPLACE policies.
func handleFunctionRestore(ctx *Ctx) {
	payload := ctx.Argv[2]
	policy := "FLUSH"
	if len(ctx.Argv) >= 4 {
		policy = strings.ToUpper(string(ctx.Argv[3]))
	}
	if policy != "FLUSH" && policy != "APPEND" && policy != "REPLACE" {
		ctx.enc().WriteError("ERR Wrong restore policy. Accept values are: FLUSH, APPEND or REPLACE.")
		return
	}
	libs, ok := decodeFunctionDump(payload)
	if !ok {
		ctx.enc().WriteError("ERR payload version or checksum are wrong")
		return
	}

	// Build each library in isolation first so a bad payload aborts before any
	// state changes.
	built := make([]*funcLib, 0, len(libs))
	for _, pl := range libs {
		name, freg, errMsg := ctx.d.buildLibrary(ctx, pl.source)
		if errMsg != "" {
			ctx.enc().WriteError(errMsg)
			return
		}
		if name != pl.name {
			ctx.enc().WriteError("ERR Library name mismatch in payload")
			return
		}
		lib := &funcLib{name: name, engine: "LUA", source: pl.source}
		for _, fname := range freg.order {
			rf := freg.funcs[fname]
			lib.funcs = append(lib.funcs, &funcMeta{
				name: rf.name, description: rf.description, flags: rf.flags, noWrites: rf.noWrites,
			})
		}
		built = append(built, lib)
	}

	fr := &ctx.d.functions
	fr.mu.Lock()
	fr.ensure()
	if policy == "FLUSH" {
		fr.libs = map[string]*funcLib{}
		fr.fnIndex = map[string]string{}
	}
	if policy == "APPEND" {
		for _, lib := range built {
			if _, exists := fr.libs[lib.name]; exists {
				fr.mu.Unlock()
				ctx.enc().WriteError("ERR Library '" + lib.name + "' already exists")
				return
			}
			for _, m := range lib.funcs {
				if owner, taken := fr.fnIndex[m.name]; taken && owner != lib.name {
					fr.mu.Unlock()
					ctx.enc().WriteError("ERR Function '" + m.name + "' already exists")
					return
				}
			}
		}
	}
	for _, lib := range built {
		if old, exists := fr.libs[lib.name]; exists {
			for _, m := range old.funcs {
				delete(fr.fnIndex, m.name)
			}
		}
		fr.libs[lib.name] = lib
		for _, m := range lib.funcs {
			fr.fnIndex[m.name] = lib.name
		}
	}
	fr.mu.Unlock()
	ctx.MarkPropagate()
	ctx.d.persistFunctions()
	ctx.Conn.WriteRaw(resp.ReplyOK)
}

// handleFunctionStats reports the library and function counts. No function is
// ever running when this is observed, since aki runs each call inline.
func handleFunctionStats(ctx *Ctx) {
	fr := &ctx.d.functions
	fr.mu.RLock()
	libs := len(fr.libs)
	funcs := len(fr.fnIndex)
	fr.mu.RUnlock()

	enc := ctx.enc()
	proto := ctx.Conn.Proto()
	if proto >= 3 {
		enc.WriteMapLen(2)
	} else {
		enc.WriteArrayLen(4)
	}
	enc.WriteBulkStringStr("running_script")
	enc.WriteNull()
	enc.WriteBulkStringStr("engines")
	if proto >= 3 {
		enc.WriteMapLen(1)
	} else {
		enc.WriteArrayLen(2)
	}
	enc.WriteBulkStringStr("LUA")
	if proto >= 3 {
		enc.WriteMapLen(2)
	} else {
		enc.WriteArrayLen(4)
	}
	enc.WriteBulkStringStr("libraries_count")
	enc.WriteInteger(int64(libs))
	enc.WriteBulkStringStr("functions_count")
	enc.WriteInteger(int64(funcs))
}

// handleFunctionKill returns NOTBUSY: a function runs inline to completion, so
// there is never a separate one to kill.
func handleFunctionKill(ctx *Ctx) {
	ctx.enc().WriteError("NOTBUSY No scripts in execution right now.")
}

// handleFunctionHelp prints the subcommand list.
func handleFunctionHelp(ctx *Ctx) {
	lines := []string{
		"FUNCTION <subcommand> [<arg> ...]. Subcommands are:",
		"LOAD [REPLACE] <code>",
		"    Create a library with the given code.",
		"DELETE <library-name>",
		"    Delete the given library.",
		"LIST [LIBRARYNAME <pattern>] [WITHCODE]",
		"    Return general information on all the libraries.",
		"DUMP",
		"    Return a serialized payload of all loaded libraries.",
		"RESTORE <payload> [FLUSH|APPEND|REPLACE]",
		"    Restore libraries from a payload.",
		"FLUSH [ASYNC|SYNC]",
		"    Destroy all libraries.",
		"STATS",
		"    Return information about the current function running.",
		"KILL",
		"    Kill the current running function.",
		"HELP",
		"    Print this help.",
	}
	enc := ctx.enc()
	enc.WriteArrayLen(len(lines))
	for _, l := range lines {
		enc.WriteStatus(l)
	}
}
