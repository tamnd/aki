package command

import (
	"sort"
	"strings"
	"sync"

	"github.com/tamnd/aki/lua"
	"github.com/tamnd/aki/networking"
)

// This file implements the function library model behind FUNCTION LOAD, FCALL and
// the rest of the FUNCTION family (spec 2064 doc 15 section 12). A library is a
// named Lua source that calls redis.register_function one or more times at its
// top level. The library source is the source of truth: the registry stores the
// source and the metadata of each function, and an FCALL re-runs the source in a
// fresh interpreter to get the callback, the same per-call clean-interpreter rule
// EVAL uses.

// regFunc is one function captured by redis.register_function during a library
// build. The callback is only meaningful within the interpreter that produced it.
type regFunc struct {
	name        string
	callback    *lua.Function
	flags       []string
	description string
	noWrites    bool
}

// funcReg collects the functions a library registers during one build. It is
// passed into the redis table so register_function writes here.
type funcReg struct {
	funcs map[string]*regFunc
	order []string
}

func newFuncReg() *funcReg { return &funcReg{funcs: map[string]*regFunc{}} }

// register implements redis.register_function in both its positional and table
// forms. It validates the arguments and records the function, raising a Lua error
// on a duplicate name or a malformed call so FUNCTION LOAD can report it.
func (fr *funcReg) register(args []lua.Value) ([]lua.Value, error) {
	var rf regFunc
	switch first := nthArg(args, 0).(type) {
	case lua.String:
		rf.name = string(first)
		cb, ok := nthArg(args, 1).(*lua.Function)
		if !ok {
			return nil, regErr("wrong number or type of arguments")
		}
		rf.callback = cb
	case *lua.Table:
		name, ok := first.Get(lua.String("function_name")).(lua.String)
		if !ok {
			return nil, regErr("missing function name")
		}
		rf.name = string(name)
		cb, ok := first.Get(lua.String("callback")).(*lua.Function)
		if !ok {
			return nil, regErr("missing function callback")
		}
		rf.callback = cb
		if d, ok := first.Get(lua.String("description")).(lua.String); ok {
			rf.description = string(d)
		}
		if flags, ok := first.Get(lua.String("flags")).(*lua.Table); ok {
			n := flags.Len()
			for idx := 1; idx <= n; idx++ {
				f, ok := flags.Get(lua.Number(idx)).(lua.String)
				if !ok {
					return nil, regErr("flags must be strings")
				}
				if !validFuncFlag(string(f)) {
					return nil, regErr("unknown flag given")
				}
				rf.flags = append(rf.flags, string(f))
				if string(f) == "no-writes" {
					rf.noWrites = true
				}
			}
		}
	default:
		return nil, regErr("wrong number or type of arguments")
	}
	if rf.name == "" {
		return nil, regErr("function name cannot be empty")
	}
	if _, dup := fr.funcs[rf.name]; dup {
		return nil, &lua.Error{Value: lua.String("Function already exists in library")}
	}
	rf.flags = append([]string(nil), rf.flags...)
	fr.funcs[rf.name] = &rf
	fr.order = append(fr.order, rf.name)
	return nil, nil
}

// regErr builds the Lua error register_function raises on bad arguments.
func regErr(msg string) error {
	return &lua.Error{Value: lua.String("Error registering function: " + msg)}
}

// validFuncFlag reports whether a flag is one aki recognizes.
func validFuncFlag(f string) bool {
	switch f {
	case "no-writes", "allow-oom", "allow-stale", "no-cluster":
		return true
	}
	return false
}

// funcMeta is the stored metadata of one registered function. The callback is not
// stored, since it belongs to a finished interpreter; FCALL rebuilds it.
type funcMeta struct {
	name        string
	description string
	flags       []string
	noWrites    bool
}

// funcLib is a stored function library: its name, engine, source, and the
// metadata of every function it registered.
type funcLib struct {
	name   string
	engine string
	source string
	funcs  []*funcMeta
}

// functionRegistry holds every loaded library and a name index so a function
// name is unique across all libraries, the rule FUNCTION LOAD enforces.
type functionRegistry struct {
	mu      sync.RWMutex
	libs    map[string]*funcLib
	fnIndex map[string]string // function name -> library name
}

func (fr *functionRegistry) ensure() {
	if fr.libs == nil {
		fr.libs = map[string]*funcLib{}
		fr.fnIndex = map[string]string{}
	}
}

// librarySources returns the source of every loaded library, sorted by library
// name so the order is stable across snapshots. SAVE, the AOF base, and a full
// sync write these into the RDB as FUNCTION2 records so a reload or a fresh replica
// gets the functions back.
func (fr *functionRegistry) librarySources() []string {
	fr.mu.RLock()
	defer fr.mu.RUnlock()
	names := make([]string, 0, len(fr.libs))
	for name := range fr.libs {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]string, 0, len(names))
	for _, name := range names {
		out = append(out, fr.libs[name].source)
	}
	return out
}

// LoadFunctions rebuilds the function registry from a set of library sources. The
// startup --load-rdb import uses it to bring over the functions an imported
// dump.rdb carried in its FUNCTION2 records.
func (d *Dispatcher) LoadFunctions(sources []string) {
	d.loadFunctionLibraries(sources)
}

// loadFunctionLibraries rebuilds the function registry from a set of library
// sources, the form FUNCTION2 records carry in an RDB. The caller has already
// cleared the registry, so each source is built and registered fresh. A source
// that fails to build is skipped rather than aborting the whole load, the same
// lenient stance the AOF replay takes for an unknown command.
func (d *Dispatcher) loadFunctionLibraries(sources []string) {
	if len(sources) == 0 {
		return
	}
	conn := networking.NewOfflineConn()
	sess := &session{authenticated: true}
	conn.SetSession(sess)
	ctx := &Ctx{Conn: conn, d: d, sess: sess}
	for _, src := range sources {
		name, freg, errMsg := d.buildLibrary(ctx, src)
		if errMsg != "" {
			continue
		}
		fr := &d.functions
		fr.mu.Lock()
		fr.ensure()
		if old, exists := fr.libs[name]; exists {
			for _, m := range old.funcs {
				delete(fr.fnIndex, m.name)
			}
		}
		lib := &funcLib{name: name, engine: "LUA", source: src}
		for _, fname := range freg.order {
			rf := freg.funcs[fname]
			lib.funcs = append(lib.funcs, &funcMeta{
				name: rf.name, description: rf.description, flags: rf.flags, noWrites: rf.noWrites,
			})
			fr.fnIndex[fname] = name
		}
		fr.libs[name] = lib
		fr.mu.Unlock()
	}
}

// parseShebang reads the "#!lua name=<libname>" first line of a library source.
// It returns the engine, the library name, and the body after the first line.
func parseShebang(source string) (engine, name, errMsg string) {
	nl := strings.IndexByte(source, '\n')
	first := source
	if nl >= 0 {
		first = source[:nl]
	}
	first = strings.TrimRight(first, "\r")
	if !strings.HasPrefix(first, "#!") {
		return "", "", "Missing library metadata"
	}
	fields := strings.Fields(first[2:])
	if len(fields) == 0 {
		return "", "", "Missing library metadata"
	}
	engine = fields[0]
	for _, f := range fields[1:] {
		if strings.HasPrefix(f, "name=") {
			name = strings.TrimPrefix(f, "name=")
		}
	}
	if name == "" {
		return "", "", "Missing library name"
	}
	if !validLibName(name) {
		return "", "", "Library names can only contain letters, numbers, or underscores(_) and must be at least one character long"
	}
	return engine, name, ""
}

// stripShebang blanks the first line of a library source so the Lua lexer does
// not choke on the "#!" while line numbers in any error stay aligned. The
// original source is kept for FUNCTION LIST WITHCODE and FUNCTION DUMP.
func stripShebang(source string) string {
	nl := strings.IndexByte(source, '\n')
	if nl < 0 {
		return ""
	}
	return source[nl:]
}

// validLibName matches [a-zA-Z0-9_][a-zA-Z0-9_-]*.
func validLibName(name string) bool {
	for i, c := range name {
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '_':
		case c == '-' && i > 0:
		default:
			return false
		}
	}
	return name != ""
}

// buildLibrary parses the shebang and runs the library source once in a fresh
// interpreter, collecting the registered functions. It returns the library name
// and the populated registry, or a wire-ready error string. The returned funcReg
// holds live callbacks tied to the interpreter that built it.
func (d *Dispatcher) buildLibrary(ctx *Ctx, source string) (string, *funcReg, string) {
	engine, name, errMsg := parseShebang(source)
	if errMsg != "" {
		return "", nil, "ERR " + errMsg
	}
	if !strings.EqualFold(engine, "lua") && !strings.EqualFold(engine, "#!lua") {
		return "", nil, "ERR Could not find library engine"
	}
	freg := newFuncReg()
	i, _ := d.newScriptInterp(ctx, false, freg)
	if _, err := i.Run(stripShebang(source)); err != nil {
		return "", nil, "ERR Error compiling function: " + scriptErrorBody(err)
	}
	if len(freg.order) == 0 {
		return "", nil, "ERR No functions registered"
	}
	return name, freg, ""
}

// scriptErrorBody returns the bare message of a Lua error without an added code,
// for embedding inside a larger error string.
func scriptErrorBody(err error) string {
	if le, ok := err.(*lua.Error); ok {
		return lua.ToString(le.Value)
	}
	return err.Error()
}

// runFunction runs a registered function by re-running its library source in a
// fresh interpreter, then calling the named callback with the keys and args
// tables. readonly rejects write commands inside the call.
func (d *Dispatcher) runFunction(ctx *Ctx, lib *funcLib, fname string, keys, args [][]byte, readonly bool) {
	freg := newFuncReg()
	i, sc := d.newScriptInterp(ctx, readonly, freg)
	if _, err := i.Run(stripShebang(lib.source)); err != nil {
		ctx.enc().WriteError(scriptError(err))
		return
	}
	rf, ok := freg.funcs[fname]
	if !ok {
		ctx.enc().WriteError("ERR Function not found")
		return
	}
	rets, err := i.Call(rf.callback, bytesToTable(keys), bytesToTable(args))
	if err != nil {
		ctx.enc().WriteError(scriptError(err))
		return
	}
	result := lua.Value(lua.Nil)
	if len(rets) > 0 {
		result = rets[0]
	}
	luaToRESP(ctx.enc(), result, sc.resp)
}
