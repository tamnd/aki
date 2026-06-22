package command

import (
	"strings"
	"testing"
)

const libGetSet = `#!lua name=mylib
redis.register_function('myget', function(keys, args)
  return redis.call('GET', keys[1])
end)
redis.register_function('myset', function(keys, args)
  return redis.call('SET', keys[1], args[1])
end)`

func TestFunctionLoadAndFCall(t *testing.T) {
	r, c := startData(t)
	if got := sendArgs(t, r, c, "FUNCTION", "LOAD", libGetSet); got != "mylib" {
		t.Fatalf("FUNCTION LOAD = %v", got)
	}
	if got := sendArgs(t, r, c, "FCALL", "myset", "1", "k", "hello"); got != "OK" {
		t.Fatalf("FCALL myset = %v", got)
	}
	if got := sendArgs(t, r, c, "FCALL", "myget", "1", "k"); got != "hello" {
		t.Fatalf("FCALL myget = %v", got)
	}
}

func TestFunctionLoadDuplicate(t *testing.T) {
	r, c := startData(t)
	sendArgs(t, r, c, "FUNCTION", "LOAD", libGetSet)
	got := sendArgs(t, r, c, "FUNCTION", "LOAD", libGetSet)
	if e, ok := got.(cmdErr); !ok || !strings.Contains(string(e), "already exists") {
		t.Fatalf("duplicate load = %v (%T)", got, got)
	}
	// REPLACE succeeds.
	if got := sendArgs(t, r, c, "FUNCTION", "LOAD", "REPLACE", libGetSet); got != "mylib" {
		t.Fatalf("LOAD REPLACE = %v", got)
	}
}

func TestFunctionLoadNoShebang(t *testing.T) {
	r, c := startData(t)
	got := sendArgs(t, r, c, "FUNCTION", "LOAD", "redis.register_function('x', function() end)")
	if e, ok := got.(cmdErr); !ok || !strings.Contains(string(e), "Missing library metadata") {
		t.Fatalf("no shebang = %v (%T)", got, got)
	}
}

func TestFunctionLoadNoFunctions(t *testing.T) {
	r, c := startData(t)
	got := sendArgs(t, r, c, "FUNCTION", "LOAD", "#!lua name=empty\nreturn 1")
	if e, ok := got.(cmdErr); !ok || !strings.Contains(string(e), "No functions registered") {
		t.Fatalf("no functions = %v (%T)", got, got)
	}
}

const libRO = `#!lua name=rolib
redis.register_function{
  function_name = 'roget',
  callback = function(keys, args) return redis.call('GET', keys[1]) end,
  flags = {'no-writes'},
  description = 'read a key'
}
redis.register_function('rowrite', function(keys, args)
  return redis.call('SET', keys[1], 'x')
end)`

func TestFCallRO(t *testing.T) {
	r, c := startData(t)
	sendArgs(t, r, c, "FUNCTION", "LOAD", libRO)
	sendArgs(t, r, c, "SET", "k", "v")
	// A no-writes function runs under FCALL_RO.
	if got := sendArgs(t, r, c, "FCALL_RO", "roget", "1", "k"); got != "v" {
		t.Fatalf("FCALL_RO roget = %v", got)
	}
	// A write function is rejected by FCALL_RO.
	got := sendArgs(t, r, c, "FCALL_RO", "rowrite", "1", "k")
	if e, ok := got.(cmdErr); !ok || !strings.Contains(string(e), "write flag using *_ro") {
		t.Fatalf("FCALL_RO write = %v (%T)", got, got)
	}
}

func TestFCallNotFound(t *testing.T) {
	r, c := startData(t)
	got := sendArgs(t, r, c, "FCALL", "ghost", "0")
	if e, ok := got.(cmdErr); !ok || string(e) != "ERR Function not found" {
		t.Fatalf("FCALL ghost = %v", got)
	}
}

func TestFunctionList(t *testing.T) {
	r, c := startData(t)
	sendArgs(t, r, c, "FUNCTION", "LOAD", libRO)
	reply := asArray(t, sendArgs(t, r, c, "FUNCTION", "LIST"))
	if len(reply) != 1 {
		t.Fatalf("LIST len = %d", len(reply))
	}
	lib := asArray(t, reply[0])
	// Flat array of key,value pairs on RESP2.
	fields := map[string]any{}
	for i := 0; i+1 < len(lib); i += 2 {
		fields[lib[i].(string)] = lib[i+1]
	}
	if fields["library_name"] != "rolib" || fields["engine"] != "LUA" {
		t.Fatalf("lib fields = %v", fields)
	}
	funcs := asArray(t, fields["functions"])
	if len(funcs) != 2 {
		t.Fatalf("functions count = %d", len(funcs))
	}
}

func TestFunctionListWithCode(t *testing.T) {
	r, c := startData(t)
	sendArgs(t, r, c, "FUNCTION", "LOAD", libRO)
	lib := asArray(t, asArray(t, sendArgs(t, r, c, "FUNCTION", "LIST", "WITHCODE"))[0])
	fields := map[string]any{}
	for i := 0; i+1 < len(lib); i += 2 {
		fields[lib[i].(string)] = lib[i+1]
	}
	if code, _ := fields["library_code"].(string); !strings.Contains(code, "register_function") {
		t.Fatalf("library_code = %v", fields["library_code"])
	}
}

func TestFunctionDelete(t *testing.T) {
	r, c := startData(t)
	sendArgs(t, r, c, "FUNCTION", "LOAD", libGetSet)
	if got := sendArgs(t, r, c, "FUNCTION", "DELETE", "mylib"); got != "OK" {
		t.Fatalf("DELETE = %v", got)
	}
	if got := sendArgs(t, r, c, "FCALL", "myget", "1", "k"); func() bool { _, ok := got.(cmdErr); return !ok }() {
		t.Fatalf("FCALL after delete = %v", got)
	}
	got := sendArgs(t, r, c, "FUNCTION", "DELETE", "mylib")
	if e, ok := got.(cmdErr); !ok || string(e) != "ERR Library not found" {
		t.Fatalf("DELETE missing = %v", got)
	}
}

func TestFunctionFlush(t *testing.T) {
	r, c := startData(t)
	sendArgs(t, r, c, "FUNCTION", "LOAD", libGetSet)
	if got := sendArgs(t, r, c, "FUNCTION", "FLUSH"); got != "OK" {
		t.Fatalf("FLUSH = %v", got)
	}
	if reply := asArray(t, sendArgs(t, r, c, "FUNCTION", "LIST")); len(reply) != 0 {
		t.Fatalf("LIST after flush = %v", reply)
	}
}

func TestFunctionDumpRestore(t *testing.T) {
	r, c := startData(t)
	sendArgs(t, r, c, "FUNCTION", "LOAD", libGetSet)
	dump, ok := sendArgs(t, r, c, "FUNCTION", "DUMP").(string)
	if !ok || dump == "" {
		t.Fatalf("DUMP = %v", dump)
	}
	sendArgs(t, r, c, "FUNCTION", "FLUSH")
	if got := sendArgs(t, r, c, "FUNCTION", "RESTORE", dump); got != "OK" {
		t.Fatalf("RESTORE = %v", got)
	}
	// The restored function works.
	sendArgs(t, r, c, "FCALL", "myset", "1", "k", "back")
	if got := sendArgs(t, r, c, "FCALL", "myget", "1", "k"); got != "back" {
		t.Fatalf("FCALL after restore = %v", got)
	}
}

func TestFunctionRestoreBadPayload(t *testing.T) {
	r, c := startData(t)
	got := sendArgs(t, r, c, "FUNCTION", "RESTORE", "garbage")
	if e, ok := got.(cmdErr); !ok || !strings.Contains(string(e), "checksum are wrong") {
		t.Fatalf("RESTORE bad = %v (%T)", got, got)
	}
}

func TestFunctionStats(t *testing.T) {
	r, c := startData(t)
	sendArgs(t, r, c, "FUNCTION", "LOAD", libGetSet)
	reply := asArray(t, sendArgs(t, r, c, "FUNCTION", "STATS"))
	fields := map[string]any{}
	for i := 0; i+1 < len(reply); i += 2 {
		fields[reply[i].(string)] = reply[i+1]
	}
	engines := asArray(t, fields["engines"])
	if engines[0] != "LUA" {
		t.Fatalf("engines = %v", engines)
	}
	lua := asArray(t, engines[1])
	luaFields := map[string]any{}
	for i := 0; i+1 < len(lua); i += 2 {
		luaFields[lua[i].(string)] = lua[i+1]
	}
	if luaFields["libraries_count"] != int64(1) || luaFields["functions_count"] != int64(2) {
		t.Fatalf("lua stats = %v", luaFields)
	}
}

func TestFunctionKill(t *testing.T) {
	r, c := startData(t)
	got := sendArgs(t, r, c, "FUNCTION", "KILL")
	if e, ok := got.(cmdErr); !ok || !strings.HasPrefix(string(e), "NOTBUSY") {
		t.Fatalf("KILL = %v", got)
	}
}
