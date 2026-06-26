package command

import (
	"crypto/sha1"
	"encoding/hex"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/tamnd/aki/lua"
	"github.com/tamnd/aki/networking"
	"github.com/tamnd/aki/resp"
)

// This file is the scripting bridge: it runs a Lua script on the engine from
// package lua, exposes the redis.* API, and converts values between Lua and RESP
// (spec 2064 doc 15 sections 6, 7 and 9). A script runs synchronously in the
// calling goroutine. redis.call executes a real command through the dispatch
// pipeline on an offline connection and converts the reply back to a Lua value.

// scriptCtx is the per-execution state a running script reaches through the Lua
// interpreter registry. It carries the database, the calling user for ACL, the
// read-only flag for EVAL_RO, the RESP version the script asked for with
// redis.setresp, and a count of write commands the script has issued.
type scriptCtx struct {
	d        *Dispatcher
	sess     *session
	db       int
	readonly bool
	resp     int
	writes   int
}

// checkScriptCompiles parses a script body and discards the result, so SCRIPT
// LOAD can reject a script that does not parse before it goes in the cache.
func checkScriptCompiles(body string) error {
	_, err := lua.Parse(body)
	return err
}

// sha1hex returns the lowercase hex SHA1 of a body, the script cache key.
func sha1hex(body string) string {
	sum := sha1.Sum([]byte(body))
	return hex.EncodeToString(sum[:])
}

// newScriptInterp builds a fresh interpreter wired with the redis table, the
// script context, and the runaway-script deadline hook. freg is non-nil only on
// FUNCTION LOAD and FCALL, where redis.register_function captures into it; for an
// EVAL script it is nil and register_function is absent. Each call gets a clean
// interpreter so one script never sees another's globals.
func (d *Dispatcher) newScriptInterp(ctx *Ctx, readonly bool, freg *funcReg) (*lua.Interp, *scriptCtx) {
	i := lua.New()
	sc := &scriptCtx{d: d, sess: ctx.sess, db: ctx.Conn.DB(), readonly: readonly, resp: 2}
	i.Registry["script"] = sc

	limit := d.luaTimeLimit()
	if limit > 0 {
		deadline := time.Now().Add(limit)
		i.SetHook(100, func() error {
			if time.Now().After(deadline) {
				return &lua.Error{Value: lua.String("BUSY Redis is busy running a script. You can only call SCRIPT KILL or SHUTDOWN NOSAVE.")}
			}
			return nil
		})
	}
	installRedis(i, sc, freg)
	return i, sc
}

// evalScript compiles and runs a script body with the given keys and args and
// writes the result to the client. readonly rejects write commands inside the
// script. It is the shared core of EVAL, EVALSHA, and their _RO forms.
func (d *Dispatcher) evalScript(ctx *Ctx, body string, keys, args [][]byte, readonly bool) {
	i, sc := d.newScriptInterp(ctx, readonly, nil)
	i.Globals().Set(lua.String("KEYS"), bytesToTable(keys))
	i.Globals().Set(lua.String("ARGV"), bytesToTable(args))

	rets, err := i.Run(body)
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

// scriptError renders a Lua error as the error string EVAL returns. A string
// that already looks like a Redis error (an uppercase code prefix) passes
// through; anything else is wrapped so the client sees a user-script error.
func scriptError(err error) string {
	le, ok := err.(*lua.Error)
	if !ok {
		return "ERR " + err.Error()
	}
	if tbl, ok := le.Value.(*lua.Table); ok {
		if e, ok := tbl.Get(lua.String("err")).(lua.String); ok {
			return string(e)
		}
	}
	msg := lua.ToString(le.Value)
	if hasErrorCode(msg) {
		return msg
	}
	return "ERR " + msg
}

// hasErrorCode reports whether a message already carries an error code. Redis
// derives the code purely from the presence of a space-delimited first token,
// not its letter case: luaPushErrorBuff prepends a generic "ERR" only when the
// message has no space at all, and otherwise treats the leading word as the
// code and keeps the string verbatim (so "my error" stays "my error", same as
// "WRONGTYPE nope"). aki used to require the first word to be uppercase, which
// wrongly wrapped lowercase multi-word messages like error_reply('my error').
func hasErrorCode(msg string) bool {
	return strings.IndexByte(msg, ' ') > 0
}

// bytesToTable builds a 1-based Lua array table from a slice of byte strings.
func bytesToTable(items [][]byte) *lua.Table {
	t := lua.NewTable()
	for _, it := range items {
		t.Append(lua.String(it))
	}
	return t
}

// installRedis builds the redis.* table and installs it as a global. When freg
// is non-nil it also installs redis.register_function, which a function library's
// top-level code calls to register its callbacks.
func installRedis(i *lua.Interp, sc *scriptCtx, freg *funcReg) {
	r := lua.NewTable()
	set := func(name string, fn lua.GoFunc) {
		r.Set(lua.String(name), lua.NewGoFunc(name, fn))
	}
	if freg != nil {
		set("register_function", func(_ *lua.Interp, args []lua.Value) ([]lua.Value, error) {
			return freg.register(args)
		})
	}
	set("call", func(in *lua.Interp, args []lua.Value) ([]lua.Value, error) {
		return redisCall(in, sc, args, true)
	})
	set("pcall", func(in *lua.Interp, args []lua.Value) ([]lua.Value, error) {
		return redisCall(in, sc, args, false)
	})
	set("error_reply", redisErrorReply)
	set("status_reply", redisStatusReply)
	set("sha1hex", redisSha1hex)
	set("log", redisLog)
	set("setresp", func(_ *lua.Interp, args []lua.Value) ([]lua.Value, error) {
		return redisSetResp(sc, args)
	})
	set("replicate_commands", func(_ *lua.Interp, _ []lua.Value) ([]lua.Value, error) {
		return []lua.Value{lua.Bool(true)}, nil
	})
	set("set_repl", func(_ *lua.Interp, _ []lua.Value) ([]lua.Value, error) { return nil, nil })
	set("breakpoint", func(_ *lua.Interp, _ []lua.Value) ([]lua.Value, error) {
		return []lua.Value{lua.Bool(false)}, nil
	})
	set("debug", func(_ *lua.Interp, _ []lua.Value) ([]lua.Value, error) { return nil, nil })

	r.Set(lua.String("LOG_DEBUG"), lua.Number(0))
	r.Set(lua.String("LOG_VERBOSE"), lua.Number(1))
	r.Set(lua.String("LOG_NOTICE"), lua.Number(2))
	r.Set(lua.String("LOG_WARNING"), lua.Number(3))
	r.Set(lua.String("REPL_NONE"), lua.Number(0))
	r.Set(lua.String("REPL_AOF"), lua.Number(1))
	r.Set(lua.String("REPL_REPLICA"), lua.Number(2))
	r.Set(lua.String("REPL_SLAVE"), lua.Number(2))
	r.Set(lua.String("REPL_ALL"), lua.Number(3))
	r.Set(lua.String("REDIS_VERSION"), lua.String("7.2.0"))
	r.Set(lua.String("REDIS_VERSION_NUM"), lua.Number(0x070200))

	i.Globals().Set(lua.String("redis"), r)
	i.Globals().Set(lua.String("server"), r)

	lua.OpenRedisLibs(i)
}

// redisCall runs a Redis command from a script. raise selects redis.call
// behavior (raise a Lua error on a command error) versus redis.pcall (return the
// error as a {err=...} table). On any failure it routes through fail, which
// either raises a Lua error or returns the error table as a value to match the
// two functions.
func redisCall(_ *lua.Interp, sc *scriptCtx, args []lua.Value, raise bool) ([]lua.Value, error) {
	fail := func(msg string) ([]lua.Value, error) {
		full := msg
		if !hasErrorCode(full) {
			full = "ERR " + full
		}
		if raise {
			return nil, &lua.Error{Value: errTable(full)}
		}
		return []lua.Value{errTable(full)}, nil
	}

	if len(args) == 0 {
		return fail("Please specify at least one argument for this redis lib call")
	}
	argv := make([][]byte, 0, len(args))
	for _, a := range args {
		switch v := a.(type) {
		case lua.String:
			argv = append(argv, []byte(v))
		case lua.Number:
			argv = append(argv, []byte(lua.ToString(v)))
		default:
			return fail("Lua redis lib command arguments must be strings or integers")
		}
	}

	cmd, lookupErr := sc.d.table.lookup(argv)
	if lookupErr != nil {
		return fail("Unknown Redis command called from script")
	}
	if cmd.Flags.Has(FlagNoScript) || cmd.Flags.Has(FlagBlocking) || cmd.Flags.Has(FlagPubSub) {
		return fail("This Redis command is not allowed from script")
	}
	if !checkArity(cmd, len(argv)) {
		return fail("Wrong number of args calling Redis command from script")
	}
	if sc.readonly && cmd.Flags.Has(FlagWrite) {
		return fail("Write commands are not allowed from read-only scripts.")
	}

	conn := networking.NewOfflineConn()
	conn.SetDB(sc.db)
	conn.SetProto(sc.resp)
	csess := &session{authenticated: true, user: sc.sess.user, username: sc.sess.username}
	conn.SetSession(csess)
	cctx := &Ctx{Conn: conn, Argv: argv, d: sc.d, sess: csess}

	if msg := sc.d.aclEnforce(conn, csess, cmd, argv); msg != "" {
		return fail(msg)
	}
	sc.d.runCommand(cctx, cmd)
	if cmd.Flags.Has(FlagWrite) {
		sc.writes++
	}

	val, _, err := resp.Decode(conn.OutBytes(), 0)
	if err != nil {
		return fail("Error decoding command reply")
	}
	if val.Type == resp.TypeError {
		if raise {
			return nil, &lua.Error{Value: errTable(val.Err)}
		}
		return []lua.Value{errTable(val.Err)}, nil
	}
	return []lua.Value{respToLua(val, sc.resp)}, nil
}

func errTable(msg string) lua.Value {
	t := lua.NewTable()
	t.Set(lua.String("err"), lua.String(msg))
	return t
}

func redisErrorReply(_ *lua.Interp, args []lua.Value) ([]lua.Value, error) {
	s := ""
	if v, ok := nthArg(args, 0).(lua.String); ok {
		s = string(v)
	}
	if !hasErrorCode(s) {
		s = "ERR " + s
	}
	return []lua.Value{errTable(s)}, nil
}

func redisStatusReply(_ *lua.Interp, args []lua.Value) ([]lua.Value, error) {
	t := lua.NewTable()
	s := ""
	if v, ok := nthArg(args, 0).(lua.String); ok {
		s = string(v)
	}
	t.Set(lua.String("ok"), lua.String(s))
	return []lua.Value{t}, nil
}

func redisSha1hex(_ *lua.Interp, args []lua.Value) ([]lua.Value, error) {
	s := ""
	switch v := nthArg(args, 0).(type) {
	case lua.String:
		s = string(v)
	case lua.Number:
		s = lua.ToString(v)
	}
	return []lua.Value{lua.String(sha1hex(s))}, nil
}

// redisLog accepts a level and message and drops them. aki has no script log
// sink yet, but the function must exist so scripts that call it do not fail.
func redisLog(_ *lua.Interp, _ []lua.Value) ([]lua.Value, error) {
	return nil, nil
}

func redisSetResp(sc *scriptCtx, args []lua.Value) ([]lua.Value, error) {
	n, ok := nthArg(args, 0).(lua.Number)
	if !ok || (int(n) != 2 && int(n) != 3) {
		return nil, &lua.Error{Value: lua.String("RESP version must be 2 or 3.")}
	}
	sc.resp = int(n)
	return nil, nil
}

func nthArg(args []lua.Value, i int) lua.Value {
	if i < len(args) {
		return args[i]
	}
	return lua.Nil
}

// luaTimeLimit reads the lua-time-limit config (milliseconds) as a duration. A
// value of 0 disables the deadline, matching real Redis where 0 means no limit.
func (d *Dispatcher) luaTimeLimit() time.Duration {
	ms, err := strconv.Atoi(d.confValue("lua-time-limit", "5000"))
	if err != nil || ms <= 0 {
		return 0
	}
	return time.Duration(ms) * time.Millisecond
}

// respToLua converts a command reply into the Lua value a script sees. The rules
// follow spec 2064 doc 15 section 7. proto selects the RESP2 or RESP3 mapping the
// script asked for with redis.setresp; the default is RESP2.
func respToLua(v resp.RESPValue, proto int) lua.Value {
	switch v.Type {
	case resp.TypeSimpleString:
		t := lua.NewTable()
		t.Set(lua.String("ok"), lua.String(v.Str))
		return t
	case resp.TypeError:
		return errTable(v.Err)
	case resp.TypeInteger:
		return lua.Number(v.Integer)
	case resp.TypeBulkString, resp.TypeVerbatim:
		if v.IsNull {
			return lua.Bool(false)
		}
		return lua.String(v.Str)
	case resp.TypeNull:
		return lua.Bool(false)
	case resp.TypeArray, resp.TypeSet, resp.TypePush:
		if v.IsNull {
			return lua.Bool(false)
		}
		if proto >= 3 && v.Type == resp.TypeSet {
			t := lua.NewTable()
			inner := lua.NewTable()
			for _, e := range v.Elems {
				inner.Set(respToLua(e, proto), lua.Bool(true))
			}
			t.Set(lua.String("set"), inner)
			return t
		}
		if proto >= 3 && v.Type == resp.TypePush {
			t := lua.NewTable()
			for _, e := range v.Elems {
				t.Append(respToLua(e, proto))
			}
			return t
		}
		t := lua.NewTable()
		for _, e := range v.Elems {
			t.Append(respToLua(e, proto))
		}
		return t
	case resp.TypeMap:
		if proto >= 3 {
			t := lua.NewTable()
			inner := lua.NewTable()
			for _, kv := range v.Map {
				inner.Set(respToLua(kv[0], proto), respToLua(kv[1], proto))
			}
			t.Set(lua.String("map"), inner)
			return t
		}
		t := lua.NewTable()
		for _, kv := range v.Map {
			t.Append(respToLua(kv[0], proto))
			t.Append(respToLua(kv[1], proto))
		}
		return t
	case resp.TypeBool:
		if proto >= 3 {
			return lua.Bool(v.Bool)
		}
		if v.Bool {
			return lua.Number(1)
		}
		return lua.Bool(false)
	case resp.TypeDouble:
		if proto >= 3 {
			t := lua.NewTable()
			t.Set(lua.String("double"), lua.Number(v.Float))
			return t
		}
		return lua.String(formatDouble(v.Float))
	case resp.TypeBigNumber:
		if proto >= 3 {
			t := lua.NewTable()
			t.Set(lua.String("big_number"), lua.String(v.BigInt.String()))
			return t
		}
		return lua.String(v.BigInt.String())
	default:
		return lua.Bool(false)
	}
}

// luaToRESP writes a script's return value to the client encoder. The rules
// follow spec 2064 doc 15 section 9. A table is inspected for the special status,
// error, double, map, set, and big_number fields before it is treated as a plain
// 1-based array. proto selects the RESP version of the output, the client's own
// negotiated version.
func luaToRESP(enc *resp.Encoder, v lua.Value, proto int) {
	switch x := v.(type) {
	case nil:
		enc.WriteNull()
	case lua.Bool:
		if proto >= 3 {
			enc.WriteBool(bool(x))
			return
		}
		if x {
			enc.WriteInteger(1)
		} else {
			enc.WriteNull()
		}
	case lua.Number:
		enc.WriteInteger(int64(x))
	case lua.String:
		enc.WriteBulkString([]byte(x))
	case *lua.Table:
		luaTableToRESP(enc, x, proto)
	default:
		if !lua.Truthy(v) {
			enc.WriteNull()
			return
		}
		enc.WriteNull()
	}
}

// luaTableToRESP encodes a Lua table following the field rules a script may use
// to ask for a specific reply shape, then falls back to a plain array.
func luaTableToRESP(enc *resp.Encoder, t *lua.Table, proto int) {
	if e, ok := t.Get(lua.String("err")).(lua.String); ok {
		// A returned {err=...} table is sent verbatim. Redis converts it with
		// addReplyErrorFormatEx("-%s", msg) and never inserts a generic ERR
		// code, so {err='oneword'} stays "oneword", unlike redis.error_reply
		// which does prepend ERR for a code-less (space-less) message.
		enc.WriteError(string(e))
		return
	}
	if s, ok := t.Get(lua.String("ok")).(lua.String); ok {
		enc.WriteStatus(string(s))
		return
	}
	if d, ok := t.Get(lua.String("double")).(lua.Number); ok {
		if proto >= 3 {
			enc.WriteDouble(float64(d))
		} else {
			enc.WriteBulkString([]byte(formatDouble(float64(d))))
		}
		return
	}
	if bn, ok := t.Get(lua.String("big_number")).(lua.String); ok {
		if proto >= 3 {
			if n, ok := new(big.Int).SetString(strings.TrimSpace(string(bn)), 10); ok {
				enc.WriteBigNumber(n)
				return
			}
		}
		enc.WriteBulkString([]byte(bn))
		return
	}
	if m, ok := t.Get(lua.String("map")).(*lua.Table); ok {
		luaMapToRESP(enc, m, proto)
		return
	}
	if s, ok := t.Get(lua.String("set")).(*lua.Table); ok {
		luaSetToRESP(enc, s, proto)
		return
	}
	n := t.Len()
	enc.WriteArrayLen(n)
	for idx := 1; idx <= n; idx++ {
		luaToRESP(enc, t.Get(lua.Number(idx)), proto)
	}
}

// luaMapToRESP encodes a {map=...} table as a RESP3 map, or a flat array of
// key then value pairs on RESP2.
func luaMapToRESP(enc *resp.Encoder, m *lua.Table, proto int) {
	keys, vals := tablePairs(m)
	if proto >= 3 {
		enc.WriteMapLen(len(keys))
	} else {
		enc.WriteArrayLen(len(keys) * 2)
	}
	for i := range keys {
		luaToRESP(enc, keys[i], proto)
		luaToRESP(enc, vals[i], proto)
	}
}

// luaSetToRESP encodes a {set=...} table as a RESP3 set, or a flat array of the
// members on RESP2. Only keys with a truthy value are members.
func luaSetToRESP(enc *resp.Encoder, s *lua.Table, proto int) {
	keys, vals := tablePairs(s)
	members := keys[:0]
	for i := range keys {
		if lua.Truthy(vals[i]) {
			members = append(members, keys[i])
		}
	}
	if proto >= 3 {
		enc.WriteSetLen(len(members))
	} else {
		enc.WriteArrayLen(len(members))
	}
	for _, k := range members {
		luaToRESP(enc, k, proto)
	}
}

// tablePairs returns the hash-part keys and values of a table in the table's own
// deterministic traversal order, so a map or set reply is stable.
func tablePairs(t *lua.Table) (keys, vals []lua.Value) {
	for _, k := range t.Keys() {
		keys = append(keys, k)
		vals = append(vals, t.Get(k))
	}
	return keys, vals
}

// formatDouble renders a float the way Redis renders a script double on RESP2,
// as a plain decimal with the integer fast path matching real Redis.
func formatDouble(f float64) string {
	if f == float64(int64(f)) {
		return strconv.FormatInt(int64(f), 10)
	}
	return strconv.FormatFloat(f, 'g', 17, 64)
}
