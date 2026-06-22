package lua

import (
	"math"
	"sort"
	"strconv"
	"strings"
)

// This file installs the sandboxed standard library: the base global functions,
// the table library, and the math library. The string library lives in
// strlib.go. The set follows what Lua 5.1 exposes minus the unsafe pieces (io,
// os file access, require, coroutine) that the script sandbox forbids.

// openBase installs all standard library tables and globals into the interpreter.
func openBase(i *Interp) {
	g := i.globals
	reg := func(name string, fn GoFunc) {
		g.Set(String(name), &Function{gofn: fn, name: name})
	}

	reg("type", biType)
	reg("tostring", biToString)
	reg("tonumber", biToNumber)
	reg("pairs", biPairs)
	reg("ipairs", biIpairs)
	reg("next", biNext)
	reg("select", biSelect)
	reg("error", biError)
	reg("assert", biAssert)
	reg("pcall", biPcall)
	reg("xpcall", biXpcall)
	reg("rawget", biRawget)
	reg("rawset", biRawset)
	reg("rawequal", biRawequal)
	reg("rawlen", biRawlen)
	reg("setmetatable", biSetmetatable)
	reg("getmetatable", biGetmetatable)
	reg("unpack", biUnpack)
	g.Set(String("_G"), g)

	openTable(i)
	openMath(i)
	openString(i)
}

func biType(_ *Interp, args []Value) ([]Value, error) {
	if len(args) == 0 {
		return nil, argError(1, "type", "value expected")
	}
	return []Value{String(args[0].luaType())}, nil
}

func biToString(i *Interp, args []Value) ([]Value, error) {
	v := nth(args, 0)
	if t, ok := v.(*Table); ok && t.meta != nil {
		if mm, ok := t.meta.Get(String("__tostring")).(*Function); ok {
			rets, err := i.call(mm, []Value{v}, 1)
			if err != nil {
				return nil, err
			}
			return []Value{nth(rets, 0)}, nil
		}
	}
	return []Value{String(ToString(v))}, nil
}

func biToNumber(_ *Interp, args []Value) ([]Value, error) {
	v := nth(args, 0)
	if base := optInt(args, 2, 10); base != 10 {
		s, ok := v.(String)
		if !ok {
			return []Value{Nil}, nil
		}
		n, err := strconv.ParseInt(strings.TrimSpace(string(s)), base, 64)
		if err != nil {
			return []Value{Nil}, nil
		}
		return []Value{Number(n)}, nil
	}
	if f, ok := toNumber(v); ok {
		return []Value{Number(f)}, nil
	}
	return []Value{Nil}, nil
}

func biPairs(_ *Interp, args []Value) ([]Value, error) {
	t, ok := nth(args, 0).(*Table)
	if !ok {
		return nil, argError(1, "pairs", "table expected, got "+nth(args, 0).luaType())
	}
	return []Value{&Function{gofn: biNext, name: "next"}, t, Nil}, nil
}

func biIpairs(_ *Interp, args []Value) ([]Value, error) {
	t, ok := nth(args, 0).(*Table)
	if !ok {
		return nil, argError(1, "ipairs", "table expected, got "+nth(args, 0).luaType())
	}
	iter := func(_ *Interp, a []Value) ([]Value, error) {
		tbl := a[0].(*Table)
		n := int(a[1].(Number)) + 1
		v := tbl.Get(Number(n))
		if _, ok := v.(nilValue); ok {
			return []Value{Nil}, nil
		}
		return []Value{Number(n), v}, nil
	}
	return []Value{&Function{gofn: iter, name: "ipairs_iter"}, t, Number(0)}, nil
}

// biNext walks a table's array part then its hash part in a stable order.
func biNext(_ *Interp, args []Value) ([]Value, error) {
	t, ok := nth(args, 0).(*Table)
	if !ok {
		return nil, argError(1, "next", "table expected, got "+nth(args, 0).luaType())
	}
	key := nth(args, 1)
	order := t.iterOrder()
	if _, ok := key.(nilValue); ok {
		if len(order) == 0 {
			return []Value{Nil}, nil
		}
		return []Value{order[0], t.Get(order[0])}, nil
	}
	key = normalizeKey(key)
	for idx, k := range order {
		if valuesEqual(k, key) {
			if idx+1 < len(order) {
				nk := order[idx+1]
				return []Value{nk, t.Get(nk)}, nil
			}
			return []Value{Nil}, nil
		}
	}
	return []Value{Nil}, nil
}

// iterOrder returns all keys of a table, array part first then sorted hash keys.
func (t *Table) iterOrder() []Value {
	out := make([]Value, 0, len(t.arr)+len(t.hash))
	for idx, v := range t.arr {
		if _, ok := v.(nilValue); ok {
			continue
		}
		out = append(out, Number(idx+1))
	}
	out = append(out, t.hashKeys()...)
	return out
}

func biSelect(_ *Interp, args []Value) ([]Value, error) {
	sel := nth(args, 0)
	if s, ok := sel.(String); ok && s == "#" {
		return []Value{Number(len(args) - 1)}, nil
	}
	n, ok := toNumber(sel)
	if !ok {
		return nil, argError(1, "select", "number expected")
	}
	idx := int(n)
	if idx < 0 {
		idx = len(args) + idx
	}
	if idx < 1 {
		return nil, argError(1, "select", "index out of range")
	}
	if idx >= len(args) {
		return nil, nil
	}
	return args[idx:], nil
}

func biError(_ *Interp, args []Value) ([]Value, error) {
	v := nth(args, 0)
	level := optInt(args, 2, 1)
	if s, ok := v.(String); ok && level > 0 {
		// Lua prepends position info; the engine keeps it simple and passes the
		// message through unchanged so callers see the raw string.
		return nil, &Error{Value: s}
	}
	return nil, &Error{Value: v}
}

func biAssert(_ *Interp, args []Value) ([]Value, error) {
	if len(args) == 0 || !Truthy(args[0]) {
		msg := nth(args, 1)
		if _, ok := msg.(nilValue); ok {
			return nil, runtimeErr("assertion failed!")
		}
		return nil, &Error{Value: msg}
	}
	return args, nil
}

func biPcall(i *Interp, args []Value) ([]Value, error) {
	if len(args) == 0 {
		return nil, argError(1, "pcall", "value expected")
	}
	rets, err := i.call(args[0], args[1:], -1)
	if err != nil {
		return []Value{Bool(false), asError(err).Value}, nil
	}
	return append([]Value{Bool(true)}, rets...), nil
}

func biXpcall(i *Interp, args []Value) ([]Value, error) {
	if len(args) < 2 {
		return nil, argError(2, "xpcall", "value expected")
	}
	handler := args[1]
	rets, err := i.call(args[0], nil, -1)
	if err != nil {
		hret, herr := i.call(handler, []Value{asError(err).Value}, 1)
		if herr != nil {
			return nil, herr
		}
		return []Value{Bool(false), nth(hret, 0)}, nil
	}
	return append([]Value{Bool(true)}, rets...), nil
}

func biRawget(_ *Interp, args []Value) ([]Value, error) {
	t, ok := nth(args, 0).(*Table)
	if !ok {
		return nil, argError(1, "rawget", "table expected")
	}
	return []Value{t.Get(nth(args, 1))}, nil
}

func biRawset(_ *Interp, args []Value) ([]Value, error) {
	t, ok := nth(args, 0).(*Table)
	if !ok {
		return nil, argError(1, "rawset", "table expected")
	}
	t.Set(nth(args, 1), nth(args, 2))
	return []Value{t}, nil
}

func biRawequal(_ *Interp, args []Value) ([]Value, error) {
	return []Value{Bool(valuesEqual(nth(args, 0), nth(args, 1)))}, nil
}

func biRawlen(_ *Interp, args []Value) ([]Value, error) {
	switch x := nth(args, 0).(type) {
	case String:
		return []Value{Number(len(x))}, nil
	case *Table:
		return []Value{Number(x.Len())}, nil
	default:
		return nil, argError(1, "rawlen", "table or string expected")
	}
}

func biSetmetatable(_ *Interp, args []Value) ([]Value, error) {
	t, ok := nth(args, 0).(*Table)
	if !ok {
		return nil, argError(1, "setmetatable", "table expected, got "+nth(args, 0).luaType())
	}
	switch m := nth(args, 1).(type) {
	case nilValue:
		t.meta = nil
	case *Table:
		t.meta = m
	default:
		return nil, argError(2, "setmetatable", "nil or table expected")
	}
	return []Value{t}, nil
}

func biGetmetatable(_ *Interp, args []Value) ([]Value, error) {
	t, ok := nth(args, 0).(*Table)
	if !ok || t.meta == nil {
		return []Value{Nil}, nil
	}
	return []Value{t.meta}, nil
}

func biUnpack(_ *Interp, args []Value) ([]Value, error) {
	t, ok := nth(args, 0).(*Table)
	if !ok {
		return nil, argError(1, "unpack", "table expected")
	}
	from := optInt(args, 2, 1)
	to := optInt(args, 3, t.Len())
	var out []Value
	for k := from; k <= to; k++ {
		out = append(out, t.Get(Number(k)))
	}
	return out, nil
}

// openTable installs the table library.
func openTable(i *Interp) {
	lib := NewTable()
	set := func(name string, fn GoFunc) {
		lib.Set(String(name), &Function{gofn: fn, name: "table." + name})
	}
	set("insert", tblInsert)
	set("remove", tblRemove)
	set("concat", tblConcat)
	set("sort", tblSort)
	set("getn", tblGetn)
	i.globals.Set(String("table"), lib)
}

func tblInsert(_ *Interp, args []Value) ([]Value, error) {
	t, ok := nth(args, 0).(*Table)
	if !ok {
		return nil, argError(1, "insert", "table expected")
	}
	switch len(args) {
	case 2:
		t.Append(args[1])
	case 3:
		pos := int(mustNum(args[1]))
		n := t.Len()
		for k := n; k >= pos; k-- {
			t.Set(Number(k+1), t.Get(Number(k)))
		}
		t.Set(Number(pos), args[2])
	default:
		return nil, runtimeErr("wrong number of arguments to 'insert'")
	}
	return nil, nil
}

func tblRemove(_ *Interp, args []Value) ([]Value, error) {
	t, ok := nth(args, 0).(*Table)
	if !ok {
		return nil, argError(1, "remove", "table expected")
	}
	n := t.Len()
	pos := optInt(args, 2, n)
	if n == 0 {
		return []Value{Nil}, nil
	}
	removed := t.Get(Number(pos))
	for k := pos; k < n; k++ {
		t.Set(Number(k), t.Get(Number(k+1)))
	}
	t.Set(Number(n), Nil)
	return []Value{removed}, nil
}

func tblConcat(_ *Interp, args []Value) ([]Value, error) {
	t, ok := nth(args, 0).(*Table)
	if !ok {
		return nil, argError(1, "concat", "table expected")
	}
	sep := ""
	if s, ok := nth(args, 1).(String); ok {
		sep = string(s)
	}
	from := optInt(args, 3, 1)
	to := optInt(args, 4, t.Len())
	var b strings.Builder
	for k := from; k <= to; k++ {
		if k > from {
			b.WriteString(sep)
		}
		s, ok := concatString(t.Get(Number(k)))
		if !ok {
			return nil, runtimeErr("invalid value (at index %d) in table for 'concat'", k)
		}
		b.WriteString(s)
	}
	return []Value{String(b.String())}, nil
}

func tblSort(i *Interp, args []Value) ([]Value, error) {
	t, ok := nth(args, 0).(*Table)
	if !ok {
		return nil, argError(1, "sort", "table expected")
	}
	n := t.Len()
	items := make([]Value, n)
	for k := 1; k <= n; k++ {
		items[k-1] = t.Get(Number(k))
	}
	var cmp *Function
	if f, ok := nth(args, 1).(*Function); ok {
		cmp = f
	}
	var sortErr error
	sort.SliceStable(items, func(a, b int) bool {
		if sortErr != nil {
			return false
		}
		if cmp != nil {
			rets, err := i.call(cmp, []Value{items[a], items[b]}, 1)
			if err != nil {
				sortErr = err
				return false
			}
			return Truthy(nth(rets, 0))
		}
		less, err := i.compare(tLt, items[a], items[b])
		if err != nil {
			sortErr = err
			return false
		}
		return Truthy(less)
	})
	if sortErr != nil {
		return nil, sortErr
	}
	for k := 1; k <= n; k++ {
		t.Set(Number(k), items[k-1])
	}
	return nil, nil
}

func tblGetn(_ *Interp, args []Value) ([]Value, error) {
	t, ok := nth(args, 0).(*Table)
	if !ok {
		return nil, argError(1, "getn", "table expected")
	}
	return []Value{Number(t.Len())}, nil
}

func mustNum(v Value) float64 {
	f, _ := toNumber(v)
	return f
}

// openMath installs the math library.
func openMath(i *Interp) {
	lib := NewTable()
	set := func(name string, fn GoFunc) {
		lib.Set(String(name), &Function{gofn: fn, name: "math." + name})
	}
	lib.Set(String("pi"), Number(math.Pi))
	lib.Set(String("huge"), Number(math.Inf(1)))
	lib.Set(String("maxinteger"), Number(math.MaxInt64))
	lib.Set(String("mininteger"), Number(math.MinInt64))

	one := func(name string, fn func(float64) float64) {
		set(name, func(_ *Interp, args []Value) ([]Value, error) {
			f, err := checkNumber(args, 1, name)
			if err != nil {
				return nil, err
			}
			return []Value{Number(fn(f))}, nil
		})
	}
	one("abs", math.Abs)
	one("ceil", math.Ceil)
	one("floor", math.Floor)
	one("sqrt", math.Sqrt)
	one("sin", math.Sin)
	one("cos", math.Cos)
	one("tan", math.Tan)
	one("asin", math.Asin)
	one("acos", math.Acos)
	one("atan", math.Atan)
	one("exp", math.Exp)
	one("rad", func(d float64) float64 { return d * math.Pi / 180 })
	one("deg", func(r float64) float64 { return r * 180 / math.Pi })
	set("log", mathLog)
	set("max", mathMax)
	set("min", mathMin)
	set("fmod", mathFmod)
	set("modf", mathModf)
	set("pow", mathPow)
	set("random", mathRandom)
	set("randomseed", func(_ *Interp, _ []Value) ([]Value, error) { return nil, nil })
	i.globals.Set(String("math"), lib)
}

func mathLog(_ *Interp, args []Value) ([]Value, error) {
	x, err := checkNumber(args, 1, "log")
	if err != nil {
		return nil, err
	}
	if len(args) >= 2 {
		base, err := checkNumber(args, 2, "log")
		if err != nil {
			return nil, err
		}
		return []Value{Number(math.Log(x) / math.Log(base))}, nil
	}
	return []Value{Number(math.Log(x))}, nil
}

func mathMax(_ *Interp, args []Value) ([]Value, error) {
	if len(args) == 0 {
		return nil, argError(1, "max", "number expected, got no value")
	}
	best, err := checkNumber(args, 1, "max")
	if err != nil {
		return nil, err
	}
	for k := 2; k <= len(args); k++ {
		f, err := checkNumber(args, k, "max")
		if err != nil {
			return nil, err
		}
		if f > best {
			best = f
		}
	}
	return []Value{Number(best)}, nil
}

func mathMin(_ *Interp, args []Value) ([]Value, error) {
	if len(args) == 0 {
		return nil, argError(1, "min", "number expected, got no value")
	}
	best, err := checkNumber(args, 1, "min")
	if err != nil {
		return nil, err
	}
	for k := 2; k <= len(args); k++ {
		f, err := checkNumber(args, k, "min")
		if err != nil {
			return nil, err
		}
		if f < best {
			best = f
		}
	}
	return []Value{Number(best)}, nil
}

func mathFmod(_ *Interp, args []Value) ([]Value, error) {
	a, err := checkNumber(args, 1, "fmod")
	if err != nil {
		return nil, err
	}
	b, err := checkNumber(args, 2, "fmod")
	if err != nil {
		return nil, err
	}
	return []Value{Number(math.Mod(a, b))}, nil
}

func mathModf(_ *Interp, args []Value) ([]Value, error) {
	f, err := checkNumber(args, 1, "modf")
	if err != nil {
		return nil, err
	}
	ip, fp := math.Modf(f)
	return []Value{Number(ip), Number(fp)}, nil
}

func mathPow(_ *Interp, args []Value) ([]Value, error) {
	a, err := checkNumber(args, 1, "pow")
	if err != nil {
		return nil, err
	}
	b, err := checkNumber(args, 2, "pow")
	if err != nil {
		return nil, err
	}
	return []Value{Number(math.Pow(a, b))}, nil
}

// mathRandom uses the interpreter's deterministic PRNG so a host can seed it per
// script. With no PRNG installed it returns 0, which keeps replicated scripts
// from depending on a hidden source of entropy.
func mathRandom(i *Interp, args []Value) ([]Value, error) {
	r := i.rand()
	switch len(args) {
	case 0:
		return []Value{Number(r)}, nil
	case 1:
		m, err := checkNumber(args, 1, "random")
		if err != nil {
			return nil, err
		}
		return []Value{Number(math.Floor(r*m) + 1)}, nil
	default:
		lo, err := checkNumber(args, 1, "random")
		if err != nil {
			return nil, err
		}
		hi, err := checkNumber(args, 2, "random")
		if err != nil {
			return nil, err
		}
		return []Value{Number(math.Floor(r*(hi-lo+1)) + lo)}, nil
	}
}

// rand returns the next pseudo-random number in [0,1). The generator is a small
// xorshift seeded from the Registry so a host controls determinism.
func (i *Interp) rand() float64 {
	state, _ := i.Registry["randstate"].(uint64)
	if state == 0 {
		state = 0x2545F4914F6CDD1D
	}
	state ^= state << 13
	state ^= state >> 7
	state ^= state << 17
	i.Registry["randstate"] = state
	return float64(state>>11) / float64(1<<53)
}
