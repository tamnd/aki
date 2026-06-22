package lua

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
)

// This file defines the runtime value model for the Lua engine. Lua has a small
// set of types: nil, boolean, number, string, table, and function. The Value
// interface is the common type; concrete values are lightweight wrappers so the
// interpreter can pass them by value where it helps.

// Value is any Lua runtime value. Nil is represented by the Nil singleton, not
// by a Go nil, so a stored Value is never the untyped nil.
type Value interface{ luaType() string }

// Nil is the Lua nil value.
type nilValue struct{}

// Nil is the single nil instance.
var Nil Value = nilValue{}

// Bool is a Lua boolean.
type Bool bool

// Number is a Lua number (Lua 5.1 has one numeric type, a float64).
type Number float64

// String is a Lua string. Lua strings are byte strings, so this wraps a Go
// string which may hold arbitrary bytes.
type String string

func (nilValue) luaType() string { return "nil" }
func (Bool) luaType() string     { return "boolean" }
func (Number) luaType() string   { return "number" }
func (String) luaType() string   { return "string" }
func (*Table) luaType() string   { return "table" }
func (*Function) luaType() string {
	return "function"
}

// Table is a Lua table: an array part for dense integer keys plus a hash part
// for everything else. The array part holds keys 1..len(arr); the hash part
// holds all other keys. A table may carry a metatable.
type Table struct {
	arr  []Value
	hash map[Value]Value
	meta *Table
}

// NewTable returns an empty table.
func NewTable() *Table {
	return &Table{}
}

// Get returns the value at key, or Nil if absent. Raw access, no metamethods.
func (t *Table) Get(key Value) Value {
	if n, ok := key.(Number); ok {
		if i := int(n); Number(i) == n && i >= 1 && i <= len(t.arr) {
			return t.arr[i-1]
		}
	}
	if t.hash == nil {
		return Nil
	}
	key = normalizeKey(key)
	if v, ok := t.hash[key]; ok {
		return v
	}
	return Nil
}

// Set stores value at key. Setting Nil removes the key. Raw access, no
// metamethods. Integer keys that extend the array part are absorbed into it, and
// a following run already in the hash part migrates over.
func (t *Table) Set(key, val Value) {
	if n, ok := key.(Number); ok {
		if i := int(n); Number(i) == n && i >= 1 {
			t.setInt(i, val)
			return
		}
	}
	key = normalizeKey(key)
	if _, ok := val.(nilValue); ok {
		delete(t.hash, key)
		return
	}
	if t.hash == nil {
		t.hash = map[Value]Value{}
	}
	t.hash[key] = val
}

func (t *Table) setInt(i int, val Value) {
	_, isNil := val.(nilValue)
	switch {
	case i >= 1 && i <= len(t.arr):
		t.arr[i-1] = val
		if isNil && i == len(t.arr) {
			t.shrink()
		}
	case i == len(t.arr)+1 && !isNil:
		t.arr = append(t.arr, val)
		t.absorbFromHash()
	default:
		key := Number(i)
		if isNil {
			delete(t.hash, key)
			return
		}
		if t.hash == nil {
			t.hash = map[Value]Value{}
		}
		t.hash[key] = val
	}
}

// absorbFromHash pulls keys len(arr)+1, +2, ... out of the hash part into the
// array part after an append makes them contiguous.
func (t *Table) absorbFromHash() {
	for {
		k := Number(len(t.arr) + 1)
		v, ok := t.hash[k]
		if !ok {
			return
		}
		t.arr = append(t.arr, v)
		delete(t.hash, k)
	}
}

// shrink drops trailing nils from the array part.
func (t *Table) shrink() {
	for len(t.arr) > 0 {
		if _, ok := t.arr[len(t.arr)-1].(nilValue); !ok {
			return
		}
		t.arr = t.arr[:len(t.arr)-1]
	}
}

// Len returns a border of the table, the Lua # operator on a table. For a
// sequence this is its length.
func (t *Table) Len() int {
	return len(t.arr)
}

// Append adds a value at the end of the array part.
func (t *Table) Append(v Value) {
	t.arr = append(t.arr, v)
	t.absorbFromHash()
}

// normalizeKey collapses a float key with an integer value to its integer form
// so that t[1] and t[1.0] address the same slot.
func normalizeKey(key Value) Value {
	if n, ok := key.(Number); ok {
		if i := math.Trunc(float64(n)); i == float64(n) && !math.IsInf(i, 0) {
			return Number(i)
		}
	}
	return key
}

// iterArray exposes the array part for ipairs-style iteration.
func (t *Table) iterArray() []Value { return t.arr }

// Keys returns every key of the table in deterministic order, the array part
// first then the hash part sorted. Host code uses it to walk a script-built map
// or set table in a stable order.
func (t *Table) Keys() []Value { return t.iterOrder() }

// hashKeys returns the hash-part keys in a stable order so that traversal and
// serialization are deterministic.
func (t *Table) hashKeys() []Value {
	keys := make([]Value, 0, len(t.hash))
	for k := range t.hash {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keyLess(keys[i], keys[j])
	})
	return keys
}

// keyLess gives a total order over table keys for deterministic traversal:
// numbers before strings before booleans before everything else, each group
// ordered naturally.
func keyLess(a, b Value) bool {
	ra, rb := keyRank(a), keyRank(b)
	if ra != rb {
		return ra < rb
	}
	switch av := a.(type) {
	case Number:
		return av < b.(Number)
	case String:
		return av < b.(String)
	case Bool:
		return !bool(av) && bool(b.(Bool))
	default:
		return fmt.Sprintf("%p", a) < fmt.Sprintf("%p", b)
	}
}

func keyRank(v Value) int {
	switch v.(type) {
	case Number:
		return 0
	case String:
		return 1
	case Bool:
		return 2
	default:
		return 3
	}
}

// GoFunc is a Go-implemented Lua function. It receives the call arguments and
// returns result values or an error.
type GoFunc func(i *Interp, args []Value) ([]Value, error)

// Function is a callable value, either a Lua closure or a Go builtin.
type Function struct {
	proto *funcExpr // set for Lua closures
	env   *scope    // captured environment for closures
	name  string    // best-effort name for diagnostics
	gofn  GoFunc    // set for Go builtins
}

// NewGoFunc wraps a Go function as a callable Lua value. Host code outside the
// lua package uses it to install builtins, for example the redis.* table.
func NewGoFunc(name string, fn GoFunc) *Function {
	return &Function{gofn: fn, name: name}
}

// Error is a Lua error carrying an arbitrary Lua value (usually a string). It
// implements the Go error interface so it can travel up the Go call stack.
type Error struct {
	Value     Value
	Traceback string
}

func (e *Error) Error() string {
	return ToString(e.Value)
}

// Truthy reports Lua truthiness: everything except nil and false is true.
func Truthy(v Value) bool {
	switch x := v.(type) {
	case nilValue:
		return false
	case Bool:
		return bool(x)
	default:
		return true
	}
}

// ToString renders a value the way Lua's tostring does for the common cases the
// engine needs.
func ToString(v Value) string {
	switch x := v.(type) {
	case nilValue:
		return "nil"
	case Bool:
		if x {
			return "true"
		}
		return "false"
	case Number:
		return numberToString(float64(x))
	case String:
		return string(x)
	case *Table:
		return fmt.Sprintf("table: %p", x)
	case *Function:
		return fmt.Sprintf("function: %p", x)
	default:
		return "nil"
	}
}

// numberToString formats a Lua number with the %.14g rule Lua 5.1 uses.
func numberToString(f float64) string {
	if f == math.Trunc(f) && !math.IsInf(f, 0) && math.Abs(f) < 1e15 {
		return strconv.FormatInt(int64(f), 10)
	}
	return strconv.FormatFloat(f, 'g', 14, 64)
}

// toNumber coerces a value to a number following Lua rules: numbers pass
// through, strings are parsed (decimal or 0x hex), everything else fails.
func toNumber(v Value) (float64, bool) {
	switch x := v.(type) {
	case Number:
		return float64(x), true
	case String:
		return parseNumber(string(x))
	default:
		return 0, false
	}
}

// parseNumber parses a Lua numeric string, allowing surrounding spaces, a
// leading sign, and 0x hex integers.
func parseNumber(s string) (float64, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, false
	}
	neg := false
	body := s
	if body[0] == '+' || body[0] == '-' {
		neg = body[0] == '-'
		body = body[1:]
	}
	if len(body) > 2 && body[0] == '0' && (body[1] == 'x' || body[1] == 'X') {
		u, err := strconv.ParseUint(body[2:], 16, 64)
		if err != nil {
			return 0, false
		}
		f := float64(u)
		if neg {
			f = -f
		}
		return f, true
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, false
	}
	return f, true
}
