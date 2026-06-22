package lua

import (
	"fmt"
	"math"
)

// This file is the tree-walking interpreter for the Lua AST. It evaluates
// expressions and executes statements over a chain of lexical scopes. Control
// flow (break and return) is carried back up through a small control signal
// rather than Go panics. Errors are *Error values that unwind the Go stack.

// scope is one lexical frame. Locals live here; lookups fall through to the
// parent and finally to the globals table held by the Interp.
type scope struct {
	vars   map[string]Value
	parent *scope
}

func newScope(parent *scope) *scope {
	return &scope{vars: map[string]Value{}, parent: parent}
}

// lookup finds a variable by name, returning the scope that holds it.
func (s *scope) lookup(name string) (*scope, bool) {
	for cur := s; cur != nil; cur = cur.parent {
		if _, ok := cur.vars[name]; ok {
			return cur, true
		}
	}
	return nil, false
}

// define declares a new local in this scope.
func (s *scope) define(name string, v Value) { s.vars[name] = v }

// Interp is a single Lua execution state. It is not safe for concurrent use; a
// fresh Interp is created per script execution to keep the sandbox isolated.
type Interp struct {
	globals  *Table
	depth    int
	maxDepth int

	// hook is called every few statements so a host can enforce a time limit or
	// a kill request. A non-nil error returned from the hook aborts the script.
	hook    func() error
	hookGas int

	// Registry is a free-form map a host can use to stash state reachable from
	// Go builtins (for example the bridge back into the database).
	Registry map[string]any
}

// New returns an interpreter with an empty global table and the standard
// sandboxed library installed.
func New() *Interp {
	i := &Interp{
		globals:  NewTable(),
		maxDepth: 200,
		Registry: map[string]any{},
	}
	openBase(i)
	return i
}

// Globals exposes the global table so a host can install or read globals.
func (i *Interp) Globals() *Table { return i.globals }

// SetHook installs a periodic hook and the number of statements between calls.
func (i *Interp) SetHook(every int, fn func() error) {
	i.hook = fn
	i.hookGas = every
}

// control is the result of executing a statement or block.
type controlKind int

const (
	ctrlNormal controlKind = iota
	ctrlBreak
	ctrlReturn
)

type control struct {
	kind   controlKind
	values []Value
}

var normalControl = control{kind: ctrlNormal}

// runtimeErr builds a Lua runtime error carrying a string value.
func runtimeErr(format string, args ...any) *Error {
	return &Error{Value: String(fmt.Sprintf(format, args...))}
}

// Run compiles and executes a chunk, returning its return values.
func (i *Interp) Run(src string) ([]Value, error) {
	chunk, err := Parse(src)
	if err != nil {
		return nil, err
	}
	return i.RunBlock(chunk)
}

// RunBlock executes an already-parsed chunk at global scope.
func (i *Interp) RunBlock(chunk *block) ([]Value, error) {
	root := newScope(nil)
	ctrl, err := i.execBlock(chunk, root)
	if err != nil {
		return nil, err
	}
	if ctrl.kind == ctrlReturn {
		return ctrl.values, nil
	}
	return nil, nil
}

func (i *Interp) tick() error {
	if i.hook == nil {
		return nil
	}
	i.hookGas--
	if i.hookGas <= 0 {
		i.hookGas = 100
		return i.hook()
	}
	return nil
}

// execBlock runs the statements of a block in a child scope.
func (i *Interp) execBlock(b *block, parent *scope) (control, error) {
	s := newScope(parent)
	return i.execStmts(b, s)
}

// execStmts runs statements directly in the given scope (no new frame). It is
// used where the caller already created the scope, such as loop bodies that need
// the loop variable visible.
func (i *Interp) execStmts(b *block, s *scope) (control, error) {
	for _, st := range b.stmts {
		if err := i.tick(); err != nil {
			return normalControl, asError(err)
		}
		ctrl, err := i.execStmt(st, s)
		if err != nil {
			return normalControl, err
		}
		if ctrl.kind != ctrlNormal {
			return ctrl, nil
		}
	}
	if b.hasRet {
		vals, err := i.evalMulti(b.ret, s)
		if err != nil {
			return normalControl, err
		}
		return control{kind: ctrlReturn, values: vals}, nil
	}
	return normalControl, nil
}

func (i *Interp) execStmt(st stmt, s *scope) (control, error) {
	switch n := st.(type) {
	case *localStmt:
		return i.execLocal(n, s)
	case *assignStmt:
		return normalControl, i.execAssign(n, s)
	case *callStmt:
		_, err := i.evalExprN(n.call, s, 0)
		return normalControl, err
	case *doStmt:
		return i.execBlock(n.body, s)
	case *ifStmt:
		return i.execIf(n, s)
	case *whileStmt:
		return i.execWhile(n, s)
	case *repeatStmt:
		return i.execRepeat(n, s)
	case *numForStmt:
		return i.execNumFor(n, s)
	case *genForStmt:
		return i.execGenFor(n, s)
	case *funcStmt:
		return normalControl, i.execFunc(n, s)
	case *breakStmt:
		return control{kind: ctrlBreak}, nil
	default:
		return normalControl, runtimeErr("unsupported statement %T", st)
	}
}

func (i *Interp) execLocal(n *localStmt, s *scope) (control, error) {
	vals, err := i.evalMulti(n.values, s)
	if err != nil {
		return normalControl, err
	}
	for idx, name := range n.names {
		s.define(name, nth(vals, idx))
	}
	return normalControl, nil
}

func (i *Interp) execAssign(n *assignStmt, s *scope) error {
	vals, err := i.evalMulti(n.values, s)
	if err != nil {
		return err
	}
	for idx, target := range n.targets {
		if err := i.assignTo(target, nth(vals, idx), s); err != nil {
			return err
		}
	}
	return nil
}

func (i *Interp) assignTo(target expr, v Value, s *scope) error {
	switch t := target.(type) {
	case *nameExpr:
		if owner, ok := s.lookup(t.name); ok {
			owner.vars[t.name] = v
			return nil
		}
		i.globals.Set(String(t.name), v)
		return nil
	case *indexExpr:
		obj, err := i.evalExpr(t.obj, s)
		if err != nil {
			return err
		}
		key, err := i.evalExpr(t.key, s)
		if err != nil {
			return err
		}
		return i.setIndex(obj, key, v)
	default:
		return runtimeErr("cannot assign to this expression")
	}
}

func (i *Interp) execIf(n *ifStmt, s *scope) (control, error) {
	for idx, cond := range n.conds {
		v, err := i.evalExpr(cond, s)
		if err != nil {
			return normalControl, err
		}
		if Truthy(v) {
			return i.execBlock(n.blocks[idx], s)
		}
	}
	if n.els != nil {
		return i.execBlock(n.els, s)
	}
	return normalControl, nil
}

func (i *Interp) execWhile(n *whileStmt, s *scope) (control, error) {
	for {
		// Tick per iteration so the deadline hook fires even when the body has no
		// statements of its own, as in `while true do end`.
		if err := i.tick(); err != nil {
			return normalControl, asError(err)
		}
		v, err := i.evalExpr(n.cond, s)
		if err != nil {
			return normalControl, err
		}
		if !Truthy(v) {
			return normalControl, nil
		}
		ctrl, err := i.execBlock(n.body, s)
		if err != nil {
			return normalControl, err
		}
		if ctrl.kind == ctrlBreak {
			return normalControl, nil
		}
		if ctrl.kind == ctrlReturn {
			return ctrl, nil
		}
	}
}

func (i *Interp) execRepeat(n *repeatStmt, s *scope) (control, error) {
	for {
		if err := i.tick(); err != nil {
			return normalControl, asError(err)
		}
		// The until condition can see the body's locals, so run the body and the
		// test in the same scope.
		inner := newScope(s)
		ctrl, err := i.execStmts(n.body, inner)
		if err != nil {
			return normalControl, err
		}
		if ctrl.kind == ctrlBreak {
			return normalControl, nil
		}
		if ctrl.kind == ctrlReturn {
			return ctrl, nil
		}
		v, err := i.evalExpr(n.cond, inner)
		if err != nil {
			return normalControl, err
		}
		if Truthy(v) {
			return normalControl, nil
		}
	}
}

func (i *Interp) execNumFor(n *numForStmt, s *scope) (control, error) {
	start, err := i.evalNumber(n.start, s, "'for' initial value")
	if err != nil {
		return normalControl, err
	}
	stop, err := i.evalNumber(n.stop, s, "'for' limit")
	if err != nil {
		return normalControl, err
	}
	step := 1.0
	if n.step != nil {
		step, err = i.evalNumber(n.step, s, "'for' step")
		if err != nil {
			return normalControl, err
		}
	}
	if step == 0 {
		return normalControl, runtimeErr("'for' step is zero")
	}
	for v := start; (step > 0 && v <= stop) || (step < 0 && v >= stop); v += step {
		if err := i.tick(); err != nil {
			return normalControl, asError(err)
		}
		inner := newScope(s)
		inner.define(n.name, Number(v))
		ctrl, err := i.execStmts(n.body, inner)
		if err != nil {
			return normalControl, err
		}
		if ctrl.kind == ctrlBreak {
			return normalControl, nil
		}
		if ctrl.kind == ctrlReturn {
			return ctrl, nil
		}
	}
	return normalControl, nil
}

func (i *Interp) execGenFor(n *genForStmt, s *scope) (control, error) {
	init, err := i.evalMulti(n.exprs, s)
	if err != nil {
		return normalControl, err
	}
	iter := nth(init, 0)
	state := nth(init, 1)
	ctrlVar := nth(init, 2)
	for {
		if err := i.tick(); err != nil {
			return normalControl, asError(err)
		}
		rets, err := i.call(iter, []Value{state, ctrlVar}, 0)
		if err != nil {
			return normalControl, err
		}
		first := nth(rets, 0)
		if _, ok := first.(nilValue); ok {
			return normalControl, nil
		}
		ctrlVar = first
		inner := newScope(s)
		for idx, name := range n.names {
			inner.define(name, nth(rets, idx))
		}
		c, err := i.execStmts(n.body, inner)
		if err != nil {
			return normalControl, err
		}
		if c.kind == ctrlBreak {
			return normalControl, nil
		}
		if c.kind == ctrlReturn {
			return c, nil
		}
	}
}

func (i *Interp) execFunc(n *funcStmt, s *scope) error {
	if n.isLocal {
		// Predeclare so the function can recurse by name.
		s.define(n.name, Nil)
		fn := &Function{proto: n.fn, env: s, name: n.name}
		s.vars[n.name] = fn
		return nil
	}
	fn := &Function{proto: n.fn, env: s, name: funcName(n.target)}
	return i.assignTo(n.target, fn, s)
}

func funcName(target expr) string {
	switch t := target.(type) {
	case *nameExpr:
		return t.name
	case *indexExpr:
		if k, ok := t.key.(*stringExpr); ok {
			return k.val
		}
	}
	return "?"
}

// evalNumber evaluates an expression and coerces it to a number for numeric for.
func (i *Interp) evalNumber(e expr, s *scope, what string) (float64, error) {
	v, err := i.evalExpr(e, s)
	if err != nil {
		return 0, err
	}
	f, ok := toNumber(v)
	if !ok {
		return 0, runtimeErr("%s must be a number", what)
	}
	return f, nil
}

// nth returns the value at index idx or Nil if out of range.
func nth(vals []Value, idx int) Value {
	if idx < len(vals) {
		return vals[idx]
	}
	return Nil
}

// asError converts a Go error into a *Error, wrapping plain errors as strings.
func asError(err error) *Error {
	if le, ok := err.(*Error); ok {
		return le
	}
	return &Error{Value: String(err.Error())}
}

// evalMulti evaluates an expression list with Lua's last-expr-expands rule: all
// but the last expression yield one value, and the last expands to all of its
// values.
func (i *Interp) evalMulti(exprs []expr, s *scope) ([]Value, error) {
	if len(exprs) == 0 {
		return nil, nil
	}
	var out []Value
	for idx, e := range exprs {
		if idx == len(exprs)-1 {
			vals, err := i.evalExprN(e, s, -1)
			if err != nil {
				return nil, err
			}
			out = append(out, vals...)
		} else {
			v, err := i.evalExpr(e, s)
			if err != nil {
				return nil, err
			}
			out = append(out, v)
		}
	}
	return out, nil
}

// evalExpr evaluates an expression to exactly one value.
func (i *Interp) evalExpr(e expr, s *scope) (Value, error) {
	vals, err := i.evalExprN(e, s, 1)
	if err != nil {
		return nil, err
	}
	return nth(vals, 0), nil
}

// evalExprN evaluates an expression that may produce multiple values (a call or
// vararg). want is the number of values requested: 1 for a single value, -1 for
// all, 0 when the result is discarded.
func (i *Interp) evalExprN(e expr, s *scope, want int) ([]Value, error) {
	switch n := e.(type) {
	case *nilExpr:
		return []Value{Nil}, nil
	case *trueExpr:
		return []Value{Bool(true)}, nil
	case *falseExpr:
		return []Value{Bool(false)}, nil
	case *numberExpr:
		return []Value{Number(n.val)}, nil
	case *stringExpr:
		return []Value{String(n.val)}, nil
	case *nameExpr:
		return []Value{i.readName(n.name, s)}, nil
	case *parenExpr:
		v, err := i.evalExpr(n.e, s)
		return []Value{v}, err
	case *varargExpr:
		return i.readVararg(s, want), nil
	case *indexExpr:
		v, err := i.evalIndex(n, s)
		return []Value{v}, err
	case *funcExpr:
		return []Value{&Function{proto: n, env: s}}, nil
	case *unExpr:
		v, err := i.evalUnary(n, s)
		return []Value{v}, err
	case *binExpr:
		v, err := i.evalBinary(n, s)
		return []Value{v}, err
	case *tableExpr:
		v, err := i.evalTable(n, s)
		return []Value{v}, err
	case *callExpr:
		return i.evalCall(n, s, want)
	default:
		return nil, runtimeErr("unsupported expression %T", e)
	}
}

func (i *Interp) readName(name string, s *scope) Value {
	if owner, ok := s.lookup(name); ok {
		return owner.vars[name]
	}
	return i.globals.Get(String(name))
}

func (i *Interp) readVararg(s *scope, want int) []Value {
	owner, ok := s.lookup("...")
	if !ok {
		return []Value{Nil}
	}
	tbl, ok := owner.vars["..."].(*Table)
	if !ok {
		return []Value{Nil}
	}
	all := append([]Value(nil), tbl.iterArray()...)
	if want == 1 {
		return []Value{nth(all, 0)}
	}
	return all
}

func (i *Interp) evalIndex(n *indexExpr, s *scope) (Value, error) {
	obj, err := i.evalExpr(n.obj, s)
	if err != nil {
		return nil, err
	}
	key, err := i.evalExpr(n.key, s)
	if err != nil {
		return nil, err
	}
	return i.index(obj, key)
}

// index reads obj[key] honoring the __index metamethod on tables and the string
// library on string values.
func (i *Interp) index(obj, key Value) (Value, error) {
	switch o := obj.(type) {
	case *Table:
		v := o.Get(key)
		if _, ok := v.(nilValue); !ok {
			return v, nil
		}
		if o.meta == nil {
			return Nil, nil
		}
		return i.indexMeta(o.meta, obj, key)
	case String:
		if lib := i.globals.Get(String("string")); lib != nil {
			if t, ok := lib.(*Table); ok {
				return t.Get(key), nil
			}
		}
		return Nil, nil
	default:
		return nil, runtimeErr("attempt to index a %s value", obj.luaType())
	}
}

func (i *Interp) indexMeta(meta *Table, obj, key Value) (Value, error) {
	mi := meta.Get(String("__index"))
	switch m := mi.(type) {
	case nilValue:
		return Nil, nil
	case *Table:
		return i.index(m, key)
	case *Function:
		rets, err := i.call(m, []Value{obj, key}, 1)
		if err != nil {
			return nil, err
		}
		return nth(rets, 0), nil
	default:
		return i.index(mi, key)
	}
}

func (i *Interp) setIndex(obj, key, v Value) error {
	t, ok := obj.(*Table)
	if !ok {
		return runtimeErr("attempt to index a %s value", obj.luaType())
	}
	if _, isNil := key.(nilValue); isNil {
		return runtimeErr("table index is nil")
	}
	if t.meta != nil {
		if _, ok := t.Get(key).(nilValue); ok {
			ni := t.meta.Get(String("__newindex"))
			switch m := ni.(type) {
			case *Function:
				_, err := i.call(m, []Value{obj, key, v}, 0)
				return err
			case *Table:
				return i.setIndex(m, key, v)
			}
		}
	}
	t.Set(key, v)
	return nil
}

func (i *Interp) evalTable(n *tableExpr, s *scope) (Value, error) {
	t := NewTable()
	for idx, ae := range n.arrayParts {
		if idx == len(n.arrayParts)-1 {
			vals, err := i.evalExprN(ae, s, -1)
			if err != nil {
				return nil, err
			}
			for _, v := range vals {
				t.Append(v)
			}
		} else {
			v, err := i.evalExpr(ae, s)
			if err != nil {
				return nil, err
			}
			t.Append(v)
		}
	}
	for idx := range n.keys {
		k, err := i.evalExpr(n.keys[idx], s)
		if err != nil {
			return nil, err
		}
		v, err := i.evalExpr(n.vals[idx], s)
		if err != nil {
			return nil, err
		}
		t.Set(k, v)
	}
	return t, nil
}

func (i *Interp) evalCall(n *callExpr, s *scope, want int) ([]Value, error) {
	fnVal, err := i.evalExpr(n.fn, s)
	if err != nil {
		return nil, err
	}
	var args []Value
	if n.method != "" {
		m, err := i.index(fnVal, String(n.method))
		if err != nil {
			return nil, err
		}
		args = append(args, fnVal)
		fnVal = m
	}
	rest, err := i.evalMulti(n.args, s)
	if err != nil {
		return nil, err
	}
	args = append(args, rest...)
	return i.call(fnVal, args, want)
}

// call invokes a function value with args, returning its results.
func (i *Interp) call(fnVal Value, args []Value, want int) ([]Value, error) {
	fn, ok := fnVal.(*Function)
	if !ok {
		return nil, runtimeErr("attempt to call a %s value", fnVal.luaType())
	}
	i.depth++
	if i.depth > i.maxDepth {
		i.depth--
		return nil, runtimeErr("stack overflow")
	}
	defer func() { i.depth-- }()

	if fn.gofn != nil {
		return fn.gofn(i, args)
	}
	return i.callLua(fn, args)
}

func (i *Interp) callLua(fn *Function, args []Value) ([]Value, error) {
	local := newScope(fn.env)
	for idx, p := range fn.proto.params {
		local.define(p, nth(args, idx))
	}
	if fn.proto.isVararg {
		extra := NewTable()
		for idx := len(fn.proto.params); idx < len(args); idx++ {
			extra.Append(args[idx])
		}
		local.define("...", extra)
	}
	ctrl, err := i.execStmts(fn.proto.body, local)
	if err != nil {
		return nil, err
	}
	if ctrl.kind == ctrlReturn {
		return ctrl.values, nil
	}
	return nil, nil
}

// Call is the public entry to invoke a Lua function value from Go.
func (i *Interp) Call(fn Value, args ...Value) ([]Value, error) {
	return i.call(fn, args, -1)
}

func (i *Interp) evalUnary(n *unExpr, s *scope) (Value, error) {
	v, err := i.evalExpr(n.e, s)
	if err != nil {
		return nil, err
	}
	switch n.op {
	case tNot:
		return Bool(!Truthy(v)), nil
	case tMinus:
		f, ok := toNumber(v)
		if !ok {
			return nil, runtimeErr("attempt to perform arithmetic on a %s value", v.luaType())
		}
		return Number(-f), nil
	case tHash:
		switch x := v.(type) {
		case String:
			return Number(len(x)), nil
		case *Table:
			return Number(x.Len()), nil
		default:
			return nil, runtimeErr("attempt to get length of a %s value", v.luaType())
		}
	}
	return nil, runtimeErr("bad unary operator")
}

func (i *Interp) evalBinary(n *binExpr, s *scope) (Value, error) {
	// Short-circuit operators evaluate the right side lazily.
	if n.op == tAnd {
		l, err := i.evalExpr(n.l, s)
		if err != nil || !Truthy(l) {
			return l, err
		}
		return i.evalExpr(n.r, s)
	}
	if n.op == tOr {
		l, err := i.evalExpr(n.l, s)
		if err != nil || Truthy(l) {
			return l, err
		}
		return i.evalExpr(n.r, s)
	}
	l, err := i.evalExpr(n.l, s)
	if err != nil {
		return nil, err
	}
	r, err := i.evalExpr(n.r, s)
	if err != nil {
		return nil, err
	}
	switch n.op {
	case tPlus, tMinus, tStar, tSlash, tPercent, tCaret:
		return i.arith(n.op, l, r)
	case tConcat:
		return i.concat(l, r)
	case tEq:
		return Bool(valuesEqual(l, r)), nil
	case tNe:
		return Bool(!valuesEqual(l, r)), nil
	case tLt, tLe, tGt, tGe:
		return i.compare(n.op, l, r)
	}
	return nil, runtimeErr("bad binary operator")
}

func (i *Interp) arith(op tokenKind, l, r Value) (Value, error) {
	lf, lok := toNumber(l)
	rf, rok := toNumber(r)
	if !lok {
		return nil, runtimeErr("attempt to perform arithmetic on a %s value", l.luaType())
	}
	if !rok {
		return nil, runtimeErr("attempt to perform arithmetic on a %s value", r.luaType())
	}
	switch op {
	case tPlus:
		return Number(lf + rf), nil
	case tMinus:
		return Number(lf - rf), nil
	case tStar:
		return Number(lf * rf), nil
	case tSlash:
		return Number(lf / rf), nil
	case tPercent:
		return Number(lf - math.Floor(lf/rf)*rf), nil
	case tCaret:
		return Number(math.Pow(lf, rf)), nil
	}
	return nil, runtimeErr("bad arithmetic operator")
}

func (i *Interp) concat(l, r Value) (Value, error) {
	ls, lok := concatString(l)
	rs, rok := concatString(r)
	if !lok {
		return nil, runtimeErr("attempt to concatenate a %s value", l.luaType())
	}
	if !rok {
		return nil, runtimeErr("attempt to concatenate a %s value", r.luaType())
	}
	return String(ls + rs), nil
}

func concatString(v Value) (string, bool) {
	switch x := v.(type) {
	case String:
		return string(x), true
	case Number:
		return numberToString(float64(x)), true
	default:
		return "", false
	}
}

func (i *Interp) compare(op tokenKind, l, r Value) (Value, error) {
	// Normalize > and >= into < and <= with swapped operands.
	switch op {
	case tGt:
		return i.compare(tLt, r, l)
	case tGe:
		return i.compare(tLe, r, l)
	}
	if ln, ok := l.(Number); ok {
		if rn, ok := r.(Number); ok {
			if op == tLt {
				return Bool(ln < rn), nil
			}
			return Bool(ln <= rn), nil
		}
	}
	if ls, ok := l.(String); ok {
		if rs, ok := r.(String); ok {
			if op == tLt {
				return Bool(ls < rs), nil
			}
			return Bool(ls <= rs), nil
		}
	}
	return nil, runtimeErr("attempt to compare %s with %s", l.luaType(), r.luaType())
}

// valuesEqual implements Lua raw equality: same type and same value. Tables and
// functions compare by identity.
func valuesEqual(a, b Value) bool {
	switch av := a.(type) {
	case nilValue:
		_, ok := b.(nilValue)
		return ok
	case Bool:
		bv, ok := b.(Bool)
		return ok && av == bv
	case Number:
		bv, ok := b.(Number)
		return ok && av == bv
	case String:
		bv, ok := b.(String)
		return ok && av == bv
	case *Table:
		bv, ok := b.(*Table)
		return ok && av == bv
	case *Function:
		bv, ok := b.(*Function)
		return ok && av == bv
	}
	return false
}

// SetMeta sets a table's metatable.
func (t *Table) SetMeta(m *Table) { t.meta = m }

// Meta returns a table's metatable, or nil.
func (t *Table) Meta() *Table { return t.meta }

// argError builds the standard "bad argument" error Lua builtins raise.
func argError(n int, fname, extra string) *Error {
	return runtimeErr("bad argument #%d to '%s' (%s)", n, fname, extra)
}

// checkString returns arg n as a string, coercing numbers, or raises.
func checkString(args []Value, n int, fname string) (string, error) {
	v := nth(args, n-1)
	switch x := v.(type) {
	case String:
		return string(x), nil
	case Number:
		return numberToString(float64(x)), nil
	default:
		return "", argError(n, fname, "string expected, got "+v.luaType())
	}
}

// checkNumber returns arg n as a number, coercing numeric strings, or raises.
func checkNumber(args []Value, n int, fname string) (float64, error) {
	v := nth(args, n-1)
	f, ok := toNumber(v)
	if !ok {
		return 0, argError(n, fname, "number expected, got "+v.luaType())
	}
	return f, nil
}

// optInt returns arg n as an int, or def when absent or nil.
func optInt(args []Value, n int, def int) int {
	v := nth(args, n-1)
	if _, ok := v.(nilValue); ok {
		return def
	}
	if f, ok := toNumber(v); ok {
		return int(f)
	}
	return def
}
