package lua

// This file defines the abstract syntax tree the parser builds and the
// interpreter walks. The shapes follow the Lua 5.1 grammar. Statements and
// expressions are separate interfaces so the interpreter can switch on them.

// stmt is a statement node.
type stmt interface{ stmtNode() }

// expr is an expression node.
type expr interface{ exprNode() }

// block is a sequence of statements with an optional trailing return.
type block struct {
	stmts  []stmt
	ret    []expr // return expressions, nil when the block has no return
	hasRet bool
}

// --- statements ---

// localStmt is `local a, b = e1, e2`.
type localStmt struct {
	names  []string
	values []expr
}

// assignStmt is `lhs1, lhs2 = e1, e2` where each lhs is a name or index target.
type assignStmt struct {
	targets []expr // each is *nameExpr or *indexExpr
	values  []expr
}

// callStmt is an expression statement, always a function call.
type callStmt struct {
	call expr
}

// doStmt is `do ... end`.
type doStmt struct{ body *block }

// whileStmt is `while cond do ... end`.
type whileStmt struct {
	cond expr
	body *block
}

// repeatStmt is `repeat ... until cond`.
type repeatStmt struct {
	body *block
	cond expr
}

// ifStmt is `if/elseif/else`. Conds and blocks line up by index; els is the
// optional trailing else block.
type ifStmt struct {
	conds  []expr
	blocks []*block
	els    *block
}

// numForStmt is `for v = start, stop[, step] do ... end`.
type numForStmt struct {
	name  string
	start expr
	stop  expr
	step  expr // nil means 1
	body  *block
}

// genForStmt is `for a, b in explist do ... end`.
type genForStmt struct {
	names []string
	exprs []expr
	body  *block
}

// funcStmt is a function definition statement. target is where the function is
// stored (name or index path); isLocal marks `local function`.
type funcStmt struct {
	target  expr
	isLocal bool
	name    string // for the local-function pre-declaration
	fn      *funcExpr
}

// returnStmt is only used as block.ret, but breakStmt is a real statement.
type breakStmt struct{}

func (*localStmt) stmtNode()  {}
func (*assignStmt) stmtNode() {}
func (*callStmt) stmtNode()   {}
func (*doStmt) stmtNode()     {}
func (*whileStmt) stmtNode()  {}
func (*repeatStmt) stmtNode() {}
func (*ifStmt) stmtNode()     {}
func (*numForStmt) stmtNode() {}
func (*genForStmt) stmtNode() {}
func (*funcStmt) stmtNode()   {}
func (*breakStmt) stmtNode()  {}

// --- expressions ---

// nilExpr, trueExpr, falseExpr are the literal singletons.
type nilExpr struct{}
type trueExpr struct{}
type falseExpr struct{}

// numberExpr is a numeric literal.
type numberExpr struct{ val float64 }

// stringExpr is a string literal.
type stringExpr struct{ val string }

// varargExpr is `...`.
type varargExpr struct{}

// nameExpr is a variable reference by name.
type nameExpr struct{ name string }

// indexExpr is `obj[key]` (dot access is lowered to a string key).
type indexExpr struct {
	obj expr
	key expr
}

// callExpr is `fn(args)`. When method is set it is `recv:method(args)`.
type callExpr struct {
	fn     expr
	method string
	args   []expr
}

// funcExpr is a function literal.
type funcExpr struct {
	params   []string
	isVararg bool
	body     *block
	line     int
}

// binExpr is a binary operation.
type binExpr struct {
	op   tokenKind
	l, r expr
	line int
}

// unExpr is a unary operation (-, not, #).
type unExpr struct {
	op   tokenKind
	e    expr
	line int
}

// parenExpr wraps an expression in parentheses. It matters because parentheses
// truncate a multi-value expression to a single value.
type parenExpr struct{ e expr }

// tableExpr is a table constructor. arrayParts are positional entries; keyed
// holds explicit [k]=v and name=v entries.
type tableExpr struct {
	arrayParts []expr
	keys       []expr
	vals       []expr
}

func (*nilExpr) exprNode()    {}
func (*trueExpr) exprNode()   {}
func (*falseExpr) exprNode()  {}
func (*numberExpr) exprNode() {}
func (*stringExpr) exprNode() {}
func (*varargExpr) exprNode() {}
func (*nameExpr) exprNode()   {}
func (*indexExpr) exprNode()  {}
func (*callExpr) exprNode()   {}
func (*funcExpr) exprNode()   {}
func (*binExpr) exprNode()    {}
func (*unExpr) exprNode()     {}
func (*tableExpr) exprNode()  {}
func (*parenExpr) exprNode()  {}
