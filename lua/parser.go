package lua

import "fmt"

// This file is a recursive descent parser for Lua 5.1. It consumes the token
// slice from the lexer and produces a block AST. Operator precedence is handled
// with a precedence-climbing expression parser. Parse errors are returned as
// *Error with the offending line.

// parser holds the token stream and a cursor.
type parser struct {
	toks []token
	pos  int
}

// Parse compiles Lua source into a chunk (a block) ready for the interpreter.
func Parse(src string) (*block, error) {
	toks, err := newLexer(src).tokenize()
	if err != nil {
		return nil, err
	}
	p := &parser{toks: toks}
	b, err := p.parseBlock()
	if err != nil {
		return nil, err
	}
	if p.cur().kind != tEOF {
		return nil, p.errf("'<eof>' expected")
	}
	return b, nil
}

func (p *parser) cur() token  { return p.toks[p.pos] }
func (p *parser) peek() token { return p.toks[p.pos+1] }
func (p *parser) advance() token {
	t := p.toks[p.pos]
	if p.pos < len(p.toks)-1 {
		p.pos++
	}
	return t
}

func (p *parser) errf(format string, args ...any) error {
	return &Error{Value: String(fmt.Sprintf("%d: %s", p.cur().line, fmt.Sprintf(format, args...)))}
}

func (p *parser) accept(k tokenKind) bool {
	if p.cur().kind == k {
		p.advance()
		return true
	}
	return false
}

func (p *parser) expect(k tokenKind, what string) (token, error) {
	if p.cur().kind != k {
		return token{}, p.errf("'%s' expected", what)
	}
	return p.advance(), nil
}

// blockEnd reports whether the current token closes a block.
func (p *parser) blockEnd() bool {
	switch p.cur().kind {
	case tEOF, tEnd, tElse, tElseif, tUntil:
		return true
	}
	return false
}

func (p *parser) parseBlock() (*block, error) {
	b := &block{}
	for !p.blockEnd() {
		if p.cur().kind == tReturn {
			p.advance()
			b.hasRet = true
			if !p.blockEnd() && p.cur().kind != tSemi {
				exprs, err := p.parseExprList()
				if err != nil {
					return nil, err
				}
				b.ret = exprs
			}
			p.accept(tSemi)
			break
		}
		if p.cur().kind == tBreak {
			p.advance()
			b.stmts = append(b.stmts, &breakStmt{})
			p.accept(tSemi)
			continue
		}
		s, err := p.parseStatement()
		if err != nil {
			return nil, err
		}
		if s != nil {
			b.stmts = append(b.stmts, s)
		}
		p.accept(tSemi)
	}
	return b, nil
}

func (p *parser) parseStatement() (stmt, error) {
	switch p.cur().kind {
	case tSemi:
		p.advance()
		return nil, nil
	case tIf:
		return p.parseIf()
	case tWhile:
		return p.parseWhile()
	case tDo:
		p.advance()
		body, err := p.parseBlock()
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(tEnd, "end"); err != nil {
			return nil, err
		}
		return &doStmt{body: body}, nil
	case tFor:
		return p.parseFor()
	case tRepeat:
		return p.parseRepeat()
	case tFunction:
		return p.parseFunctionStmt()
	case tLocal:
		return p.parseLocal()
	default:
		return p.parseExprStatement()
	}
}

func (p *parser) parseIf() (stmt, error) {
	p.advance() // if
	s := &ifStmt{}
	cond, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	if _, err := p.expect(tThen, "then"); err != nil {
		return nil, err
	}
	body, err := p.parseBlock()
	if err != nil {
		return nil, err
	}
	s.conds = append(s.conds, cond)
	s.blocks = append(s.blocks, body)
	for p.cur().kind == tElseif {
		p.advance()
		c, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(tThen, "then"); err != nil {
			return nil, err
		}
		bl, err := p.parseBlock()
		if err != nil {
			return nil, err
		}
		s.conds = append(s.conds, c)
		s.blocks = append(s.blocks, bl)
	}
	if p.accept(tElse) {
		els, err := p.parseBlock()
		if err != nil {
			return nil, err
		}
		s.els = els
	}
	if _, err := p.expect(tEnd, "end"); err != nil {
		return nil, err
	}
	return s, nil
}

func (p *parser) parseWhile() (stmt, error) {
	p.advance()
	cond, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	if _, err := p.expect(tDo, "do"); err != nil {
		return nil, err
	}
	body, err := p.parseBlock()
	if err != nil {
		return nil, err
	}
	if _, err := p.expect(tEnd, "end"); err != nil {
		return nil, err
	}
	return &whileStmt{cond: cond, body: body}, nil
}

func (p *parser) parseRepeat() (stmt, error) {
	p.advance()
	body, err := p.parseBlock()
	if err != nil {
		return nil, err
	}
	if _, err := p.expect(tUntil, "until"); err != nil {
		return nil, err
	}
	cond, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	return &repeatStmt{body: body, cond: cond}, nil
}

func (p *parser) parseFor() (stmt, error) {
	p.advance() // for
	first, err := p.expect(tName, "name")
	if err != nil {
		return nil, err
	}
	if p.cur().kind == tAssign {
		p.advance()
		start, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(tComma, ","); err != nil {
			return nil, err
		}
		stop, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		var step expr
		if p.accept(tComma) {
			step, err = p.parseExpr()
			if err != nil {
				return nil, err
			}
		}
		if _, err := p.expect(tDo, "do"); err != nil {
			return nil, err
		}
		body, err := p.parseBlock()
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(tEnd, "end"); err != nil {
			return nil, err
		}
		return &numForStmt{name: first.str, start: start, stop: stop, step: step, body: body}, nil
	}

	names := []string{first.str}
	for p.accept(tComma) {
		n, err := p.expect(tName, "name")
		if err != nil {
			return nil, err
		}
		names = append(names, n.str)
	}
	if _, err := p.expect(tIn, "in"); err != nil {
		return nil, err
	}
	exprs, err := p.parseExprList()
	if err != nil {
		return nil, err
	}
	if _, err := p.expect(tDo, "do"); err != nil {
		return nil, err
	}
	body, err := p.parseBlock()
	if err != nil {
		return nil, err
	}
	if _, err := p.expect(tEnd, "end"); err != nil {
		return nil, err
	}
	return &genForStmt{names: names, exprs: exprs, body: body}, nil
}

func (p *parser) parseFunctionStmt() (stmt, error) {
	p.advance() // function
	first, err := p.expect(tName, "name")
	if err != nil {
		return nil, err
	}
	var target expr = &nameExpr{name: first.str}
	isMethod := false
	for p.cur().kind == tDot {
		p.advance()
		field, err := p.expect(tName, "name")
		if err != nil {
			return nil, err
		}
		target = &indexExpr{obj: target, key: &stringExpr{val: field.str}}
	}
	if p.accept(tColon) {
		field, err := p.expect(tName, "name")
		if err != nil {
			return nil, err
		}
		target = &indexExpr{obj: target, key: &stringExpr{val: field.str}}
		isMethod = true
	}
	fn, err := p.parseFuncBody(isMethod)
	if err != nil {
		return nil, err
	}
	return &funcStmt{target: target, fn: fn}, nil
}

// parseFuncBody parses the parameter list and body after the function keyword
// and optional name. When isMethod is set an implicit self parameter is added.
func (p *parser) parseFuncBody(isMethod bool) (*funcExpr, error) {
	line := p.cur().line
	if _, err := p.expect(tLParen, "("); err != nil {
		return nil, err
	}
	fn := &funcExpr{line: line}
	if isMethod {
		fn.params = append(fn.params, "self")
	}
	for p.cur().kind != tRParen {
		if p.cur().kind == tEllipsis {
			p.advance()
			fn.isVararg = true
			break
		}
		n, err := p.expect(tName, "name")
		if err != nil {
			return nil, err
		}
		fn.params = append(fn.params, n.str)
		if !p.accept(tComma) {
			break
		}
	}
	if _, err := p.expect(tRParen, ")"); err != nil {
		return nil, err
	}
	body, err := p.parseBlock()
	if err != nil {
		return nil, err
	}
	if _, err := p.expect(tEnd, "end"); err != nil {
		return nil, err
	}
	fn.body = body
	return fn, nil
}

func (p *parser) parseLocal() (stmt, error) {
	p.advance() // local
	if p.accept(tFunction) {
		name, err := p.expect(tName, "name")
		if err != nil {
			return nil, err
		}
		fn, err := p.parseFuncBody(false)
		if err != nil {
			return nil, err
		}
		return &funcStmt{isLocal: true, name: name.str, fn: fn}, nil
	}
	names := []string{}
	for {
		n, err := p.expect(tName, "name")
		if err != nil {
			return nil, err
		}
		names = append(names, n.str)
		if !p.accept(tComma) {
			break
		}
	}
	var values []expr
	if p.accept(tAssign) {
		vs, err := p.parseExprList()
		if err != nil {
			return nil, err
		}
		values = vs
	}
	return &localStmt{names: names, values: values}, nil
}

// parseExprStatement parses either an assignment or a bare function call.
func (p *parser) parseExprStatement() (stmt, error) {
	first, err := p.parseSuffixedExpr()
	if err != nil {
		return nil, err
	}
	if p.cur().kind == tAssign || p.cur().kind == tComma {
		targets := []expr{first}
		for p.accept(tComma) {
			t, err := p.parseSuffixedExpr()
			if err != nil {
				return nil, err
			}
			targets = append(targets, t)
		}
		if _, err := p.expect(tAssign, "="); err != nil {
			return nil, err
		}
		values, err := p.parseExprList()
		if err != nil {
			return nil, err
		}
		for _, t := range targets {
			switch t.(type) {
			case *nameExpr, *indexExpr:
			default:
				return nil, p.errf("cannot assign to this expression")
			}
		}
		return &assignStmt{targets: targets, values: values}, nil
	}
	if _, ok := first.(*callExpr); !ok {
		return nil, p.errf("syntax error near unexpected expression")
	}
	return &callStmt{call: first}, nil
}

func (p *parser) parseExprList() ([]expr, error) {
	var out []expr
	e, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	out = append(out, e)
	for p.accept(tComma) {
		e, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, nil
}

// binPrec gives the left and right binding power of a binary operator. A right
// binding power lower than the left makes the operator right associative.
func binPrec(k tokenKind) (left, right int, ok bool) {
	switch k {
	case tOr:
		return 1, 1, true
	case tAnd:
		return 2, 2, true
	case tLt, tGt, tLe, tGe, tNe, tEq:
		return 3, 3, true
	case tConcat:
		return 5, 4, true // right associative
	case tPlus, tMinus:
		return 6, 6, true
	case tStar, tSlash, tPercent:
		return 7, 7, true
	case tCaret:
		return 10, 9, true // right associative, binds tighter than unary
	}
	return 0, 0, false
}

const unaryPrec = 8

// parseExpr parses a full expression with precedence climbing.
func (p *parser) parseExpr() (expr, error) {
	return p.parseBinExpr(0)
}

func (p *parser) parseBinExpr(limit int) (expr, error) {
	var left expr
	var err error
	switch p.cur().kind {
	case tNot, tMinus, tHash:
		op := p.advance()
		operand, err := p.parseBinExpr(unaryPrec)
		if err != nil {
			return nil, err
		}
		left = &unExpr{op: op.kind, e: operand, line: op.line}
	default:
		left, err = p.parseSimpleExpr()
		if err != nil {
			return nil, err
		}
	}
	for {
		lp, rp, ok := binPrec(p.cur().kind)
		if !ok || lp <= limit {
			break
		}
		op := p.advance()
		right, err := p.parseBinExpr(rp)
		if err != nil {
			return nil, err
		}
		left = &binExpr{op: op.kind, l: left, r: right, line: op.line}
	}
	return left, nil
}

func (p *parser) parseSimpleExpr() (expr, error) {
	switch p.cur().kind {
	case tNil:
		p.advance()
		return &nilExpr{}, nil
	case tTrue:
		p.advance()
		return &trueExpr{}, nil
	case tFalse:
		p.advance()
		return &falseExpr{}, nil
	case tNumber:
		t := p.advance()
		return &numberExpr{val: t.num}, nil
	case tString:
		t := p.advance()
		return &stringExpr{val: t.str}, nil
	case tEllipsis:
		p.advance()
		return &varargExpr{}, nil
	case tFunction:
		p.advance()
		return p.parseFuncBody(false)
	case tLBrace:
		return p.parseTable()
	default:
		return p.parseSuffixedExpr()
	}
}

// parsePrimaryExpr parses a name or a parenthesized expression, the base of a
// suffixed expression chain.
func (p *parser) parsePrimaryExpr() (expr, error) {
	switch p.cur().kind {
	case tName:
		t := p.advance()
		return &nameExpr{name: t.str}, nil
	case tLParen:
		p.advance()
		e, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(tRParen, ")"); err != nil {
			return nil, err
		}
		return &parenExpr{e: e}, nil
	}
	return nil, p.errf("unexpected symbol")
}

// parseSuffixedExpr parses a primary expression followed by any number of
// index, dot, call, or method-call suffixes.
func (p *parser) parseSuffixedExpr() (expr, error) {
	e, err := p.parsePrimaryExpr()
	if err != nil {
		return nil, err
	}
	for {
		switch p.cur().kind {
		case tDot:
			p.advance()
			field, err := p.expect(tName, "name")
			if err != nil {
				return nil, err
			}
			e = &indexExpr{obj: e, key: &stringExpr{val: field.str}}
		case tLBracket:
			p.advance()
			key, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			if _, err := p.expect(tRBracket, "]"); err != nil {
				return nil, err
			}
			e = &indexExpr{obj: e, key: key}
		case tColon:
			p.advance()
			method, err := p.expect(tName, "name")
			if err != nil {
				return nil, err
			}
			args, err := p.parseCallArgs()
			if err != nil {
				return nil, err
			}
			e = &callExpr{fn: e, method: method.str, args: args}
		case tLParen, tString, tLBrace:
			args, err := p.parseCallArgs()
			if err != nil {
				return nil, err
			}
			e = &callExpr{fn: e, args: args}
		default:
			return e, nil
		}
	}
}

// parseCallArgs parses the arguments of a call: a parenthesized list, a single
// string literal, or a single table constructor.
func (p *parser) parseCallArgs() ([]expr, error) {
	switch p.cur().kind {
	case tString:
		t := p.advance()
		return []expr{&stringExpr{val: t.str}}, nil
	case tLBrace:
		tbl, err := p.parseTable()
		if err != nil {
			return nil, err
		}
		return []expr{tbl}, nil
	case tLParen:
		p.advance()
		if p.accept(tRParen) {
			return nil, nil
		}
		args, err := p.parseExprList()
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(tRParen, ")"); err != nil {
			return nil, err
		}
		return args, nil
	}
	return nil, p.errf("function arguments expected")
}

func (p *parser) parseTable() (expr, error) {
	if _, err := p.expect(tLBrace, "{"); err != nil {
		return nil, err
	}
	t := &tableExpr{}
	for p.cur().kind != tRBrace {
		switch {
		case p.cur().kind == tLBracket:
			p.advance()
			key, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			if _, err := p.expect(tRBracket, "]"); err != nil {
				return nil, err
			}
			if _, err := p.expect(tAssign, "="); err != nil {
				return nil, err
			}
			val, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			t.keys = append(t.keys, key)
			t.vals = append(t.vals, val)
		case p.cur().kind == tName && p.peek().kind == tAssign:
			name := p.advance()
			p.advance() // =
			val, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			t.keys = append(t.keys, &stringExpr{val: name.str})
			t.vals = append(t.vals, val)
		default:
			val, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			t.arrayParts = append(t.arrayParts, val)
		}
		if !p.accept(tComma) && !p.accept(tSemi) {
			break
		}
	}
	if _, err := p.expect(tRBrace, "}"); err != nil {
		return nil, err
	}
	return t, nil
}
