package lua

import (
	"fmt"
	"strconv"
	"strings"
)

// This file turns Lua 5.1 source text into a token stream. It handles line and
// block comments, single and double quoted strings with the standard escapes,
// long bracket strings, and decimal and hexadecimal numbers. Lexing errors are
// reported as *Error with the source line.

// lexer scans source one rune at a time.
type lexer struct {
	src  string
	pos  int
	line int
}

func newLexer(src string) *lexer {
	return &lexer{src: src, line: 1}
}

func (lx *lexer) errf(format string, args ...any) error {
	return &Error{Value: String(fmt.Sprintf("%d: %s", lx.line, fmt.Sprintf(format, args...)))}
}

func (lx *lexer) peek() byte {
	if lx.pos >= len(lx.src) {
		return 0
	}
	return lx.src[lx.pos]
}

func (lx *lexer) peek2() byte {
	if lx.pos+1 >= len(lx.src) {
		return 0
	}
	return lx.src[lx.pos+1]
}

func (lx *lexer) next() byte {
	c := lx.src[lx.pos]
	lx.pos++
	if c == '\n' {
		lx.line++
	}
	return c
}

func (lx *lexer) eof() bool { return lx.pos >= len(lx.src) }

// tokenize scans the whole source into a token slice ending with tEOF.
func (lx *lexer) tokenize() ([]token, error) {
	var out []token
	for {
		tok, err := lx.scan()
		if err != nil {
			return nil, err
		}
		out = append(out, tok)
		if tok.kind == tEOF {
			return out, nil
		}
	}
}

func isDigit(c byte) bool  { return c >= '0' && c <= '9' }
func isHexDig(c byte) bool { return isDigit(c) || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F') }
func isAlpha(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}
func isAlnum(c byte) bool { return isAlpha(c) || isDigit(c) }

// scan returns the next token, skipping whitespace and comments first.
func (lx *lexer) scan() (token, error) {
	for !lx.eof() {
		c := lx.peek()
		switch {
		case c == ' ' || c == '\t' || c == '\r' || c == '\n':
			lx.next()
		case c == '-' && lx.peek2() == '-':
			if err := lx.skipComment(); err != nil {
				return token{}, err
			}
		default:
			return lx.scanToken()
		}
	}
	return token{kind: tEOF, line: lx.line}, nil
}

// skipComment consumes a line comment or a long bracket comment.
func (lx *lexer) skipComment() error {
	lx.next() // -
	lx.next() // -
	if lx.peek() == '[' {
		if level, ok := lx.longBracketLevel(); ok {
			_, err := lx.readLongString(level)
			return err
		}
	}
	for !lx.eof() && lx.peek() != '\n' {
		lx.next()
	}
	return nil
}

// longBracketLevel checks for a long bracket opener [[ or [=*[ at the cursor and
// consumes it, returning the equals-sign level. It leaves the cursor unchanged
// when there is no opener.
func (lx *lexer) longBracketLevel() (int, bool) {
	save := lx.pos
	saveLine := lx.line
	if lx.peek() != '[' {
		return 0, false
	}
	lx.next()
	level := 0
	for lx.peek() == '=' {
		level++
		lx.next()
	}
	if lx.peek() == '[' {
		lx.next()
		return level, true
	}
	lx.pos = save
	lx.line = saveLine
	return 0, false
}

// readLongString reads the body of a long bracket string up to its matching
// closer. The opener has already been consumed. A first newline is dropped to
// match Lua.
func (lx *lexer) readLongString(level int) (string, error) {
	if lx.peek() == '\r' {
		lx.next()
	}
	if lx.peek() == '\n' {
		lx.next()
	}
	var b strings.Builder
	closer := "]" + strings.Repeat("=", level) + "]"
	for {
		if lx.eof() {
			return "", lx.errf("unfinished long string/comment")
		}
		if lx.peek() == ']' && strings.HasPrefix(lx.src[lx.pos:], closer) {
			for range closer {
				lx.next()
			}
			return b.String(), nil
		}
		b.WriteByte(lx.next())
	}
}

// scanToken reads a single non-trivia token.
func (lx *lexer) scanToken() (token, error) {
	line := lx.line
	c := lx.peek()
	switch {
	case isAlpha(c):
		return lx.scanName(line), nil
	case isDigit(c) || (c == '.' && isDigit(lx.peek2())):
		return lx.scanNumber(line)
	case c == '"' || c == '\'':
		return lx.scanString(line)
	case c == '[' && (lx.peek2() == '[' || lx.peek2() == '='):
		if level, ok := lx.longBracketLevel(); ok {
			s, err := lx.readLongString(level)
			if err != nil {
				return token{}, err
			}
			return token{kind: tString, str: s, line: line}, nil
		}
		lx.next()
		return token{kind: tLBracket, line: line}, nil
	}
	return lx.scanSymbol(line)
}

func (lx *lexer) scanName(line int) token {
	start := lx.pos
	for !lx.eof() && isAlnum(lx.peek()) {
		lx.next()
	}
	word := lx.src[start:lx.pos]
	if kw, ok := keywords[word]; ok {
		return token{kind: kw, str: word, line: line}
	}
	return token{kind: tName, str: word, line: line}
}

func (lx *lexer) scanNumber(line int) (token, error) {
	start := lx.pos
	if lx.peek() == '0' && (lx.peek2() == 'x' || lx.peek2() == 'X') {
		lx.next()
		lx.next()
		for !lx.eof() && isHexDig(lx.peek()) {
			lx.next()
		}
		raw := lx.src[start:lx.pos]
		v, err := strconv.ParseUint(raw[2:], 16, 64)
		if err != nil {
			return token{}, lx.errf("malformed number near '%s'", raw)
		}
		return token{kind: tNumber, str: raw, num: float64(v), line: line}, nil
	}
	for !lx.eof() && isDigit(lx.peek()) {
		lx.next()
	}
	if lx.peek() == '.' {
		lx.next()
		for !lx.eof() && isDigit(lx.peek()) {
			lx.next()
		}
	}
	if lx.peek() == 'e' || lx.peek() == 'E' {
		lx.next()
		if lx.peek() == '+' || lx.peek() == '-' {
			lx.next()
		}
		for !lx.eof() && isDigit(lx.peek()) {
			lx.next()
		}
	}
	raw := lx.src[start:lx.pos]
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return token{}, lx.errf("malformed number near '%s'", raw)
	}
	return token{kind: tNumber, str: raw, num: v, line: line}, nil
}

func (lx *lexer) scanString(line int) (token, error) {
	quote := lx.next()
	var b strings.Builder
	for {
		if lx.eof() {
			return token{}, lx.errf("unfinished string")
		}
		c := lx.next()
		if c == quote {
			return token{kind: tString, str: b.String(), line: line}, nil
		}
		if c == '\n' {
			return token{}, lx.errf("unfinished string")
		}
		if c != '\\' {
			b.WriteByte(c)
			continue
		}
		if err := lx.scanEscape(&b); err != nil {
			return token{}, err
		}
	}
}

// scanEscape handles one backslash escape sequence and writes its bytes.
func (lx *lexer) scanEscape(b *strings.Builder) error {
	if lx.eof() {
		return lx.errf("unfinished string")
	}
	e := lx.next()
	switch e {
	case 'n':
		b.WriteByte('\n')
	case 't':
		b.WriteByte('\t')
	case 'r':
		b.WriteByte('\r')
	case 'a':
		b.WriteByte(7)
	case 'b':
		b.WriteByte(8)
	case 'f':
		b.WriteByte(12)
	case 'v':
		b.WriteByte(11)
	case '\\':
		b.WriteByte('\\')
	case '"':
		b.WriteByte('"')
	case '\'':
		b.WriteByte('\'')
	case '\n':
		b.WriteByte('\n')
	default:
		if isDigit(e) {
			n := int(e - '0')
			for i := 0; i < 2 && isDigit(lx.peek()); i++ {
				n = n*10 + int(lx.next()-'0')
			}
			if n > 255 {
				return lx.errf("decimal escape too large")
			}
			b.WriteByte(byte(n))
			return nil
		}
		return lx.errf("invalid escape sequence '\\%c'", e)
	}
	return nil
}

// scanSymbol reads an operator or punctuation token.
func (lx *lexer) scanSymbol(line int) (token, error) {
	c := lx.next()
	mk := func(k tokenKind) (token, error) { return token{kind: k, line: line}, nil }
	switch c {
	case '+':
		return mk(tPlus)
	case '-':
		return mk(tMinus)
	case '*':
		return mk(tStar)
	case '/':
		return mk(tSlash)
	case '%':
		return mk(tPercent)
	case '^':
		return mk(tCaret)
	case '#':
		return mk(tHash)
	case '(':
		return mk(tLParen)
	case ')':
		return mk(tRParen)
	case '{':
		return mk(tLBrace)
	case '}':
		return mk(tRBrace)
	case '[':
		return mk(tLBracket)
	case ']':
		return mk(tRBracket)
	case ';':
		return mk(tSemi)
	case ':':
		return mk(tColon)
	case ',':
		return mk(tComma)
	case '=':
		if lx.peek() == '=' {
			lx.next()
			return mk(tEq)
		}
		return mk(tAssign)
	case '~':
		if lx.peek() == '=' {
			lx.next()
			return mk(tNe)
		}
		return token{}, lx.errf("unexpected symbol near '~'")
	case '<':
		if lx.peek() == '=' {
			lx.next()
			return mk(tLe)
		}
		return mk(tLt)
	case '>':
		if lx.peek() == '=' {
			lx.next()
			return mk(tGe)
		}
		return mk(tGt)
	case '.':
		if lx.peek() == '.' {
			lx.next()
			if lx.peek() == '.' {
				lx.next()
				return mk(tEllipsis)
			}
			return mk(tConcat)
		}
		return mk(tDot)
	}
	return token{}, lx.errf("unexpected symbol near '%c'", c)
}
