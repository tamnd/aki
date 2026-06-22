package lua

// This file defines the token kinds the lexer produces for Lua 5.1 source.
// The set follows the reference Lua 5.1 grammar: keywords, names, numeric and
// string literals, and the fixed operator and punctuation symbols.

// tokenKind identifies a lexical token category.
type tokenKind int

const (
	tEOF tokenKind = iota
	tName
	tNumber
	tString

	// keywords
	tAnd
	tBreak
	tDo
	tElse
	tElseif
	tEnd
	tFalse
	tFor
	tFunction
	tIf
	tIn
	tLocal
	tNil
	tNot
	tOr
	tRepeat
	tReturn
	tThen
	tTrue
	tUntil
	tWhile

	// symbols
	tPlus     // +
	tMinus    // -
	tStar     // *
	tSlash    // /
	tPercent  // %
	tCaret    // ^
	tHash     // #
	tEq       // ==
	tNe       // ~=
	tLe       // <=
	tGe       // >=
	tLt       // <
	tGt       // >
	tAssign   // =
	tLParen   // (
	tRParen   // )
	tLBrace   // {
	tRBrace   // }
	tLBracket // [
	tRBracket // ]
	tSemi     // ;
	tColon    // :
	tComma    // ,
	tDot      // .
	tConcat   // ..
	tEllipsis // ...
)

// keywords maps reserved words to their token kind.
var keywords = map[string]tokenKind{
	"and":      tAnd,
	"break":    tBreak,
	"do":       tDo,
	"else":     tElse,
	"elseif":   tElseif,
	"end":      tEnd,
	"false":    tFalse,
	"for":      tFor,
	"function": tFunction,
	"if":       tIf,
	"in":       tIn,
	"local":    tLocal,
	"nil":      tNil,
	"not":      tNot,
	"or":       tOr,
	"repeat":   tRepeat,
	"return":   tReturn,
	"then":     tThen,
	"true":     tTrue,
	"until":    tUntil,
	"while":    tWhile,
}

// token is one lexical token with its source line for error reporting.
type token struct {
	kind tokenKind
	str  string  // text for names and strings, raw text for numbers
	num  float64 // parsed value for tNumber
	line int
}
