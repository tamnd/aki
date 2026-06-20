// Package resp implements the Redis Serialization Protocol in both its RESP2
// and RESP3 variants. It is a pure codec: it transforms byte buffers and has no
// dependency on net or on any other aki package, so the encoder and decoder can
// be fuzzed and unit-tested against in-memory buffers without a socket. The
// networking layer drives this package over a real connection.
//
// The byte-level contract is doc 06 of the aki specification. Every framing
// decision here is made to be byte-identical to Redis so a client written for
// Redis talks to aki unchanged.
package resp

import (
	"errors"
	"fmt"
	"math/big"
)

// RESPType identifies a decoded value by its RESP leading byte. The constant
// values are the leading bytes themselves, so a decoder can switch on the byte
// read from the wire and a debugger shows a readable character.
type RESPType int

const (
	// RESP2 base types, valid on every connection regardless of version.
	TypeSimpleString RESPType = '+'
	TypeError        RESPType = '-'
	TypeInteger      RESPType = ':'
	TypeBulkString   RESPType = '$'
	TypeArray        RESPType = '*'

	// RESP3 additions, sent only after a successful HELLO 3 but always accepted
	// from the wire for protocol symmetry.
	TypeNull      RESPType = '_'
	TypeBool      RESPType = '#'
	TypeDouble    RESPType = ','
	TypeBigNumber RESPType = '('
	TypeBulkError RESPType = '!'
	TypeVerbatim  RESPType = '='
	TypeMap       RESPType = '%'
	TypeSet       RESPType = '~'
	TypeAttribute RESPType = '|'
	TypePush      RESPType = '>'
)

// RESPValue is a tagged union of every value the decoder can produce. Only the
// fields relevant to Type carry meaning; the rest are zero. It mirrors the
// shape Redis clients expect and is the type the aki cli decodes server replies
// into.
type RESPValue struct {
	Type     RESPType
	Str      []byte      // simple string, bulk string, verbatim string payload
	Integer  int64       // integer
	Float    float64     // double
	BigInt   *big.Int    // big number
	Bool     bool        // boolean
	Err      string      // simple error or bulk error message
	VerbEnc  string      // 3-char encoding prefix of a verbatim string
	Elems    []RESPValue // array, set, push elements
	Map      [][2]RESPValue
	Attrs    [][2]RESPValue // attribute metadata pairs (Type == TypeAttribute)
	AttrBody *RESPValue     // the reply an attribute is attached to
	IsNull   bool           // null bulk string, null array, or RESP3 null
}

// ErrNeedMore signals that the buffer does not yet hold a complete value. The
// decoder never advances its position when returning ErrNeedMore, so the caller
// can read more bytes from the socket and retry from the same offset.
var ErrNeedMore = errors.New("need more data")

// ProtocolError is a fatal decode error: the bytes are not valid RESP and the
// connection must be closed after the error string is sent to the client. Its
// Error string is already prefixed so it can be written straight to the wire as
// a RESP simple error.
type ProtocolError struct{ Msg string }

func (e ProtocolError) Error() string { return "ERR Protocol error: " + e.Msg }

// ErrProtocol builds a ProtocolError with the given description.
func ErrProtocol(msg string) ProtocolError { return ProtocolError{Msg: msg} }

// Protocol limits from doc 06 §6.4. The multibulk and inline caps are fixed;
// the bulk and query-buffer caps are defaults the networking layer may override
// from configuration.
const (
	// MaxMultibulkLen is the largest element count accepted in a client
	// multibulk request (1M).
	MaxMultibulkLen = 1024 * 1024
	// DefaultMaxBulkLen is the default proto-max-bulk-len (512 MiB): the largest
	// single bulk argument accepted.
	DefaultMaxBulkLen = 512 * 1024 * 1024
	// MaxInlineLen is the largest inline request line, including the newline
	// (64 KiB).
	MaxInlineLen = 64 * 1024
)

// errExpectedDollar formats the "expected '$', got 'X'" protocol error for a
// multibulk element that is not a bulk string.
func errExpectedDollar(got byte) ProtocolError {
	return ErrProtocol(fmt.Sprintf("expected '$', got '%c'", got))
}
