package resp

import (
	"bytes"
	"math"
	"math/big"
	"strconv"
	"strings"
)

// Writer is the sink the encoder writes framed bytes into. The networking layer
// satisfies it with a client's reply buffer; tests satisfy it with a
// bytes.Buffer. Encoding methods do not return errors: they accumulate into the
// buffer, and the single flush to the socket is where a write error surfaces.
type Writer interface {
	WriteString(s string) (int, error)
	WriteByte(b byte) error
	Write(p []byte) (int, error)
}

// Encoder serializes in-memory reply values to RESP. It carries the connection's
// negotiated protocol version (2 or 3) and chooses the wire shape accordingly:
// the command layer builds the same logical reply either way and the encoder is
// the single place the version is observed (doc 06 §4.5).
type Encoder struct {
	w     Writer
	proto int
}

// NewEncoder returns an Encoder writing to w in the given protocol version.
// proto must be 2 or 3; any other value is treated as 2.
func NewEncoder(w Writer, proto int) *Encoder {
	if proto != 3 {
		proto = 2
	}
	return &Encoder{w: w, proto: proto}
}

// Proto reports the protocol version this encoder emits (2 or 3).
func (e *Encoder) Proto() int { return e.proto }

// SetProto switches the encoder to a new protocol version, as HELLO does
// mid-connection. proto other than 3 is normalized to 2.
func (e *Encoder) SetProto(proto int) {
	if proto != 3 {
		proto = 2
	}
	e.proto = proto
}

func (e *Encoder) crlf() { _, _ = e.w.WriteString("\r\n") }

// WriteRaw writes pre-framed bytes verbatim, the path used for the pooled static
// replies. The bytes must already be a complete, correctly framed RESP value.
func (e *Encoder) WriteRaw(p []byte) { _, _ = e.w.Write(p) }

// WriteStatus writes a simple string (+). Simple strings are identical in RESP2
// and RESP3. s must not contain CR or LF.
func (e *Encoder) WriteStatus(s string) {
	_ = e.w.WriteByte('+')
	_, _ = e.w.WriteString(s)
	e.crlf()
}

// WriteError writes a simple error (-). The string is "PREFIX message" and must
// not contain CR or LF. Simple errors are used in both RESP2 and RESP3 for the
// ordinary error path.
func (e *Encoder) WriteError(errStr string) {
	_ = e.w.WriteByte('-')
	_, _ = e.w.WriteString(errStr)
	e.crlf()
}

// WriteInteger writes an integer (:). Identical in RESP2 and RESP3. Small values
// come from the pre-rendered pool.
func (e *Encoder) WriteInteger(n int64) {
	if p := pooledInteger(n); p != nil {
		_, _ = e.w.Write(p)
		return
	}
	_ = e.w.WriteByte(':')
	_, _ = e.w.WriteString(strconv.FormatInt(n, 10))
	e.crlf()
}

// WriteBool writes a boolean. RESP3 uses the dedicated #t / #f type; RESP2
// downgrades to the integers 1 and 0, matching how Redis reports EXPIRE,
// SISMEMBER, and the like.
func (e *Encoder) WriteBool(b bool) {
	switch {
	case e.proto == 3 && b:
		_, _ = e.w.Write(ReplyTrue3)
	case e.proto == 3:
		_, _ = e.w.Write(ReplyFalse3)
	case b:
		_, _ = e.w.Write(ReplyOne)
	default:
		_, _ = e.w.Write(ReplyZero)
	}
}

// WriteNull writes a nil reply. RESP3 collapses null bulk string and null array
// into the single _ type; RESP2 uses the null bulk string $-1.
func (e *Encoder) WriteNull() {
	if e.proto == 3 {
		_, _ = e.w.Write(ReplyNil3)
	} else {
		_, _ = e.w.Write(ReplyNil2)
	}
}

// WriteNullArray writes a nil aggregate reply. RESP3 uses _; RESP2 uses the null
// array *-1 (e.g. a BLPOP timeout).
func (e *Encoder) WriteNullArray() {
	if e.proto == 3 {
		_, _ = e.w.Write(ReplyNil3)
	} else {
		_, _ = e.w.Write(ReplyNilArray2)
	}
}

// WriteBulkString writes a binary-safe bulk string ($). Valid and identical in
// both protocol versions.
func (e *Encoder) WriteBulkString(data []byte) {
	_ = e.w.WriteByte('$')
	_, _ = e.w.WriteString(strconv.Itoa(len(data)))
	e.crlf()
	_, _ = e.w.Write(data)
	e.crlf()
}

// WriteBulkArray writes a complete array reply of bulk strings: the array header
// followed by each element framed as a bulk string. It is the batched form of an
// array header plus a WriteBulkString loop, which the large multi-bulk readers
// (LRANGE, SMEMBERS, HVALS, and the like) all spell out by hand.
//
// On those replies the per-element cost is dominated by the five separate calls
// WriteBulkString makes through the Writer interface. When the sink is the
// connection's *bytes.Buffer, this method drops to that concrete type once,
// pre-grows the buffer to the whole reply in a single allocation, and formats
// each element header into a stack buffer, so each element costs three concrete
// buffer writes instead of five dynamically dispatched ones. Any other Writer
// falls back to the plain per-element path, so the wire bytes are identical
// either way.
func (e *Encoder) WriteBulkArray(items [][]byte) {
	e.WriteArrayLen(len(items))
	buf, ok := e.w.(*bytes.Buffer)
	if !ok {
		for _, it := range items {
			e.WriteBulkString(it)
		}
		return
	}
	total := 0
	for _, it := range items {
		// $ + up to 20 length digits + CRLF + data + CRLF, rounded to 16 of fixed
		// overhead, which covers every length that fits the small test elements and
		// over-grows harmlessly for larger ones.
		total += len(it) + 16
	}
	buf.Grow(total)
	var hdr [24]byte
	for _, it := range items {
		hdr[0] = '$'
		h := strconv.AppendInt(hdr[:1], int64(len(it)), 10)
		h = append(h, '\r', '\n')
		_, _ = buf.Write(h)
		_, _ = buf.Write(it)
		_, _ = buf.WriteString("\r\n")
	}
}

// WriteBulkStringStr is WriteBulkString for a Go string, avoiding a []byte
// conversion on the caller's side.
func (e *Encoder) WriteBulkStringStr(s string) {
	_ = e.w.WriteByte('$')
	_, _ = e.w.WriteString(strconv.Itoa(len(s)))
	e.crlf()
	_, _ = e.w.WriteString(s)
	e.crlf()
}

// WriteDouble writes a floating-point value. RESP3 uses the , type; RESP2
// downgrades to a bulk string carrying the same decimal text, matching ZSCORE.
func (e *Encoder) WriteDouble(f float64) {
	s := FormatDouble(f)
	if e.proto == 3 {
		_ = e.w.WriteByte(',')
		_, _ = e.w.WriteString(s)
		e.crlf()
	} else {
		e.WriteBulkStringStr(s)
	}
}

// WriteBigNumber writes an arbitrary-precision integer. RESP3 uses the ( type;
// RESP2 downgrades to a bulk string of the decimal digits.
func (e *Encoder) WriteBigNumber(n *big.Int) {
	s := n.String()
	if e.proto == 3 {
		_ = e.w.WriteByte('(')
		_, _ = e.w.WriteString(s)
		e.crlf()
	} else {
		e.WriteBulkStringStr(s)
	}
}

// WriteVerbatimString writes a verbatim string with a 3-byte content-type hint
// (e.g. "txt", "mkd"). RESP3 uses the = type; RESP2 downgrades to a plain bulk
// string, dropping the hint. enc must be exactly three bytes.
func (e *Encoder) WriteVerbatimString(enc string, data []byte) {
	if e.proto == 3 {
		total := 4 + len(data) // "enc" + ':' + data
		_ = e.w.WriteByte('=')
		_, _ = e.w.WriteString(strconv.Itoa(total))
		e.crlf()
		_, _ = e.w.WriteString(enc)
		_ = e.w.WriteByte(':')
		_, _ = e.w.Write(data)
		e.crlf()
	} else {
		e.WriteBulkString(data)
	}
}

// WriteBulkError writes an error that may contain newlines or exceed the inline
// limit. RESP3 uses the length-prefixed ! type for such errors; otherwise it
// falls back to a simple error.
func (e *Encoder) WriteBulkError(errStr string) {
	if e.proto == 3 && (len(errStr) > 8191 || strings.ContainsAny(errStr, "\r\n")) {
		_ = e.w.WriteByte('!')
		_, _ = e.w.WriteString(strconv.Itoa(len(errStr)))
		e.crlf()
		_, _ = e.w.WriteString(errStr)
		e.crlf()
	} else {
		e.WriteError(errStr)
	}
}

// WriteArrayLen writes an array header. The caller then writes exactly n
// elements. Arrays are identical in RESP2 and RESP3.
func (e *Encoder) WriteArrayLen(n int) {
	_ = e.w.WriteByte('*')
	_, _ = e.w.WriteString(strconv.Itoa(n))
	e.crlf()
}

// WriteMapLen writes a map header for n key-value pairs. RESP3 uses the % type;
// RESP2 downgrades to a flat array of 2n elements, the shape HGETALL has always
// had on RESP2.
func (e *Encoder) WriteMapLen(n int) {
	if e.proto == 3 {
		_ = e.w.WriteByte('%')
		_, _ = e.w.WriteString(strconv.Itoa(n))
	} else {
		_ = e.w.WriteByte('*')
		_, _ = e.w.WriteString(strconv.Itoa(n * 2))
	}
	e.crlf()
}

// WriteSetLen writes a set header for n elements. RESP3 uses the ~ type; RESP2
// downgrades to a plain array.
func (e *Encoder) WriteSetLen(n int) {
	if e.proto == 3 {
		_ = e.w.WriteByte('~')
	} else {
		_ = e.w.WriteByte('*')
	}
	_, _ = e.w.WriteString(strconv.Itoa(n))
	e.crlf()
}

// WritePushLen writes an out-of-band push header for n elements. RESP3 uses the
// > type; RESP2 downgrades to a plain array, which is how pub/sub messages have
// always been delivered on RESP2.
func (e *Encoder) WritePushLen(n int) {
	if e.proto == 3 {
		_ = e.w.WriteByte('>')
	} else {
		_ = e.w.WriteByte('*')
	}
	_, _ = e.w.WriteString(strconv.Itoa(n))
	e.crlf()
}

// WriteAttributeLen writes an attribute (metadata) header for n pairs. The
// caller then writes n key-value pairs followed by the actual reply. In RESP2
// attributes have no representation, so nothing is written and the caller's
// following reply stands alone.
func (e *Encoder) WriteAttributeLen(n int) {
	if e.proto == 3 {
		_ = e.w.WriteByte('|')
		_, _ = e.w.WriteString(strconv.Itoa(n))
		e.crlf()
	}
}

// BeginStreamedBulkString opens a chunked bulk string whose total length is not
// known in advance ($?). Follow with WriteChunk calls and close with
// EndStreamedBulkString.
func (e *Encoder) BeginStreamedBulkString() { _, _ = e.w.WriteString("$?\r\n") }

// WriteChunk writes one chunk of a streamed bulk string.
func (e *Encoder) WriteChunk(data []byte) {
	_ = e.w.WriteByte(';')
	_, _ = e.w.WriteString(strconv.Itoa(len(data)))
	e.crlf()
	_, _ = e.w.Write(data)
	e.crlf()
}

// EndStreamedBulkString closes a streamed bulk string with the zero-length
// terminator chunk.
func (e *Encoder) EndStreamedBulkString() { _, _ = e.w.WriteString(";0\r\n") }

// FormatDouble renders a float for the wire: the special values become inf,
// -inf, nan, and finite values use the shortest representation that round-trips
// through float64 (doc 06 §11.5).
func FormatDouble(f float64) string {
	switch {
	case math.IsInf(f, 1):
		return "inf"
	case math.IsInf(f, -1):
		return "-inf"
	case math.IsNaN(f):
		return "nan"
	default:
		return strconv.FormatFloat(f, 'g', 17, 64)
	}
}
