package resp

// The RESP2 request parser (spec 2064/f3/08 section 2). It parses commands out
// of a caller-owned buffer and returns argument views into that buffer: no
// copies, no per-command allocation once the parser's argument slice is warm.
// The caller owns buffer lifetime; the views are valid until the caller
// compacts or refills the parsed region, which the doc 08 batch-boundary rule
// makes safe because every returned command is consumed (routed and copied
// into its hop node) before the next read touches the buffer.
//
// The parser is stateless across calls: Next always parses from the head of
// the bytes it is given, and a NeedMore answer means the caller reads more and
// calls again with the longer prefix. That restart-from-head shape is what the
// fuzz harness pins: parsing a stream in any chopping yields the same command
// sequence as parsing it whole.

// Status is the outcome of one Next call.
type Status int

const (
	// OK: one command parsed; consumed bytes are done with. A command with
	// zero arguments (an empty inline line, a *0 array) is a no-op the caller
	// skips.
	OK Status = iota
	// NeedMore: the buffer holds a prefix of a command; read more and retry.
	NeedMore
	// ProtoErr: the buffer head is not RESP. The connection cannot resync
	// after a framing error; the caller replies with LastError and closes.
	ProtoErr
)

const (
	// maxMultibulk caps the element count of one command array, Redis's
	// 1024*1024 multibulk bound.
	maxMultibulk = 1024 * 1024

	// MaxBulk caps one argument's byte length, the proto-max-bulk-len default.
	MaxBulk = 512 << 20

	// maxInline caps one inline command line, Redis's 64KiB inline bound. The
	// cap is on the line itself, not the buffer fill, so a chopped read and a
	// whole-buffer parse reject the same inputs.
	maxInline = 64 << 10

	// maxHeaderDigits bounds a length line so its value stays far from int64
	// overflow; 18 digits already exceeds every cap above by ten orders.
	maxHeaderDigits = 18
)

// Parser carries the reused argument slice and the last protocol error. One
// parser per connection, driven by one goroutine.
type Parser struct {
	args [][]byte
	err  string
}

// LastError is the message for the most recent ProtoErr, in Redis's wording
// ("invalid multibulk length" and friends) without any prefix.
func (p *Parser) LastError() string { return p.err }

// Next parses one command from the head of b. On OK the returned args view
// into b and stay valid until the caller reuses the buffer or calls Next
// again; consumed is how many bytes the command took. On NeedMore and ProtoErr
// nothing is consumed.
func (p *Parser) Next(b []byte) (args [][]byte, consumed int, st Status) {
	if len(b) == 0 {
		return nil, 0, NeedMore
	}
	if b[0] != '*' {
		return p.inline(b)
	}
	n, i, st := p.line(b, 1, "invalid multibulk length")
	if st != OK {
		return nil, 0, st
	}
	if n > maxMultibulk {
		return p.fail("invalid multibulk length")
	}
	// *0 and the negative counts are consumed as an empty command the
	// dispatcher skips, the same silent no-op Redis makes of them.
	if n <= 0 {
		return p.args[:0], i, OK
	}
	args = p.args[:0]
	for k := int64(0); k < n; k++ {
		if i == len(b) {
			return nil, 0, NeedMore
		}
		if b[i] != '$' {
			return p.fail("expected '$', got '" + printable(b[i]) + "'")
		}
		bl, j, st := p.line(b, i+1, "invalid bulk length")
		if st != OK {
			return nil, 0, st
		}
		if bl < 0 || bl > MaxBulk {
			return p.fail("invalid bulk length")
		}
		end := j + int(bl)
		if end+2 > len(b) {
			return nil, 0, NeedMore
		}
		args = append(args, b[j:end])
		i = end + 2
	}
	p.args = args
	return args, i, OK
}

// line reads a decimal length line ending in CRLF starting at i, returning the
// value and the index past the terminator. The digit cap keeps the value far
// from overflow, so the callers' range checks are exact.
func (p *Parser) line(b []byte, i int, msg string) (int64, int, Status) {
	neg := false
	if i < len(b) && b[i] == '-' {
		neg = true
		i++
	}
	start := i
	var v int64
	for ; i < len(b); i++ {
		c := b[i]
		if c >= '0' && c <= '9' {
			if i-start >= maxHeaderDigits {
				_, _, st := p.fail(msg)
				return 0, 0, st
			}
			v = v*10 + int64(c-'0')
			continue
		}
		if c != '\r' || i == start {
			_, _, st := p.fail(msg)
			return 0, 0, st
		}
		if i+1 == len(b) {
			return 0, 0, NeedMore
		}
		if b[i+1] != '\n' {
			_, _, st := p.fail(msg)
			return 0, 0, st
		}
		if neg {
			v = -v
		}
		return v, i + 2, OK
	}
	return 0, 0, NeedMore
}

// inline is the telnet slow path: one line, whitespace-split. The quoted forms
// of Redis's inline protocol are not carried; inline exists for a bare netcat,
// and every client speaks the array form.
func (p *Parser) inline(b []byte) ([][]byte, int, Status) {
	nl := -1
	for i, c := range b {
		if c == '\n' {
			nl = i
			break
		}
	}
	if nl < 0 {
		if len(b) > maxInline {
			return p.fail("too big inline request")
		}
		return nil, 0, NeedMore
	}
	if nl > maxInline {
		return p.fail("too big inline request")
	}
	line := b[:nl]
	if n := len(line); n > 0 && line[n-1] == '\r' {
		line = line[:n-1]
	}
	args := p.args[:0]
	i := 0
	for i < len(line) {
		for i < len(line) && (line[i] == ' ' || line[i] == '\t') {
			i++
		}
		start := i
		for i < len(line) && line[i] != ' ' && line[i] != '\t' {
			i++
		}
		if i > start {
			args = append(args, line[start:i])
		}
	}
	p.args = args
	return args, nl + 1, OK
}

func (p *Parser) fail(msg string) ([][]byte, int, Status) {
	p.err = msg
	return nil, 0, ProtoErr
}

// printable renders one byte for the expected-'$' message the way the error
// will travel on the wire: printable ASCII as itself, anything else as a dot,
// so a fuzzed byte can never smuggle a CR into the error reply.
func printable(c byte) string {
	if c >= 0x20 && c < 0x7f {
		return string([]byte{c})
	}
	return "."
}
