package sqlo1

import (
	"errors"
	"fmt"
	"strconv"
)

// RESP2 wire handling: the request parser and the reply builder. Both sides
// work on plain byte slices so the server owns all buffering; the parser
// returns arguments that alias the input buffer and the builder appends to
// a caller-provided reply buffer, so a command that hits the hot tier can
// run parse-execute-reply without allocating.

// Request size limits, matching the Redis server's own.
const (
	maxArgs    = 1024 * 1024 // elements in a multibulk request
	maxBulkLen = 512 << 20   // bytes in one argument
	// maxInlineLen bounds an inline command line, matching
	// PROTO_INLINE_MAX_SIZE.
	maxInlineLen = 64 << 10
)

// ErrIncomplete reports that buf holds only a prefix of a command; the
// caller reads more bytes and retries from the same offset.
var ErrIncomplete = errors.New("sqlo1: incomplete command")

// ProtoError is a client protocol violation. The server replies with the
// message and closes the connection, which is what Redis does.
type ProtoError struct{ msg string }

func (e *ProtoError) Error() string { return e.msg }

func protoErrf(format string, args ...any) error {
	return &ProtoError{msg: "ERR Protocol error: " + fmt.Sprintf(format, args...)}
}

// ParseCommand parses one client command from the front of buf, appends
// the arguments onto args, and returns the extended slice and the number
// of bytes consumed. Append-style so a connection loop reuses one argument
// slice for its whole life: pass args[:0] and keep the return value, which
// comes back on every path (partially appended on errors, contents valid
// only when err is nil) so its capacity survives ErrIncomplete waits and
// the steady state never allocates. The arguments alias buf and are valid
// until the caller reuses it. A multibulk request is the normal path; a
// line not starting with '*' is an inline command. An empty inline line is
// consumed and returns zero arguments, which the caller skips.
func ParseCommand(buf []byte, args [][]byte) (out [][]byte, n int, err error) {
	if len(buf) == 0 {
		return args, 0, ErrIncomplete
	}
	if buf[0] != '*' {
		return parseInline(buf, args)
	}

	count, p, err := parseNumberLine(buf, 1)
	if err != nil {
		return args, 0, err
	}
	if count > maxArgs {
		return args, 0, protoErrf("invalid multibulk length")
	}
	// Redis consumes *0 and negative counts as an empty command.
	if count <= 0 {
		return args, p, nil
	}

	for range count {
		if p >= len(buf) {
			return args, 0, ErrIncomplete
		}
		if buf[p] != '$' {
			return args, 0, protoErrf("expected '$', got '%c'", buf[p])
		}
		blen, q, err := parseNumberLine(buf, p+1)
		if err != nil {
			return args, 0, err
		}
		if blen < 0 || blen > maxBulkLen {
			return args, 0, protoErrf("invalid bulk length")
		}
		if q+blen+2 > len(buf) {
			return args, 0, ErrIncomplete
		}
		if buf[q+blen] != '\r' || buf[q+blen+1] != '\n' {
			return args, 0, protoErrf("invalid bulk terminator")
		}
		args = append(args, buf[q:q+blen])
		p = q + blen + 2
	}
	return args, p, nil
}

// parseNumberLine reads a decimal integer starting at buf[p] terminated by
// CRLF and returns the value and the offset just past the terminator.
func parseNumberLine(buf []byte, p int) (int, int, error) {
	neg := false
	if p < len(buf) && buf[p] == '-' {
		neg = true
		p++
	}
	v := 0
	digits := 0
	for ; p < len(buf); p++ {
		c := buf[p]
		if c >= '0' && c <= '9' {
			if digits > 10 {
				return 0, 0, protoErrf("invalid multibulk length")
			}
			v = v*10 + int(c-'0')
			digits++
			continue
		}
		break
	}
	if digits == 0 {
		if p >= len(buf) {
			return 0, 0, ErrIncomplete
		}
		return 0, 0, protoErrf("invalid length")
	}
	if p+1 >= len(buf) {
		return 0, 0, ErrIncomplete
	}
	if buf[p] != '\r' || buf[p+1] != '\n' {
		return 0, 0, protoErrf("invalid length terminator")
	}
	if neg {
		v = -v
	}
	return v, p + 2, nil
}

// parseInline parses an inline command: arguments separated by spaces or
// tabs on one newline-terminated line. Quoting is not supported; inline
// exists for humans poking a socket, and the G5 seed script, not clients.
func parseInline(buf []byte, args [][]byte) (out [][]byte, n int, err error) {
	end := -1
	for i, c := range buf {
		if c == '\n' {
			end = i
			break
		}
		if i >= maxInlineLen {
			return args, 0, protoErrf("too big inline request")
		}
	}
	if end < 0 {
		if len(buf) > maxInlineLen {
			return args, 0, protoErrf("too big inline request")
		}
		return args, 0, ErrIncomplete
	}
	line := buf[:end]
	if len(line) > 0 && line[len(line)-1] == '\r' {
		line = line[:len(line)-1]
	}
	p := 0
	for p < len(line) {
		for p < len(line) && (line[p] == ' ' || line[p] == '\t') {
			p++
		}
		start := p
		for p < len(line) && line[p] != ' ' && line[p] != '\t' {
			p++
		}
		if p > start {
			args = append(args, line[start:p])
		}
	}
	return args, end + 1, nil
}

// Reply builder. Each Append writes one RESP2 reply element onto b and
// returns the extended slice; the caller presizes b once per output batch.

// AppendSimple appends a simple string reply, +s\r\n.
func AppendSimple(b []byte, s string) []byte {
	b = append(b, '+')
	b = append(b, s...)
	return append(b, '\r', '\n')
}

// AppendError appends an error reply, -msg\r\n.
func AppendError(b []byte, msg string) []byte {
	b = append(b, '-')
	b = append(b, msg...)
	return append(b, '\r', '\n')
}

// AppendInt appends an integer reply, :n\r\n.
func AppendInt(b []byte, n int64) []byte {
	b = append(b, ':')
	b = strconv.AppendInt(b, n, 10)
	return append(b, '\r', '\n')
}

// AppendBulk appends a bulk string reply, $len\r\nv\r\n.
func AppendBulk(b []byte, v []byte) []byte {
	b = append(b, '$')
	b = strconv.AppendInt(b, int64(len(v)), 10)
	b = append(b, '\r', '\n')
	b = append(b, v...)
	return append(b, '\r', '\n')
}

// AppendNullBulk appends the RESP2 null bulk reply, $-1\r\n.
func AppendNullBulk(b []byte) []byte {
	return append(b, '$', '-', '1', '\r', '\n')
}

// AppendNullArray appends the RESP2 null array reply, *-1\r\n: the
// nil-answer shape of the commands whose present answer is an array
// (ZRANK WITHSCORE).
func AppendNullArray(b []byte) []byte {
	return append(b, '*', '-', '1', '\r', '\n')
}

// AppendArray appends an array header for n following elements.
func AppendArray(b []byte, n int) []byte {
	b = append(b, '*')
	b = strconv.AppendInt(b, int64(n), 10)
	return append(b, '\r', '\n')
}

// BulkSize returns the encoded size of a bulk reply for a value of length
// vlen, for presizing reply buffers.
func BulkSize(vlen int) int {
	d := 1
	for v := vlen; v >= 10; v /= 10 {
		d++
	}
	return 1 + d + 2 + vlen + 2
}
