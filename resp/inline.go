package resp

import (
	"strconv"
)

// ParseInline parses one inline command line from buf starting at pos: the
// telnet-friendly form where a human types space-separated words instead of a
// multibulk frame (doc 06 §5). It returns the argument vector, the position past
// the terminating newline, and an error. ErrNeedMore means no complete line is
// buffered yet. A ProtocolError ("too big inline request" or "unbalanced quotes
// in request") is fatal and the connection is closed.
//
// The parser strips a trailing CR (so both \n and \r\n terminate), splits on
// runs of whitespace, and honours single and double quoting with the same
// escape rules redis-cli uses.
func ParseInline(buf []byte, pos int) ([][]byte, int, error) {
	end := -1
	for i := pos; i < len(buf); i++ {
		if buf[i] == '\n' {
			end = i
			break
		}
		if i-pos+1 > MaxInlineLen {
			return nil, pos, ErrProtocol("too big inline request")
		}
	}
	if end < 0 {
		if len(buf)-pos > MaxInlineLen {
			return nil, pos, ErrProtocol("too big inline request")
		}
		return nil, pos, ErrNeedMore
	}
	line := buf[pos:end]
	if len(line) > 0 && line[len(line)-1] == '\r' {
		line = line[:len(line)-1]
	}
	args, err := splitInlineArgs(line)
	if err != nil {
		return nil, pos, err
	}
	return args, end + 1, nil
}

// splitInlineArgs tokenizes one inline line into arguments following the
// redis-cli quoting rules: double quotes allow C-style escapes, single quotes
// are literal, and unquoted runs split on whitespace.
func splitInlineArgs(line []byte) ([][]byte, error) {
	var args [][]byte
	i := 0
	n := len(line)
	for {
		for i < n && isInlineSpace(line[i]) {
			i++
		}
		if i >= n {
			break
		}
		var cur []byte
		inDQ, inSQ := false, false
		for i < n {
			c := line[i]
			switch {
			case inDQ:
				if c == '\\' && i+1 < n {
					esc, adv, ok := unescapeDouble(line[i+1:])
					if !ok {
						return nil, ErrProtocol("unbalanced quotes in request")
					}
					cur = append(cur, esc...)
					i += 1 + adv
					continue
				}
				if c == '"' {
					inDQ = false
					i++
					// A closing quote must be followed by whitespace or EOL.
					if i < n && !isInlineSpace(line[i]) {
						return nil, ErrProtocol("unbalanced quotes in request")
					}
					continue
				}
				cur = append(cur, c)
				i++
			case inSQ:
				if c == '\'' {
					inSQ = false
					i++
					if i < n && !isInlineSpace(line[i]) {
						return nil, ErrProtocol("unbalanced quotes in request")
					}
					continue
				}
				cur = append(cur, c)
				i++
			default:
				if isInlineSpace(c) {
					i++
					goto done
				}
				switch c {
				case '"':
					inDQ = true
					i++
				case '\'':
					inSQ = true
					i++
				default:
					cur = append(cur, c)
					i++
				}
			}
		}
		if inDQ || inSQ {
			return nil, ErrProtocol("unbalanced quotes in request")
		}
	done:
		if cur == nil {
			cur = []byte{}
		}
		args = append(args, cur)
	}
	return args, nil
}

// unescapeDouble decodes one backslash escape inside a double-quoted token,
// given the bytes following the backslash. It returns the decoded bytes, how
// many input bytes it consumed (after the backslash), and whether the escape was
// well-formed.
func unescapeDouble(rest []byte) ([]byte, int, bool) {
	if len(rest) == 0 {
		return nil, 0, false
	}
	switch rest[0] {
	case 'n':
		return []byte{'\n'}, 1, true
	case 'r':
		return []byte{'\r'}, 1, true
	case 't':
		return []byte{'\t'}, 1, true
	case 'b':
		return []byte{'\b'}, 1, true
	case 'a':
		return []byte{'\a'}, 1, true
	case 'x':
		if len(rest) >= 3 && isHex(rest[1]) && isHex(rest[2]) {
			v := hexVal(rest[1])<<4 | hexVal(rest[2])
			return []byte{v}, 3, true
		}
		// Not a valid hex escape; pass the 'x' through literally.
		return []byte{'x'}, 1, true
	case 'u':
		if len(rest) >= 5 && isHex(rest[1]) && isHex(rest[2]) && isHex(rest[3]) && isHex(rest[4]) {
			cp, _ := strconv.ParseInt(string(rest[1:5]), 16, 32)
			return []byte(string(rune(cp))), 5, true
		}
		return []byte{'u'}, 1, true
	default:
		// Any other escaped byte is taken literally, including \" and \\.
		return []byte{rest[0]}, 1, true
	}
}

func isInlineSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\v' || c == '\f' || c == '\r'
}

func isHex(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}

func hexVal(c byte) byte {
	switch {
	case c >= '0' && c <= '9':
		return c - '0'
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10
	default:
		return c - 'A' + 10
	}
}
