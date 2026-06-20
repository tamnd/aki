package resp

import "strconv"

// ParseRequest extracts one client command from a query buffer. Client commands
// arrive either as a multibulk frame (every real client library) or as an inline
// line (a human on telnet); the first non-blank byte selects the path: '*' means
// multibulk, anything else means inline (doc 06 §5.5, §6).
//
// It returns the argument vector (argv[0] is the command name), the position
// just past the consumed bytes, and an error. ErrNeedMore means the buffer does
// not yet hold a complete command and pos is returned unchanged, so the read
// loop can append more bytes and call again from the same offset. A
// ProtocolError is fatal: the read loop sends it and closes the connection.
//
// A blank line (a lone CRLF or LF, which telnet clients send) is consumed and
// reported as an empty argv with a nil error; the caller skips it and retries.
//
// maxBulkLen caps a single bulk argument (proto-max-bulk-len); pass
// DefaultMaxBulkLen for the default 512 MiB.
func ParseRequest(buf []byte, pos int, maxBulkLen int64) ([][]byte, int, error) {
	if pos >= len(buf) {
		return nil, pos, ErrNeedMore
	}
	switch buf[pos] {
	case '\r', '\n':
		// Blank line: consume the terminator and return an empty command.
		if buf[pos] == '\r' {
			if pos+1 >= len(buf) {
				return nil, pos, ErrNeedMore
			}
			if buf[pos+1] != '\n' {
				return nil, pos, ErrProtocol("invalid blank line")
			}
			return nil, pos + 2, nil
		}
		return nil, pos + 1, nil
	case '*':
		return parseMultibulk(buf, pos, maxBulkLen)
	default:
		return ParseInline(buf, pos)
	}
}

// parseMultibulk parses a "*<argc>\r\n" header followed by argc "$<len>\r\n<data>\r\n"
// bulk elements. Every element must be a bulk string; any other leading byte is
// the "expected '$', got 'X'" fatal error.
func parseMultibulk(buf []byte, pos int, maxBulkLen int64) ([][]byte, int, error) {
	start := pos
	line, p, err := readLine(buf, pos+1) // skip '*'
	if err != nil {
		return nil, start, err
	}
	argc, perr := strconv.ParseInt(string(line), 10, 64)
	if perr != nil || argc < -1 {
		return nil, start, ErrProtocol("invalid multibulk length")
	}
	if argc <= 0 {
		// *0 and *-1 are empty commands the caller skips.
		return nil, p, nil
	}
	if argc > MaxMultibulkLen {
		return nil, start, ErrProtocol("invalid multibulk length")
	}
	args := make([][]byte, 0, argc)
	for range argc {
		if p >= len(buf) {
			return nil, start, ErrNeedMore
		}
		if buf[p] != '$' {
			return nil, start, errExpectedDollar(buf[p])
		}
		lenLine, dataPos, err := readLine(buf, p+1)
		if err != nil {
			return nil, start, err
		}
		blen, lerr := strconv.ParseInt(string(lenLine), 10, 64)
		if lerr != nil || blen < 0 {
			return nil, start, ErrProtocol("invalid bulk length")
		}
		if blen > maxBulkLen {
			return nil, start, ErrProtocol("invalid bulk length")
		}
		if dataPos+int(blen)+2 > len(buf) {
			return nil, start, ErrNeedMore
		}
		if buf[dataPos+int(blen)] != '\r' || buf[dataPos+int(blen)+1] != '\n' {
			return nil, start, ErrProtocol("invalid bulk string CRLF")
		}
		args = append(args, cloneBytes(buf[dataPos:dataPos+int(blen)]))
		p = dataPos + int(blen) + 2
	}
	return args, p, nil
}
