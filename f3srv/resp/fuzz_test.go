package resp

import (
	"bytes"
	"testing"
	"unsafe"
)

// FuzzParse drives the parser with arbitrary bytes under two invariants:
//
//  1. Safety: no panic, no out-of-bounds view, forward progress on every OK
//     (consumed > 0 and within the buffer).
//  2. Chop invariance: parsing the stream in any read chopping yields the same
//     command sequence and the same terminal state as parsing it whole. This
//     is the property the driver leans on when it restarts the parser on a
//     NeedMore after the next read.
//
// The seed corpus in testdata/fuzz/FuzzParse covers the command shapes the M0
// surface speaks: point strings, SET option runs, pipelines, partial frames,
// empty bulks, skipped arrays, inline lines, and malformed headers.
func FuzzParse(f *testing.F) {
	seeds := []string{
		"*1\r\n$4\r\nPING\r\n",
		"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
		"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n",
		"*5\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n$2\r\nPX\r\n$3\r\n100\r\n",
		"*3\r\n$6\r\nAPPEND\r\n$1\r\nk\r\n$5\r\nworld\r\n",
		"*4\r\n$8\r\nSETRANGE\r\n$1\r\nk\r\n$2\r\n10\r\n$3\r\nabc\r\n",
		"*3\r\n$11\r\nINCRBYFLOAT\r\n$1\r\nk\r\n$4\r\n1.5e\r\n",
		"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$0\r\n\r\n",
		"*0\r\n*-1\r\nPING\r\n",
		"PING\r\nSET foo bar\r\n*1\r\n$4\r\nPING\r\n",
		"*2\r\n$4\r\nECHO\r\n$5\r\nhel",
		"*3\r\n$3\r\nSET\r\n",
		"*abc\r\n",
		"*1\r\n$-1\r\n",
		"*1\r\n@4\r\nPING\r\n",
		"$5\r\nhello\r\n",
		"\r\n\n  \n",
	}
	for _, s := range seeds {
		f.Add([]byte(s), uint8(1))
		f.Add([]byte(s), uint8(7))
	}
	f.Fuzz(func(t *testing.T, data []byte, chunk uint8) {
		if len(data) > 1<<18 {
			t.Skip()
		}
		wholeCmds, wholeEnd := fuzzParseChopped(t, data, len(data))
		k := int(chunk)
		if k < 1 {
			k = 1
		}
		chopCmds, chopEnd := fuzzParseChopped(t, data, k)
		if wholeEnd != chopEnd {
			t.Fatalf("terminal state differs: whole %q, chunk %d %q", wholeEnd, k, chopEnd)
		}
		if len(wholeCmds) != len(chopCmds) {
			t.Fatalf("command count differs: whole %d, chunk %d %d", len(wholeCmds), k, len(chopCmds))
		}
		for i := range wholeCmds {
			if !bytes.Equal(wholeCmds[i], chopCmds[i]) {
				t.Fatalf("command %d differs: whole %q, chunk %d %q", i, wholeCmds[i], k, chopCmds[i])
			}
		}
	})
}

// fuzzParseChopped runs the parser over data delivered in chunk-sized reads
// and returns the parsed commands (args joined with NUL) plus the terminal
// state ("drained", "partial", or the protocol error text). It enforces the
// safety invariants on every step.
func fuzzParseChopped(t *testing.T, data []byte, chunk int) ([][]byte, string) {
	t.Helper()
	var p Parser
	var out [][]byte
	buf := make([]byte, 0, len(data))
	fed, pos := 0, 0
	for {
		window := buf[pos:]
		args, consumed, st := p.Next(window)
		switch st {
		case OK:
			if consumed <= 0 || consumed > len(window) {
				t.Fatalf("OK consumed %d of %d", consumed, len(window))
			}
			for _, a := range args {
				if !inBounds(window[:consumed], a) {
					t.Fatalf("argument view escapes the consumed region")
				}
			}
			pos += consumed
			if len(args) > 0 {
				out = append(out, bytes.Join(args, []byte{0}))
			}
		case NeedMore:
			if consumed != 0 {
				t.Fatalf("NeedMore consumed %d", consumed)
			}
			if fed == len(data) {
				if pos == len(buf) {
					return out, "drained"
				}
				return out, "partial"
			}
			n := fed + chunk
			if n > len(data) {
				n = len(data)
			}
			buf = append(buf, data[fed:n]...)
			fed = n
		case ProtoErr:
			if consumed != 0 {
				t.Fatalf("ProtoErr consumed %d", consumed)
			}
			if p.LastError() == "" {
				t.Fatal("ProtoErr with empty LastError")
			}
			return out, p.LastError()
		default:
			t.Fatalf("unknown status %v", st)
		}
	}
}

// inBounds reports whether view is a subslice of region, by address.
func inBounds(region, view []byte) bool {
	if len(view) == 0 {
		return true
	}
	if len(region) == 0 {
		return false
	}
	r0 := uintptr(unsafe.Pointer(&region[0]))
	rend := uintptr(unsafe.Pointer(&region[len(region)-1]))
	v0 := uintptr(unsafe.Pointer(&view[0]))
	vend := uintptr(unsafe.Pointer(&view[len(view)-1]))
	return v0 >= r0 && vend <= rend
}
