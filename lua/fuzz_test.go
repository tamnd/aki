package lua

import "testing"

// FuzzParse feeds arbitrary source to the Lua parser, which must never panic.
// A syntax error is a fine outcome; a panic is a bug (doc 23 §7.6).
func FuzzParse(f *testing.F) {
	seeds := []string{
		"return 1",
		"return KEYS[1]",
		"local x = 1 + 2 return x",
		"if true then return 'a' else return 'b' end",
		"for i=1,10 do end",
		"return {1, 2, 3}",
		"return string.format('%d', 42)",
		"",
		"((((",
		"return",
	}
	for _, s := range seeds {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, src string) {
		_, _ = Parse(src)
	})
}

// FuzzRun feeds arbitrary source to the interpreter. A deadline hook bounds the
// work so an infinite loop in the input terminates instead of hanging the
// fuzzer, mirroring the script-kill timeout. The run must never panic and must
// always return.
func FuzzRun(f *testing.F) {
	seeds := []string{
		"return 1 + 1",
		"local t = {} for i=1,100 do t[i] = i end return #t",
		"return string.rep('x', 10)",
		"while true do end", // must be cut short by the hook
		"return nil",
	}
	for _, s := range seeds {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, src string) {
		i := New()
		ticks := 0
		i.SetHook(100, func() error {
			ticks++
			if ticks > 1000 {
				return errFuzzDeadline
			}
			return nil
		})
		_, _ = i.Run(src)
	})
}

// errFuzzDeadline aborts a fuzzed script that runs too long.
var errFuzzDeadline = fuzzErr("fuzz deadline")

type fuzzErr string

func (e fuzzErr) Error() string { return string(e) }
