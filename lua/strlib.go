package lua

import (
	"fmt"
	"strconv"
	"strings"
)

// This file implements the string library, including a from-scratch Lua 5.1
// pattern matcher used by find, match, gmatch, and gsub. Lua patterns are not
// regular expressions; they are the small pattern language the reference manual
// describes (classes like %a and %d, sets, the * + - ? quantifiers, captures,
// anchors, %b balanced match, and %f frontier).

// openString installs the string library.
func openString(i *Interp) {
	lib := NewTable()
	set := func(name string, fn GoFunc) {
		lib.Set(String(name), &Function{gofn: fn, name: "string." + name})
	}
	set("len", strLen)
	set("sub", strSub)
	set("upper", strUpper)
	set("lower", strLower)
	set("rep", strRep)
	set("reverse", strReverse)
	set("byte", strByte)
	set("char", strChar)
	set("format", strFormat)
	set("find", strFind)
	set("match", strMatch)
	set("gmatch", strGmatch)
	set("gsub", strGsub)
	i.globals.Set(String("string"), lib)
}

func strLen(_ *Interp, args []Value) ([]Value, error) {
	s, err := checkString(args, 1, "len")
	if err != nil {
		return nil, err
	}
	return []Value{Number(len(s))}, nil
}

// strIndex converts a possibly-negative Lua string index to a 1-based position.
func strIndex(i, length int) int {
	if i < 0 {
		i = length + i + 1
	}
	return i
}

func strSub(_ *Interp, args []Value) ([]Value, error) {
	s, err := checkString(args, 1, "sub")
	if err != nil {
		return nil, err
	}
	l := len(s)
	from := strIndex(optInt(args, 2, 1), l)
	to := strIndex(optInt(args, 3, -1), l)
	if from < 1 {
		from = 1
	}
	if to > l {
		to = l
	}
	if from > to {
		return []Value{String("")}, nil
	}
	return []Value{String(s[from-1 : to])}, nil
}

func strUpper(_ *Interp, args []Value) ([]Value, error) {
	s, err := checkString(args, 1, "upper")
	if err != nil {
		return nil, err
	}
	return []Value{String(strings.ToUpper(s))}, nil
}

func strLower(_ *Interp, args []Value) ([]Value, error) {
	s, err := checkString(args, 1, "lower")
	if err != nil {
		return nil, err
	}
	return []Value{String(strings.ToLower(s))}, nil
}

func strRep(_ *Interp, args []Value) ([]Value, error) {
	s, err := checkString(args, 1, "rep")
	if err != nil {
		return nil, err
	}
	n := optInt(args, 2, 0)
	if n <= 0 {
		return []Value{String("")}, nil
	}
	return []Value{String(strings.Repeat(s, n))}, nil
}

func strReverse(_ *Interp, args []Value) ([]Value, error) {
	s, err := checkString(args, 1, "reverse")
	if err != nil {
		return nil, err
	}
	b := []byte(s)
	for i, j := 0, len(b)-1; i < j; i, j = i+1, j-1 {
		b[i], b[j] = b[j], b[i]
	}
	return []Value{String(b)}, nil
}

func strByte(_ *Interp, args []Value) ([]Value, error) {
	s, err := checkString(args, 1, "byte")
	if err != nil {
		return nil, err
	}
	l := len(s)
	from := strIndex(optInt(args, 2, 1), l)
	to := strIndex(optInt(args, 3, from), l)
	if from < 1 {
		from = 1
	}
	if to > l {
		to = l
	}
	var out []Value
	for k := from; k <= to; k++ {
		out = append(out, Number(s[k-1]))
	}
	return out, nil
}

func strChar(_ *Interp, args []Value) ([]Value, error) {
	b := make([]byte, len(args))
	for k := range args {
		f, err := checkNumber(args, k+1, "char")
		if err != nil {
			return nil, err
		}
		b[k] = byte(int(f))
	}
	return []Value{String(b)}, nil
}

// strFormat implements string.format for the directives Redis scripts use.
func strFormat(_ *Interp, args []Value) ([]Value, error) {
	format, err := checkString(args, 1, "format")
	if err != nil {
		return nil, err
	}
	var b strings.Builder
	argN := 2
	for i := 0; i < len(format); i++ {
		c := format[i]
		if c != '%' {
			b.WriteByte(c)
			continue
		}
		j := i + 1
		for j < len(format) && strings.IndexByte("-+ #0123456789.", format[j]) >= 0 {
			j++
		}
		if j >= len(format) {
			return nil, runtimeErr("invalid conversion '%%' to 'format'")
		}
		spec := format[i : j+1]
		verb := format[j]
		i = j
		if verb == '%' {
			b.WriteByte('%')
			continue
		}
		out, used, err := formatOne(spec, verb, args, argN)
		if err != nil {
			return nil, err
		}
		b.WriteString(out)
		argN += used
	}
	return []Value{String(b.String())}, nil
}

func formatOne(spec string, verb byte, args []Value, argN int) (string, int, error) {
	switch verb {
	case 'd', 'i':
		f, err := checkNumber(args, argN, "format")
		if err != nil {
			return "", 0, err
		}
		return fmt.Sprintf(strings.Replace(spec, string(verb), "d", 1), int64(f)), 1, nil
	case 'u':
		f, err := checkNumber(args, argN, "format")
		if err != nil {
			return "", 0, err
		}
		return fmt.Sprintf(strings.Replace(spec, "u", "d", 1), int64(f)), 1, nil
	case 'c':
		f, err := checkNumber(args, argN, "format")
		if err != nil {
			return "", 0, err
		}
		return string([]byte{byte(int(f))}), 1, nil
	case 'o', 'x', 'X':
		f, err := checkNumber(args, argN, "format")
		if err != nil {
			return "", 0, err
		}
		return fmt.Sprintf(spec, int64(f)), 1, nil
	case 'e', 'E', 'f', 'g', 'G':
		f, err := checkNumber(args, argN, "format")
		if err != nil {
			return "", 0, err
		}
		return fmt.Sprintf(spec, f), 1, nil
	case 's':
		s, err := checkString(args, argN, "format")
		if err != nil {
			return "", 0, err
		}
		return fmt.Sprintf(spec, s), 1, nil
	case 'q':
		s, err := checkString(args, argN, "format")
		if err != nil {
			return "", 0, err
		}
		return strconv.Quote(s), 1, nil
	default:
		return "", 0, runtimeErr("invalid conversion '%s' to 'format'", spec)
	}
}

func strFind(_ *Interp, args []Value) ([]Value, error) {
	s, err := checkString(args, 1, "find")
	if err != nil {
		return nil, err
	}
	pat, err := checkString(args, 2, "find")
	if err != nil {
		return nil, err
	}
	init := normInit(optInt(args, 3, 1), len(s))
	plain := len(args) >= 4 && Truthy(args[3])
	if plain || !hasSpecial(pat) {
		idx := strings.Index(s[init:], pat)
		if idx < 0 {
			return []Value{Nil}, nil
		}
		start := init + idx
		return []Value{Number(start + 1), Number(start + len(pat))}, nil
	}
	m := newMatcher(s, pat)
	start, end, caps, ok := m.match(init)
	if !ok {
		return []Value{Nil}, nil
	}
	out := []Value{Number(start + 1), Number(end)}
	out = append(out, caps...)
	return out, nil
}

func strMatch(_ *Interp, args []Value) ([]Value, error) {
	s, err := checkString(args, 1, "match")
	if err != nil {
		return nil, err
	}
	pat, err := checkString(args, 2, "match")
	if err != nil {
		return nil, err
	}
	init := normInit(optInt(args, 3, 1), len(s))
	m := newMatcher(s, pat)
	start, end, caps, ok := m.match(init)
	if !ok {
		return []Value{Nil}, nil
	}
	if len(caps) == 0 {
		return []Value{String(s[start:end])}, nil
	}
	return caps, nil
}

func strGmatch(_ *Interp, args []Value) ([]Value, error) {
	s, err := checkString(args, 1, "gmatch")
	if err != nil {
		return nil, err
	}
	pat, err := checkString(args, 2, "gmatch")
	if err != nil {
		return nil, err
	}
	pos := 0
	iter := func(_ *Interp, _ []Value) ([]Value, error) {
		for pos <= len(s) {
			m := newMatcher(s, pat)
			start, end, caps, ok := m.matchAt(pos)
			if ok {
				if end == pos {
					pos++
				} else {
					pos = end
				}
				if len(caps) == 0 {
					return []Value{String(s[start:end])}, nil
				}
				return caps, nil
			}
			pos++
		}
		return []Value{Nil}, nil
	}
	return []Value{&Function{gofn: iter, name: "gmatch_iter"}}, nil
}

func strGsub(i *Interp, args []Value) ([]Value, error) {
	s, err := checkString(args, 1, "gsub")
	if err != nil {
		return nil, err
	}
	pat, err := checkString(args, 2, "gsub")
	if err != nil {
		return nil, err
	}
	repl := nth(args, 2)
	maxN := optInt(args, 4, -1)
	var b strings.Builder
	count := 0
	pos := 0
	for pos <= len(s) {
		if maxN >= 0 && count >= maxN {
			break
		}
		m := newMatcher(s, pat)
		start, end, caps, ok := m.matchAt(pos)
		if !ok {
			if pos < len(s) {
				b.WriteByte(s[pos])
			}
			pos++
			continue
		}
		b.WriteString(s[pos:start])
		whole := s[start:end]
		rep, err := gsubRepl(i, repl, whole, caps)
		if err != nil {
			return nil, err
		}
		b.WriteString(rep)
		count++
		if end == pos {
			if pos < len(s) {
				b.WriteByte(s[pos])
			}
			pos++
		} else {
			pos = end
		}
	}
	if pos < len(s) {
		b.WriteString(s[pos:])
	}
	return []Value{String(b.String()), Number(count)}, nil
}

// gsubRepl computes the replacement for one match given the replacement value,
// which can be a string template, a table lookup, or a function.
func gsubRepl(i *Interp, repl Value, whole string, caps []Value) (string, error) {
	first := whole
	if len(caps) > 0 {
		first = ToString(caps[0])
	}
	switch r := repl.(type) {
	case String:
		return expandTemplate(string(r), whole, caps), nil
	case Number:
		return expandTemplate(numberToString(float64(r)), whole, caps), nil
	case *Table:
		v := r.Get(String(first))
		return replResult(v, whole)
	case *Function:
		callArgs := caps
		if len(callArgs) == 0 {
			callArgs = []Value{String(whole)}
		}
		rets, err := i.call(r, callArgs, 1)
		if err != nil {
			return "", err
		}
		return replResult(nth(rets, 0), whole)
	default:
		return "", runtimeErr("bad argument #3 to 'gsub' (string/function/table expected)")
	}
}

func replResult(v Value, whole string) (string, error) {
	switch x := v.(type) {
	case nilValue:
		return whole, nil
	case Bool:
		if !bool(x) {
			return whole, nil
		}
		return "", runtimeErr("invalid replacement value (a boolean)")
	case String:
		return string(x), nil
	case Number:
		return numberToString(float64(x)), nil
	default:
		return "", runtimeErr("invalid replacement value (a %s)", v.luaType())
	}
}

// expandTemplate handles %0 to %9 references in a gsub string replacement.
func expandTemplate(tmpl, whole string, caps []Value) string {
	var b strings.Builder
	for i := 0; i < len(tmpl); i++ {
		if tmpl[i] != '%' || i+1 >= len(tmpl) {
			b.WriteByte(tmpl[i])
			continue
		}
		n := tmpl[i+1]
		i++
		switch {
		case n == '%':
			b.WriteByte('%')
		case n == '0':
			b.WriteString(whole)
		case n >= '1' && n <= '9':
			idx := int(n - '1')
			if idx < len(caps) {
				b.WriteString(ToString(caps[idx]))
			} else if idx == 0 {
				b.WriteString(whole)
			}
		default:
			b.WriteByte('%')
			b.WriteByte(n)
		}
	}
	return b.String()
}

func normInit(init, length int) int {
	if init < 0 {
		init = length + init + 1
	}
	if init < 1 {
		init = 1
	}
	return init - 1
}

// hasSpecial reports whether a pattern contains any magic character, which tells
// find whether it can use a plain substring search.
func hasSpecial(pat string) bool {
	return strings.ContainsAny(pat, "^$*+?.([%-")
}
