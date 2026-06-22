package lua

import "testing"

// runExpr runs a chunk that returns one value and returns it as a string via the
// engine's tostring rule. It fails the test on any error.
func runValue(t *testing.T, src string) Value {
	t.Helper()
	i := New()
	vals, err := i.Run(src)
	if err != nil {
		t.Fatalf("run %q: %v", src, err)
	}
	if len(vals) == 0 {
		return Nil
	}
	return vals[0]
}

func runStr(t *testing.T, src string) string {
	return ToString(runValue(t, src))
}

func TestArithmetic(t *testing.T) {
	cases := map[string]string{
		"return 1 + 2 * 3":        "7",
		"return (1 + 2) * 3":      "9",
		"return 2 ^ 10":           "1024",
		"return 7 % 3":            "1",
		"return 10 / 4":           "2.5",
		"return -5 + 3":           "-2",
		"return 2 ^ 2 ^ 3":        "256", // right associative
		"return 1 < 2 and 3 or 4": "3",
		"return 1 > 2 and 3 or 4": "4",
	}
	for src, want := range cases {
		if got := runStr(t, src); got != want {
			t.Errorf("%q = %q, want %q", src, got, want)
		}
	}
}

func TestStringsAndConcat(t *testing.T) {
	cases := map[string]string{
		`return "a" .. "b" .. "c"`:              "abc",
		`return "x" .. 1 .. 2`:                  "x12",
		`return #"hello"`:                       "5",
		`return string.upper("abc")`:            "ABC",
		`return string.sub("hello", 2, 4)`:      "ell",
		`return string.rep("ab", 3)`:            "ababab",
		`return string.format("%d-%s", 7, "x")`: "7-x",
		`return tostring(1 == 1)`:               "true",
		`return string.sub("hello", -3)`:        "llo",
	}
	for src, want := range cases {
		if got := runStr(t, src); got != want {
			t.Errorf("%q = %q, want %q", src, got, want)
		}
	}
}

func TestControlFlow(t *testing.T) {
	src := `
		local sum = 0
		for i = 1, 10 do
			sum = sum + i
		end
		return sum`
	if got := runStr(t, src); got != "55" {
		t.Errorf("for-sum = %q, want 55", got)
	}

	whileSrc := `
		local n, f = 5, 1
		while n > 1 do
			f = f * n
			n = n - 1
		end
		return f`
	if got := runStr(t, whileSrc); got != "120" {
		t.Errorf("while-fact = %q, want 120", got)
	}

	breakSrc := `
		local i = 0
		while true do
			i = i + 1
			if i == 3 then break end
		end
		return i`
	if got := runStr(t, breakSrc); got != "3" {
		t.Errorf("break = %q, want 3", got)
	}
}

func TestFunctionsAndClosures(t *testing.T) {
	src := `
		local function adder(n)
			return function(x) return x + n end
		end
		local add5 = adder(5)
		return add5(10)`
	if got := runStr(t, src); got != "15" {
		t.Errorf("closure = %q, want 15", got)
	}

	recur := `
		local function fib(n)
			if n < 2 then return n end
			return fib(n-1) + fib(n-2)
		end
		return fib(10)`
	if got := runStr(t, recur); got != "55" {
		t.Errorf("fib = %q, want 55", got)
	}

	multi := `
		local function two() return 1, 2 end
		local a, b = two()
		return a + b`
	if got := runStr(t, multi); got != "3" {
		t.Errorf("multi-return = %q, want 3", got)
	}
}

func TestTables(t *testing.T) {
	src := `
		local t = {10, 20, 30}
		t[4] = 40
		local sum = 0
		for i, v in ipairs(t) do sum = sum + v end
		return sum`
	if got := runStr(t, src); got != "100" {
		t.Errorf("ipairs-sum = %q, want 100", got)
	}

	keyed := `
		local t = {a = 1, b = 2, c = 3}
		local sum = 0
		for k, v in pairs(t) do sum = sum + v end
		return sum`
	if got := runStr(t, keyed); got != "6" {
		t.Errorf("pairs-sum = %q, want 6", got)
	}

	insert := `
		local t = {}
		table.insert(t, "x")
		table.insert(t, "y")
		table.insert(t, 1, "z")
		return table.concat(t, ",")`
	if got := runStr(t, insert); got != "z,x,y" {
		t.Errorf("insert/concat = %q, want z,x,y", got)
	}

	sortSrc := `
		local t = {3, 1, 2}
		table.sort(t)
		return table.concat(t, "")`
	if got := runStr(t, sortSrc); got != "123" {
		t.Errorf("sort = %q, want 123", got)
	}
}

func TestPatterns(t *testing.T) {
	cases := map[string]string{
		`return string.match("hello123world", "%d+")`:                 "123",
		`return string.match("key=value", "(%w+)=(%w+)")`:             "key",
		`return ({string.match("key=value", "(%w+)=(%w+)")})[2]`:      "value",
		`return string.gsub("hello world", "o", "0")`:                 "hell0 w0rld",
		`local n = select(2, string.gsub("aaa", "a", "b")); return n`: "3",
		`return string.find("abcdef", "cd")`:                          "3",
	}
	for src, want := range cases {
		if got := runStr(t, src); got != want {
			t.Errorf("%q = %q, want %q", src, got, want)
		}
	}

	gmatchSrc := `
		local out = {}
		for word in string.gmatch("the quick brown", "%a+") do
			table.insert(out, word)
		end
		return table.concat(out, "-")`
	if got := runStr(t, gmatchSrc); got != "the-quick-brown" {
		t.Errorf("gmatch = %q, want the-quick-brown", got)
	}
}

func TestPcallAndError(t *testing.T) {
	ok := `
		local ok, err = pcall(function() error("boom") end)
		if ok then return "no-error" end
		return err`
	if got := runStr(t, ok); got != "boom" {
		t.Errorf("pcall error = %q, want boom", got)
	}

	good := `
		local ok, v = pcall(function() return 42 end)
		return tostring(ok) .. ":" .. tostring(v)`
	if got := runStr(t, good); got != "true:42" {
		t.Errorf("pcall ok = %q, want true:42", got)
	}
}

func TestMetatableIndex(t *testing.T) {
	src := `
		local base = {greet = function() return "hi" end}
		local obj = setmetatable({}, {__index = base})
		return obj.greet()`
	if got := runStr(t, src); got != "hi" {
		t.Errorf("__index = %q, want hi", got)
	}
}

func TestMathLib(t *testing.T) {
	cases := map[string]string{
		"return math.floor(3.7)":   "3",
		"return math.max(1, 9, 4)": "9",
		"return math.min(1, 9, 4)": "1",
		"return math.abs(-5)":      "5",
	}
	for src, want := range cases {
		if got := runStr(t, src); got != want {
			t.Errorf("%q = %q, want %q", src, got, want)
		}
	}
}

func TestSyntaxError(t *testing.T) {
	i := New()
	if _, err := i.Run("local x ="); err == nil {
		t.Fatal("expected a parse error for incomplete assignment")
	}
}

func TestHookAborts(t *testing.T) {
	i := New()
	i.SetHook(1, func() error { return runtimeErr("interrupted") })
	_, err := i.Run("while true do end")
	if err == nil {
		t.Fatal("expected the hook to abort the infinite loop")
	}
}
