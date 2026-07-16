package sqlo1b

// The R-I4 boundary scan (doc 04 section 13): no code above the IO
// layer calls pread or pwrite directly. The cold read command path
// resolves everything through GroupSource and DirSource, so its files
// must contain zero direct file calls; the lanes that legitimately
// own a file handle are enumerated below with their reasons. A new
// file failing the scan is not an error in the scan: classify it into
// one of the two lists and say which lane it belongs to.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
)

// directIOLanes are the files allowed to touch the data file: the IO
// backends themselves, the open and recovery lane that runs before
// the store serves anything, the drain and checkpoint write lane, and
// the maintenance lane whose reads are bg-priority by construction.
var directIOLanes = map[string]string{
	"iopool.go":     "the portable IO backend",
	"faultio.go":    "the fault-injection backend for crash tests",
	"superblock.go": "open lane: superblock read and write",
	"grid.go":       "open lane: extent header scan",
	"store.go":      "drain and checkpoint write lane",
	"extent.go":     "maintenance lane: extent header IO",
	"compact.go":    "maintenance lane: relocation reads",
	"debt.go":       "maintenance lane: garbage accounting reads",
	"scrub.go":      "maintenance lane: rolling crc reads",
	"blob.go":       "blob lane: escape reads through the store's handle",
}

// directIOCalls are the selector names that mean a direct file call.
var directIOCalls = map[string]bool{
	"ReadAt": true, "WriteAt": true, "Pread": true, "Pwrite": true,
}

func TestColdReadPathHasNoDirectIO(t *testing.T) {
	fset := token.NewFileSet()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		calls := 0
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			if sel, ok := call.Fun.(*ast.SelectorExpr); ok && directIOCalls[sel.Sel.Name] {
				calls++
				if _, allowed := directIOLanes[name]; !allowed {
					t.Errorf("%s: %s call at %s outside every sanctioned lane",
						name, sel.Sel.Name, fset.Position(call.Pos()))
				}
			}
			return true
		})
		if calls > 0 {
			seen[name] = true
		}
	}
	// A lane entry nothing uses anymore is a stale allowance; shrink
	// the list rather than let it grow silently permissive.
	for name, lane := range directIOLanes {
		if !seen[name] {
			t.Errorf("%s (%s) no longer performs direct IO; drop it from the lane list", name, lane)
		}
	}
}
