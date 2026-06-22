package command

import (
	"sort"
	"strings"
)

// Table is the command registry: a name -> descriptor map built once at startup
// from a slice of descriptors. Container commands also index their subcommands.
type Table struct {
	byName map[string]*CmdDesc
}

// NewTable builds a Table from the given descriptors. A descriptor with a
// non-empty SubName is not a top-level entry; it is reached through its parent's
// SubCmds list.
func NewTable(cmds []*CmdDesc) *Table {
	t := &Table{byName: make(map[string]*CmdDesc, len(cmds))}
	for _, c := range cmds {
		t.byName[c.Name] = c
	}
	return t
}

// Count returns the number of top-level commands in the table, which is what
// COMMAND COUNT reports.
func (t *Table) Count() int { return len(t.byName) }

// commands returns every top-level descriptor sorted by name, so COMMAND and
// COMMAND LIST produce a stable order across runs.
func (t *Table) commands() []*CmdDesc {
	out := make([]*CmdDesc, 0, len(t.byName))
	for _, c := range t.byName {
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// get returns the top-level descriptor for name, or nil. COMMAND INFO uses it.
func (t *Table) get(name string) *CmdDesc { return t.byName[name] }

// lookup resolves a (lowercased) command name and its argv to a descriptor,
// descending into a container command's subcommands when present. The returned
// error is already a RESP-ready "ERR ..." string.
func (t *Table) lookup(name string, argv [][]byte) (*CmdDesc, error) {
	cmd, ok := t.byName[name]
	if !ok {
		return nil, unknownCommandError(argv)
	}
	if len(cmd.SubCmds) > 0 && len(argv) >= 2 {
		sub := strings.ToLower(string(argv[1]))
		for _, s := range cmd.SubCmds {
			if s.Name == sub {
				return s, nil
			}
		}
		return nil, unknownSubcmdError(argv)
	}
	return cmd, nil
}

// checkArity reports whether argc satisfies the command's arity (doc 07 §10.3).
func checkArity(cmd *CmdDesc, argc int) bool {
	if cmd.Arity > 0 {
		return argc == cmd.Arity
	}
	return argc >= -cmd.Arity
}
