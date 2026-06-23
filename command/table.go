package command

import "sort"

// Table is the command registry built once at startup from a slice of
// descriptors. Lookup is keyed by cmdKey (FNV-1a with ASCII case folding) so
// dispatch never allocates a lowercase string for the command name.
type Table struct {
	byName map[uint64]*CmdDesc
}

// cmdKey computes a case-folded FNV-1a hash of the command name bytes. Using a
// uint64 key for the dispatch map avoids string allocation and string comparison
// on the hot path: the hash is computed directly over argv[0] without
// materializing a lowercase string, and map lookup compares two uint64 values.
func cmdKey(b []byte) uint64 {
	const offset, prime = 14695981039346656037, 1099511628211
	h := uint64(offset)
	for _, c := range b {
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		h ^= uint64(c)
		h *= prime
	}
	return h
}

// NewTable builds a Table from the given descriptors. A descriptor with a
// non-empty SubName is not a top-level entry; it is reached through its parent's
// SubCmds list.
func NewTable(cmds []*CmdDesc) *Table {
	t := &Table{byName: make(map[uint64]*CmdDesc, len(cmds))}
	for _, c := range cmds {
		t.byName[cmdKey([]byte(c.Name))] = c
		if len(c.SubCmds) > 0 {
			c.subByKey = make(map[uint64]*CmdDesc, len(c.SubCmds))
			for _, s := range c.SubCmds {
				c.subByKey[cmdKey([]byte(s.Name))] = s
			}
		}
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
func (t *Table) get(name string) *CmdDesc { return t.byName[cmdKey([]byte(name))] }

// lookup resolves argv to a descriptor, descending into subcommands when
// present. It hashes argv[0] and argv[1] directly with cmdKey so no lowercase
// string is allocated. The returned error is already a RESP-ready "ERR ..."
// string.
func (t *Table) lookup(argv [][]byte) (*CmdDesc, error) {
	cmd, ok := t.byName[cmdKey(argv[0])]
	if !ok {
		return nil, unknownCommandError(argv)
	}
	if len(cmd.subByKey) > 0 && len(argv) >= 2 {
		sub, ok := cmd.subByKey[cmdKey(argv[1])]
		if !ok {
			return nil, unknownSubcmdError(argv)
		}
		return sub, nil
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
