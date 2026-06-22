package command

import (
	"fmt"
	"strings"
	"time"

	"github.com/tamnd/aki/keyspace"
)

// aclEntryPrefix namespaces ACL users in the .aki system table. Each user is one
// entry, keyed aclEntryPrefix+name, valued with its full "user ... " line. The
// prefix keeps ACL entries apart from other system table tenants like functions.
const aclEntryPrefix = "acl:"

// aclLoad reads every persisted ACL user line from the .aki system table and
// returns them keyed by username. An empty result means the table has no ACL
// entries, which is the normal first-boot state.
func (e *Engine) aclLoad() (map[string]string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	keys, err := e.ks.SystemList(aclEntryPrefix)
	if err != nil {
		return nil, err
	}
	out := make(map[string]string, len(keys))
	for _, k := range keys {
		v, ok, err := e.ks.SystemGet(k)
		if err != nil {
			return nil, err
		}
		if ok {
			out[k[len(aclEntryPrefix):]] = string(v)
		}
	}
	return out, nil
}

// aclStore replaces the ACL entries in the system table with lines and commits.
// Entries present in the table but absent from lines are deleted, so dropping a
// user with ACL DELUSER removes it from the file too.
func (e *Engine) aclStore(lines map[string]string) error {
	return e.updateKeyspace(func(ks *keyspace.Keyspace) error {
		existing, err := ks.SystemList(aclEntryPrefix)
		if err != nil {
			return err
		}
		keep := make(map[string]bool, len(lines))
		for name := range lines {
			keep[aclEntryPrefix+name] = true
		}
		for _, k := range existing {
			if !keep[k] {
				if _, err := ks.SystemDelete(k); err != nil {
					return err
				}
			}
		}
		for name, line := range lines {
			if err := ks.SystemPut(aclEntryPrefix+name, []byte(line)); err != nil {
				return err
			}
		}
		return nil
	})
}

// lines renders the current users as a map of username to ACL line. The caller
// uses it to mirror the registry into the .aki system table.
func (a *aclRegistry) lines() map[string]string {
	a.mu.RLock()
	defer a.mu.RUnlock()
	out := make(map[string]string, len(a.users))
	for name, u := range a.users {
		out[name] = aclLine(u)
	}
	return out
}

// loadLines rebuilds the user map from persisted ACL lines and swaps it in. Each
// value is a full "user <name> <rules...>" line, the same form saveFile writes. A
// default user is synthesized when the table does not carry one, so the instance
// always has a usable default. A parse error leaves the live users unchanged.
func (a *aclRegistry) loadLines(lines map[string]string) error {
	staging := map[string]*aclUser{}
	for name, line := range lines {
		fields := strings.Fields(line)
		if len(fields) < 2 || fields[0] != "user" {
			return fmt.Errorf("bad ACL entry for user %q", name)
		}
		u := &aclUser{name: fields[1], created: time.Now()}
		if err := applyACLRules(u, fields[2:]); err != nil {
			return fmt.Errorf("user %q: %w", name, err)
		}
		staging[fields[1]] = u
	}
	if _, ok := staging["default"]; !ok {
		staging["default"] = &aclUser{
			name: "default", on: true, nopass: true,
			cmdRules:  []aclCmdRule{{grant: true, category: "@all"}},
			keyRules:  []aclKeyRule{{pattern: "*", read: true, write: true}},
			chanRules: []aclChanRule{{pattern: "*"}},
			created:   time.Now(),
		}
	}
	a.mu.Lock()
	a.users = staging
	a.mu.Unlock()
	return nil
}

// persistACL mirrors the current ACL into the .aki system table. It is a no-op
// when an external aclfile is configured, since that file is authoritative then,
// or when no engine is attached. A write failure is logged, not returned, so an
// ACL command still succeeds on the wire even if the durable copy lags.
func (d *Dispatcher) persistACL() {
	if d.engine == nil || d.acl == nil || d.acl.aclFile != "" {
		return
	}
	if err := d.engine.aclStore(d.acl.lines()); err != nil {
		d.logWarning("failed to persist ACL to the data file", logField{"err", err.Error()})
	}
}

// LoadACLFromKeyspace restores ACL users persisted in the .aki system table. It
// runs once at startup when no external aclfile is configured. An empty table
// leaves the default user built by New in place.
func (d *Dispatcher) LoadACLFromKeyspace() error {
	if d.engine == nil || d.acl == nil || d.acl.aclFile != "" {
		return nil
	}
	lines, err := d.engine.aclLoad()
	if err != nil {
		return err
	}
	if len(lines) == 0 {
		return nil
	}
	return d.acl.loadLines(lines)
}
