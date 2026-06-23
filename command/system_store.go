package command

import "github.com/tamnd/aki/keyspace"

// The system table inside the .aki file holds small named blobs that are not user
// keys: ACL users under the "acl:" prefix and function libraries under "fn:". Each
// tenant stores one entry per item so a value fits in a single B-tree leaf and a
// single item can be removed on its own. These two helpers give every tenant the
// same read and replace path over a prefix.

// systemEntries returns every system-table entry whose name starts with prefix,
// keyed by the part of the name after the prefix. An empty result means the prefix
// has no entries, which is the normal first-boot state.
func (e *Engine) systemEntries(prefix string) (map[string]string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	keys, err := e.ks.SystemList(prefix)
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
			out[k[len(prefix):]] = string(v)
		}
	}
	return out, nil
}

// systemSet stores one entry under prefix+name and commits. It is the add-one
// path for a tenant that grows entry by entry, like the script cache, where
// rewriting the whole prefix on each addition would be wasteful.
func (e *Engine) systemSet(prefix, name, val string) error {
	return e.updateKeyspaceDurable(func(ks *keyspace.Keyspace) error {
		return ks.SystemPut(prefix+name, []byte(val))
	})
}

// systemReplace makes the entries under prefix exactly match entries and commits.
// Names present in the table but absent from entries are deleted, so dropping an
// item leaves no stale entry behind. The keys of entries are the suffixes after
// the prefix.
func (e *Engine) systemReplace(prefix string, entries map[string]string) error {
	return e.updateKeyspaceDurable(func(ks *keyspace.Keyspace) error {
		existing, err := ks.SystemList(prefix)
		if err != nil {
			return err
		}
		keep := make(map[string]bool, len(entries))
		for name := range entries {
			keep[prefix+name] = true
		}
		for _, k := range existing {
			if !keep[k] {
				if _, err := ks.SystemDelete(k); err != nil {
					return err
				}
			}
		}
		for name, val := range entries {
			if err := ks.SystemPut(prefix+name, []byte(val)); err != nil {
				return err
			}
		}
		return nil
	})
}
