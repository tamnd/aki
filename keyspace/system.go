package keyspace

import (
	"strings"

	"github.com/tamnd/aki/btree"
	"github.com/tamnd/aki/format"
)

// The system table is a dedicated B-tree inside the .aki file for small named
// blobs that are not user keys: ACL users, function libraries, and cached
// scripts (doc 19 §11.11 calls for an "ACL table within the .aki storage file").
// It rides the same copy-on-write commit as the keyspace, so its root advances
// atomically with the data through the meta page. Each value must fit in one
// B-tree leaf; the table is meant for many small entries, not large blobs, so
// callers store one entry per item (one ACL line per user, for example) rather
// than one giant value.

// normalizeRoot maps a zero root to NullPage. A file written before the system
// table existed carries a zero in the meta slot, since the reserved bytes were
// zeroed; page 0 is the file header and can never be a B-tree root, so a zero
// means the table is absent.
func normalizeRoot(root uint32) uint32 {
	if root == 0 {
		return format.NullPage
	}
	return root
}

// systemTree returns the system B-tree, opening it from the stored root, or nil
// when the table has never been written.
func (ks *Keyspace) systemTree() *btree.Tree {
	if ks.sysTree != nil {
		return ks.sysTree
	}
	if ks.sysRoot == format.NullPage {
		return nil
	}
	ks.sysTree = btree.Open(ks.pgr, ks.sysRoot)
	return ks.sysTree
}

// ensureSystemTree returns the system B-tree, creating it on first use.
func (ks *Keyspace) ensureSystemTree() (*btree.Tree, error) {
	if t := ks.systemTree(); t != nil {
		return t, nil
	}
	t, err := btree.Create(ks.pgr)
	if err != nil {
		return nil, err
	}
	ks.sysTree = t
	ks.sysRoot = t.Root()
	return t, nil
}

// SystemGet returns the blob stored under name. The second result is false when
// no entry exists. The returned slice is a copy the caller may keep.
func (ks *Keyspace) SystemGet(name string) ([]byte, bool, error) {
	t := ks.systemTree()
	if t == nil {
		return nil, false, nil
	}
	v, ok, err := t.Get([]byte(name))
	if err != nil || !ok {
		return nil, false, err
	}
	return append([]byte(nil), v...), true, nil
}

// SystemPut stores val under name, replacing any current value. The change
// becomes durable on the next Commit. A value too large for one leaf returns the
// B-tree's ErrCellTooLarge.
func (ks *Keyspace) SystemPut(name string, val []byte) error {
	t, err := ks.ensureSystemTree()
	if err != nil {
		return err
	}
	if err := t.Put([]byte(name), val); err != nil {
		return err
	}
	ks.sysRoot = t.Root()
	return nil
}

// SystemDelete removes name and reports whether it existed.
func (ks *Keyspace) SystemDelete(name string) (bool, error) {
	t := ks.systemTree()
	if t == nil {
		return false, nil
	}
	ok, err := t.Delete([]byte(name))
	if err != nil {
		return false, err
	}
	ks.sysRoot = t.Root()
	return ok, nil
}

// SystemList returns every entry name that starts with prefix, in sorted order.
// An empty prefix lists every entry.
func (ks *Keyspace) SystemList(prefix string) ([]string, error) {
	t := ks.systemTree()
	if t == nil {
		return nil, nil
	}
	var out []string
	c := t.Cursor()
	if err := c.Seek([]byte(prefix)); err != nil {
		return nil, err
	}
	for c.Valid() {
		k := string(c.Key())
		if !strings.HasPrefix(k, prefix) {
			break
		}
		out = append(out, k)
		if err := c.Next(); err != nil {
			return nil, err
		}
	}
	return out, nil
}
