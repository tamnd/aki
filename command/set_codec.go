package command

import (
	"errors"

	"github.com/tamnd/aki/encoding"
	"github.com/tamnd/aki/keyspace"
)

// errCorruptSet is returned when a stored set body cannot be decoded, which
// means the value record is damaged.
var errCorruptSet = errors.New("corrupt set value")

// Set thresholds for the reported OBJECT ENCODING. aki stores its own physical
// set form (a length-prefixed member sequence in insertion order), so these only
// decide which Redis encoding name the key reports, matching the t_set.c
// constants.
const (
	// setMaxIntsetEntries is set-max-intset-entries: the all-integer member
	// count at or below which a set reports intset.
	setMaxIntsetEntries = 512
	// setMaxListpackEntries is set-max-listpack-entries: the member count at or
	// below which a non-integer set reports listpack.
	setMaxListpackEntries = 128
	// setMaxListpackValue is set-max-listpack-value: the per-member byte cap for
	// listpack.
	setMaxListpackValue = 64
)

// setDecode unpacks a stored set body into its members in insertion order. The
// body is a uvarint member count followed by each member as a uvarint length and
// bytes.
func setDecode(body []byte) ([][]byte, error) {
	if len(body) == 0 {
		return nil, nil
	}
	n, off, err := encoding.Uvarint(body)
	if err != nil {
		return nil, err
	}
	members := make([][]byte, 0, n)
	for range n {
		l, m, err := encoding.Uvarint(body[off:])
		if err != nil {
			return nil, err
		}
		off += m
		if off+int(l) > len(body) {
			return nil, errCorruptSet
		}
		member := make([]byte, l)
		copy(member, body[off:off+int(l)])
		members = append(members, member)
		off += int(l)
	}
	return members, nil
}

// setEncode packs members back into the stored body form.
func setEncode(members [][]byte) []byte {
	body := encoding.AppendUvarint(nil, uint64(len(members)))
	for _, m := range members {
		body = encoding.AppendUvarint(body, uint64(len(m)))
		body = append(body, m...)
	}
	return body
}

// setEncoding picks the reported encoding for a set. A set never downgrades, so
// prev pins the floor: once listpack it cannot report intset again, and once
// hashtable it stays hashtable.
func setEncoding(members [][]byte, prev uint8) uint8 {
	if prev == keyspace.EncHashtable {
		return keyspace.EncHashtable
	}
	allInt := true
	maxLen := 0
	for _, m := range members {
		if _, ok := parseInteger(m); !ok {
			allInt = false
		}
		if len(m) > maxLen {
			maxLen = len(m)
		}
	}
	n := len(members)
	if allInt && n <= setMaxIntsetEntries && prev != keyspace.EncListpack {
		return keyspace.EncIntset
	}
	if n <= setMaxListpackEntries && maxLen <= setMaxListpackValue {
		return keyspace.EncListpack
	}
	return keyspace.EncHashtable
}

// setFind returns the index of a member, or -1 when it is absent.
func setFind(members [][]byte, member []byte) int {
	for i := range members {
		if string(members[i]) == string(member) {
			return i
		}
	}
	return -1
}

// getSet reads the set at key and decodes it. The returned header carries the
// type and encoding so callers can check for WRONGTYPE and keep the encoding
// floor. A missing key returns found false with no error.
func getSet(db *keyspace.DB, key []byte) ([][]byte, keyspace.ValueHeader, bool, error) {
	body, hdr, found, err := db.Get(key)
	if err != nil || !found {
		return nil, hdr, found, err
	}
	if hdr.Type != keyspace.TypeSet {
		return nil, hdr, true, nil
	}
	members, err := setDecode(body)
	if err != nil {
		return nil, hdr, true, err
	}
	return members, hdr, true, nil
}
