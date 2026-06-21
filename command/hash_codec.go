package command

import (
	"errors"

	"github.com/tamnd/aki/encoding"
	"github.com/tamnd/aki/keyspace"
)

// errCorruptHash is returned when a stored hash body cannot be decoded, which
// means the value record is damaged.
var errCorruptHash = errors.New("corrupt hash value")

// Hash thresholds for the reported OBJECT ENCODING. aki stores its own physical
// hash form (a length-prefixed field/value sequence), so these only decide which
// Redis encoding name the key reports, matching the t_hash.c constants.
const (
	// hashMaxListpackEntries is hash-max-listpack-entries: the field-pair count
	// at or below which a hash reports listpack.
	hashMaxListpackEntries = 128
	// hashMaxListpackValue is hash-max-listpack-value: the per-field and
	// per-value byte cap for listpack.
	hashMaxListpackValue = 64
)

// hashField is one field/value pair in insertion order.
type hashField struct {
	field []byte
	value []byte
}

// hashDecode unpacks a stored hash body into its fields in insertion order. The
// body is a uvarint pair count followed by each field and value as a uvarint
// length and bytes.
func hashDecode(body []byte) ([]hashField, error) {
	if len(body) == 0 {
		return nil, nil
	}
	n, off, err := encoding.Uvarint(body)
	if err != nil {
		return nil, err
	}
	fields := make([]hashField, 0, n)
	for range n {
		f, m, err := readChunk(body, off)
		if err != nil {
			return nil, err
		}
		off = m
		v, m2, err := readChunk(body, off)
		if err != nil {
			return nil, err
		}
		off = m2
		fields = append(fields, hashField{field: f, value: v})
	}
	return fields, nil
}

// readChunk reads one uvarint-length-prefixed byte run at off, returning a copy
// and the offset past it.
func readChunk(body []byte, off int) ([]byte, int, error) {
	l, m, err := encoding.Uvarint(body[off:])
	if err != nil {
		return nil, 0, err
	}
	off += m
	if off+int(l) > len(body) {
		return nil, 0, errCorruptHash
	}
	out := make([]byte, l)
	copy(out, body[off:off+int(l)])
	return out, off + int(l), nil
}

// hashEncode packs fields back into the stored body form.
func hashEncode(fields []hashField) []byte {
	body := encoding.AppendUvarint(nil, uint64(len(fields)))
	for _, f := range fields {
		body = encoding.AppendUvarint(body, uint64(len(f.field)))
		body = append(body, f.field...)
		body = encoding.AppendUvarint(body, uint64(len(f.value)))
		body = append(body, f.value...)
	}
	return body
}

// hashEncoding picks the reported encoding for a hash. Once a key is a hashtable
// it never goes back to listpack, so prev pins the floor.
func hashEncoding(fields []hashField, prev uint8) uint8 {
	if prev == keyspace.EncHashtable {
		return keyspace.EncHashtable
	}
	if len(fields) > hashMaxListpackEntries {
		return keyspace.EncHashtable
	}
	for _, f := range fields {
		if len(f.field) > hashMaxListpackValue || len(f.value) > hashMaxListpackValue {
			return keyspace.EncHashtable
		}
	}
	return keyspace.EncListpack
}

// hashFind returns the index of a field by name, or -1 when it is absent.
func hashFind(fields []hashField, name []byte) int {
	for i := range fields {
		if string(fields[i].field) == string(name) {
			return i
		}
	}
	return -1
}
