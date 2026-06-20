package command

import (
	"errors"

	"github.com/tamnd/aki/encoding"
	"github.com/tamnd/aki/keyspace"
)

// errCorruptList is returned when a stored list body cannot be decoded, which
// means the value record is damaged.
var errCorruptList = errors.New("corrupt list value")

// List thresholds for the reported OBJECT ENCODING. aki stores its own physical
// list form (a length-prefixed element sequence), so these only decide which
// Redis encoding name the key reports, matching the t_list.c constants.
const (
	// listMaxListpackBytes is abs(list-max-listpack-size) for the default -2.
	listMaxListpackBytes = 8192
	// listMaxListpackEntries is the hard 128-element cap for listpack.
	listMaxListpackEntries = 128
	// listMaxListpackElemBytes is the per-element 64-byte cap for listpack.
	listMaxListpackElemBytes = 64
	// listEntryOverhead approximates a listpack entry's header and backlen when
	// estimating the total blob size against the byte cap.
	listEntryOverhead = 11
)

// listDecode unpacks a stored list body into its elements. The body is a
// uvarint element count followed by each element as a uvarint length and bytes.
func listDecode(body []byte) ([][]byte, error) {
	if len(body) == 0 {
		return nil, nil
	}
	n, off, err := encoding.Uvarint(body)
	if err != nil {
		return nil, err
	}
	var elems [][]byte
	for range n {
		l, m, err := encoding.Uvarint(body[off:])
		if err != nil {
			return nil, err
		}
		off += m
		if off+int(l) > len(body) {
			return nil, errCorruptList
		}
		elem := make([]byte, l)
		copy(elem, body[off:off+int(l)])
		elems = append(elems, elem)
		off += int(l)
	}
	return elems, nil
}

// listEncode packs elements back into the stored body form.
func listEncode(elems [][]byte) []byte {
	body := encoding.AppendUvarint(nil, uint64(len(elems)))
	for _, e := range elems {
		body = encoding.AppendUvarint(body, uint64(len(e)))
		body = append(body, e...)
	}
	return body
}

// listEncoding picks the reported encoding for a list. Once a key is a
// quicklist it never goes back to listpack, so prev pins the floor.
func listEncoding(elems [][]byte, prev uint8) uint8 {
	if prev == keyspace.EncQuicklist {
		return keyspace.EncQuicklist
	}
	if len(elems) > listMaxListpackEntries {
		return keyspace.EncQuicklist
	}
	total := listEntryOverhead
	for _, e := range elems {
		if len(e) > listMaxListpackElemBytes {
			return keyspace.EncQuicklist
		}
		total += len(e) + listEntryOverhead
		if total > listMaxListpackBytes {
			return keyspace.EncQuicklist
		}
	}
	return keyspace.EncListpack
}
