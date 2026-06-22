package main

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"unicode/utf8"

	"github.com/tamnd/aki/rdb"
)

// JSONL is the human-readable dump format: one JSON object per line, one object
// per key. It is the recovery and inspection path that sits next to the RDB
// format. Each record carries the database index, the key, the logical type, the
// TTL in milliseconds (-1 for none), and the value shaped by type.
//
// Redis keys and values are arbitrary byte strings, but JSON strings must be
// valid UTF-8. When every byte string in a record is valid UTF-8 the record
// writes them as plain JSON strings. When any one of them is not, the record sets
// "binary": true and base64-encodes all of its byte strings, so the round trip is
// exact either way.

// jsonlRecord is the on-disk shape of one key. Value is typed per record: a
// string for a string key, an array of strings for a list or set, an object for a
// hash, and an array of member/score objects for a sorted set.
type jsonlRecord struct {
	DB     int    `json:"db"`
	Key    string `json:"key"`
	Type   string `json:"type"`
	TTL    int64  `json:"ttl"`
	Binary bool   `json:"binary,omitempty"`
	Value  any    `json:"value"`
}

// jsonlZMember is one sorted-set entry in a JSONL record.
type jsonlZMember struct {
	Member string  `json:"member"`
	Score  float64 `json:"score"`
}

// kindName returns the JSONL type string for a value kind.
func kindName(k rdb.Kind) string {
	switch k {
	case rdb.KindString:
		return "string"
	case rdb.KindList:
		return "list"
	case rdb.KindSet:
		return "set"
	case rdb.KindHash:
		return "hash"
	case rdb.KindZSet:
		return "zset"
	default:
		return "unknown"
	}
}

// recordIsBinary reports whether any byte string in the entry is not valid UTF-8,
// which forces the whole record onto the base64 path.
func recordIsBinary(e rdb.Entry) bool {
	if !utf8Clean(e.Key) {
		return true
	}
	v := e.Value
	switch v.Kind {
	case rdb.KindString:
		return !utf8Clean(v.Str)
	case rdb.KindList:
		return anyBinary(v.List)
	case rdb.KindSet:
		return anyBinary(v.Set)
	case rdb.KindHash:
		for _, f := range v.Hash {
			if !utf8Clean(f.Field) || !utf8Clean(f.Value) {
				return true
			}
		}
	case rdb.KindZSet:
		for _, m := range v.ZSet {
			if !utf8Clean(m.Member) {
				return true
			}
		}
	}
	return false
}

// anyBinary reports whether any element is not valid UTF-8.
func anyBinary(items [][]byte) bool {
	for _, b := range items {
		if !utf8Clean(b) {
			return true
		}
	}
	return false
}

// utf8Clean reports whether b is valid UTF-8. JSON escapes control characters,
// quotes, and backslashes and restores them exactly, so those stay on the text
// path; only invalid UTF-8 bytes, which JSON would replace with U+FFFD and lose,
// route the record to base64.
func utf8Clean(b []byte) bool {
	return utf8.Valid(b)
}

// encByte encodes one byte string for a record: plain string on the text path,
// base64 on the binary path.
func encByte(b []byte, binary bool) string {
	if binary {
		return base64.StdEncoding.EncodeToString(b)
	}
	return string(b)
}

// decByte decodes one byte string from a record.
func decByte(s string, binary bool) ([]byte, error) {
	if binary {
		return base64.StdEncoding.DecodeString(s)
	}
	return []byte(s), nil
}

// valueToJSON shapes a value for the record's value field, encoding each byte
// string by the record's binary flag.
func valueToJSON(v rdb.Value, binary bool) any {
	switch v.Kind {
	case rdb.KindString:
		return encByte(v.Str, binary)
	case rdb.KindList:
		return bytesToStrings(v.List, binary)
	case rdb.KindSet:
		return bytesToStrings(v.Set, binary)
	case rdb.KindHash:
		obj := make(map[string]string, len(v.Hash))
		for _, f := range v.Hash {
			obj[encByte(f.Field, binary)] = encByte(f.Value, binary)
		}
		return obj
	case rdb.KindZSet:
		out := make([]jsonlZMember, len(v.ZSet))
		for i, m := range v.ZSet {
			out[i] = jsonlZMember{Member: encByte(m.Member, binary), Score: m.Score}
		}
		return out
	default:
		return nil
	}
}

// bytesToStrings encodes a slice of byte strings for a record.
func bytesToStrings(items [][]byte, binary bool) []string {
	out := make([]string, len(items))
	for i, b := range items {
		out[i] = encByte(b, binary)
	}
	return out
}

// dumpJSONL writes one JSONL record per key in the snapshot and returns the
// number of keys written. It writes incrementally so a large dataset never has to
// be buffered whole.
func dumpJSONL(snap rdb.Snapshot, w io.Writer) (int, error) {
	bw := bufio.NewWriter(w)
	enc := json.NewEncoder(bw)
	count := 0
	for _, db := range snap.DBs {
		for _, e := range db.Entries {
			binary := recordIsBinary(e)
			rec := jsonlRecord{
				DB:     db.Index,
				Key:    encByte(e.Key, binary),
				Type:   kindName(e.Value.Kind),
				TTL:    e.ExpireMS,
				Binary: binary,
				Value:  valueToJSON(e.Value, binary),
			}
			if err := enc.Encode(&rec); err != nil {
				return count, fmt.Errorf("encode record for key: %w", err)
			}
			count++
		}
	}
	if err := bw.Flush(); err != nil {
		return count, err
	}
	return count, nil
}

// importJSONL parses a JSONL dump into a snapshot, grouping records by database
// index in the order the databases first appear.
func importJSONL(blob []byte) (rdb.Snapshot, error) {
	snap := rdb.Snapshot{}
	byDB := map[int]int{} // db index to position in snap.DBs

	sc := bufio.NewScanner(bytes.NewReader(blob))
	sc.Buffer(make([]byte, 0, 64*1024), 64*1024*1024)
	line := 0
	for sc.Scan() {
		line++
		raw := bytes.TrimSpace(sc.Bytes())
		if len(raw) == 0 {
			continue
		}
		var in struct {
			DB     int             `json:"db"`
			Key    string          `json:"key"`
			Type   string          `json:"type"`
			TTL    int64           `json:"ttl"`
			Binary bool            `json:"binary"`
			Value  json.RawMessage `json:"value"`
		}
		if err := json.Unmarshal(raw, &in); err != nil {
			return rdb.Snapshot{}, fmt.Errorf("line %d: %w", line, err)
		}
		key, err := decByte(in.Key, in.Binary)
		if err != nil {
			return rdb.Snapshot{}, fmt.Errorf("line %d: decode key: %w", line, err)
		}
		val, err := jsonToValue(in.Type, in.Value, in.Binary)
		if err != nil {
			return rdb.Snapshot{}, fmt.Errorf("line %d: %w", line, err)
		}
		entry := rdb.Entry{Key: key, Value: val, ExpireMS: in.TTL}

		pos, ok := byDB[in.DB]
		if !ok {
			pos = len(snap.DBs)
			snap.DBs = append(snap.DBs, rdb.DBData{Index: in.DB})
			byDB[in.DB] = pos
		}
		snap.DBs[pos].Entries = append(snap.DBs[pos].Entries, entry)
	}
	if err := sc.Err(); err != nil {
		return rdb.Snapshot{}, err
	}
	return snap, nil
}

// jsonToValue rebuilds a value from a record's type and raw value field.
func jsonToValue(typ string, raw json.RawMessage, binary bool) (rdb.Value, error) {
	switch typ {
	case "string":
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return rdb.Value{}, fmt.Errorf("decode string value: %w", err)
		}
		b, err := decByte(s, binary)
		if err != nil {
			return rdb.Value{}, err
		}
		return rdb.Value{Kind: rdb.KindString, Str: b}, nil
	case "list":
		items, err := decStrings(raw, binary)
		if err != nil {
			return rdb.Value{}, err
		}
		return rdb.Value{Kind: rdb.KindList, List: items}, nil
	case "set":
		items, err := decStrings(raw, binary)
		if err != nil {
			return rdb.Value{}, err
		}
		return rdb.Value{Kind: rdb.KindSet, Set: items}, nil
	case "hash":
		var obj map[string]string
		if err := json.Unmarshal(raw, &obj); err != nil {
			return rdb.Value{}, fmt.Errorf("decode hash value: %w", err)
		}
		fields := make([]rdb.Field, 0, len(obj))
		for k, v := range obj {
			fb, err := decByte(k, binary)
			if err != nil {
				return rdb.Value{}, err
			}
			vb, err := decByte(v, binary)
			if err != nil {
				return rdb.Value{}, err
			}
			fields = append(fields, rdb.Field{Field: fb, Value: vb})
		}
		return rdb.Value{Kind: rdb.KindHash, Hash: fields}, nil
	case "zset":
		var members []jsonlZMember
		if err := json.Unmarshal(raw, &members); err != nil {
			return rdb.Value{}, fmt.Errorf("decode zset value: %w", err)
		}
		out := make([]rdb.Member, len(members))
		for i, m := range members {
			mb, err := decByte(m.Member, binary)
			if err != nil {
				return rdb.Value{}, err
			}
			out[i] = rdb.Member{Member: mb, Score: m.Score}
		}
		return rdb.Value{Kind: rdb.KindZSet, ZSet: out}, nil
	default:
		return rdb.Value{}, fmt.Errorf("unknown type %q", typ)
	}
}

// decStrings decodes a JSON array of strings into byte slices.
func decStrings(raw json.RawMessage, binary bool) ([][]byte, error) {
	var ss []string
	if err := json.Unmarshal(raw, &ss); err != nil {
		return nil, fmt.Errorf("decode array value: %w", err)
	}
	out := make([][]byte, len(ss))
	for i, s := range ss {
		b, err := decByte(s, binary)
		if err != nil {
			return nil, err
		}
		out[i] = b
	}
	return out, nil
}
