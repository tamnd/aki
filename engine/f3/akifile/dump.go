package akifile

import (
	"encoding/json"
	"io"
	"sort"
)

// The dump half of the file's dump/load tooling (spec 2064/f3/07 section 6). A dump
// is the file's logical contents, the live key set every shard's record log folds to,
// read out without a store: a crash-matrix cell diffs a before and after dump to prove
// what survived, and an operator reads a file's keys without standing a server up.
//
// It reads only. It walks the append space the same way recovery does, folds each
// shard's log to its live records, and hands them back in a deterministic order, so
// two dumps of one file are identical and a torn frame fails the walk closed rather
// than emitting rot. It does not resolve separated values through the value log, which
// is the store's job and its bytes; a separated record carries its pointer word so a
// reader knows where the value lives, and an inline record carries its value outright.

// DumpRecord is one live record a Dump emits: the shard that owns it, its key, and its
// value as the record frames it. An inline record hands back its value bytes; a
// separated or chunked record hands back the pointer word and length a value-log read
// would follow. Flags names which form it is.
type DumpRecord struct {
	Shard     uint16
	Key       []byte
	Flags     uint32
	Value     []byte // inline value bytes, set only when Flags has RecFlagInline
	ValueWord uint64 // the value-log pointer word when separated or chunked
	ValueLen  uint32
	ExpireAt  uint64
}

// Dump folds every shard's record log to its live key set and calls visit once per
// live record, shards in order and keys sorted within a shard so the stream is
// deterministic across runs. It is read-only: it walks the append space and never
// writes, so it is safe against a live or a damaged file. A tombstone drops its key
// and a later record for a key supersedes an earlier one, so visit sees exactly the
// records a recovery would rebuild. The DumpRecord's Key and Value are freshly copied,
// so a visit may hold them past the call. A torn frame fails the walk closed.
func Dump(f *File, visit func(DumpRecord) error) error {
	shards := f.Prefix().ShardCount
	for s := uint32(0); s < shards; s++ {
		live := make(map[string]DumpRecord)
		err := f.WalkShardRecords(uint16(s), PageSize, func(_ uint64, row RecordRow) error {
			k := string(row.Key)
			if row.Flags&RecFlagTombstone != 0 {
				delete(live, k)
				return nil
			}
			rec := DumpRecord{
				Shard:     uint16(s),
				Key:       append([]byte(nil), row.Key...),
				Flags:     row.Flags,
				ValueWord: row.ValueWord,
				ValueLen:  row.ValueLen,
				ExpireAt:  row.ExpireAt,
			}
			if row.Flags&RecFlagInline != 0 {
				rec.Value = append([]byte(nil), row.Value...)
			}
			live[k] = rec
			return nil
		})
		if err != nil {
			return err
		}
		keys := make([]string, 0, len(live))
		for k := range live {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			if err := visit(live[k]); err != nil {
				return err
			}
		}
	}
	return nil
}

// dumpLine is the JSON shape one dumped record serializes to. Key and inline value are
// raw bytes, so they ride as base64 (encoding/json's []byte default), which round-trips
// any byte a key or value may hold. A separated record omits value and carries the
// pointer word instead.
type dumpLine struct {
	Shard     uint16 `json:"shard"`
	Key       []byte `json:"key"`
	Flags     uint32 `json:"flags"`
	Inline    bool   `json:"inline"`
	Value     []byte `json:"value,omitempty"`
	ValueWord uint64 `json:"value_word,omitempty"`
	ValueLen  uint32 `json:"value_len"`
	ExpireAt  uint64 `json:"expire_at,omitempty"`
}

// WriteDump streams the file's live records to w as one JSON object per line (JSONL),
// the export half of the dump/load pair. It is read-only and deterministic, so two
// dumps of one file are byte-identical and a crash-matrix cell can diff a before and
// after dump. An inline record's value rides the line; a separated record carries its
// pointer word so a reader sees where the value lives without this tool touching the
// value log.
func WriteDump(w io.Writer, f *File) error {
	enc := json.NewEncoder(w)
	return Dump(f, func(r DumpRecord) error {
		return enc.Encode(dumpLine{
			Shard:     r.Shard,
			Key:       r.Key,
			Flags:     r.Flags,
			Inline:    r.Flags&RecFlagInline != 0,
			Value:     r.Value,
			ValueWord: r.ValueWord,
			ValueLen:  r.ValueLen,
			ExpireAt:  r.ExpireAt,
		})
	})
}
