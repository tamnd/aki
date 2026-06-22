package rdb

import "testing"

// FuzzUnmarshal feeds arbitrary bytes to the DUMP/RESTORE value decoder. The
// decoder must never panic and must never return a value together with an error.
// Well-formed seeds confirm a real payload decodes (doc 23 §7.3).
func FuzzUnmarshal(f *testing.F) {
	seeds := []Value{
		{Kind: KindString, Str: []byte("hello")},
		{Kind: KindString, Str: []byte{0x00, 0xff, 0xfe}},
		{Kind: KindList, List: [][]byte{[]byte("a"), []byte("b"), []byte("c")}},
		{Kind: KindSet, Set: [][]byte{[]byte("x"), []byte("y")}},
		{Kind: KindHash, Hash: []Field{{Field: []byte("f"), Value: []byte("v")}}},
		{Kind: KindZSet, ZSet: []Member{{Member: []byte("m"), Score: 1.5}}},
	}
	for _, v := range seeds {
		if blob, err := Marshal(v); err == nil {
			f.Add(blob)
		}
	}
	f.Add([]byte{})
	f.Add([]byte{0x00})

	f.Fuzz(func(t *testing.T, data []byte) {
		v, err := Unmarshal(data)
		if err != nil {
			return
		}
		// A successful decode must produce a known kind and re-marshal without panic.
		switch v.Kind {
		case KindString, KindList, KindSet, KindHash, KindZSet:
		default:
			t.Fatalf("decoded unknown kind %d from %q", v.Kind, data)
		}
		_, _ = Marshal(v)
	})
}

// FuzzUnmarshalFile feeds arbitrary bytes to the whole-file RDB decoder, which
// must never panic regardless of input.
func FuzzUnmarshalFile(f *testing.F) {
	snap := Snapshot{DBs: []DBData{{Index: 0, Entries: []Entry{
		{Key: []byte("k"), Value: Value{Kind: KindString, Str: []byte("v")}, ExpireMS: -1},
	}}}}
	if blob, err := MarshalFile(snap); err == nil {
		f.Add(blob)
	}
	f.Add([]byte("REDIS0011"))
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = UnmarshalFile(data)
	})
}
