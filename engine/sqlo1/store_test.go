package sqlo1

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"testing"
)

// testStoreContract is the behavioral suite every Store backend must pass.
// MemStore runs it now; sqlo1a and sqlo1b point their own tests at it when
// they land, so the contract stays in one place.
func testStoreContract(t *testing.T, open func() Store) {
	ctx := context.Background()

	rec := func(k, v string, exp int64) Record {
		return Record{Key: []byte(k), Value: []byte(v), ExpireMs: exp}
	}
	put := func(r Record) Op { return Op{Rec: r} }
	del := func(k string) Op { return Op{Del: true, Rec: Record{Key: []byte(k)}} }

	t.Run("get missing", func(t *testing.T) {
		s := open()
		if _, err := s.Get(ctx, []byte("nope")); !errors.Is(err, ErrNotFound) {
			t.Fatalf("Get on empty store: err = %v, want ErrNotFound", err)
		}
	})

	t.Run("apply then read", func(t *testing.T) {
		s := open()
		b := &DrainBatch{Seq: 1, Ops: []Op{put(rec("a", "1", 0)), put(rec("b", "2", 99))}}
		if err := s.ApplyBatch(ctx, b); err != nil {
			t.Fatal(err)
		}
		got, err := s.Get(ctx, []byte("b"))
		if err != nil {
			t.Fatal(err)
		}
		if string(got.Value) != "2" || got.ExpireMs != 99 {
			t.Fatalf("Get(b) = %+v", got)
		}
		if st := s.Stats(); st.Keys != 2 || st.HighWater != 1 {
			t.Fatalf("Stats = %+v, want 2 keys at high water 1", st)
		}
	})

	t.Run("batchget order and misses", func(t *testing.T) {
		s := open()
		if err := s.ApplyBatch(ctx, &DrainBatch{Seq: 1, Ops: []Op{put(rec("a", "1", 0)), put(rec("c", "3", 0))}}); err != nil {
			t.Fatal(err)
		}
		out, err := s.BatchGet(ctx, [][]byte{[]byte("c"), []byte("b"), []byte("a")})
		if err != nil {
			t.Fatal(err)
		}
		if len(out) != 3 {
			t.Fatalf("BatchGet returned %d records, want 3", len(out))
		}
		if string(out[0].Value) != "3" || string(out[2].Value) != "1" {
			t.Fatalf("BatchGet out of order: %+v", out)
		}
		if out[1].Key != nil {
			t.Fatalf("missing key must come back with a nil Key, got %+v", out[1])
		}
	})

	t.Run("delete", func(t *testing.T) {
		s := open()
		if err := s.ApplyBatch(ctx, &DrainBatch{Seq: 1, Ops: []Op{put(rec("a", "1", 0))}}); err != nil {
			t.Fatal(err)
		}
		if err := s.ApplyBatch(ctx, &DrainBatch{Seq: 2, Ops: []Op{del("a")}}); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Get(ctx, []byte("a")); !errors.Is(err, ErrNotFound) {
			t.Fatalf("Get after delete: err = %v, want ErrNotFound", err)
		}
		if st := s.Stats(); st.Keys != 0 {
			t.Fatalf("Stats.Keys = %d after delete, want 0", st.Keys)
		}
	})

	t.Run("replay is a no-op", func(t *testing.T) {
		s := open()
		if err := s.ApplyBatch(ctx, &DrainBatch{Seq: 5, Ops: []Op{put(rec("a", "new", 0))}}); err != nil {
			t.Fatal(err)
		}
		// A replayed batch at or below the high-water mark must not apply,
		// even though its ops differ: that is what makes WAL replay after a
		// crash exactly-once (doc 02 section 5).
		if err := s.ApplyBatch(ctx, &DrainBatch{Seq: 5, Ops: []Op{put(rec("a", "stale", 0))}}); err != nil {
			t.Fatal(err)
		}
		if err := s.ApplyBatch(ctx, &DrainBatch{Seq: 4, Ops: []Op{del("a")}}); err != nil {
			t.Fatal(err)
		}
		got, err := s.Get(ctx, []byte("a"))
		if err != nil {
			t.Fatal(err)
		}
		if string(got.Value) != "new" {
			t.Fatalf("replayed batch applied: Get(a) = %q", got.Value)
		}
		if st := s.Stats(); st.HighWater != 5 {
			t.Fatalf("HighWater = %d, want 5", st.HighWater)
		}
	})

	t.Run("scan visits everything once", func(t *testing.T) {
		s := open()
		var ops []Op
		want := make(map[string]bool)
		for i := range 100 {
			k := fmt.Sprintf("k%03d", i)
			ops = append(ops, put(rec(k, "v", 0)))
			want[k] = true
		}
		if err := s.ApplyBatch(ctx, &DrainBatch{Seq: 1, Ops: ops}); err != nil {
			t.Fatal(err)
		}

		// Walk in chunks of 7 through the resume cursor.
		seen := make(map[string]int)
		var cur Cursor
		for {
			n := 0
			next, err := s.Scan(ctx, cur, func(r Record) bool {
				seen[string(r.Key)]++
				n++
				return n < 7
			})
			if err != nil {
				t.Fatal(err)
			}
			if next == nil {
				break
			}
			cur = next
		}
		if len(seen) != len(want) {
			t.Fatalf("scan saw %d keys, want %d", len(seen), len(want))
		}
		for k, c := range seen {
			if c != 1 {
				t.Fatalf("key %s visited %d times", k, c)
			}
		}
	})
}

func TestMemStoreContract(t *testing.T) {
	testStoreContract(t, func() Store { return NewMemStore() })
}

func TestMemStoreScanOrder(t *testing.T) {
	ctx := context.Background()
	s := NewMemStore()
	keys := []string{"m", "a", "z", "k"}
	var ops []Op
	for _, k := range keys {
		ops = append(ops, Op{Rec: Record{Key: []byte(k), Value: []byte("v")}})
	}
	if err := s.ApplyBatch(ctx, &DrainBatch{Seq: 1, Ops: ops}); err != nil {
		t.Fatal(err)
	}
	var got []string
	if _, err := s.Scan(ctx, nil, func(r Record) bool {
		got = append(got, string(r.Key))
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if !sort.StringsAreSorted(got) {
		t.Fatalf("MemStore scan not in key order: %v", got)
	}
}
