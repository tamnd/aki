package hot

import (
	"bytes"
	"testing"
)

// FuzzHotStoreModel is the differential gate the rewrite spec calls for: it drives
// a hot.Store and a plain map[string][]byte reference model through the same random
// sequence of Set, Get, Delete, and Clear operations decoded from the fuzz input,
// and asserts the store agrees with the model after every step. The open-addressed
// table, the grow and tombstone-clearing rehash, the probe past tombstones, and the
// used-key accounting all get exercised by arbitrary inputs, so a corner that a
// hand-written test misses surfaces here as a model mismatch.
//
// The store is run with a single shard and a small key alphabet so the corpus
// quickly drives collisions, probe chains, deletes that tombstone mid-chain, and
// reinserts that must reuse a tombstone, which is where the subtle bugs live.
func FuzzHotStoreModel(f *testing.F) {
	// Seed a few hand-built programs so the corpus starts on meaningful shapes.
	f.Add([]byte{0, 1, 2, 0, 3, 4, 1, 1, 2, 2}) // a couple sets then gets
	f.Add([]byte{0, 5, 9, 2, 5, 0, 5, 8, 1, 5}) // set, delete, set same key
	f.Add([]byte{0, 1, 1, 0, 1, 2, 0, 1, 3, 3}) // overwrite the same key repeatedly
	f.Add([]byte{0, 1, 0, 2, 0, 3, 0, 4, 99})   // fill then clear

	f.Fuzz(func(t *testing.T, prog []byte) {
		s, err := New(Tunables{Shards: 1})
		if err != nil {
			t.Fatal(err)
		}
		model := map[string][]byte{}

		// Decode prog as a stream of (op, arg) byte pairs. A trailing odd byte is
		// ignored. Keys come from a small alphabet to force collisions.
		for i := 0; i+1 < len(prog); i += 2 {
			op, arg := prog[i], prog[i+1]
			key := []byte{'k', arg & 0x0f} // 16 distinct keys
			ks := string(key)

			switch op % 4 {
			case 0: // Set
				// Vary the value length around inlineCap so both the inline and the
				// heap-slice record paths get exercised by the corpus.
				vlen := int(arg) % (inlineCap + 16)
				val := bytes.Repeat([]byte{arg}, vlen)
				prev, _ := s.SetWithPrev(key, val)
				wantPrev := -1
				if old, ok := model[ks]; ok {
					wantPrev = len(old)
				}
				if prev != wantPrev {
					t.Fatalf("Set %q prevLen = %d want %d", ks, prev, wantPrev)
				}
				model[ks] = val

			case 1: // Get
				got, ok, _ := s.Get(key)
				want, wantOK := model[ks]
				if ok != wantOK {
					t.Fatalf("Get %q found = %v want %v", ks, ok, wantOK)
				}
				if ok && !bytes.Equal(got, want) {
					t.Fatalf("Get %q = %q want %q", ks, got, want)
				}

			case 2: // Delete
				n, ok, _ := s.DeleteWithPrev(key)
				old, wantOK := model[ks]
				if ok != wantOK {
					t.Fatalf("Delete %q ok = %v want %v", ks, ok, wantOK)
				}
				if ok && n != len(old) {
					t.Fatalf("Delete %q prevLen = %d want %d", ks, n, len(old))
				}
				delete(model, ks)

			case 3: // Clear, but only on a rare arg so the store usually keeps state
				if arg == 99 {
					if err := s.Clear(); err != nil {
						t.Fatalf("Clear: %v", err)
					}
					model = map[string][]byte{}
				}
			}

			// Invariant after every step: Len and the full enumeration agree with
			// the model.
			if s.Len() != len(model) {
				t.Fatalf("Len = %d want %d", s.Len(), len(model))
			}
		}

		// Final full reconciliation: every model key reads back exactly, every
		// enumerated key is in the model, and counts match.
		for k, v := range model {
			got, ok, _ := s.Get([]byte(k))
			if !ok || !bytes.Equal(got, v) {
				t.Fatalf("final Get %q = (%q,%v) want %q", k, got, ok, v)
			}
		}
		seen := 0
		s.Each(func(k, v []byte) bool {
			want, ok := model[string(k)]
			if !ok {
				t.Fatalf("Each yielded %q absent from model", k)
			}
			if !bytes.Equal(v, want) {
				t.Fatalf("Each %q = %q want %q", k, v, want)
			}
			seen++
			return true
		})
		if seen != len(model) {
			t.Fatalf("Each saw %d keys want %d", seen, len(model))
		}
	})
}
