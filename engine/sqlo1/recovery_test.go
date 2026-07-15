package sqlo1

import (
	"bytes"
	"context"
	"fmt"
	"maps"
	"math/rand"
	"path/filepath"
	"testing"
)

func TestPayloadRoundTrip(t *testing.T) {
	cases := []Record{
		{Key: []byte("k"), Value: []byte("v")},
		{Key: []byte("k"), Value: nil},
		{Key: []byte(""), Value: []byte("empty key is legal")},
		{Key: []byte("k"), Value: []byte("v"), ExpireMs: 123456789},
		{Key: []byte("k"), Value: []byte("v"), Gen: 42},
		{Key: []byte("k"), Value: []byte("v"), ExpireMs: 1 << 50, Gen: 7},
	}
	for i, want := range cases {
		got, err := parsePutPayload(appendPutPayload(nil, &want))
		if err != nil {
			t.Fatalf("case %d: %v", i, err)
		}
		if !bytes.Equal(got.Key, want.Key) || !bytes.Equal(got.Value, want.Value) ||
			got.ExpireMs != want.ExpireMs || got.Gen != want.Gen {
			t.Fatalf("case %d: got %+v, want %+v", i, got, want)
		}
	}

	key, err := parseDelPayload(appendDelPayload(nil, []byte("dk")))
	if err != nil || !bytes.Equal(key, []byte("dk")) {
		t.Fatalf("del roundtrip: %q, %v", key, err)
	}
	key, exp, err := parsePexpirePayload(appendPexpirePayload(nil, []byte("pk"), 987654))
	if err != nil || !bytes.Equal(key, []byte("pk")) || exp != 987654 {
		t.Fatalf("pexpire roundtrip: %q, %d, %v", key, exp, err)
	}
	key, gen, err := parseGenbumpPayload(appendGenbumpPayload(nil, []byte("gk"), 31))
	if err != nil || !bytes.Equal(key, []byte("gk")) || gen != 31 {
		t.Fatalf("genbump roundtrip: %q, %d, %v", key, gen, err)
	}

	full := appendPutPayload(nil, &Record{Key: []byte("kk"), Value: []byte("vv"), ExpireMs: 5, Gen: 6})
	for cut := range len(full) {
		if _, err := parsePutPayload(full[:cut]); err == nil {
			t.Fatalf("put payload cut to %d bytes parsed", cut)
		}
	}
	if _, err := parseDelPayload([]byte{9, 0, 'x'}); err == nil {
		t.Fatal("del payload with a lying klen parsed")
	}
}

// The recovery property, doc 02 section 5 and doc 03 section 14: after a
// crash that loses any un-fsynced WAL tail, opening the store, reading its
// high-water mark, and replaying the aki WAL from there yields exactly the
// acknowledged state, and doing it again changes nothing. The model acks
// on Flush (the group-commit barrier), drains random subsets of acked
// post-images at the flushed barrier seq, and crashes by dropping an
// unflushed tail group.
func TestRecoverStoreExactlyOnceProperty(t *testing.T) {
	ctx := context.Background()
	for round := range 40 {
		rng := rand.New(rand.NewSource(int64(round)*104729 + 7))
		path := filepath.Join(t.TempDir(), "d.aki-wal")
		w, err := openWAL(path, 99, 1<<16)
		if err != nil {
			t.Fatalf("round %d: openWAL: %v", round, err)
		}
		store := NewMemStore()

		walState := map[string]Record{} // effect of every appended frame
		acked := map[string]Record{}    // effect of every flushed frame
		dirty := map[string]bool{}      // acked keys not yet drained
		var group []string              // keys touched since the last flush
		var lastAppended, lastFlushed uint64

		keys := make([]string, 16)
		for i := range keys {
			keys[i] = fmt.Sprintf("k%02d", i)
		}
		appendOp := func() {
			k := keys[rng.Intn(len(keys))]
			kb := []byte(k)
			var seq uint64
			var err error
			switch r := rng.Intn(10); {
			case r < 6:
				rec := Record{Key: kb, Value: fmt.Appendf(nil, "v%d", rng.Intn(1000000))}
				if rng.Intn(3) == 0 {
					rec.ExpireMs = 1<<50 + int64(rng.Intn(1000))
				}
				if rng.Intn(3) == 0 {
					rec.Gen = uint32(rng.Intn(9)) + 1
				}
				seq, err = w.Append(0, walOpPut, 0, appendPutPayload(nil, &rec))
				walState[k] = rec
			case r < 8:
				seq, err = w.Append(0, walOpDel, 0, appendDelPayload(nil, kb))
				delete(walState, k)
			case r < 9:
				exp := 1<<50 + int64(rng.Intn(1000))
				seq, err = w.Append(0, walOpPexpire, 0, appendPexpirePayload(nil, kb, exp))
				if rec, ok := walState[k]; ok {
					rec.ExpireMs = exp
					walState[k] = rec
				}
			default:
				gen := uint32(rng.Intn(100)) + 1
				seq, err = w.Append(0, walOpGenbump, 0, appendGenbumpPayload(nil, kb, gen))
				if rec, ok := walState[k]; ok {
					rec.Gen = gen
					walState[k] = rec
				}
			}
			if err != nil {
				t.Fatalf("round %d: append: %v", round, err)
			}
			lastAppended = seq
			group = append(group, k)
		}
		drain := func() {
			if len(dirty) == 0 {
				return
			}
			b := DrainBatch{Seq: int64(lastFlushed)}
			for k := range dirty {
				if rec, ok := acked[k]; ok {
					b.Ops = append(b.Ops, Op{Rec: rec})
				} else {
					b.Ops = append(b.Ops, Op{Del: true, Rec: Record{Key: []byte(k)}})
				}
			}
			if err := store.ApplyBatch(ctx, &b); err != nil {
				t.Fatalf("round %d: drain: %v", round, err)
			}
			clear(dirty)
		}

		for range 3 + rng.Intn(12) {
			for range 1 + rng.Intn(8) {
				appendOp()
			}
			if err := w.Flush(); err != nil {
				t.Fatalf("round %d: flush: %v", round, err)
			}
			lastFlushed = lastAppended
			acked = maps.Clone(walState)
			for _, k := range group {
				dirty[k] = true
			}
			group = group[:0]
			if rng.Intn(3) == 0 {
				drain()
			}
		}
		if rng.Intn(2) == 0 {
			// The crash tail: appended, never flushed, never acknowledged.
			for range 1 + rng.Intn(8) {
				appendOp()
			}
		}
		if err := w.Close(); err != nil {
			t.Fatalf("round %d: close: %v", round, err)
		}

		w2, err := openWAL(path, 99, 1<<16)
		if err != nil {
			t.Fatalf("round %d: reopen: %v", round, err)
		}
		if _, err := recoverStore(ctx, store, w2, 1+rng.Intn(6)); err != nil {
			t.Fatalf("round %d: recover: %v", round, err)
		}
		checkState := func(when string) {
			got := map[string]Record{}
			cur, err := store.Scan(ctx, nil, func(r Record) bool {
				got[string(r.Key)] = r
				return true
			})
			if err != nil || cur != nil {
				t.Fatalf("round %d %s: scan: cur %x, %v", round, when, cur, err)
			}
			if len(got) != len(acked) {
				t.Fatalf("round %d %s: %d keys, want %d", round, when, len(got), len(acked))
			}
			for k, want := range acked {
				g, ok := got[k]
				if !ok {
					t.Fatalf("round %d %s: key %s missing", round, when, k)
				}
				if !bytes.Equal(g.Value, want.Value) || g.ExpireMs != want.ExpireMs || g.Gen != want.Gen {
					t.Fatalf("round %d %s: key %s = %+v, want %+v", round, when, k, g, want)
				}
			}
		}
		checkState("after recovery")

		applied, err := recoverStore(ctx, store, w2, 0)
		if err != nil {
			t.Fatalf("round %d: second recover: %v", round, err)
		}
		if applied != 0 {
			t.Fatalf("round %d: second recovery applied %d ops, want 0", round, applied)
		}
		checkState("after second recovery")
		if err := w2.Close(); err != nil {
			t.Fatalf("round %d: close reopened: %v", round, err)
		}
	}
}
