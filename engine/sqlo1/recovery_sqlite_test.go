// The Track A recovery order end to end, doc 02 section 5: open SQLite
// (its own WAL replays to the last checkpoint), read the high-water mark
// from the meta row, replay the aki WAL from there. External test package
// because engine/sqlo1a imports engine/sqlo1.
package sqlo1_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/tamnd/aki/engine/sqlo1"
	"github.com/tamnd/aki/engine/sqlo1a"
)

func TestTrackARecoveryOrder(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	data := filepath.Join(dir, "a.sqlo1")
	walPath := data + ".aki-wal"

	w, err := sqlo1.OpenWALForTest(walPath, 7, 1<<16)
	if err != nil {
		t.Fatalf("openWAL: %v", err)
	}
	// Ten puts and a delete of key 3, all flushed: eleven acknowledged
	// frames, seqs 1 through 11.
	recs := make([]sqlo1.Record, 10)
	for i := range recs {
		recs[i] = sqlo1.Record{
			Key:   fmt.Appendf(nil, "key%02d", i),
			Value: fmt.Appendf(nil, "val%02d", i),
			Gen:   1,
		}
		if _, err := w.Append(0, sqlo1.WalOpPutForTest, 0, sqlo1.AppendPutForTest(nil, &recs[i])); err != nil {
			t.Fatalf("append put %d: %v", i, err)
		}
	}
	if _, err := w.Append(0, sqlo1.WalOpDelForTest, 0, sqlo1.AppendDelForTest(nil, recs[3].Key)); err != nil {
		t.Fatalf("append del: %v", err)
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// The pre-crash checkpoint: a drain covered the first four frames.
	db, err := sqlo1a.Open(data)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := db.ApplyBatch(ctx, &sqlo1.DrainBatch{Seq: 4, Ops: []sqlo1.Op{
		{Rec: recs[0]}, {Rec: recs[1]}, {Rec: recs[2]}, {Rec: recs[3]},
	}}); err != nil {
		t.Fatalf("pre-crash drain: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("wal close: %v", err)
	}

	// Recovery, in the doc 02 order.
	db2, err := sqlo1a.Open(data)
	if err != nil {
		t.Fatalf("recovery Open: %v", err)
	}
	defer db2.Close()
	if hw := db2.Stats().HighWater; hw != 4 {
		t.Fatalf("high-water before replay = %d, want 4", hw)
	}
	w2, err := sqlo1.OpenWALForTest(walPath, 7, 1<<16)
	if err != nil {
		t.Fatalf("recovery openWAL: %v", err)
	}
	defer w2.Close()
	applied, err := sqlo1.RecoverStoreForTest(ctx, db2, w2, 3)
	if err != nil {
		t.Fatalf("recoverStore: %v", err)
	}
	if applied != 7 {
		t.Fatalf("replay applied %d ops, want 7 (frames 5..11)", applied)
	}

	for i := range recs {
		rec, err := db2.Get(ctx, recs[i].Key)
		if i == 3 {
			if !errors.Is(err, sqlo1.ErrNotFound) {
				t.Fatalf("key 3 after replayed delete: %v, want ErrNotFound", err)
			}
			continue
		}
		if err != nil {
			t.Fatalf("get key %d: %v", i, err)
		}
		if !bytes.Equal(rec.Value, recs[i].Value) || rec.Gen != 1 {
			t.Fatalf("key %d = %+v, want value %q gen 1", i, rec, recs[i].Value)
		}
	}
	if hw := db2.Stats().HighWater; hw != 11 {
		t.Fatalf("high-water after replay = %d, want 11", hw)
	}

	// Exactly-once: a second recovery pass finds nothing past the mark.
	applied, err = sqlo1.RecoverStoreForTest(ctx, db2, w2, 0)
	if err != nil {
		t.Fatalf("second recoverStore: %v", err)
	}
	if applied != 0 {
		t.Fatalf("second recovery applied %d ops, want 0", applied)
	}
}
