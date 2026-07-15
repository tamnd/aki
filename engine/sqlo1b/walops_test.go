package sqlo1b

import (
	"path/filepath"
	"testing"

	"github.com/tamnd/aki/engine/sqlo1"
)

func TestSealOpRoundtrip(t *testing.T) {
	want := SealOp{Extent: 42, Sum: 0xDEADBEEFCAFE, Kind: KindIndex}
	b := want.Encode()
	if len(b) != 17 {
		t.Fatalf("SEAL payload %d bytes, want 17", len(b))
	}
	got, err := DecodeSealOp(b)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("roundtrip %+v, want %+v", got, want)
	}
	if _, err := DecodeSealOp(b[:16]); err == nil {
		t.Fatal("short SEAL decoded")
	}
	b[16] = 0
	if _, err := DecodeSealOp(b); err == nil {
		t.Fatal("kind 0 decoded")
	}
	b[16] = KindStats + 1
	if _, err := DecodeSealOp(b); err == nil {
		t.Fatal("kind 7 decoded")
	}
}

func TestCkptTrimRoundtrip(t *testing.T) {
	c, err := DecodeCkptOp(CkptOp{SuperSeq: 9}.Encode())
	if err != nil {
		t.Fatal(err)
	}
	if c.SuperSeq != 9 {
		t.Fatalf("CKPT super seq %d, want 9", c.SuperSeq)
	}
	tr, err := DecodeTrimOp(TrimOp{WALSeq: 77}.Encode())
	if err != nil {
		t.Fatal(err)
	}
	if tr.WALSeq != 77 {
		t.Fatalf("TRIM echo %d, want 77", tr.WALSeq)
	}
	if _, err := DecodeCkptOp(nil); err == nil {
		t.Fatal("empty CKPT decoded")
	}
	if _, err := DecodeTrimOp(make([]byte, 9)); err == nil {
		t.Fatal("long TRIM decoded")
	}
}

func TestFormatStateFold(t *testing.T) {
	var st FormatState
	steps := []struct {
		seq     uint64
		op      uint8
		payload []byte
	}{
		{1, FrameSeal, SealOp{Extent: 1, Sum: 11, Kind: KindVlog}.Encode()},
		{2, 1, []byte("put payload, not ours")},
		{3, FrameSeal, SealOp{Extent: 2, Sum: 22, Kind: KindIndex}.Encode()},
		{4, FrameCkpt, CkptOp{SuperSeq: 5}.Encode()},
		{5, FrameTrim, TrimOp{WALSeq: 4}.Encode()},
		{6, FrameSeal, SealOp{Extent: 3, Sum: 33, Kind: KindVlog}.Encode()},
	}
	for _, s := range steps {
		if err := st.Apply(s.seq, s.op, s.payload); err != nil {
			t.Fatalf("apply seq %d: %v", s.seq, err)
		}
	}
	if len(st.Seals) != 3 || st.Seals[2].Extent != 3 || st.Seals[2].WALSeq != 6 {
		t.Fatalf("seals folded wrong: %+v", st.Seals)
	}
	if st.CkptSuper != 5 || st.CkptWALSeq != 4 || st.TrimEcho != 4 {
		t.Fatalf("ckpt/trim folded wrong: %+v", st)
	}

	// The quarantine set: seals strictly after the checkpoint's seq.
	after := st.SealsAfter(st.CkptWALSeq)
	if len(after) != 1 || after[0].Extent != 3 {
		t.Fatalf("seals after ckpt: %+v", after)
	}
	if got := st.SealsAfter(0); len(got) != 3 {
		t.Fatalf("seals after 0: %d, want all 3", len(got))
	}
	if got := st.SealsAfter(6); got != nil {
		t.Fatalf("seals after the end: %+v", got)
	}

	// Ordering is enforced, not assumed.
	if err := st.Apply(6, FrameTrim, TrimOp{}.Encode()); err == nil {
		t.Fatal("replayed seq accepted")
	}
	if err := st.Apply(7, 200, nil); err == nil {
		t.Fatal("unknown op accepted")
	}
	if err := st.Apply(8, FrameSeal, []byte{1}); err == nil {
		t.Fatal("corrupt SEAL payload accepted")
	}
}

// TestFormatOpsThroughWAL routes the format frames through the real
// S1 sidecar: append, group-commit, reopen, replay, fold. The op
// codes must agree between the packages, and the fold must see the
// transport's seq order.
func TestFormatOpsThroughWAL(t *testing.T) {
	if FrameSeal != sqlo1.WALOpSeal || FrameCkpt != sqlo1.WALOpCkpt || FrameTrim != sqlo1.WALOpTrim {
		t.Fatal("format op codes drifted from the transport's")
	}

	path := filepath.Join(t.TempDir(), "b.aki-wal")
	const dbID, segSize = 0x1234, 1 << 16
	w, err := sqlo1.OpenWAL(path, dbID, segSize)
	if err != nil {
		t.Fatal(err)
	}

	emit := func(op uint8, payload []byte) uint64 {
		t.Helper()
		seq, err := w.Append(0, op, 0, payload)
		if err != nil {
			t.Fatal(err)
		}
		return seq
	}
	emit(sqlo1.WALOpSeal, SealOp{Extent: 7, Sum: 70, Kind: KindVlog}.Encode())
	emit(sqlo1.WALOpPut, []byte("data frame the format fold skips"))
	ckptAt := emit(sqlo1.WALOpCkpt, CkptOp{SuperSeq: 2}.Encode())
	emit(sqlo1.WALOpTrim, TrimOp{WALSeq: ckptAt}.Encode())
	sealAfter := emit(sqlo1.WALOpSeal, SealOp{Extent: 8, Sum: 80, Kind: KindDirectory}.Encode())
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Crash-shaped reopen: scan, replay, fold.
	w, err = sqlo1.OpenWAL(path, dbID, segSize)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	var st FormatState
	err = w.Replay(func(fr sqlo1.WALFrame) error {
		return st.Apply(fr.Seq, fr.Op, fr.Payload)
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(st.Seals) != 2 {
		t.Fatalf("folded %d seals, want 2", len(st.Seals))
	}
	if st.CkptSuper != 2 || st.CkptWALSeq != ckptAt || st.TrimEcho != ckptAt {
		t.Fatalf("fold after reopen: %+v", st)
	}
	after := st.SealsAfter(st.CkptWALSeq)
	if len(after) != 1 || after[0].Extent != 8 || after[0].WALSeq != sealAfter {
		t.Fatalf("quarantine set after reopen: %+v", after)
	}
}
