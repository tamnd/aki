package obs1

import (
	"bytes"
	"encoding/binary"
	"math"
	"reflect"
	"strings"
	"testing"
)

// opCorpus is one canonical frame per kind and per colldelta sub-kind,
// the seed set every grid and fuzz target below sweeps.
func opCorpus(t testing.TB) []struct {
	name string
	key  []byte
	op   Op
} {
	t.Helper()
	key := []byte("k:1")
	return []struct {
		name string
		key  []byte
		op   Op
	}{
		{"strset", key, StrSet{Value: []byte("value"), ExpiryMS: 5000, Ladder: LadderCounter}},
		{"keydel", key, KeyDel{}},
		{"expire", key, Expire{ExpiryMS: 123456}},
		{"collnew", key, CollNew{Type: CollZSet, Hints: []byte{0x07}}},
		{"colldrop", key, CollDrop{}},
		{"txn-begin", nil, Txn{Begin: true}},
		{"txn-end", nil, Txn{End: true}},
		{"noop", nil, Noop{Pad: []byte("padpad")}},
		{"hset", key, CollDelta{Sub: HSet{Pairs: []FieldValue{{[]byte("f1"), []byte("v1")}, {[]byte("f2"), nil}}}}},
		{"hdel", key, CollDelta{Sub: HDel{Fields: [][]byte{[]byte("f1"), []byte("f2")}}}},
		{"sadd", key, CollDelta{Sub: SAdd{Members: [][]byte{[]byte("m1"), nil}}}},
		{"srem", key, CollDelta{Sub: SRem{Members: [][]byte{[]byte("m1")}}}},
		{"zadd", key, CollDelta{Sub: ZAdd{Entries: []ScoreMember{
			{Score: 1.5, Member: []byte("a")},
			{Score: math.Inf(-1), Member: []byte("b")},
			{Score: math.Copysign(0, -1), Member: []byte("c")},
		}}}},
		{"zrem", key, CollDelta{Sub: ZRem{Members: [][]byte{[]byte("a"), []byte("b")}}}},
		{"lpush", key, CollDelta{Sub: LPush{Values: [][]byte{[]byte("v")}}}},
		{"rpush", key, CollDelta{Sub: RPush{Values: [][]byte{[]byte("v1"), []byte("v2")}}}},
		{"lpop", key, CollDelta{Sub: LPop{Count: 2}}},
		{"rpop", key, CollDelta{Sub: RPop{Count: 1}}},
		{"lset", key, CollDelta{Sub: LSet{Index: -1, Value: []byte("v")}}},
		{"xadd", key, CollDelta{Sub: XAdd{IDMs: 1700000000000, IDSeq: 3, Pairs: []FieldValue{{[]byte("f"), []byte("v")}}}}},
		{"hexpire", key, CollDelta{Sub: HExpire{AtMs: 1700000000000, Fields: [][]byte{[]byte("f1"), []byte("f2")}}}},
		{"hpersist", key, CollDelta{Sub: HExpire{AtMs: 0, Fields: [][]byte{[]byte("f1")}}}},
		{"lrem", key, CollDelta{Sub: LRem{Indices: []uint32{0, 3, 7}}}},
		{"lins", key, CollDelta{Sub: LIns{Index: 2, Value: []byte("v")}}},
	}
}

// Every corpus op survives the full path: EncodeOp, a real WAL object,
// ParseWAL, DecodeOp, and the decoded op re-encodes to the same frame.
func TestOpRoundtripThroughWAL(t *testing.T) {
	corpus := opCorpus(t)
	frames := make([]WALFrame, len(corpus))
	for i, c := range corpus {
		f, err := EncodeOp(uint16(i), uint64(i+1), c.key, c.op)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		frames[i] = f
	}
	wal, err := AppendWAL(nil, 7, []WALSection{{Group: 3, Epoch: 2, Frames: frames}})
	if err != nil {
		t.Fatal(err)
	}
	secs, _, err := ParseWAL(wal)
	if err != nil {
		t.Fatal(err)
	}
	for i, f := range secs[0].Frames {
		c := corpus[i]
		op, err := DecodeOp(f)
		if err != nil {
			t.Fatalf("%s: decode: %v", c.name, err)
		}
		want, err := EncodeOp(f.Slot, f.Seq, f.Key, c.op)
		if err != nil {
			t.Fatal(err)
		}
		got, err := EncodeOp(f.Slot, f.Seq, f.Key, op)
		if err != nil {
			t.Fatal(err)
		}
		if got.Flags != want.Flags || !bytes.Equal(got.Payload, want.Payload) {
			t.Fatalf("%s: decoded op re-encodes differently", c.name)
		}
	}
}

// Typed equality on the kinds whose fields are all non-empty, so a
// codec that swaps two fields of equal length cannot hide behind the
// byte comparison above.
func TestOpDecodeTyped(t *testing.T) {
	for _, c := range opCorpus(t) {
		f, err := EncodeOp(9, 42, c.key, c.op)
		if err != nil {
			t.Fatal(err)
		}
		op, err := DecodeOp(f)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		switch c.name {
		case "strset", "expire", "collnew", "txn-begin", "txn-end", "hdel", "zrem", "lpop", "rpop", "xadd", "hexpire", "hpersist", "lrem", "lins":
			if !reflect.DeepEqual(op, c.op) {
				t.Fatalf("%s: decoded %#v, want %#v", c.name, op, c.op)
			}
		}
	}
}

// The post-decision effects rule (doc 04 section 2) is a vocabulary
// fact: SPOP has no sub-kind because the owner records the members it
// removed as an srem, and INCR has none because the owner records the
// resulting value as a strset with the counter hint. This test is the
// rule written down where a grep finds it.
func TestPostDecisionEffects(t *testing.T) {
	spop, err := EncodeOp(1, 1, []byte("s"), CollDelta{Sub: SRem{Members: [][]byte{[]byte("chosen1"), []byte("chosen2")}}})
	if err != nil {
		t.Fatal(err)
	}
	if spop.Kind != OpCollDelta || spop.Payload[0] != SubSRem {
		t.Fatalf("an SPOP effect must travel as an srem sub-op")
	}
	incr, err := EncodeOp(1, 2, []byte("n"), StrSet{Value: []byte("6"), Ladder: LadderCounter})
	if err != nil {
		t.Fatal(err)
	}
	if incr.Kind != OpStrSet || incr.Payload[len(incr.Payload)-1]&LadderCounter == 0 {
		t.Fatalf("an INCR effect must travel as a counter-hinted strset")
	}
}

// The hot-path encoder and the typed path must produce identical bytes:
// AppendStrSetFrame against the frame AppendWAL writes for the same op.
func TestAppendStrSetFramePinned(t *testing.T) {
	key, val := []byte("k:12345678"), []byte("valuevalue")
	op := StrSet{Value: val, ExpiryMS: 5000, Ladder: 0x03}
	f, err := EncodeOp(0x1234, 99, key, op)
	if err != nil {
		t.Fatal(err)
	}
	wal, err := AppendWAL(nil, 7, []WALSection{{Group: 1, Epoch: 1, Frames: []WALFrame{f}}})
	if err != nil {
		t.Fatal(err)
	}
	flen := walFrameFixed + len(key) + len(val) + 9
	slow := wal[HeaderSize+walSectionHdr : HeaderSize+walSectionHdr+flen]
	fast, err := AppendStrSetFrame(nil, 0x1234, 99, key, val, 5000, 0x03)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(fast, slow) {
		t.Fatalf("hot-path frame differs from the AppendWAL frame\nfast %x\nslow %x", fast, slow)
	}
	if got := binary.LittleEndian.Uint32(fast[:4]); got != uint32(flen) {
		t.Fatalf("flen = %d, want %d", got, flen)
	}
	if _, err := AppendStrSetFrame(nil, 0, 1, make([]byte, 0x10000), nil, 0, 0); err == nil {
		t.Fatal("a 64 KiB key must be rejected")
	}
}

// Key rules: txn and noop carry no key, everything else needs one, on
// both the encode and decode side.
func TestOpKeyRules(t *testing.T) {
	if _, err := EncodeOp(0, 1, []byte("k"), Noop{}); err == nil {
		t.Fatal("noop with a key encoded")
	}
	if _, err := EncodeOp(0, 1, nil, KeyDel{}); err == nil {
		t.Fatal("keydel without a key encoded")
	}
	if _, err := DecodeOp(WALFrame{Kind: OpNoop, Seq: 1, Key: []byte("k")}); err == nil {
		t.Fatal("noop frame with a key decoded")
	}
	if _, err := DecodeOp(WALFrame{Kind: OpKeyDel, Seq: 1}); err == nil {
		t.Fatal("keydel frame without a key decoded")
	}
	if _, err := EncodeOp(0, 1, make([]byte, 0x10000), KeyDel{}); err == nil {
		t.Fatal("a 64 KiB key encoded")
	}
}

// Structural rejects: unknown kinds, sub-kinds, and collection types
// bounce; flags outside a txn marker bounce; txn markers take exactly
// one of begin and end.
func TestOpStructuralRejects(t *testing.T) {
	key := []byte("k")
	for _, kind := range []uint8{0x00, 0x09, 0x7F, 0xFF} {
		if _, err := DecodeOp(WALFrame{Kind: kind, Seq: 1, Key: key}); err == nil {
			t.Fatalf("op kind 0x%02x decoded", kind)
		}
	}
	for _, sub := range []uint8{0x00, 0x10, 0xFF} {
		if _, err := DecodeOp(WALFrame{Kind: OpCollDelta, Seq: 1, Key: key, Payload: []byte{sub, 1, 0, 0, 0}}); err == nil {
			t.Fatalf("colldelta sub-kind 0x%02x decoded", sub)
		}
	}
	for _, typ := range []uint8{0x00, 0x06, 0xFF} {
		if _, err := DecodeOp(WALFrame{Kind: OpCollNew, Seq: 1, Key: key, Payload: []byte{typ}}); err == nil {
			t.Fatalf("collnew type 0x%02x decoded", typ)
		}
		if _, err := EncodeOp(0, 1, key, CollNew{Type: typ}); err == nil {
			t.Fatalf("collnew type 0x%02x encoded", typ)
		}
	}
	if _, err := DecodeOp(WALFrame{Kind: OpStrSet, Flags: 0x01, Seq: 1, Key: key, Payload: make([]byte, 9)}); err == nil {
		t.Fatal("strset with frame flags decoded")
	}
	for _, flags := range []uint8{0x00, 0x03, 0x04, 0xFF} {
		if _, err := DecodeOp(WALFrame{Kind: OpTxn, Flags: flags, Seq: 1}); err == nil {
			t.Fatalf("txn marker with flags 0x%02x decoded", flags)
		}
	}
	for _, op := range []Op{Txn{}, Txn{Begin: true, End: true}} {
		if _, err := EncodeOp(0, 1, nil, op); err == nil {
			t.Fatalf("txn %#v encoded", op)
		}
	}
	if _, err := EncodeOp(0, 1, key, CollDelta{}); err == nil {
		t.Fatal("colldelta without a sub-op encoded")
	}
}

// Effects rule on sizes: zero-count lists and pops reject on both
// sides, NaN scores reject, exact-size payloads take nothing else.
func TestOpSizeRejects(t *testing.T) {
	key := []byte("k")
	if _, err := EncodeOp(0, 1, key, CollDelta{Sub: SAdd{}}); err == nil {
		t.Fatal("empty sadd encoded")
	}
	if _, err := EncodeOp(0, 1, key, CollDelta{Sub: HSet{}}); err == nil {
		t.Fatal("empty hset encoded")
	}
	if _, err := EncodeOp(0, 1, key, CollDelta{Sub: LPop{}}); err == nil {
		t.Fatal("lpop of zero encoded")
	}
	if _, err := EncodeOp(0, 1, key, CollDelta{Sub: HExpire{AtMs: 5}}); err == nil {
		t.Fatal("hexpire with no fields encoded")
	}
	if _, err := DecodeOp(WALFrame{Kind: OpCollDelta, Seq: 1, Key: key, Payload: append([]byte{SubHExpire}, make([]byte, 12)...)}); err == nil {
		t.Fatal("hexpire with zero field count decoded")
	}
	if _, err := DecodeOp(WALFrame{Kind: OpCollDelta, Seq: 1, Key: key, Payload: append([]byte{SubHExpire}, make([]byte, 7)...)}); err == nil {
		t.Fatal("hexpire truncated at its deadline decoded")
	}
	if _, err := EncodeOp(0, 1, key, CollDelta{Sub: ZAdd{Entries: []ScoreMember{{Score: math.NaN()}}}}); err == nil {
		t.Fatal("NaN score encoded")
	}
	if _, err := EncodeOp(0, 1, key, CollDelta{Sub: LRem{}}); err == nil {
		t.Fatal("empty lrem encoded")
	}
	if _, err := EncodeOp(0, 1, key, CollDelta{Sub: LRem{Indices: []uint32{3, 3}}}); err == nil {
		t.Fatal("lrem with a repeated index encoded")
	}
	if _, err := EncodeOp(0, 1, key, CollDelta{Sub: LRem{Indices: []uint32{3, 1}}}); err == nil {
		t.Fatal("lrem with descending indices encoded")
	}
	if _, err := EncodeOp(0, 1, key, CollDelta{Sub: LIns{Index: -1, Value: []byte("v")}}); err == nil {
		t.Fatal("lins with a negative index encoded")
	}
	shortRem := binary.LittleEndian.AppendUint32([]byte{SubLRem}, 2)
	shortRem = binary.LittleEndian.AppendUint32(shortRem, 0)
	if _, err := DecodeOp(WALFrame{Kind: OpCollDelta, Seq: 1, Key: key, Payload: shortRem}); err == nil {
		t.Fatal("lrem truncated inside its index list decoded")
	}
	descRem := binary.LittleEndian.AppendUint32([]byte{SubLRem}, 2)
	descRem = binary.LittleEndian.AppendUint32(descRem, 3)
	descRem = binary.LittleEndian.AppendUint32(descRem, 1)
	if _, err := DecodeOp(WALFrame{Kind: OpCollDelta, Seq: 1, Key: key, Payload: descRem}); err == nil {
		t.Fatal("lrem with descending wire indices decoded")
	}
	if _, err := DecodeOp(WALFrame{Kind: OpCollDelta, Seq: 1, Key: key, Payload: append([]byte{SubLIns}, make([]byte, 7)...)}); err == nil {
		t.Fatal("lins truncated at its index decoded")
	}
	zero := []byte{0, 0, 0, 0}
	for _, sub := range []uint8{SubSAdd, SubLPop, SubRPop, SubLRem} {
		if _, err := DecodeOp(WALFrame{Kind: OpCollDelta, Seq: 1, Key: key, Payload: append([]byte{sub}, zero...)}); err == nil {
			t.Fatalf("sub-kind 0x%02x with zero count decoded", sub)
		}
	}
	if _, err := DecodeOp(WALFrame{Kind: OpExpire, Seq: 1, Key: key, Payload: make([]byte, 7)}); err == nil {
		t.Fatal("7-byte expire decoded")
	}
	if _, err := DecodeOp(WALFrame{Kind: OpExpire, Seq: 1, Key: key, Payload: make([]byte, 9)}); err == nil {
		t.Fatal("9-byte expire decoded")
	}
	if _, err := DecodeOp(WALFrame{Kind: OpKeyDel, Seq: 1, Key: key, Payload: []byte{0}}); err == nil {
		t.Fatal("keydel with payload decoded")
	}
	if _, err := DecodeOp(WALFrame{Kind: OpStrSet, Seq: 1, Key: key, Payload: make([]byte, 8)}); err == nil {
		t.Fatal("8-byte strset decoded")
	}
	// A count no remaining bytes could satisfy must reject before it
	// allocates, the parseCount guard.
	huge := binary.LittleEndian.AppendUint32([]byte{SubSAdd}, 0xFFFFFFFF)
	_, err := DecodeOp(WALFrame{Kind: OpCollDelta, Seq: 1, Key: key, Payload: huge})
	if err == nil {
		t.Fatal("overrunning count decoded")
	}
	if !strings.Contains(err.Error(), "overruns") {
		t.Fatalf("overrunning count rejected for the wrong reason: %v", err)
	}
}

// checkOpBytes decodes a synthetic frame and holds the one universal
// rule: acceptance implies canonical re-encode, which DecodeOp enforces
// internally, so any accept that returns is fine and the grids only
// hunt panics and drift.
func checkOpBytes(t *testing.T, f WALFrame) {
	t.Helper()
	op, err := DecodeOp(f)
	if err != nil {
		return
	}
	again, err := EncodeOp(f.Slot, f.Seq, f.Key, op)
	if err != nil {
		t.Fatalf("accepted op fails re-encode: %v", err)
	}
	if again.Kind != f.Kind || again.Flags != f.Flags || !bytes.Equal(again.Payload, f.Payload) {
		t.Fatal("accepted op re-encodes differently")
	}
}

// TestOpPayloadGrid sweeps every truncation, every single-byte
// corruption under the three masks, and tail extensions across every
// corpus payload. Unlike the doc 03 object grids, acceptance here is
// legal (payloads carry free bytes like values and members), so the
// asserted invariant is re-encode fidelity, with the structural rejects
// pinned by the targeted tests above.
func TestOpPayloadGrid(t *testing.T) {
	for _, c := range opCorpus(t) {
		t.Run(c.name, func(t *testing.T) {
			f, err := EncodeOp(3, 7, c.key, c.op)
			if err != nil {
				t.Fatal(err)
			}
			for n := range f.Payload {
				g := f
				g.Payload = f.Payload[:n]
				checkOpBytes(t, g)
			}
			for off := range f.Payload {
				for _, m := range corruptionMasks {
					g := f
					g.Payload = bytes.Clone(f.Payload)
					was := g.Payload[off]
					g.Payload[off] = m.mut(was)
					if g.Payload[off] == was {
						continue
					}
					checkOpBytes(t, g)
				}
			}
			for _, tail := range [][]byte{{0x00}, {0xFF}, bytes.Repeat([]byte{0xEE}, 16)} {
				g := f
				g.Payload = append(bytes.Clone(f.Payload), tail...)
				checkOpBytes(t, g)
			}
		})
	}
}

// FuzzDecodeOp lets coverage-guided mutation explore past the grids.
// The input encodes kind, flags, a key, and the payload.
func FuzzDecodeOp(f *testing.F) {
	pack := func(fr WALFrame) []byte {
		b := []byte{fr.Kind, fr.Flags, uint8(len(fr.Key))}
		b = append(b, fr.Key...)
		return append(b, fr.Payload...)
	}
	for _, c := range opCorpus(f) {
		fr, err := EncodeOp(3, 7, c.key, c.op)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(pack(fr))
		if len(fr.Payload) > 0 {
			short := fr
			short.Payload = fr.Payload[:len(fr.Payload)-1]
			f.Add(pack(short))
		}
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) < 3 {
			return
		}
		klen := int(b[2])
		if len(b) < 3+klen {
			return
		}
		fr := WALFrame{Kind: b[0], Flags: b[1], Seq: 1, Key: b[3 : 3+klen], Payload: b[3+klen:]}
		checkOpBytes(t, fr)
	})
}
