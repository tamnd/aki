package obs1

// Rejection grid for the group delta vocabulary. The round-trip,
// canonical-form, and fuzz coverage rides the shared opCorpus in
// opframe_test.go; this file pins the refusals: zero-effect lists,
// column mismatches, flag bytes outside 0 and 1, the unowned-claim
// consumer rule, truncations, and trailing bytes.

import (
	"encoding/binary"
	"testing"
)

func rejectGroupPayload(t *testing.T, name string, payload []byte) {
	t.Helper()
	if _, err := DecodeOp(WALFrame{Kind: OpGroupDelta, Seq: 1, Key: []byte("k"), Payload: payload}); err == nil {
		t.Fatalf("%s decoded", name)
	}
}

func TestGroupDeltaRejects(t *testing.T) {
	key := []byte("k")

	// Encode-side: effect-free and inconsistent sub-ops never frame.
	encodeRejects := []struct {
		name string
		op   Op
	}{
		{"empty group delta", GroupDelta{}},
		{"empty ack", GroupDelta{Sub: GAck{Group: []byte("g")}}},
		{"ack id halves mismatched", GroupDelta{Sub: GAck{Group: []byte("g"), IDMs: []uint64{1, 2}, IDSeq: []uint64{0}}}},
		{"empty deliver", GroupDelta{Sub: GDeliver{Group: []byte("g"), Consumer: []byte("c"), TimeMs: 5}}},
		{"empty claim", GroupDelta{Sub: GClaim{Group: []byte("g"), Consumer: []byte("c")}}},
		{"claim columns mismatched", GroupDelta{Sub: GClaim{
			Group: []byte("g"), Consumer: []byte("c"),
			IDMs: []uint64{1, 2}, IDSeq: []uint64{0, 0}, TimeMs: []int64{5}, Counts: []uint16{1, 1},
		}}},
		{"unowned claim naming a consumer", GroupDelta{Sub: GClaim{
			Group: []byte("g"), Consumer: []byte("c"), Unowned: true,
			IDMs: []uint64{1}, IDSeq: []uint64{0}, TimeMs: []int64{5}, Counts: []uint16{1},
		}}},
	}
	for _, r := range encodeRejects {
		if _, err := EncodeOp(0, 1, key, r.op); err == nil {
			t.Fatalf("%s encoded", r.name)
		}
	}

	item := func(b, it []byte) []byte {
		b = binary.LittleEndian.AppendUint32(b, uint32(len(it)))
		return append(b, it...)
	}

	rejectGroupPayload(t, "empty payload", nil)
	rejectGroupPayload(t, "unknown sub-kind 0x09", []byte{0x09})

	// Cursor bodies: 24- and 26-byte tails around the exact 25, and a
	// read-valid byte of 2.
	for _, n := range []int{24, 26} {
		body := item([]byte{GSubNew}, []byte("g"))
		rejectGroupPayload(t, "gnew off-size cursor", append(body, make([]byte, n)...))
		body = item([]byte{GSubSetID}, []byte("g"))
		rejectGroupPayload(t, "gsetid off-size cursor", append(body, make([]byte, n)...))
	}
	badValid := item([]byte{GSubNew}, []byte("g"))
	badValid = append(badValid, make([]byte, 24)...)
	rejectGroupPayload(t, "gnew read-valid byte 2", append(badValid, 2))

	// Drop and consumer-del with trailing bytes, consumer-new off-size.
	rejectGroupPayload(t, "gdrop trailing byte", append(item([]byte{GSubDrop}, []byte("g")), 0))
	cdel := item(item([]byte{GSubConsumerDel}, []byte("g")), []byte("c"))
	rejectGroupPayload(t, "consumer-del trailing byte", append(cdel, 0))
	cnew := item(item([]byte{GSubConsumerNew}, []byte("g")), []byte("c"))
	rejectGroupPayload(t, "consumer-new 7-byte tail", append(cnew, make([]byte, 7)...))
	rejectGroupPayload(t, "consumer-new 9-byte tail", append(cnew, make([]byte, 9)...))

	// Ack: zero count and an id list cut inside an entry.
	ack := item([]byte{GSubAck}, []byte("g"))
	rejectGroupPayload(t, "ack zero count", binary.LittleEndian.AppendUint32(ack, 0))
	shortAck := binary.LittleEndian.AppendUint32(item([]byte{GSubAck}, []byte("g")), 2)
	shortAck = binary.LittleEndian.AppendUint64(shortAck, 5)
	rejectGroupPayload(t, "ack cut inside its id list", shortAck)

	// Deliver: noack byte 2, and a truncated time.
	del := item(item([]byte{GSubDeliver}, []byte("g")), []byte("c"))
	bad := append(append([]byte(nil), del...), 2)
	bad = binary.LittleEndian.AppendUint64(bad, 5)
	bad = binary.LittleEndian.AppendUint32(bad, 1)
	bad = binary.LittleEndian.AppendUint64(bad, 1)
	bad = binary.LittleEndian.AppendUint64(bad, 0)
	rejectGroupPayload(t, "deliver noack byte 2", bad)
	rejectGroupPayload(t, "deliver cut inside its time", append(append([]byte(nil), del...), 0, 1, 2))

	// Claim: unowned byte 2, unowned naming a consumer, entry list cut
	// inside the count column.
	claim := item(item([]byte{GSubClaim}, []byte("g")), nil)
	rejectGroupPayload(t, "claim unowned byte 2", append(append([]byte(nil), claim...), 2))
	named := item(item([]byte{GSubClaim}, []byte("g")), []byte("c"))
	named = append(named, 1)
	named = binary.LittleEndian.AppendUint32(named, 1)
	named = append(named, make([]byte, 26)...)
	rejectGroupPayload(t, "unowned claim naming a consumer", named)
	cut := append(append([]byte(nil), claim...), 0)
	cut = binary.LittleEndian.AppendUint32(cut, 1)
	cut = append(cut, make([]byte, 25)...)
	rejectGroupPayload(t, "claim cut inside an entry", cut)
}
