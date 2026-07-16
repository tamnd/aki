package akifile

import "testing"

func encodeBarrier(h BarrierHeader, shards []BarrierShard) []byte {
	payload := AppendBarrierHeader(nil, h)
	for _, s := range shards {
		payload = AppendBarrierShard(payload, s)
	}
	return payload
}

// TestBarrierRoundTrip builds a barrier over four shards and reads back the watermark
// and every shard's tail position unchanged.
func TestBarrierRoundTrip(t *testing.T) {
	shards := []BarrierShard{
		{TailSeg: 0x007000, TailSeq: 1001},
		{TailSeg: 0x250000, TailSeq: 1009},
		{TailSeg: 0x04D000, TailSeq: 1013},
		{TailSeg: 0x14F000, TailSeq: 1007},
	}
	h := BarrierHeader{Wbar: 1013, ShardCount: uint64(len(shards))}
	payload := encodeBarrier(h, shards)
	if len(payload) != BarrierHeaderLen+len(shards)*BarrierShardSize {
		t.Fatalf("payload len = %d, want %d", len(payload), BarrierHeaderLen+len(shards)*BarrierShardSize)
	}

	got, err := ParseBarrierHeader(payload)
	if err != nil {
		t.Fatalf("parse header: %v", err)
	}
	if got != h {
		t.Fatalf("header = %+v, want %+v", got, h)
	}
	decoded, err := BarrierShards(payload, got)
	if err != nil {
		t.Fatalf("shards: %v", err)
	}
	if len(decoded) != len(shards) {
		t.Fatalf("got %d shards, want %d", len(decoded), len(shards))
	}
	for i := range shards {
		if decoded[i] != shards[i] {
			t.Fatalf("shard %d = %+v, want %+v", i, decoded[i], shards[i])
		}
	}
}

// TestBarrierConsistentAcceptsGenuineCut confirms a barrier whose every shard seq is
// at or below Wbar is a genuine cut.
func TestBarrierConsistentAcceptsGenuineCut(t *testing.T) {
	h := BarrierHeader{Wbar: 1013, ShardCount: 2}
	shards := []BarrierShard{{TailSeq: 1001}, {TailSeq: 1013}} // one below, one exactly at
	if !BarrierConsistent(h, shards) {
		t.Fatalf("genuine cut rejected")
	}
}

// TestBarrierConsistentRejectsSeqPastWbar refuses a barrier whose shard seq outruns
// the watermark, which the single writer's total order cannot produce.
func TestBarrierConsistentRejectsSeqPastWbar(t *testing.T) {
	h := BarrierHeader{Wbar: 1013, ShardCount: 2}
	shards := []BarrierShard{{TailSeq: 1001}, {TailSeq: 1014}}
	if BarrierConsistent(h, shards) {
		t.Fatalf("barrier with seq past Wbar accepted")
	}
}

// TestParseBarrierHeaderShort refuses a header buffer below the fixed size.
func TestParseBarrierHeaderShort(t *testing.T) {
	if _, err := ParseBarrierHeader(make([]byte, BarrierHeaderLen-1)); err != ErrShort {
		t.Fatalf("short err = %v, want ErrShort", err)
	}
}

// TestParseBarrierHeaderBadMagic refuses a payload that is not a barrier.
func TestParseBarrierHeaderBadMagic(t *testing.T) {
	b := make([]byte, BarrierHeaderLen)
	copy(b[0:4], "XXXX")
	if _, err := ParseBarrierHeader(b); err != ErrMagic {
		t.Fatalf("bad magic err = %v, want ErrMagic", err)
	}
}

// TestBarrierShardsRejectsOverrunCount catches a shard_count that claims more rows
// than the payload holds, so a torn barrier cannot over-read.
func TestBarrierShardsRejectsOverrunCount(t *testing.T) {
	shards := []BarrierShard{{TailSeg: 0x1000, TailSeq: 1}}
	payload := encodeBarrier(BarrierHeader{Wbar: 1, ShardCount: 1}, shards)
	le.PutUint64(payload[16:24], 50) // claim far more shards than are present
	bad, err := ParseBarrierHeader(payload)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if _, err := BarrierShards(payload, bad); err != ErrLength {
		t.Fatalf("overrun count err = %v, want ErrLength", err)
	}
}

// TestBarrierShardsEmpty decodes a barrier with no shard rows: a header and nothing
// after it.
func TestBarrierShardsEmpty(t *testing.T) {
	payload := AppendBarrierHeader(nil, BarrierHeader{Wbar: 5})
	h, err := ParseBarrierHeader(payload)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	shards, err := BarrierShards(payload, h)
	if err != nil {
		t.Fatalf("shards: %v", err)
	}
	if len(shards) != 0 {
		t.Fatalf("empty barrier decoded %d shards", len(shards))
	}
}
