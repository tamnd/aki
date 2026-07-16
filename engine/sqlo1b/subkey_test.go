package sqlo1b

import "testing"

// The subkey codec tests moved to engine/sqlo1 with the codec. This
// one stays: it closes the loop with the record envelope, which lives
// here. A seg record built from an encoded subkey roundtrips and the
// decoded key parses back to the same subkey.
func TestSubkeyThroughEnvelope(t *testing.T) {
	rooth, err := MintRooth(7, 42)
	if err != nil {
		t.Fatal(err)
	}
	s, err := NewSubkey(rooth, SubkindSeg, 9)
	if err != nil {
		t.Fatal(err)
	}
	enc, err := (&Record{RType: RecSeg, RFlags: RFlagRootgen, Key: s.Encode(), Value: []byte("elems"), Rootgen: 2}).Encode()
	if err != nil {
		t.Fatal(err)
	}
	rec, err := DecodeRecord(enc)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeSubkey(rec.Key)
	if err != nil {
		t.Fatal(err)
	}
	if got != s {
		t.Fatalf("subkey through envelope %+v, want %+v", got, s)
	}
}
