package sqlo1b

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"testing"
)

// cascadePayload encodes a record chain: one RecString per value with
// a unique key, which is the shape a frame group holds.
func cascadePayload(t testing.TB, values [][]byte) []byte {
	t.Helper()
	var payload []byte
	for i, v := range values {
		rec := &Record{RType: RecString, Key: fmt.Appendf(nil, "key-%04d", i), Value: v}
		b, err := rec.Encode()
		if err != nil {
			t.Fatalf("encode record %d: %v", i, err)
		}
		payload = append(payload, b...)
	}
	return payload
}

// cascadeShapes are the lab corpus shapes the codecs were swept on,
// plus stem variety (expiry fields, segment subkeys, empty values).
func cascadeShapes(t testing.TB) map[string][]byte {
	t.Helper()
	lowCard := make([][]byte, 200)
	for i := range lowCard {
		lowCard[i] = fmt.Appendf(nil, "status-%d", i%4)
	}
	lowCard[17] = nil // an empty value dictionary-codes too
	clustered := make([][]byte, 200)
	for i := range clustered {
		clustered[i] = fmt.Appendf(nil, "shard-%d", i/50)
	}
	counters := make([][]byte, 150)
	for i := range counters {
		counters[i] = fmt.Appendf(nil, "%d", 1700000000000+i*7)
	}
	words := make([][]byte, 150)
	for i := range words {
		var w [8]byte
		binary.LittleEndian.PutUint64(w[:], uint64(1<<40+i*i))
		words[i] = w[:]
	}
	shapes := map[string][]byte{
		"lowcard":   cascadePayload(t, lowCard),
		"clustered": cascadePayload(t, clustered),
		"counters":  cascadePayload(t, counters),
		"words":     cascadePayload(t, words),
	}
	// A mixed-stem chain: expiry bits and a segment subkey between
	// plain strings, so stems of different lengths interleave.
	var mixed []byte
	for i := range 40 {
		rec := &Record{RType: RecString, Key: fmt.Appendf(nil, "mix-%02d", i), Value: []byte("v")}
		if i%3 == 0 {
			rec.RFlags = RFlagExpiry
			rec.ExpireMS = uint64(1700000000000 + i)
		}
		if i%5 == 0 {
			rec = &Record{
				RType: RecSeg, RFlags: RFlagRootgen, Rootgen: uint32(i),
				Key:   bytes.Repeat([]byte{byte(i)}, SubkeySize),
				Value: []byte("segment payload"),
			}
		}
		b, err := rec.Encode()
		if err != nil {
			t.Fatalf("encode mixed record %d: %v", i, err)
		}
		mixed = append(mixed, b...)
	}
	shapes["mixed"] = mixed
	return shapes
}

// cascadeVerify round-trips one payload through one scheme and hands
// back the compressed bytes.
func cascadeVerify(t testing.TB, scheme uint8, payload []byte) []byte {
	t.Helper()
	comp, err := cEncode(scheme, payload)
	if err != nil {
		t.Fatalf("scheme %d encode: %v", scheme, err)
	}
	got, err := cDecode(scheme, 0, comp, len(payload))
	if err != nil {
		t.Fatalf("scheme %d decode: %v", scheme, err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("scheme %d round trip diverged at %d bytes", scheme, len(payload))
	}
	for off := 0; off < len(got); {
		rec, err := DecodeRecord(got[off:])
		if err != nil {
			t.Fatalf("scheme %d decoded payload breaks at %d: %v", scheme, off, err)
		}
		off += rec.EncodedLen()
	}
	return comp
}

func TestCascadeRoundTrip(t *testing.T) {
	shapes := cascadeShapes(t)
	applicable := map[string][]uint8{
		"lowcard":   {SchemeDict, SchemeDictRLE},
		"clustered": {SchemeDict, SchemeDictRLE},
		"counters":  {SchemeDict, SchemeDictRLE, SchemeFor},
		"words":     {SchemeDict, SchemeDictRLE, SchemeFor},
		"mixed":     {SchemeDict, SchemeDictRLE},
	}
	for name, payload := range shapes {
		for _, scheme := range applicable[name] {
			cascadeVerify(t, scheme, payload)
		}
	}
}

func TestCascadeCompresses(t *testing.T) {
	// The lab verdicts in miniature: dict wins low-cardinality, rle
	// wins clustered repeats over plain dict, for+pack wins integer
	// shapes. The stems ride raw, so wins are on the value stream
	// only; these bounds just pin that each scheme pays on its shape.
	shapes := cascadeShapes(t)
	dict := cascadeVerify(t, SchemeDict, shapes["lowcard"])
	if len(dict) >= len(shapes["lowcard"]) {
		t.Errorf("dict on lowcard: %d bytes of %d raw", len(dict), len(shapes["lowcard"]))
	}
	rle := cascadeVerify(t, SchemeDictRLE, shapes["clustered"])
	plain := cascadeVerify(t, SchemeDict, shapes["clustered"])
	if len(rle) >= len(plain) {
		t.Errorf("dict+rle on clustered runs: %d bytes vs dict %d", len(rle), len(plain))
	}
	fp := cascadeVerify(t, SchemeFor, shapes["counters"])
	dictCounters := cascadeVerify(t, SchemeDict, shapes["counters"])
	if len(fp) >= len(dictCounters) {
		t.Errorf("for+pack on counters: %d bytes vs dict %d", len(fp), len(dictCounters))
	}
}

func TestCascadeInapplicable(t *testing.T) {
	strings := cascadePayload(t, [][]byte{[]byte("alpha"), []byte("beta")})
	if _, err := cEncode(SchemeFor, strings); !errors.Is(err, errCascadeInapplicable) {
		t.Errorf("for+pack on strings: %v", err)
	}
	// Non-canonical decimals are out too: leading zero, sign, overflow.
	for _, v := range []string{"007", "+1", "18446744073709551616"} {
		payload := cascadePayload(t, [][]byte{[]byte(v)})
		if _, err := cEncode(SchemeFor, payload); !errors.Is(err, errCascadeInapplicable) {
			t.Errorf("for+pack on %q: %v", v, err)
		}
	}
	if _, err := cEncode(SchemeDict, nil); !errors.Is(err, errCascadeInapplicable) {
		t.Errorf("dict on empty payload: %v", err)
	}
	if _, err := cEncode(SchemeZstd, cascadePayload(t, [][]byte{[]byte("x")})); err == nil {
		t.Error("zstd has no cascade encoder yet")
	}
	if _, err := cEncode(SchemeDict, []byte("not a record chain")); err == nil {
		t.Error("garbage payload must not encode")
	}
}

func TestCascadeFrameIntegration(t *testing.T) {
	// A hand-assembled frame image proves the cascade payload and the
	// u16 uslot table compose: the offsets index the uncompressed
	// bytes ParseCGroup reconstructs.
	values := make([][]byte, 64)
	for i := range values {
		values[i] = fmt.Appendf(nil, "tier-%d", i%3)
	}
	payload := cascadePayload(t, values)
	comp, err := cEncode(SchemeDictRLE, payload)
	if err != nil {
		t.Fatal(err)
	}
	img := make([]byte, GroupSize)
	img[0] = SchemeDictRLE
	binary.LittleEndian.PutUint16(img[2:], uint16(len(values)))
	binary.LittleEndian.PutUint32(img[4:], uint32(len(payload)))
	binary.LittleEndian.PutUint32(img[8:], uint32(len(comp)))
	copy(img[CFrameHeader:], comp)
	table := CFrameHeader + len(comp)
	off := 0
	for i := range values {
		binary.LittleEndian.PutUint16(img[table+2*i:], uint16(off))
		rec := &Record{RType: RecString, Key: fmt.Appendf(nil, "key-%04d", i), Value: values[i]}
		off += rec.EncodedLen()
	}
	view, err := ParseCGroup(img)
	if err != nil {
		t.Fatal(err)
	}
	if view.Scheme() != SchemeDictRLE || view.Records() != len(values) {
		t.Fatalf("frame parsed as scheme %d with %d records", view.Scheme(), view.Records())
	}
	for i, want := range values {
		raw, err := view.Record(uint16(i))
		if err != nil {
			t.Fatal(err)
		}
		rec, err := DecodeRecord(raw)
		if err != nil {
			t.Fatalf("record %d: %v", i, err)
		}
		if !bytes.Equal(rec.Value, want) {
			t.Fatalf("record %d value %q, want %q", i, rec.Value, want)
		}
	}
}

func TestCascadeDecodeRejections(t *testing.T) {
	shapes := cascadeShapes(t)
	valid := map[uint8][]byte{
		SchemeDict:    cascadeVerify(t, SchemeDict, shapes["lowcard"]),
		SchemeDictRLE: cascadeVerify(t, SchemeDictRLE, shapes["clustered"]),
		SchemeFor:     cascadeVerify(t, SchemeFor, shapes["counters"]),
	}
	ulens := map[uint8]int{
		SchemeDict:    len(shapes["lowcard"]),
		SchemeDictRLE: len(shapes["clustered"]),
		SchemeFor:     len(shapes["counters"]),
	}
	for scheme, comp := range valid {
		ulen := ulens[scheme]
		if _, err := cascadeDecode(scheme, comp[:len(comp)/2], ulen); err == nil {
			t.Errorf("scheme %d accepted a truncated section", scheme)
		}
		if _, err := cascadeDecode(scheme, comp[:0], ulen); err == nil {
			t.Errorf("scheme %d accepted an empty section", scheme)
		}
		if _, err := cascadeDecode(scheme, comp, ulen-1); err == nil {
			t.Errorf("scheme %d accepted a short ulen", scheme)
		}
		if _, err := cascadeDecode(scheme, append(bytes.Clone(comp), 0), ulen); err == nil {
			t.Errorf("scheme %d accepted trailing bytes", scheme)
		}
		if _, err := cascadeDecode(scheme, comp, cframeMaxUlen+1); err == nil {
			t.Errorf("scheme %d accepted ulen past the u16 bound", scheme)
		}
	}
	// The count claims more records than ulen can hold.
	big := binary.AppendUvarint(nil, 1<<40)
	if _, err := cascadeDecode(SchemeDict, big, 64); err == nil {
		t.Error("accepted a record count past ulen")
	}
	// A dictionary index past the dictionary.
	payload := cascadePayload(t, [][]byte{[]byte("a"), []byte("a")})
	comp, err := cEncode(SchemeDict, payload)
	if err != nil {
		t.Fatal(err)
	}
	bad := bytes.Clone(comp)
	bad[len(bad)-1] = 9 // last value's index, dictionary has 1 entry
	if _, err := cascadeDecode(SchemeDict, bad, len(payload)); err == nil {
		t.Error("accepted a dictionary index out of range")
	}
	// A decoded value whose length disagrees with its stem's vlen.
	bad = bytes.Clone(comp)
	di := bytes.LastIndexByte(bad[:len(bad)-3], 1) // dict entry length byte
	bad[di], bad[di+1] = 2, 'a'
	bad = append(bad[:di+3], bad[di+2:]...)
	if _, err := cascadeDecode(SchemeDict, bad, len(payload)); err == nil {
		t.Error("accepted a value length off its stem's vlen")
	}
	// A for+pack width past 64.
	fp := valid[SchemeFor]
	n, k := binary.Uvarint(fp)
	_ = n
	base := k // stems for counters start here
	_ = base
	bad = bytes.Clone(fp)
	// Walk to the mode byte: skip the count, then the stems.
	off := k
	for range int(n) {
		rlen := int(binary.LittleEndian.Uint32(bad[off:]))
		vlen := int(binary.LittleEndian.Uint32(bad[off+8:]))
		off += rlen - vlen
	}
	if bad[off] != 0 {
		t.Fatalf("expected mode 0 at %d, found %d", off, bad[off])
	}
	bad[off] = 7
	if _, err := cascadeDecode(SchemeFor, bad, ulens[SchemeFor]); err == nil {
		t.Error("accepted a for+pack mode past 1")
	}
	_, k2 := binary.Uvarint(bad[off+1:]) // first block's base
	bad[off] = 0
	bad[off+1+k2] = 65
	if _, err := cascadeDecode(SchemeFor, bad, ulens[SchemeFor]); err == nil {
		t.Error("accepted a for+pack width past 64")
	}
}

func FuzzCascadeDecode(f *testing.F) {
	shapes := cascadeShapes(f)
	for name, payload := range shapes {
		for _, scheme := range []uint8{SchemeDict, SchemeDictRLE, SchemeFor} {
			comp, err := cEncode(scheme, payload)
			if errors.Is(err, errCascadeInapplicable) {
				continue
			}
			if err != nil {
				f.Fatalf("%s scheme %d: %v", name, scheme, err)
			}
			f.Add(scheme, uint16(len(payload)), comp)
		}
	}
	f.Fuzz(func(t *testing.T, scheme uint8, ulen uint16, comp []byte) {
		out, err := cascadeDecode(1+scheme%3, comp, int(ulen))
		if err == nil && len(out) != int(ulen) {
			t.Fatalf("decode returned %d bytes for ulen %d", len(out), ulen)
		}
	})
}

func FuzzCascadeRoundTrip(f *testing.F) {
	f.Add([]byte("seed"), uint8(3), uint8(1))
	f.Add([]byte("12345678абв\x00\xff90"), uint8(8), uint8(2))
	f.Add([]byte("11122233344455566677"), uint8(2), uint8(3))
	f.Fuzz(func(t *testing.T, data []byte, chunk, scheme uint8) {
		if len(data) == 0 {
			return
		}
		// Slice the fuzz bytes into values; a zero chunk exercises
		// empty values, chunk 8 hits for+pack mode 1 shapes.
		var values [][]byte
		step := int(chunk % 24)
		for off := 0; off < len(data) && len(values) < 128; off += max(step, 1) {
			values = append(values, data[off:min(off+step, len(data))])
		}
		payload := cascadePayload(t, values)
		comp, err := cEncode(1+scheme%3, payload)
		if errors.Is(err, errCascadeInapplicable) {
			return
		}
		if err != nil {
			t.Fatal(err)
		}
		got, err := cDecode(1+scheme%3, 0, comp, len(payload))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, payload) {
			t.Fatal("round trip diverged")
		}
	})
}

func TestCascadeSelect(t *testing.T) {
	shapes := cascadeShapes(t)
	// Each lab shape lands on its verdict scheme; incompressible
	// values fall through the floor to raw.
	for name, want := range map[string][]uint8{
		"lowcard":   {SchemeDict, SchemeDictRLE},
		"clustered": {SchemeDictRLE},
		"counters":  {SchemeFor},
		"words":     {SchemeFor},
	} {
		scheme, comp := cSelect(shapes[name])
		ok := false
		for _, w := range want {
			ok = ok || scheme == w
		}
		if !ok {
			t.Errorf("%s selected scheme %d, want one of %v", name, scheme, want)
			continue
		}
		if 100*len(comp) > (100-cSelectFloor)*len(shapes[name]) {
			t.Errorf("%s winner of %d bytes is under the floor on %d raw", name, len(comp), len(shapes[name]))
		}
		got, err := cDecode(scheme, 0, comp, len(shapes[name]))
		if err != nil || !bytes.Equal(got, shapes[name]) {
			t.Errorf("%s winner does not round trip: %v", name, err)
		}
	}
	entropy := make([][]byte, 24)
	for i := range entropy {
		v := make([]byte, 96)
		x := uint64(i)*0x9e3779b97f4a7c15 + 1
		for j := range v {
			x ^= x << 13
			x ^= x >> 7
			x ^= x << 17
			v[j] = byte(x)
		}
		entropy[i] = v
	}
	// High-entropy values kill every lightweight scheme. The zstd
	// fall-through may still clear the floor on the stems (headers and
	// keys stay compressible), which the cascade lab accepted too; the
	// winner just has to round trip.
	ep := cascadePayload(t, entropy)
	if scheme, comp := cSelect(ep); scheme != SchemeRaw {
		if scheme != SchemeZstd {
			t.Errorf("high-entropy payload selected scheme %d, want raw or zstd", scheme)
		} else if got, err := cDecode(scheme, 0, comp, len(ep)); err != nil || !bytes.Equal(got, ep) {
			t.Errorf("high-entropy zstd winner does not round trip: %v", err)
		}
	}
	if scheme, _ := cSelect([]byte("not a record chain")); scheme != SchemeRaw {
		t.Error("garbage payload must select raw")
	}
}

func TestCGroupSealSelects(t *testing.T) {
	g := NewCGroupBuilder(GroupSize)
	var recs [][]byte
	for i := 0; g.Fits(1000); i++ {
		rec := &Record{RType: RecString, Key: fmt.Appendf(nil, "seal-%02d", i), Value: bytes.Repeat([]byte{'v'}, 950)}
		b, err := rec.Encode()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := g.Append(b); err != nil {
			t.Fatal(err)
		}
		recs = append(recs, b)
		// The open group's flush-through image stays raw and parseable.
		if v, err := ParseCGroup(g.Image()); err != nil || v.Scheme() != SchemeRaw {
			t.Fatalf("open image: scheme %d, err %v", v.Scheme(), err)
		}
	}
	img := g.Seal()
	if g.Scheme() != SchemeDict && g.Scheme() != SchemeDictRLE {
		t.Fatalf("sealed constant values as scheme %d", g.Scheme())
	}
	v, err := ParseCGroup(img)
	if err != nil {
		t.Fatal(err)
	}
	if v.Scheme() != g.Scheme() || v.Records() != len(recs) {
		t.Fatalf("sealed image parses as scheme %d with %d records", v.Scheme(), v.Records())
	}
	if clen := int(binary.LittleEndian.Uint32(img[8:])); clen >= g.used {
		t.Fatalf("sealed clen %d did not shrink %d payload bytes", clen, g.used)
	}
	for i, want := range recs {
		got, err := v.Record(uint16(i))
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("sealed record %d diverged: %v", i, err)
		}
	}
}

func TestCompactSelectionStats(t *testing.T) {
	// Store-level: compacting constant-valued records books dictionary
	// groups in the histogram and Stats, and the compressed extents
	// survive verify, checkpoint, reopen, and scrub.
	r := newStoreRig(t)
	if st := r.s.Stats(); st.SchemeGroups != nil {
		t.Fatal("scheme telemetry before any frame group closed")
	}
	first := r.fillFrameExtent(t)
	if r.extFlagsOf(t, first)&EFlagCompressed == 0 {
		t.Fatalf("frame extent %d misses the compressed flag", first)
	}
	sg := r.s.SchemeGroups()
	if sg[SchemeDict]+sg[SchemeDictRLE] == 0 {
		t.Fatalf("no dictionary groups selected: %v", sg)
	}
	st := r.s.Stats()
	if st.SchemeGroups == nil || st.SchemeGroups[SchemeDict]+st.SchemeGroups[SchemeDictRLE] == 0 {
		t.Fatalf("Stats does not carry the selection histogram: %v", st.SchemeGroups)
	}
	r.verify(t)
	// The sealed frame extent holds dictionary-coded groups; scrub
	// must decode them through the registry and stay clean.
	sc := &Scrubber{File: r.s.f, ExtentSize: r.s.sb.ExtentSize, Grid: r.s.grid}
	if rep := sc.Sweep(); !rep.Clean() || rep.Scanned == 0 {
		t.Fatalf("scrub over selected groups (scanned %d): %+v", rep.Scanned, rep.Findings)
	}
	if err := r.s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	r.verify(t)
	r.reopen(t)
	r.verify(t)
}
