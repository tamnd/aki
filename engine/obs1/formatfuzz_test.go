// The format-fuzz suite (doc 03, milestone O0b slice 10). Each format's
// own fuzz target keeps its hand-picked seeds; this file adds the
// systematic half: one canonical object per format pushed through every
// truncation length, every single-byte corruption under three masks, and
// tail extensions, all deterministic so the whole corpus runs on every CI
// test job with no fuzz mode needed. FuzzParseAny is the suite's fuzz
// entry: it dispatches on the parsed header's format id, so one fuzz
// session exercises every parser through the same door a reader would
// use.
package obs1

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
)

// corpusObject is one format's canonical encoding plus its check: check
// returns (false, nil) on clean rejection, (true, nil) on canonical
// acceptance, and a non-nil error when accepted bytes fail to re-encode
// to the input, which is the invariant every parser owes (#886).
type corpusObject struct {
	name  string
	valid []byte
	check func(b []byte) (accepted bool, err error)
}

func reencoded(b, again []byte) error {
	if !bytes.Equal(again, b) {
		return errors.New("accepted bytes re-encode differently")
	}
	return nil
}

func formatCorpus(t testing.TB) []corpusObject {
	t.Helper()
	must := func(b []byte, err error) []byte {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	root := AppendRoot(nil, 7, Root{DBID: [16]byte{1, 2, 3}, CreatedMS: 99, G: 8, D: 1, Settings: []byte("s")})
	wal := must(AppendWAL(nil, 7, sampleWAL()))
	seg, err := BuildSegment(sampleSegmentFooter(), []SegmentChunk{{Key: []byte("k"), Count: 1, Data: chunkFrame(16)}}, [][]byte{[]byte("k")}, 0)
	if err != nil {
		t.Fatal(err)
	}
	segb := must(AppendSegment(nil, 7, seg))
	man := must(AppendManifest(nil, 7, sampleManifest()))
	batch := must(AppendChainBatch(nil, 7, sampleBatch()))
	ckpt := must(AppendCheckpoint(nil, 7, sampleCheckpoint()))
	return []corpusObject{
		{"root", root, func(b []byte) (bool, error) {
			r, h, err := ParseRoot(b)
			if err != nil {
				return false, nil
			}
			return true, reencoded(b, AppendRoot(nil, h.Writer, r))
		}},
		{"wal", wal, func(b []byte) (bool, error) {
			secs, h, err := ParseWAL(b)
			if err != nil {
				return false, nil
			}
			again, err := AppendWAL(nil, h.Writer, secs)
			if err != nil {
				return true, fmt.Errorf("accepted wal fails re-encode: %w", err)
			}
			return true, reencoded(b, again)
		}},
		{"segment", segb, func(b []byte) (bool, error) {
			sg, h, err := ParseSegment(b)
			if err != nil {
				return false, nil
			}
			again, err := AppendSegment(nil, h.Writer, sg)
			if err != nil {
				return true, fmt.Errorf("accepted segment fails re-encode: %w", err)
			}
			return true, reencoded(b, again)
		}},
		{"manifest", man, func(b []byte) (bool, error) {
			m, h, err := ParseManifest(b)
			if err != nil {
				return false, nil
			}
			again, err := AppendManifest(nil, h.Writer, m)
			if err != nil {
				return true, fmt.Errorf("accepted manifest fails re-encode: %w", err)
			}
			return true, reencoded(b, again)
		}},
		{"chain", batch, func(b []byte) (bool, error) {
			ba, h, err := ParseChainBatch(b)
			if err != nil {
				return false, nil
			}
			again, err := AppendChainBatch(nil, h.Writer, ba)
			if err != nil {
				return true, fmt.Errorf("accepted batch fails re-encode: %w", err)
			}
			return true, reencoded(b, again)
		}},
		{"checkpoint", ckpt, func(b []byte) (bool, error) {
			c, h, err := ParseCheckpoint(b)
			if err != nil {
				return false, nil
			}
			again, err := AppendCheckpoint(nil, h.Writer, c)
			if err != nil {
				return true, fmt.Errorf("accepted checkpoint fails re-encode: %w", err)
			}
			return true, reencoded(b, again)
		}},
	}
}

// corruptionMasks are the three single-byte mutations swept across every
// offset: low bit, high bit, and forced zero.
var corruptionMasks = []struct {
	name string
	mut  func(byte) byte
}{
	{"xor01", func(b byte) byte { return b ^ 0x01 }},
	{"xor80", func(b byte) byte { return b ^ 0x80 }},
	{"zero", func(byte) byte { return 0 }},
}

// TestTruncationCorpus: every strict prefix of every format must be
// rejected. No parser may accept a shorter object, canonical form fixes
// the length.
func TestTruncationCorpus(t *testing.T) {
	for _, obj := range formatCorpus(t) {
		t.Run(obj.name, func(t *testing.T) {
			if ok, err := obj.check(obj.valid); !ok || err != nil {
				t.Fatalf("canonical object rejected or non-canonical: ok=%v err=%v", ok, err)
			}
			for n := range len(obj.valid) {
				if ok, _ := obj.check(obj.valid[:n]); ok {
					t.Fatalf("truncation to %d of %d bytes accepted", n, len(obj.valid))
				}
			}
		})
	}
}

// TestCorruptHeaderCorpus pins the header's coverage map from doc 03
// section 2: bytes 0 to 19 (magic, format, fversion) are covered by the
// hcrc at 20 to 23, so any corruption in 0 to 23 must be rejected; bytes
// 24 to 31 are the writer field, uncovered by spec, so a
// corruption there must be accepted and re-encode canonically with the
// mutated writer. This asserts the deliberate hole stays exactly the
// hole the spec dug, no wider and no narrower.
func TestCorruptHeaderCorpus(t *testing.T) {
	for _, obj := range formatCorpus(t) {
		t.Run(obj.name, func(t *testing.T) {
			for off := range HeaderSize {
				for _, m := range corruptionMasks {
					b := bytes.Clone(obj.valid)
					was := b[off]
					b[off] = m.mut(was)
					if b[off] == was {
						continue
					}
					accepted, err := obj.check(b)
					if err != nil {
						t.Fatalf("offset %d %s: %v", off, m.name, err)
					}
					writerField := off >= 24 && off < 32
					if writerField && !accepted {
						t.Fatalf("offset %d %s: writer byte is uncovered by spec, must be accepted", off, m.name)
					}
					if !writerField && accepted {
						t.Fatalf("offset %d %s: covered header byte corrupted yet accepted", off, m.name)
					}
				}
			}
		})
	}
}

// TestCorruptBodyCorpus sweeps the three masks over every body byte and
// holds the one universal rule: acceptance implies canonical re-encode.
// A body corruption slipping past a crc would either be rejected here or
// caught re-encoding differently; the count of accepted mutants is
// reported so a coverage collapse is visible in the log.
func TestCorruptBodyCorpus(t *testing.T) {
	for _, obj := range formatCorpus(t) {
		t.Run(obj.name, func(t *testing.T) {
			accepted := 0
			total := 0
			for off := HeaderSize; off < len(obj.valid); off++ {
				for _, m := range corruptionMasks {
					b := bytes.Clone(obj.valid)
					was := b[off]
					b[off] = m.mut(was)
					if b[off] == was {
						continue
					}
					total++
					ok, err := obj.check(b)
					if err != nil {
						t.Fatalf("offset %d %s: %v", off, m.name, err)
					}
					if ok {
						accepted++
					}
				}
			}
			if accepted > 0 {
				t.Fatalf("%d of %d body corruptions accepted, every body byte should sit under a crc", accepted, total)
			}
		})
	}
}

// TestExtensionCorpus: canonical form fixes the length from the other
// side too, so trailing bytes must be rejected.
func TestExtensionCorpus(t *testing.T) {
	tails := [][]byte{{0x00}, {0xFF}, bytes.Repeat([]byte{0xEE}, TailSize)}
	for _, obj := range formatCorpus(t) {
		t.Run(obj.name, func(t *testing.T) {
			for _, tail := range tails {
				b := append(bytes.Clone(obj.valid), tail...)
				if ok, _ := obj.check(b); ok {
					t.Fatalf("object with %d trailing bytes accepted", len(tail))
				}
			}
		})
	}
}

// TestCrossFormatRejection feeds every format's valid bytes to every
// other format's parser; the header's format id must bounce all of them.
func TestCrossFormatRejection(t *testing.T) {
	objs := formatCorpus(t)
	for _, obj := range objs {
		for _, other := range objs {
			if obj.name == other.name {
				continue
			}
			if ok, _ := other.check(obj.valid); ok {
				t.Fatalf("%s parser accepted a %s object", other.name, obj.name)
			}
		}
	}
}

// parseAny is the reader's door: parse the header, dispatch on the
// format id. It returns nil on clean rejection and an error only when a
// parser accepted bytes that break the canonical re-encode invariant.
// FuzzParseAny drives every doc 03 parser through it.
func parseAny(b []byte) error {
	h, err := ParseHeader(b)
	if err != nil {
		return nil
	}
	var again []byte
	switch h.Format {
	case FormatRoot:
		r, h2, err := ParseRoot(b)
		if err != nil {
			return nil
		}
		again = AppendRoot(nil, h2.Writer, r)
	case FormatWAL:
		secs, h2, err := ParseWAL(b)
		if err != nil {
			return nil
		}
		again, err = AppendWAL(nil, h2.Writer, secs)
		if err != nil {
			return fmt.Errorf("accepted wal fails re-encode: %w", err)
		}
	case FormatSegment:
		sg, h2, err := ParseSegment(b)
		if err != nil {
			return nil
		}
		again, err = AppendSegment(nil, h2.Writer, sg)
		if err != nil {
			return fmt.Errorf("accepted segment fails re-encode: %w", err)
		}
	case FormatManifest:
		m, h2, err := ParseManifest(b)
		if err != nil {
			return nil
		}
		again, err = AppendManifest(nil, h2.Writer, m)
		if err != nil {
			return fmt.Errorf("accepted manifest fails re-encode: %w", err)
		}
	case FormatChain:
		ba, h2, err := ParseChainBatch(b)
		if err != nil {
			return nil
		}
		again, err = AppendChainBatch(nil, h2.Writer, ba)
		if err != nil {
			return fmt.Errorf("accepted batch fails re-encode: %w", err)
		}
	case FormatCheckpoint:
		c, h2, err := ParseCheckpoint(b)
		if err != nil {
			return nil
		}
		again, err = AppendCheckpoint(nil, h2.Writer, c)
		if err != nil {
			return fmt.Errorf("accepted checkpoint fails re-encode: %w", err)
		}
	default:
		return nil
	}
	if !bytes.Equal(again, b) {
		return errors.New("accepted bytes re-encode differently")
	}
	return nil
}

func FuzzParseAny(f *testing.F) {
	for _, obj := range formatCorpus(f) {
		f.Add(obj.valid)
		f.Add(obj.valid[:HeaderSize])
		f.Add(obj.valid[:len(obj.valid)-1])
		for _, off := range []int{0, 15, 16, 20, HeaderSize, len(obj.valid) - 5} {
			b := bytes.Clone(obj.valid)
			b[off] ^= 0x80
			f.Add(b)
		}
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		if err := parseAny(b); err != nil {
			t.Fatal(err)
		}
	})
}
