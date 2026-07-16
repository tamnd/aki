package derived

import "testing"

// TestSelfTestPasses is the estimator-and-encoding gate run in-process: the
// self check must report no failing invariant on the real port.
func TestSelfTestPasses(t *testing.T) {
	if msg := hllSelfTest(); msg != "" {
		t.Fatalf("self test failed: %s", msg)
	}
}

// TestGetRegUnpackMatchesDenseGet pins that the bulk unpack used by PFDEBUG
// GETREG returns each register the packed accessor returns, so the register
// array on the wire is exactly the sketch's contents.
func TestGetRegUnpackMatchesDenseGet(t *testing.T) {
	blob := addAllRange(t, createSparse(), 0, 40000) // dense by now
	if blob[4] != hllDense {
		t.Fatalf("40000 elements not dense (enc=%d)", blob[4])
	}
	regs := make([]byte, hllRegisters)
	unpackDenseInto(blob[hllHdrSize:], regs)
	for i := 0; i < hllRegisters; i++ {
		if got := denseGet(blob[hllHdrSize:], i); got != regs[i] {
			t.Fatalf("register %d: unpack %d, denseGet %d", i, regs[i], got)
		}
	}
}

// TestDecodeFreshIsOneXZero checks the DECODE token stream for a fresh sketch is
// the single XZERO covering all 16384 registers, matching Redis.
func TestDecodeFreshIsOneXZero(t *testing.T) {
	blob := createSparse()
	opcodes := blob[hllHdrSize:]
	// A fresh sketch is one XZERO(16384): op0=0x7f, op1=0xff.
	if len(opcodes) != 2 || opcodes[0] != 0x7f || opcodes[1] != 0xff {
		t.Fatalf("fresh sketch opcodes = %x, want 7fff", opcodes)
	}
	if got := sparseXZeroLen(opcodes[0], opcodes[1]); got != hllRegisters {
		t.Fatalf("XZERO len = %d, want %d", got, hllRegisters)
	}
}

// TestSparseValRoundtripForDecode confirms the VAL opcode fields DECODE reports
// (value and run length) round-trip through the codec, so the "v:val,len" token
// is faithful.
func TestSparseValRoundtripForDecode(t *testing.T) {
	for val := 1; val <= hllSparseValMaxValue; val++ {
		for runLen := 1; runLen <= hllSparseValMaxLen; runLen++ {
			op := sparseValByte(val, runLen)
			if got := sparseValValue(op); got != val {
				t.Fatalf("VAL value roundtrip: got %d want %d", got, val)
			}
			if got := sparseValLen(op); got != runLen {
				t.Fatalf("VAL len roundtrip: got %d want %d", got, runLen)
			}
		}
	}
}
