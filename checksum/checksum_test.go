package checksum

import "testing"

func TestSumStable(t *testing.T) {
	// CRC-32C of "123456789" is the well-known Castagnoli check value.
	const want = 0xE3069283
	if got := Sum([]byte("123456789")); got != want {
		t.Errorf("Sum=%#x want %#x", got, want)
	}
}

func TestVerify(t *testing.T) {
	p := []byte("the quick brown fox")
	s := Sum(p)
	if !Verify(p, s) {
		t.Error("Verify rejected a correct checksum")
	}
	if Verify(p, s^1) {
		t.Error("Verify accepted a wrong checksum")
	}
}

func TestNewMatchesSum(t *testing.T) {
	p := []byte("streamed checksum input")
	h := New()
	h.Write(p[:7])
	h.Write(p[7:])
	if got := h.Sum32(); got != Sum(p) {
		t.Errorf("streamed %#x != Sum %#x", got, Sum(p))
	}
}

func TestEmpty(t *testing.T) {
	if got := Sum(nil); got != 0 {
		t.Errorf("Sum(nil)=%#x want 0", got)
	}
}
