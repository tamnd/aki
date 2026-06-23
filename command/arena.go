package command

// cmdArena is a bump allocator for per-command scratch (perf/05 §4.2). It
// hands out byte slices from a reusable slab and resets in O(1) at command
// end. After warm-up the slab is never reallocated; scratch becomes zero
// heap allocations per command.
//
// The slab is pointer-free (plain bytes), so the GC sees it as one allocation
// regardless of how many sub-slices have been handed out.
//
// Contract: slices returned by bytes() are valid only until reset(). A handler
// that needs to keep scratch past the command must copy it out.
type cmdArena struct {
	buf []byte
}

// bytes returns a slice of n scratch bytes. The caller must not retain it past
// the command (i.e. past the next reset call).
func (a *cmdArena) bytes(n int) []byte {
	off := len(a.buf)
	need := off + n
	if need > cap(a.buf) {
		a.grow(need)
		off = len(a.buf)
	}
	a.buf = a.buf[:off+n]
	return a.buf[off : off+n : off+n]
}

func (a *cmdArena) grow(need int) {
	newCap := cap(a.buf) * 2
	if newCap < need {
		newCap = need
	}
	if newCap < 256 {
		newCap = 256
	}
	nb := make([]byte, len(a.buf), newCap)
	copy(nb, a.buf)
	a.buf = nb
}

// lowerASCII writes the ASCII-lowercased form of src into dst and returns dst.
// Non-ASCII bytes are passed through unchanged. The caller is responsible for
// passing a dst with sufficient capacity (len(src) bytes).
func lowerASCII(dst, src []byte) []byte {
	dst = dst[:len(src)]
	for i, c := range src {
		if c >= 'A' && c <= 'Z' {
			dst[i] = c + ('a' - 'A')
		} else {
			dst[i] = c
		}
	}
	return dst
}

// reset returns all scratch to the arena in O(1). Called by putCtx before
// the Ctx goes back to the pool.
func (a *cmdArena) reset() { a.buf = a.buf[:0] }
