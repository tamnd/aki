package list

import "testing"

// The inline point ops must not allocate (F7, spec 2064/f3/13 section 4.5): a
// push into spare capacity, a pop that advances the head cursor or truncates the
// tail, LLEN, and LINDEX all run over the packed blob in place. The pushes are
// measured as a push/pop pair on a warmed list so the blob's capacity is stable,
// which is the steady state a real workload holds; only genuine growth past the
// backing array escapes, and that is the arena's job, not the point op's.
// testing.AllocsPerRun rounds to whole allocations, so anything above zero is a
// real escape, not noise.

var (
	sinkBytes []byte
	sinkInt   int
)

// warmList builds a list of n four-byte elements and cycles a push/pop pair a
// few times so the backing array has settled to a reusable capacity.
func warmList(n int) *list {
	l := newList()
	for i := 0; i < n; i++ {
		l.pushBack([]byte("elem"))
	}
	for i := 0; i < 8; i++ {
		l.inlinePushBack([]byte("elem"))
		l.inlinePopBack()
	}
	return l
}

func TestZeroAllocLen(t *testing.T) {
	l := warmList(64)
	checkZero(t, "LLEN", func() { sinkInt = l.length() })
}

func TestZeroAllocIndex(t *testing.T) {
	l := warmList(64)
	checkZero(t, "LINDEX head", func() { sinkBytes = l.inlineAt(0) })
	checkZero(t, "LINDEX mid", func() { sinkBytes = l.inlineAt(32) })
	checkZero(t, "LINDEX tail", func() { sinkBytes = l.inlineAt(63) })
}

// RPUSH/RPOP over the tail: a push into spare capacity then a truncating pop.
func TestZeroAllocPushPopBack(t *testing.T) {
	l := warmList(64)
	v := []byte("elem")
	checkZero(t, "RPUSH+RPOP", func() {
		l.inlinePushBack(v)
		sinkBytes = l.inlinePopBack()
	})
}

// LPUSH/LPOP over the head: pop one front element first to open a dead prefix,
// then a prepend into that prefix and a cursor-advancing pop, which is the
// steady head-churn shape.
func TestZeroAllocPushPopFront(t *testing.T) {
	l := warmList(64)
	l.inlinePopFront() // open a dead prefix wide enough for a 4-byte frame
	v := []byte("elem")
	checkZero(t, "LPUSH+LPOP", func() {
		l.inlinePushFront(v)
		sinkBytes = l.inlinePopFront()
	})
}

func checkZero(t *testing.T, name string, fn func()) {
	t.Helper()
	if n := testing.AllocsPerRun(200, fn); n != 0 {
		t.Errorf("%s allocated %v times per run, want 0", name, n)
	}
}

// The native deque's edge ops must be allocation-free in steady state too (spec
// 2064/f3/13 section 2.5): a push into an established end chunk is a uvarint
// write plus a copy into the resident blob, and a pop advances a cursor and
// reclaims the just-pushed frame, so a push/pop pair neither seals a chunk nor
// pulls one off the freelist. The lists are warmed past the inline budget so
// they carry many chunks, the shape the gate measures.

// warmNativeTail builds a multi-chunk deque and settles its tail chunk with a
// push/pop cycle so the measured RPUSH+RPOP pair runs over a stable end chunk.
func warmNativeTail() *native {
	nt := &native{}
	for i := 0; i < 400; i++ {
		nt.pushBack([]byte("elem"))
	}
	for i := 0; i < 8; i++ {
		nt.pushBack([]byte("elem"))
		nt.popBack()
	}
	return nt
}

// warmNativeHead establishes a head chunk with a buffer of elements so the
// measured LPUSH+LPOP pair runs over a stable head chunk that never drains.
func warmNativeHead() *native {
	nt := &native{}
	for i := 0; i < 400; i++ {
		nt.pushBack([]byte("elem"))
	}
	for i := 0; i < 32; i++ {
		nt.pushFront([]byte("elem"))
	}
	for i := 0; i < 8; i++ {
		nt.pushFront([]byte("elem"))
		nt.popFront()
	}
	return nt
}

func TestZeroAllocNativePushPopBack(t *testing.T) {
	nt := warmNativeTail()
	v := []byte("elem")
	checkZero(t, "RPUSH+RPOP native", func() {
		nt.pushBack(v)
		sinkBytes = nt.popBack()
	})
}

func TestZeroAllocNativePushPopFront(t *testing.T) {
	nt := warmNativeHead()
	v := []byte("elem")
	checkZero(t, "LPUSH+LPOP native", func() {
		nt.pushFront(v)
		sinkBytes = nt.popFront()
	})
}
