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
