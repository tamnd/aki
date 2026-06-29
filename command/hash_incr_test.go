package command

import (
	"fmt"
	"testing"

	"github.com/tamnd/aki/networking"
)

// TestHIncrByLargeHashIsPointWrite guards HINCRBY and HINCRBYFLOAT against the
// materialize trap. Both ran through getHash, which for a btree-backed (hashtable
// coll-form) hash decodes a metadata-only body and, on the way, would have to
// clone every field of the hash to apply a single-field increment. That is O(n)
// in time and allocation for an O(1) point write, so a multi-million-field hash
// drags its whole contents through the heap on each increment and a tight memory
// cap OOM-kills the server. Worse than slow, the old path also rewrote the whole
// hash as a blob, demoting the encoding. The fix does a point read-modify-write
// on the one field row through CollUpdateRouted.
//
// The witness is allocation count: a point write touches a fixed handful of
// objects no matter how big the hash is, where the old whole-hash path allocated
// on the order of one clone per field. We build a hash far past the hashtable
// threshold with field names long enough that a whole-hash clone would move
// about a megabyte, then assert a HINCRBY plus a HINCRBYFLOAT allocate a small
// constant well under the field count.
func TestHIncrByLargeHashIsPointWrite(t *testing.T) {
	d := newFuzzDispatcher(t)
	conn := networking.NewOfflineConn()

	const n = 4000
	pad := make([]byte, 240)
	for i := range pad {
		pad[i] = 'x'
	}
	field := func(i int) []byte {
		// Entropy at the front, padding after, so a whole-hash clone would move
		// roughly a megabyte per call.
		return []byte(fmt.Sprintf("%08d", i) + string(pad))
	}
	for i := range n {
		conn.ResetOut()
		d.Handle(conn, [][]byte{[]byte("HSET"), []byte("h"), field(i), []byte("0")})
	}
	conn.ResetOut()
	d.Handle(conn, [][]byte{[]byte("OBJECT"), []byte("ENCODING"), []byte("h")})
	if got := string(conn.OutBytes()); got != "$9\r\nhashtable\r\n" {
		t.Fatalf("hash not in coll form: OBJECT ENCODING = %q", got)
	}

	hot := field(1234)
	allocs := testing.AllocsPerRun(50, func() {
		conn.ResetOut()
		d.Handle(conn, [][]byte{[]byte("HINCRBY"), []byte("h"), hot, []byte("1")})
		conn.ResetOut()
		d.Handle(conn, [][]byte{[]byte("HINCRBYFLOAT"), []byte("h"), hot, []byte("1.5")})
	})
	// One HINCRBY plus one HINCRBYFLOAT per run. A whole-hash clone would allocate
	// on the order of n field copies; a point write is a small constant. Bound it
	// well below the field count so the O(n) path can never sneak back in.
	if allocs > 200 {
		t.Fatalf("HINCRBY/HINCRBYFLOAT on a %d-field hash allocated %.0f objects per run; "+
			"the point-write path should be a small constant, not O(n)", n, allocs)
	}

	// Correctness on the point-write path, on a field untouched by the alloc loop.
	conn.ResetOut()
	d.Handle(conn, [][]byte{[]byte("HINCRBY"), []byte("h"), field(2000), []byte("5")})
	if got := string(conn.OutBytes()); got != ":5\r\n" {
		t.Fatalf("HINCRBY existing field = %q want :5", got)
	}
	conn.ResetOut()
	d.Handle(conn, [][]byte{[]byte("HINCRBYFLOAT"), []byte("h"), field(2000), []byte("2.5")})
	if got := string(conn.OutBytes()); got != "$3\r\n7.5\r\n" {
		t.Fatalf("HINCRBYFLOAT existing field = %q want 7.5", got)
	}

	// The encoding must not have demoted off the coll form under all these writes.
	conn.ResetOut()
	d.Handle(conn, [][]byte{[]byte("OBJECT"), []byte("ENCODING"), []byte("h")})
	if got := string(conn.OutBytes()); got != "$9\r\nhashtable\r\n" {
		t.Fatalf("hash demoted off coll form after increments: OBJECT ENCODING = %q", got)
	}

	// A brand-new field through HINCRBY must grow the count: the sub-tree path
	// maintains SetCount, so HLEN reflects the added row.
	conn.ResetOut()
	d.Handle(conn, [][]byte{[]byte("HLEN"), []byte("h")})
	if got := string(conn.OutBytes()); got != fmt.Sprintf(":%d\r\n", n) {
		t.Fatalf("HLEN before new field = %q want :%d", got, n)
	}
	conn.ResetOut()
	d.Handle(conn, [][]byte{[]byte("HINCRBY"), []byte("h"), []byte("brand-new-field"), []byte("7")})
	if got := string(conn.OutBytes()); got != ":7\r\n" {
		t.Fatalf("HINCRBY new field = %q want :7", got)
	}
	conn.ResetOut()
	d.Handle(conn, [][]byte{[]byte("HLEN"), []byte("h")})
	if got := string(conn.OutBytes()); got != fmt.Sprintf(":%d\r\n", n+1) {
		t.Fatalf("HLEN after new field = %q want :%d", got, n+1)
	}
}
