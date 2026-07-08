package f1srv

import (
	"strconv"
	"testing"
)

// BenchmarkSAddNewP1 isolates the SADDNEW P1 server write path: one connection streams distinct,
// brand-new padded members into a single unpartitioned set, one member per SADD. That is the exact
// shape the SADDNEW P1 2x gate turns on, minus the socket, so ns/op is the per-op server write cost:
// the ExistsKind probe, the stripe lock, the member-row PutKind, the dense-vector append, and the
// header update. The header update is the quantity the count-bump lever changed: once the padded
// member forces the encoding to hashtable on member one, every later SADD keeps enc == encBefore and
// pays an in-place count CAS instead of a full setPutHeader record rewrite. Run with a padded value
// (256 bytes) so the encoding stabilizes at hashtable immediately, matching the padded aki-bench
// SADDNEW workload rather than the tiny-integer members raw redis-benchmark sends.
func BenchmarkSAddNewP1(b *testing.B) {
	srv := newPartServer(b, 1)
	defer srv.Close()
	c := bareConn(srv)
	pad := make([]byte, 256)
	for i := range pad {
		pad[i] = byte('a' + i%26)
	}
	key := []byte("bigset")
	add := [][]byte{[]byte("SADD"), key, nil}
	var member []byte
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		// A distinct member per op: strconv.AppendInt onto the fixed pad tail keeps every member new
		// (an insert, not a re-add) and 256+ bytes long (hashtable encoding), with no per-op alloc.
		member = strconv.AppendInt(pad[:256:256], int64(i), 10)
		add[2] = member
		c.out = c.out[:0]
		c.cmdSAdd(add)
	}
}
