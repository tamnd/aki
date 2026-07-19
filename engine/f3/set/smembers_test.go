package set

import (
	"sort"
	"testing"

	"github.com/tamnd/aki/engine/f3/store"
)

// drainSource pulls a StreamRaw source to its declared total through a small
// fixed buffer, the way the shard ring does, and returns the framed reply
// bytes. The buffer is deliberately tiny so an element wider than it exercises
// the resumable encoder's straddle path.
func drainSource(src *membersStream, total int64, bufLen int) []byte {
	dst := make([]byte, bufLen)
	var out []byte
	for int64(len(out)) < total {
		n, err := src.Next(dst)
		if err != nil {
			panic(err)
		}
		if n == 0 {
			break
		}
		out = append(out, dst[:n]...)
	}
	return out
}

// parseArray reads a RESP multi-bulk array reply into its member strings. It is
// the minimal reader the streaming tests need, no status or nesting.
func parseArray(t *testing.T, b []byte) []string {
	t.Helper()
	if len(b) == 0 || b[0] != '*' {
		t.Fatalf("reply does not start with an array header: %q", b[:min(len(b), 16)])
	}
	i := 1
	readLine := func() []byte {
		start := i
		for i < len(b) && b[i] != '\r' {
			i++
		}
		line := b[start:i]
		i += 2 // skip crlf
		return line
	}
	n := atoiTest(t, readLine())
	out := make([]string, 0, n)
	for k := 0; k < n; k++ {
		if b[i] != '$' {
			t.Fatalf("element %d is not a bulk: %q", k, b[i:min(len(b), i+8)])
		}
		i++
		l := atoiTest(t, readLine())
		out = append(out, string(b[i:i+l]))
		i += l + 2
	}
	return out
}

func atoiTest(t *testing.T, b []byte) int {
	t.Helper()
	n := 0
	for _, c := range b {
		if c < '0' || c > '9' {
			t.Fatalf("not a number: %q", b)
		}
		n = n*10 + int(c-'0')
	}
	return n
}

// TestMembersStreamMatchesSet drains the streamed reply and checks it frames
// exactly the set: a bare array header, every member as a bulk, no leading
// $total bulk wrapper and no trailing bare crlf. The tiny drain buffer forces
// the encoder to straddle chunk boundaries mid-element.
func TestMembersStreamMatchesSet(t *testing.T) {
	all := members16(5000)
	s := buildHT(all)
	if s.enc != encHashtable {
		t.Fatalf("enc = %s, want hashtable", s.enc)
	}
	total := s.ht.membersTotal()
	if total <= store.ChunkSize {
		t.Fatalf("total %d not over the stream cutover; pick a bigger set", total)
	}
	src := s.ht.pinMembersStream(false)
	out := drainSource(src, total, 37)
	src.Release()

	if int64(len(out)) != total {
		t.Fatalf("drained %d bytes, want the declared total %d", len(out), total)
	}
	got := parseArray(t, out)
	if len(got) != len(all) {
		t.Fatalf("streamed %d members, want %d", len(got), len(all))
	}
	want := make([]string, len(all))
	for i, k := range all {
		want[i] = string(k)
	}
	sort.Strings(got)
	sort.Strings(want)
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("member %d = %x, want %x", i, got[i], want[i])
		}
	}
}

// TestMembersStreamPinUnderChurn is the streaming correctness proof: a stream
// opened at command time must reply the set as of that moment even though the
// owner keeps mutating the table before the pump drains it (the pipelined
// same-key hazard). The pin freezes record reuse and slab compaction, so the
// snapshot's member bytes stay valid; adds after the snapshot are absent and a
// removed member is still returned, both correct for SMEMBERS-at-command-time.
func TestMembersStreamPinUnderChurn(t *testing.T) {
	all := members16(2000)
	s := buildHT(all)
	total := s.ht.membersTotal()
	src := s.ht.pinMembersStream(false)
	if s.ht.streams != 1 {
		t.Fatalf("open stream did not pin the table: streams = %d", s.ht.streams)
	}

	// Churn hard while the stream is open: remove the first half (swap-remove
	// leaves free ordinals and dead slab bytes) and add a fresh half. Under the
	// pin, neither the freed ordinals nor the dead bytes may be reused.
	for _, k := range all[:1000] {
		s.rem(k)
	}
	more := members16(3000)[2000:]
	for _, k := range more {
		s.add(k)
	}

	out := drainSource(src, total, 512)
	src.Release()
	if s.ht.streams != 0 {
		t.Fatalf("Release did not unpin: streams = %d", s.ht.streams)
	}

	got := parseArray(t, out)
	if len(got) != len(all) {
		t.Fatalf("stream returned %d members, want the %d snapshotted at command time", len(got), len(all))
	}
	want := map[string]bool{}
	for _, k := range all {
		want[string(k)] = true
	}
	for _, m := range got {
		if !want[m] {
			t.Fatalf("stream returned %x, not in the command-time snapshot (pin let bytes move)", m)
		}
		delete(want, m)
	}
	if len(want) != 0 {
		t.Fatalf("stream dropped %d snapshotted members", len(want))
	}
}

// TestMembersStreamBoundedBuffer proves the encoder holds one element at a
// time, not the whole reply: with large members the source's working buffer
// stays around one member frame regardless of how many members the set has.
// That is the bounded-memory claim SMEMBERS rides on, doc 08 section 3.5.
func TestMembersStreamBoundedBuffer(t *testing.T) {
	s := newSet([]byte("seed-not-int"))
	member := make([]byte, 1000)
	for i := range member {
		member[i] = byte('a' + i%26)
	}
	for i := 0; i < 300; i++ { // 300 * ~1007 bytes is well over a chunk
		m := append([]byte(itoa(int64(i))+":"), member...)
		s.add(m)
	}
	total := s.ht.membersTotal()
	if total <= store.ChunkSize {
		t.Fatalf("total %d not over the cutover", total)
	}
	src := s.ht.pinMembersStream(false)
	drainSource(src, total, 64)
	src.Release()

	// One member frame is ~1010 bytes; the buffer must never have held the
	// whole reply. A few KB of slack covers the frame plus growth rounding.
	if cap(src.buf) > 4096 {
		t.Fatalf("encoder buffer grew to %d bytes, want it bounded to one element frame", cap(src.buf))
	}
}
