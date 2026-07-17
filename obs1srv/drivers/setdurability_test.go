package drivers

import (
	"bufio"
	"context"
	"io"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/tamnd/aki/engine/obs1"
	"github.com/tamnd/aki/engine/obs1/shard"
)

// readBulkArray reads a RESP array of bulks whose element order the server
// does not fix (SPOP with a count draws uniformly), so the caller compares
// as a set.
func readBulkArray(t *testing.T, r *bufio.Reader) []string {
	t.Helper()
	line, err := r.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if line[0] != '*' {
		t.Fatalf("reply %q, want an array", line)
	}
	n, err := strconv.Atoi(strings.TrimSuffix(line[1:], "\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	out := make([]string, 0, n)
	for range n {
		hdr, err := r.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		if hdr[0] != '$' {
			t.Fatalf("element %q, want a bulk", hdr)
		}
		size, err := strconv.Atoi(strings.TrimSuffix(hdr[1:], "\r\n"))
		if err != nil {
			t.Fatal(err)
		}
		buf := make([]byte, size+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			t.Fatal(err)
		}
		out = append(out, string(buf[:size]))
	}
	return out
}

// TestSetDurabilityRoundTrip drives the set write surface over the socket
// and checks the flushed frames carry post-decision effects: SADD frames
// only the members that joined, SREM and SPOP only the members that left
// with a colldrop behind an emptying removal, a co-located SMOVE frames
// both sides while a move onto a destination that already held the member
// frames srem-only, a cross-shard SMOVE frames each side on its own group
// destination first, and the STORE forms frame the destination rebuild
// (keydel for a string shadow, collnew as reset-to-empty over a live set,
// the whole result as one sadd, colldrop for an empty result).
func TestSetDurabilityRoundTrip(t *testing.T) {
	wl, store, nc, r, _ := startLoggedServer(t, false)
	const node = uint64(0xE1)

	seqs := map[uint16]uint64{}
	emit := func(key string, n uint64) {
		_, g := ClusterMapKey([]byte(key))
		seqs[g] += n
	}

	// SADD frames only what joined: the second add carries d alone, the
	// duplicate-only add frames nothing.
	send(t, nc, "SADD", "s1", "a", "b", "c")
	expect(t, r, ":3\r\n")
	emit("s1", 2) // collnew, sadd
	send(t, nc, "SADD", "s1", "b", "d")
	expect(t, r, ":1\r\n")
	emit("s1", 1)
	send(t, nc, "SADD", "s1", "a", "b")
	expect(t, r, ":0\r\n")
	// SREM frames only what left; an absent member changes nothing.
	send(t, nc, "SREM", "s1", "d", "nosuch")
	expect(t, r, ":1\r\n")
	emit("s1", 1)
	send(t, nc, "SREM", "s1", "zz")
	expect(t, r, ":0\r\n")

	// SPOP single form: the drawn member frames as an srem, and emptying
	// the set drops it behind the removal.
	send(t, nc, "SADD", "sp1", "only")
	expect(t, r, ":1\r\n")
	emit("sp1", 2)
	send(t, nc, "SPOP", "sp1")
	expectBulk(t, r, []byte("only"))
	emit("sp1", 2) // srem, colldrop
	// SPOP count form draining the set: one srem with every drawn member,
	// then the colldrop.
	send(t, nc, "SADD", "sp2", "x", "y", "z")
	expect(t, r, ":3\r\n")
	emit("sp2", 2)
	send(t, nc, "SPOP", "sp2", "5")
	drained := readBulkArray(t, r)
	if len(drained) != 3 {
		t.Fatalf("SPOP drained %v, want all 3 members", drained)
	}
	emit("sp2", 2) // srem, colldrop
	// SPOP count form leaving members: srem alone, and the frame carries
	// the member the reply carried.
	send(t, nc, "SADD", "sp3", "p", "q", "r")
	expect(t, r, ":3\r\n")
	emit("sp3", 2)
	send(t, nc, "SPOP", "sp3", "1")
	partial := readBulkArray(t, r)
	if len(partial) != 1 {
		t.Fatalf("SPOP 1 replied %v", partial)
	}
	emit("sp3", 1)
	// SPOP misses frame nothing.
	send(t, nc, "SPOP", "nosuchs")
	expect(t, r, "$-1\r\n")
	send(t, nc, "SPOP", "nosuchs", "2")
	expect(t, r, "*0\r\n")

	// Co-located SMOVE (one hash tag, one group): a creating move, then an
	// emptying one, each one atomic run in the sim suite and per-key frame
	// sequences here.
	send(t, nc, "SADD", "{m}src", "a", "b")
	expect(t, r, ":2\r\n")
	emit("{m}src", 2)
	send(t, nc, "SMOVE", "{m}src", "{m}dst", "a")
	expect(t, r, ":1\r\n")
	emit("{m}src", 3) // collnew(dst), sadd(dst), srem(src) share the group
	send(t, nc, "SMOVE", "{m}src", "{m}dst", "b")
	expect(t, r, ":1\r\n")
	emit("{m}src", 3) // sadd(dst), srem(src), colldrop(src)
	// Moving onto a destination that already holds the member: the dst side
	// changed nothing, so the emission is srem-only plus the emptying drop.
	send(t, nc, "SADD", "{m}s2", "a")
	expect(t, r, ":1\r\n")
	emit("{m}s2", 2)
	send(t, nc, "SMOVE", "{m}s2", "{m}dst", "a")
	expect(t, r, ":1\r\n")
	emit("{m}s2", 2) // srem, colldrop
	// The same-key reply and the not-in-source reply mutate nothing and
	// frame nothing.
	send(t, nc, "SMOVE", "{m}dst", "{m}dst", "a")
	expect(t, r, ":1\r\n")
	send(t, nc, "SMOVE", "{m}dst", "{m}other", "qq")
	expect(t, r, ":0\r\n")
	send(t, nc, "SMOVE", "{m}missing", "{m}dst", "a")
	expect(t, r, ":0\r\n")

	// Cross-shard SMOVE: the placements are computed, never guessed. Each
	// side frames on its own group, destination first.
	shardOf := func(key string) int {
		return shard.GroupOfSlot(shard.HashSlot([]byte(key)), shard.DefaultSlotGroups) % 2
	}
	keyOn := func(sh int, prefix string) string {
		for i := 0; ; i++ {
			k := prefix + strconv.Itoa(i)
			if shardOf(k) == sh {
				return k
			}
		}
	}
	csrc := keyOn(0, "csrc")
	cdst := keyOn(1, "cdst")
	send(t, nc, "SADD", csrc, "m1", "m2")
	expect(t, r, ":2\r\n")
	emit(csrc, 2)
	send(t, nc, "SMOVE", csrc, cdst, "m1")
	expect(t, r, ":1\r\n")
	emit(cdst, 2) // collnew, sadd on the destination's group
	emit(csrc, 1) // srem on the source's group
	// The cross move onto an existing destination member: srem-only again,
	// and the source empties.
	send(t, nc, "SADD", cdst, "m2")
	expect(t, r, ":1\r\n")
	emit(cdst, 1)
	send(t, nc, "SMOVE", csrc, cdst, "m2")
	expect(t, r, ":1\r\n")
	emit(csrc, 2) // srem, colldrop

	// The STORE forms, co-located under one tag (the point-only route).
	// Integer members keep the rebuilt destination intset-banded, so the
	// framed member order is sorted and deterministic.
	send(t, nc, "SADD", "{w}a", "1", "2", "3")
	expect(t, r, ":3\r\n")
	emit("{w}a", 2)
	send(t, nc, "SADD", "{w}b", "2", "3", "4")
	expect(t, r, ":3\r\n")
	emit("{w}b", 2)
	send(t, nc, "SINTERSTORE", "{w}d", "{w}a", "{w}b")
	expect(t, r, ":2\r\n")
	emit("{w}d", 2) // collnew, sadd of the result
	// Replacing a live destination: collnew again, the reset-to-empty replay
	// rule, with the new result behind it.
	send(t, nc, "SUNIONSTORE", "{w}d", "{w}a", "{w}b")
	expect(t, r, ":4\r\n")
	emit("{w}d", 2)
	// An empty result deletes the live destination: colldrop alone.
	send(t, nc, "SINTERSTORE", "{w}d", "{w}a", "{w}nosuch")
	expect(t, r, ":0\r\n")
	emit("{w}d", 1)
	// A string destination: the keydel for the shadow leads the rebuild.
	send(t, nc, "SET", "{w}sd", "v")
	expect(t, r, "+OK\r\n")
	emit("{w}sd", 1)
	send(t, nc, "SINTERSTORE", "{w}sd", "{w}a", "{w}b")
	expect(t, r, ":2\r\n")
	emit("{w}sd", 3) // keydel, collnew, sadd
	// Both destination and result absent: no effect, no frames.
	send(t, nc, "SDIFFSTORE", "{w}nd", "{w}nosuch", "{w}nosuch2")
	expect(t, r, ":0\r\n")

	wl.Barrier()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for g, last := range seqs {
		if err := wl.Marks().Wait(ctx, g, last); err != nil {
			t.Fatalf("Wait group %d seq %d: %v", g, last, err)
		}
	}

	byKey := map[string][]obs1.Op{}
	total := 0
	for _, f := range walFrames(t, store, node) {
		op, err := obs1.DecodeOp(f)
		if err != nil {
			t.Fatalf("DecodeOp seq %d: %v", f.Seq, err)
		}
		byKey[string(f.Key)] = append(byKey[string(f.Key)], op)
		total++
	}
	if total != 48 {
		t.Fatalf("%d frames flushed, want 48: %v", total, byKey)
	}
	members := func(op obs1.Op) []string {
		var raw [][]byte
		switch sub := op.(obs1.CollDelta).Sub.(type) {
		case obs1.SAdd:
			raw = sub.Members
		case obs1.SRem:
			raw = sub.Members
		default:
			t.Fatalf("op %+v, want an sadd or srem", op)
		}
		out := make([]string, len(raw))
		for i, m := range raw {
			out[i] = string(m)
		}
		return out
	}
	same := func(got []string, want ...string) bool {
		if len(got) != len(want) {
			return false
		}
		for i := range got {
			if got[i] != want[i] {
				return false
			}
		}
		return true
	}
	asSet := func(ms []string) map[string]bool {
		out := map[string]bool{}
		for _, m := range ms {
			out[m] = true
		}
		return out
	}

	s1 := byKey["s1"]
	if len(s1) != 4 {
		t.Fatalf("s1 ops = %v", s1)
	}
	if cn := s1[0].(obs1.CollNew); cn.Type != obs1.CollSet || len(cn.Hints) != 0 {
		t.Fatalf("s1 frame 1 = %+v, want a hintless set collnew", cn)
	}
	if !same(members(s1[1]), "a", "b", "c") {
		t.Fatalf("s1 frame 2 = %+v", s1[1])
	}
	if !same(members(s1[2]), "d") {
		t.Fatalf("s1 frame 3 = %+v, want only the joined member", s1[2])
	}
	if !same(members(s1[3]), "d") {
		t.Fatalf("s1 frame 4 = %+v, want only the removed member", s1[3])
	}

	sp1 := byKey["sp1"]
	if len(sp1) != 4 || !same(members(sp1[2]), "only") {
		t.Fatalf("sp1 ops = %v", sp1)
	}
	if _, ok := sp1[3].(obs1.CollDrop); !ok {
		t.Fatalf("sp1 frame 4 = %+v, want the emptying colldrop", sp1[3])
	}

	sp2 := byKey["sp2"]
	if len(sp2) != 4 {
		t.Fatalf("sp2 ops = %v", sp2)
	}
	if got := asSet(members(sp2[2])); len(got) != 3 || !got["x"] || !got["y"] || !got["z"] {
		t.Fatalf("sp2 frame 3 = %+v, want every drained member", sp2[2])
	}
	if _, ok := sp2[3].(obs1.CollDrop); !ok {
		t.Fatalf("sp2 frame 4 = %+v", sp2[3])
	}

	sp3 := byKey["sp3"]
	if len(sp3) != 3 || !same(members(sp3[2]), partial[0]) {
		t.Fatalf("sp3 ops = %v, want the srem carrying the replied member %q", sp3, partial[0])
	}

	msrc := byKey["{m}src"]
	if len(msrc) != 5 {
		t.Fatalf("{m}src ops = %v", msrc)
	}
	if !same(members(msrc[2]), "a") || !same(members(msrc[3]), "b") {
		t.Fatalf("{m}src move frames = %+v %+v", msrc[2], msrc[3])
	}
	if _, ok := msrc[4].(obs1.CollDrop); !ok {
		t.Fatalf("{m}src frame 5 = %+v, want the emptying colldrop", msrc[4])
	}
	mdst := byKey["{m}dst"]
	if len(mdst) != 3 {
		t.Fatalf("{m}dst ops = %v, want collnew and the two arriving sadds alone", mdst)
	}
	if cn := mdst[0].(obs1.CollNew); cn.Type != obs1.CollSet {
		t.Fatalf("{m}dst frame 1 = %+v", cn)
	}
	if !same(members(mdst[1]), "a") || !same(members(mdst[2]), "b") {
		t.Fatalf("{m}dst move frames = %+v %+v", mdst[1], mdst[2])
	}
	ms2 := byKey["{m}s2"]
	if len(ms2) != 4 || !same(members(ms2[2]), "a") {
		t.Fatalf("{m}s2 ops = %v, want the srem-only move", ms2)
	}
	if _, ok := ms2[3].(obs1.CollDrop); !ok {
		t.Fatalf("{m}s2 frame 4 = %+v", ms2[3])
	}
	if _, ok := byKey["{m}other"]; ok {
		t.Fatal("a not-in-source SMOVE flushed a frame")
	}

	cs := byKey[csrc]
	if len(cs) != 5 {
		t.Fatalf("%s ops = %v", csrc, cs)
	}
	if !same(members(cs[2]), "m1") || !same(members(cs[3]), "m2") {
		t.Fatalf("%s move frames = %+v %+v", csrc, cs[2], cs[3])
	}
	if _, ok := cs[4].(obs1.CollDrop); !ok {
		t.Fatalf("%s frame 5 = %+v", csrc, cs[4])
	}
	cd := byKey[cdst]
	if len(cd) != 3 {
		t.Fatalf("%s ops = %v, want the created move side and the plain add alone", cdst, cd)
	}
	if cn := cd[0].(obs1.CollNew); cn.Type != obs1.CollSet {
		t.Fatalf("%s frame 1 = %+v", cdst, cn)
	}
	if !same(members(cd[1]), "m1") || !same(members(cd[2]), "m2") {
		t.Fatalf("%s frames = %+v %+v", cdst, cd[1], cd[2])
	}

	wd := byKey["{w}d"]
	if len(wd) != 5 {
		t.Fatalf("{w}d ops = %v", wd)
	}
	if cn := wd[0].(obs1.CollNew); cn.Type != obs1.CollSet {
		t.Fatalf("{w}d frame 1 = %+v", cn)
	}
	if !same(members(wd[1]), "2", "3") {
		t.Fatalf("{w}d frame 2 = %+v, want the intersection sorted intset-style", wd[1])
	}
	if cn := wd[2].(obs1.CollNew); cn.Type != obs1.CollSet {
		t.Fatalf("{w}d frame 3 = %+v, want the reset-to-empty collnew over the live set", wd[2])
	}
	if !same(members(wd[3]), "1", "2", "3", "4") {
		t.Fatalf("{w}d frame 4 = %+v, want the whole union result", wd[3])
	}
	if _, ok := wd[4].(obs1.CollDrop); !ok {
		t.Fatalf("{w}d frame 5 = %+v, want the empty-result colldrop", wd[4])
	}
	sd := byKey["{w}sd"]
	if len(sd) != 4 {
		t.Fatalf("{w}sd ops = %v", sd)
	}
	if ss := sd[0].(obs1.StrSet); string(ss.Value) != "v" {
		t.Fatalf("{w}sd frame 1 = %+v", ss)
	}
	if _, ok := sd[1].(obs1.KeyDel); !ok {
		t.Fatalf("{w}sd frame 2 = %+v, want the keydel for the string shadow", sd[1])
	}
	if cn := sd[2].(obs1.CollNew); cn.Type != obs1.CollSet {
		t.Fatalf("{w}sd frame 3 = %+v", cn)
	}
	if !same(members(sd[3]), "2", "3") {
		t.Fatalf("{w}sd frame 4 = %+v", sd[3])
	}
	if _, ok := byKey["{w}nd"]; ok {
		t.Fatal("a no-effect STORE flushed a frame")
	}
}
