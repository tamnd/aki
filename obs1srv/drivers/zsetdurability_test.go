package drivers

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/tamnd/aki/engine/obs1"
	"github.com/tamnd/aki/engine/obs1/shard"
)

// TestZSetDurabilityRoundTrip drives the sorted-set and geo write surface
// over the socket and checks the flushed frames carry post-decision
// effects: ZADD frames only the pairs that added or rescored at the score
// each member now holds, the pop family and the ZREMRANGEBY* verbs frame
// as zrems with a colldrop behind an emptying removal, the STORE forms
// frame the destination rebuild, and GEOADD frames as the geohash-scored
// ZADD it is, with both geo store routes rebuilding their destination.
func TestZSetDurabilityRoundTrip(t *testing.T) {
	wl, store, nc, r, _ := startLoggedServer(t, false)
	const node = uint64(0xE1)

	seqs := map[uint16]uint64{}
	emit := func(key string, n uint64) {
		_, g := clusterMapKey([]byte(key))
		seqs[g] += n
	}

	// ZADD frames only what applied: the NX re-add carries c alone, the
	// rescore carries the new score, a same-score write and every flag
	// miss frame nothing.
	send(t, nc, "ZADD", "z1", "1", "a", "2", "b")
	expect(t, r, ":2\r\n")
	emit("z1", 2) // collnew, zadd
	send(t, nc, "ZADD", "z1", "NX", "1", "a", "3", "c")
	expect(t, r, ":1\r\n")
	emit("z1", 1)
	send(t, nc, "ZADD", "z1", "4", "a")
	expect(t, r, ":0\r\n")
	emit("z1", 1) // the rescore frames at the new score
	send(t, nc, "ZADD", "z1", "4", "a")
	expect(t, r, ":0\r\n")
	send(t, nc, "ZADD", "z1", "XX", "9", "nosuch")
	expect(t, r, ":0\r\n")
	send(t, nc, "ZADD", "z1", "GT", "1", "a")
	expect(t, r, ":0\r\n")
	// The INCR form frames its one pair; a suppressed INCR frames nothing.
	send(t, nc, "ZADD", "z1", "INCR", "2", "b")
	expectBulk(t, r, []byte("4"))
	emit("z1", 1)
	send(t, nc, "ZADD", "z1", "NX", "INCR", "5", "b")
	expect(t, r, "$-1\r\n")
	// ZINCRBY by zero neither adds nor rescores, so it frames nothing.
	send(t, nc, "ZINCRBY", "z1", "0", "c")
	expectBulk(t, r, []byte("3"))
	send(t, nc, "ZINCRBY", "z1", "1.5", "c")
	expectBulk(t, r, []byte("4.5"))
	emit("z1", 1)
	send(t, nc, "ZINCRBY", "zi", "2", "m")
	expectBulk(t, r, []byte("2"))
	emit("zi", 2) // collnew, zadd
	// ZREM frames only what left; an absent member changes nothing.
	send(t, nc, "ZREM", "z1", "a", "nosuch")
	expect(t, r, ":1\r\n")
	emit("z1", 1)
	send(t, nc, "ZREM", "z1", "zz")
	expect(t, r, ":0\r\n")

	// The pop family frames as the zrems it is, copied draws with the
	// emptying colldrop behind the last.
	send(t, nc, "ZADD", "zp1", "1", "only")
	expect(t, r, ":1\r\n")
	emit("zp1", 2)
	send(t, nc, "ZPOPMIN", "zp1")
	expect(t, r, "*2\r\n$4\r\nonly\r\n$1\r\n1\r\n")
	emit("zp1", 2) // zrem, colldrop
	send(t, nc, "ZADD", "zp2", "1", "x", "2", "y", "3", "zz")
	expect(t, r, ":3\r\n")
	emit("zp2", 2)
	send(t, nc, "ZPOPMAX", "zp2", "2")
	expect(t, r, "*4\r\n$2\r\nzz\r\n$1\r\n3\r\n$1\r\ny\r\n$1\r\n2\r\n")
	emit("zp2", 1)
	send(t, nc, "ZPOPMIN", "zp2")
	expect(t, r, "*2\r\n$1\r\nx\r\n$1\r\n1\r\n")
	emit("zp2", 2) // zrem, colldrop
	send(t, nc, "ZPOPMIN", "nosuchz")
	expect(t, r, "*0\r\n")
	// ZMPOP pops the first non-empty key (co-located under one tag) and a
	// full drain drops it; an all-empty key list frames nothing.
	send(t, nc, "ZADD", "{p}zm", "1", "m1", "2", "m2")
	expect(t, r, ":2\r\n")
	emit("{p}zm", 2)
	send(t, nc, "ZMPOP", "2", "{p}none", "{p}zm", "MIN", "COUNT", "5")
	expect(t, r, "*2\r\n$5\r\n{p}zm\r\n*2\r\n*2\r\n$2\r\nm1\r\n$1\r\n1\r\n*2\r\n$2\r\nm2\r\n$1\r\n2\r\n")
	emit("{p}zm", 2) // zrem, colldrop
	send(t, nc, "ZMPOP", "2", "{p}none", "{p}none2", "MIN")
	expect(t, r, "*-1\r\n")

	// The ZREMRANGEBY* verbs frame the window they delete, byscore and
	// byrank over one key, bylex over an equal-score one; a miss window
	// frames nothing.
	send(t, nc, "ZADD", "zr", "1", "a", "2", "b", "3", "c", "4", "d")
	expect(t, r, ":4\r\n")
	emit("zr", 2)
	send(t, nc, "ZREMRANGEBYSCORE", "zr", "2", "3")
	expect(t, r, ":2\r\n")
	emit("zr", 1)
	send(t, nc, "ZREMRANGEBYRANK", "zr", "0", "-1")
	expect(t, r, ":2\r\n")
	emit("zr", 2) // zrem, colldrop
	send(t, nc, "ZADD", "zl", "0", "a", "0", "b", "0", "c")
	expect(t, r, ":3\r\n")
	emit("zl", 2)
	send(t, nc, "ZREMRANGEBYLEX", "zl", "[a", "[b")
	expect(t, r, ":2\r\n")
	emit("zl", 1)
	send(t, nc, "ZREMRANGEBYLEX", "zl", "[x", "[z")
	expect(t, r, ":0\r\n")

	// The STORE forms, co-located under one tag (the point-only route).
	send(t, nc, "ZADD", "{v}a", "1", "one", "2", "two", "3", "three")
	expect(t, r, ":3\r\n")
	emit("{v}a", 2)
	send(t, nc, "ZADD", "{v}b", "2", "two", "3", "three", "4", "four")
	expect(t, r, ":3\r\n")
	emit("{v}b", 2)
	send(t, nc, "ZINTERSTORE", "{v}d", "2", "{v}a", "{v}b")
	expect(t, r, ":2\r\n")
	emit("{v}d", 2) // collnew, zadd of the summed intersection
	// Replacing a live destination: collnew again, the reset-to-empty
	// replay rule, with the whole union behind it.
	send(t, nc, "ZUNIONSTORE", "{v}d", "2", "{v}a", "{v}b")
	expect(t, r, ":4\r\n")
	emit("{v}d", 2)
	// An empty result deletes the live destination: colldrop alone.
	send(t, nc, "ZINTERSTORE", "{v}d", "2", "{v}a", "{v}nosuch")
	expect(t, r, ":0\r\n")
	emit("{v}d", 1)
	// A string destination: the keydel for the shadow leads the rebuild.
	send(t, nc, "SET", "{v}sd", "v")
	expect(t, r, "+OK\r\n")
	emit("{v}sd", 1)
	send(t, nc, "ZINTERSTORE", "{v}sd", "2", "{v}a", "{v}b")
	expect(t, r, ":2\r\n")
	emit("{v}sd", 3) // keydel, collnew, zadd
	// Both destination and result absent: no effect, no frames.
	send(t, nc, "ZDIFFSTORE", "{v}nd", "2", "{v}nosuch", "{v}nosuch2")
	expect(t, r, ":0\r\n")

	// GEOADD frames as the geohash-scored ZADD it is; re-adding the same
	// point resolves to the same score and frames nothing.
	send(t, nc, "GEOADD", "geo1", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania")
	expect(t, r, ":2\r\n")
	emit("geo1", 2)
	send(t, nc, "GEOADD", "geo1", "13.361389", "38.115556", "Palermo")
	expect(t, r, ":0\r\n")
	// GEOSEARCHSTORE and the GEORADIUS STORE form, co-located: each
	// rebuilds its destination with the source's geohash scores.
	send(t, nc, "GEOADD", "{g}s", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania")
	expect(t, r, ":2\r\n")
	emit("{g}s", 2)
	send(t, nc, "GEOSEARCHSTORE", "{g}d", "{g}s", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km", "ASC")
	expect(t, r, ":2\r\n")
	emit("{g}d", 2)
	send(t, nc, "GEORADIUS", "{g}s", "15", "37", "200", "km", "STORE", "{g}d2")
	expect(t, r, ":2\r\n")
	emit("{g}d2", 2)

	// The cross-shard store routes, placements computed, never guessed.
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
	gsrc := keyOn(0, "gsrc")
	gdst := keyOn(1, "gdst")
	send(t, nc, "GEOADD", gsrc, "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania")
	expect(t, r, ":2\r\n")
	emit(gsrc, 2)
	send(t, nc, "GEOSEARCHSTORE", gdst, gsrc, "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km")
	expect(t, r, ":2\r\n")
	emit(gdst, 2)
	grsrc := keyOn(0, "grs")
	grdst := keyOn(1, "grd")
	send(t, nc, "GEOADD", grsrc, "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania")
	expect(t, r, ":2\r\n")
	emit(grsrc, 2)
	send(t, nc, "GEORADIUS", grsrc, "15", "37", "200", "km", "STORE", grdst)
	expect(t, r, ":2\r\n")
	emit(grdst, 2)

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
	if total != 59 {
		t.Fatalf("%d frames flushed, want 59: %v", total, byKey)
	}
	zadd := func(op obs1.Op) map[string]float64 {
		za, ok := op.(obs1.CollDelta).Sub.(obs1.ZAdd)
		if !ok {
			t.Fatalf("op %+v, want a zadd", op)
		}
		out := map[string]float64{}
		for _, e := range za.Entries {
			out[string(e.Member)] = e.Score
		}
		return out
	}
	zrem := func(op obs1.Op) []string {
		zr, ok := op.(obs1.CollDelta).Sub.(obs1.ZRem)
		if !ok {
			t.Fatalf("op %+v, want a zrem", op)
		}
		out := make([]string, len(zr.Members))
		for i, m := range zr.Members {
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
	isColl := func(op obs1.Op) bool {
		cn, ok := op.(obs1.CollNew)
		return ok && cn.Type == obs1.CollZSet && len(cn.Hints) == 0
	}

	z1 := byKey["z1"]
	if len(z1) != 7 {
		t.Fatalf("z1 ops = %v", z1)
	}
	if !isColl(z1[0]) {
		t.Fatalf("z1 frame 1 = %+v, want a hintless zset collnew", z1[0])
	}
	if p := zadd(z1[1]); len(p) != 2 || p["a"] != 1 || p["b"] != 2 {
		t.Fatalf("z1 frame 2 = %+v", p)
	}
	if p := zadd(z1[2]); len(p) != 1 || p["c"] != 3 {
		t.Fatalf("z1 frame 3 = %+v, want only the NX-added member", p)
	}
	if p := zadd(z1[3]); len(p) != 1 || p["a"] != 4 {
		t.Fatalf("z1 frame 4 = %+v, want the rescore at the new score", p)
	}
	if p := zadd(z1[4]); len(p) != 1 || p["b"] != 4 {
		t.Fatalf("z1 frame 5 = %+v, want the INCR result", p)
	}
	if p := zadd(z1[5]); len(p) != 1 || p["c"] != 4.5 {
		t.Fatalf("z1 frame 6 = %+v, want the ZINCRBY result", p)
	}
	if !same(zrem(z1[6]), "a") {
		t.Fatalf("z1 frame 7 = %+v, want only the removed member", z1[6])
	}
	zi := byKey["zi"]
	if len(zi) != 2 || !isColl(zi[0]) {
		t.Fatalf("zi ops = %v", zi)
	}
	if p := zadd(zi[1]); len(p) != 1 || p["m"] != 2 {
		t.Fatalf("zi frame 2 = %+v", p)
	}

	zp1 := byKey["zp1"]
	if len(zp1) != 4 || !same(zrem(zp1[2]), "only") {
		t.Fatalf("zp1 ops = %v", zp1)
	}
	if _, ok := zp1[3].(obs1.CollDrop); !ok {
		t.Fatalf("zp1 frame 4 = %+v, want the emptying colldrop", zp1[3])
	}
	zp2 := byKey["zp2"]
	if len(zp2) != 5 {
		t.Fatalf("zp2 ops = %v", zp2)
	}
	if !same(zrem(zp2[2]), "zz", "y") {
		t.Fatalf("zp2 frame 3 = %+v, want the max-end draws in pop order", zp2[2])
	}
	if !same(zrem(zp2[3]), "x") {
		t.Fatalf("zp2 frame 4 = %+v", zp2[3])
	}
	if _, ok := zp2[4].(obs1.CollDrop); !ok {
		t.Fatalf("zp2 frame 5 = %+v", zp2[4])
	}
	zm := byKey["{p}zm"]
	if len(zm) != 4 || !same(zrem(zm[2]), "m1", "m2") {
		t.Fatalf("{p}zm ops = %v", zm)
	}
	if _, ok := zm[3].(obs1.CollDrop); !ok {
		t.Fatalf("{p}zm frame 4 = %+v", zm[3])
	}

	zr1 := byKey["zr"]
	if len(zr1) != 5 {
		t.Fatalf("zr ops = %v", zr1)
	}
	if !same(zrem(zr1[2]), "b", "c") {
		t.Fatalf("zr frame 3 = %+v, want the score window", zr1[2])
	}
	if !same(zrem(zr1[3]), "a", "d") {
		t.Fatalf("zr frame 4 = %+v, want the remaining rank window", zr1[3])
	}
	if _, ok := zr1[4].(obs1.CollDrop); !ok {
		t.Fatalf("zr frame 5 = %+v", zr1[4])
	}
	zl := byKey["zl"]
	if len(zl) != 3 || !same(zrem(zl[2]), "a", "b") {
		t.Fatalf("zl ops = %v, want the lex window alone", zl)
	}

	vd := byKey["{v}d"]
	if len(vd) != 5 {
		t.Fatalf("{v}d ops = %v", vd)
	}
	if !isColl(vd[0]) {
		t.Fatalf("{v}d frame 1 = %+v", vd[0])
	}
	if p := zadd(vd[1]); len(p) != 2 || p["two"] != 4 || p["three"] != 6 {
		t.Fatalf("{v}d frame 2 = %+v, want the summed intersection", p)
	}
	if !isColl(vd[2]) {
		t.Fatalf("{v}d frame 3 = %+v, want the reset-to-empty collnew over the live zset", vd[2])
	}
	if p := zadd(vd[3]); len(p) != 4 || p["one"] != 1 || p["four"] != 4 || p["two"] != 4 || p["three"] != 6 {
		t.Fatalf("{v}d frame 4 = %+v, want the whole union", p)
	}
	if _, ok := vd[4].(obs1.CollDrop); !ok {
		t.Fatalf("{v}d frame 5 = %+v, want the empty-result colldrop", vd[4])
	}
	sd := byKey["{v}sd"]
	if len(sd) != 4 {
		t.Fatalf("{v}sd ops = %v", sd)
	}
	if ss := sd[0].(obs1.StrSet); string(ss.Value) != "v" {
		t.Fatalf("{v}sd frame 1 = %+v", ss)
	}
	if _, ok := sd[1].(obs1.KeyDel); !ok {
		t.Fatalf("{v}sd frame 2 = %+v, want the keydel for the string shadow", sd[1])
	}
	if !isColl(sd[2]) {
		t.Fatalf("{v}sd frame 3 = %+v", sd[2])
	}
	if p := zadd(sd[3]); len(p) != 2 || p["two"] != 4 || p["three"] != 6 {
		t.Fatalf("{v}sd frame 4 = %+v", p)
	}
	if _, ok := byKey["{v}nd"]; ok {
		t.Fatal("a no-effect STORE flushed a frame")
	}

	g1 := byKey["geo1"]
	if len(g1) != 2 || !isColl(g1[0]) {
		t.Fatalf("geo1 ops = %v, want the creating add alone since the re-add frames nothing", g1)
	}
	geoScores := zadd(g1[1])
	if len(geoScores) != 2 || geoScores["Palermo"] <= 0 || geoScores["Catania"] <= 0 {
		t.Fatalf("geo1 frame 2 = %+v, want both members at geohash scores", geoScores)
	}
	// Every STORE destination keeps the source's geohash scores, the two
	// co-located routes and the two cross-shard routes alike.
	srcScores := zadd(byKey["{g}s"][1])
	for _, dest := range []string{"{g}d", "{g}d2"} {
		ops := byKey[dest]
		if len(ops) != 2 || !isColl(ops[0]) {
			t.Fatalf("%s ops = %v", dest, ops)
		}
		if p := zadd(ops[1]); len(p) != 2 || p["Palermo"] != srcScores["Palermo"] || p["Catania"] != srcScores["Catania"] {
			t.Fatalf("%s frame 2 = %+v, want the source geohash scores %+v", dest, p, srcScores)
		}
	}
	for src, dest := range map[string]string{gsrc: gdst, grsrc: grdst} {
		from := zadd(byKey[src][1])
		ops := byKey[dest]
		if len(ops) != 2 || !isColl(ops[0]) {
			t.Fatalf("%s ops = %v", dest, ops)
		}
		if p := zadd(ops[1]); len(p) != 2 || p["Palermo"] != from["Palermo"] || p["Catania"] != from["Catania"] {
			t.Fatalf("%s frame 2 = %+v, want the source geohash scores %+v", dest, p, from)
		}
	}
}
