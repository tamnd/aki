package drivers

import (
	"context"
	"testing"
	"time"

	"github.com/tamnd/aki/engine/obs1"
)

// TestHashDurabilityRoundTrip drives the hash write surface over the
// socket and checks the flushed frames carry post-decision effects: a
// creating write leads with a collnew, an emptying removal trails with a
// colldrop, HDEL lists only the fields that left, a refused HSETNX frames
// nothing, HINCRBY frames its resulting rendering and restores the
// deadline it preserved, and a set-to-the-past HPEXPIREAT frames as the
// hdel it is.
func TestHashDurabilityRoundTrip(t *testing.T) {
	wl, store, nc, r, _ := startLoggedServer(t, false)
	const node = uint64(0xE1)
	const farMs = "4102444800000" // 2100-01-01 in ms, under the 2^46-1 ceiling

	seqs := map[uint16]uint64{}
	emit := func(key string, n uint64) {
		_, g := ClusterMapKey([]byte(key))
		seqs[g] += n
	}

	send(t, nc, "HSET", "h", "f1", "v1", "f2", "v2")
	expect(t, r, ":2\r\n")
	emit("h", 2) // collnew, hset
	send(t, nc, "HSET", "h", "f1", "v9")
	expect(t, r, ":0\r\n")
	emit("h", 1)
	send(t, nc, "HSETNX", "h", "f3", "v3")
	expect(t, r, ":1\r\n")
	emit("h", 1)
	// A refused HSETNX changed nothing and frames nothing.
	send(t, nc, "HSETNX", "h", "f3", "zz")
	expect(t, r, ":0\r\n")
	send(t, nc, "HINCRBY", "h", "ctr", "5")
	expect(t, r, ":5\r\n")
	emit("h", 1)
	send(t, nc, "HPEXPIREAT", "h", farMs, "FIELDS", "1", "ctr")
	expect(t, r, "*1\r\n:1\r\n")
	emit("h", 1)
	// The increment preserves ctr's deadline, so the emission restores it
	// behind the hset frame.
	send(t, nc, "HINCRBY", "h", "ctr", "1")
	expect(t, r, ":6\r\n")
	emit("h", 2) // hset, hexpire
	send(t, nc, "HPERSIST", "h", "FIELDS", "1", "ctr")
	expect(t, r, "*1\r\n:1\r\n")
	emit("h", 1)
	// Only the field that left is framed.
	send(t, nc, "HDEL", "h", "f1", "nosuch")
	expect(t, r, ":1\r\n")
	emit("h", 1)
	send(t, nc, "HDEL", "h", "nosuch2")
	expect(t, r, ":0\r\n")

	send(t, nc, "HMSET", "h2", "a", "1", "b", "2")
	expect(t, r, "+OK\r\n")
	emit("h2", 2) // collnew, hset
	send(t, nc, "HINCRBYFLOAT", "h2", "c", "1.5")
	expect(t, r, "$3\r\n1.5\r\n")
	emit("h2", 1)
	// Removing the last fields drops the hash behind the hdel.
	send(t, nc, "HDEL", "h2", "a", "b", "c")
	expect(t, r, ":3\r\n")
	emit("h2", 2) // hdel, colldrop

	send(t, nc, "HSET", "h3", "x", "1")
	expect(t, r, ":1\r\n")
	emit("h3", 2) // collnew, hset
	// A deadline at or before now deletes on the spot, so it frames as the
	// hdel it is, with the colldrop for the emptied hash behind it.
	send(t, nc, "HPEXPIREAT", "h3", "1", "FIELDS", "1", "x")
	expect(t, r, "*1\r\n:2\r\n")
	emit("h3", 2) // hdel, colldrop

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
	if total != 19 {
		t.Fatalf("%d frames flushed, want 19: %v", total, byKey)
	}

	h := byKey["h"]
	if len(h) != 10 {
		t.Fatalf("h ops = %d: %v", len(h), h)
	}
	if cn := h[0].(obs1.CollNew); cn.Type != obs1.CollHash {
		t.Fatalf("h frame 1 = %+v, want a hash collnew", cn)
	}
	if hs := h[1].(obs1.CollDelta).Sub.(obs1.HSet); len(hs.Pairs) != 2 ||
		string(hs.Pairs[0].Field) != "f1" || string(hs.Pairs[0].Value) != "v1" ||
		string(hs.Pairs[1].Field) != "f2" || string(hs.Pairs[1].Value) != "v2" {
		t.Fatalf("h frame 2 = %+v", hs)
	}
	if hs := h[2].(obs1.CollDelta).Sub.(obs1.HSet); string(hs.Pairs[0].Value) != "v9" {
		t.Fatalf("h frame 3 = %+v", hs)
	}
	if hs := h[3].(obs1.CollDelta).Sub.(obs1.HSet); string(hs.Pairs[0].Field) != "f3" {
		t.Fatalf("h frame 4 = %+v", hs)
	}
	if hs := h[4].(obs1.CollDelta).Sub.(obs1.HSet); string(hs.Pairs[0].Field) != "ctr" || string(hs.Pairs[0].Value) != "5" {
		t.Fatalf("h frame 5 = %+v", hs)
	}
	he := h[5].(obs1.CollDelta).Sub.(obs1.HExpire)
	if he.AtMs != 4102444800000 || len(he.Fields) != 1 || string(he.Fields[0]) != "ctr" {
		t.Fatalf("h frame 6 = %+v", he)
	}
	if hs := h[6].(obs1.CollDelta).Sub.(obs1.HSet); string(hs.Pairs[0].Value) != "6" {
		t.Fatalf("h frame 7 = %+v", hs)
	}
	if re := h[7].(obs1.CollDelta).Sub.(obs1.HExpire); re.AtMs != he.AtMs || string(re.Fields[0]) != "ctr" {
		t.Fatalf("h frame 8 = %+v, want the preserved deadline restored", re)
	}
	if pe := h[8].(obs1.CollDelta).Sub.(obs1.HExpire); pe.AtMs != 0 {
		t.Fatalf("h frame 9 = %+v, want the cleared deadline", pe)
	}
	if hd := h[9].(obs1.CollDelta).Sub.(obs1.HDel); len(hd.Fields) != 1 || string(hd.Fields[0]) != "f1" {
		t.Fatalf("h frame 10 = %+v, want only the removed field", hd)
	}

	h2 := byKey["h2"]
	if len(h2) != 5 {
		t.Fatalf("h2 ops = %d: %v", len(h2), h2)
	}
	if cn := h2[0].(obs1.CollNew); cn.Type != obs1.CollHash {
		t.Fatalf("h2 frame 1 = %+v", cn)
	}
	if hs := h2[1].(obs1.CollDelta).Sub.(obs1.HSet); len(hs.Pairs) != 2 {
		t.Fatalf("h2 frame 2 = %+v", hs)
	}
	if hs := h2[2].(obs1.CollDelta).Sub.(obs1.HSet); string(hs.Pairs[0].Field) != "c" || string(hs.Pairs[0].Value) != "1.5" {
		t.Fatalf("h2 frame 3 = %+v", hs)
	}
	if hd := h2[3].(obs1.CollDelta).Sub.(obs1.HDel); len(hd.Fields) != 3 {
		t.Fatalf("h2 frame 4 = %+v", hd)
	}
	if _, ok := h2[4].(obs1.CollDrop); !ok {
		t.Fatalf("h2 frame 5 = %+v, want a colldrop", h2[4])
	}

	h3 := byKey["h3"]
	if len(h3) != 4 {
		t.Fatalf("h3 ops = %d: %v", len(h3), h3)
	}
	if hd := h3[2].(obs1.CollDelta).Sub.(obs1.HDel); len(hd.Fields) != 1 || string(hd.Fields[0]) != "x" {
		t.Fatalf("h3 frame 3 = %+v, want the set-to-the-past delete", hd)
	}
	if _, ok := h3[3].(obs1.CollDrop); !ok {
		t.Fatalf("h3 frame 4 = %+v, want a colldrop", h3[3])
	}
}
