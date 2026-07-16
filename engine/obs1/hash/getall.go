package hash

import (
	"github.com/tamnd/aki/engine/obs1/shard"
	"github.com/tamnd/aki/engine/obs1/store"
	"github.com/tamnd/aki/obs1srv/resp"
)

// The hash enumeration surface (spec 2064/f3/10 section 7.5): HGETALL returns
// every field and value, HKEYS every field, HVALS every value. A missing key is
// the empty array on all three. On the native band a wide reply streams through
// the shard ring (hgetall.go); a small hash and every inline hash build the reply
// in the shard scratch and hand it over whole, the same one-pass shape HMGET uses.
// The stream cutover is store.ChunkSize, the width the string band streams at, so
// a reply that fits a chunk never pays the ring's setup.

// Hgetall answers HGETALL key: a flat array of field, value, field, value ... in
// draw-vector order on the native band and blob order inline (both stable, neither
// promising a sort). RESP2 flattens the map; a RESP3 client would frame it as a
// map type, which the reply layer handles, not here.
func Hgetall(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	enumerate(cx, args[0], r, enumPairs)
}

// Hkeys answers HKEYS key: the field names alone.
func Hkeys(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	enumerate(cx, args[0], r, enumKeys)
}

// Hvals answers HVALS key: the values alone.
func Hvals(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	enumerate(cx, args[0], r, enumVals)
}

// enumerate is the shared body of the three enumeration verbs: look the hash up,
// answer WRONGTYPE or the empty array for the trivial cases, stream a wide native
// reply, and otherwise build the reply in cx.Aux.
func enumerate(cx *shard.Ctx, key []byte, r shard.Reply, mode enumMode) {
	g := registry(cx)
	h, wrong := g.lookup(cx, key)
	if wrong {
		r.Err(wrongType)
		return
	}
	if h == nil {
		r.Raw(resp.AppendArrayHeader(cx.Aux[:0], 0))
		return
	}
	if h.enc == encHashtable {
		if total := h.ft.enumTotal(mode); total > store.ChunkSize {
			r.StreamRaw(total, h.ft.pinEnumStream(mode))
			return
		}
	}
	out := resp.AppendArrayHeader(cx.Aux[:0], enumElems(h.card(), mode))
	h.each(func(field, value []byte) {
		if mode != enumVals {
			out = resp.AppendBulk(out, field)
		}
		if mode != enumKeys {
			out = resp.AppendBulk(out, value)
		}
	})
	cx.Aux = out
	r.Raw(out)
}

// enumElems is the array element count for a hash of card fields under mode: a
// pair per field for HGETALL, one per field for HKEYS and HVALS.
func enumElems(card int, mode enumMode) int {
	if mode == enumPairs {
		return 2 * card
	}
	return card
}
