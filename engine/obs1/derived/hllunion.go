package derived

// Multi-key PFCOUNT and PFMERGE (spec 2064/f3/15 section 9): both fold their
// keys with the section 8 register-merge kernel. The co-located case (every key
// on one owner) runs the whole fold on that owner through the store; the
// cross-shard case rides the F17 intent path, holding an intent on every key so
// they are frozen at one point, then folding each sketch into the coordinator's
// one register scratch as its owner is hopped. The fold is commutative and
// associative, so the order the owners are visited does not matter, and only the
// 16KiB scratch is held for the whole pass regardless of how many keys there are
// or how they are spread. PFCOUNT reads the scratch histogram and writes
// nothing; PFMERGE repacks the scratch to a dense sketch and installs it at the
// destination as the one mutation.

import (
	"github.com/tamnd/aki/engine/obs1/shard"
	"github.com/tamnd/aki/obs1srv/resp"
)

// pfCountUnion is the co-located multi-key PFCOUNT: fold every key into one
// register scratch and estimate the union, a missing key contributing nothing.
// Nothing is written; the union count belongs to no key's header.
func pfCountUnion(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	acc := make([]byte, hllRegisters)
	tmp := make([]byte, hllRegisters)
	for _, key := range args {
		blob, ok := cx.St.GetString(key, cx.NowMs, cx.Val[:0])
		cx.Val = blob
		if !ok {
			continue
		}
		if !isHLL(blob) {
			r.Err(errNotHLL)
			return
		}
		if !foldInto(acc, tmp, blob) {
			r.Err(errCorruptHLL)
			return
		}
	}
	var histo [64]int
	scratchHisto(acc, &histo)
	r.Int(int64(estimateHisto(&histo)))
}

// PfCountCross is the cross-shard multi-key PFCOUNT: under the transaction
// barrier, hop to each key's owner and fold its sketch into the coordinator
// scratch. groupSources buckets the keys by owner so each shard is hopped once.
// The fold happens on the owner into the shared scratch; t.Do is synchronous, so
// the owners are visited one at a time and the scratch needs no lock. Nothing is
// written.
func PfCountCross(t *shard.Txn, args [][]byte) []byte {
	acc := make([]byte, hllRegisters)
	tmp := make([]byte, hllRegisters)
	var bad, corrupt bool
	for _, g := range groupSources(t, args) {
		idxs := g.idxs
		t.Do(g.key, func(cx *shard.Ctx) {
			for _, j := range idxs {
				blob, ok := cx.St.GetString(args[j], cx.NowMs, cx.Val[:0])
				cx.Val = blob
				if !ok {
					continue
				}
				if !isHLL(blob) {
					bad = true
					return
				}
				if !foldInto(acc, tmp, blob) {
					corrupt = true
					return
				}
			}
		})
		if bad {
			return resp.AppendError(nil, errNotHLL)
		}
		if corrupt {
			return resp.AppendError(nil, errCorruptHLL)
		}
	}
	var histo [64]int
	scratchHisto(acc, &histo)
	return resp.AppendInt(nil, int64(estimateHisto(&histo)))
}

// PfMerge is the co-located PFMERGE destkey [srckey ...]: fold the destination
// and every source into one register scratch, repack it to a dense sketch, and
// store it at the destination, the only mutation. The destination participates
// in its own merge (the Redis contract), so a PFMERGE with no sources is a
// densify-in-place, and a PFMERGE of missing keys leaves an empty dense sketch.
func PfMerge(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	acc := make([]byte, hllRegisters)
	tmp := make([]byte, hllRegisters)
	for _, key := range args {
		blob, ok := cx.St.GetString(key, cx.NowMs, cx.Val[:0])
		cx.Val = blob
		if !ok {
			continue
		}
		if !isHLL(blob) {
			r.Err(errNotHLL)
			return
		}
		if !foldInto(acc, tmp, blob) {
			r.Err(errCorruptHLL)
			return
		}
	}
	out := denseFromScratch(acc)
	if err := cx.St.SetString(args[0], out, cx.NowMs, 0, true); err != nil {
		r.Err("ERR " + err.Error())
		return
	}
	// The merged dense sketch is in hand, so the effect frame takes it
	// directly; the keepttl write preserved any deadline, read beside it.
	if err := cx.LogStrSet(args[0], out, cx.St.ExpireAt(args[0], cx.NowMs), false); err != nil {
		r.Err(err.Error())
		return
	}
	r.Status("OK")
}

// PfMergeCross is the cross-shard PFMERGE: fold the destination and every source
// under the barrier, then repack and install the dense result at the destination
// owner. The read intents on the sources and the write intent on the destination
// freeze all of them together, so the merged bytes are a consistent snapshot.
func PfMergeCross(t *shard.Txn, args [][]byte) []byte {
	dest := args[0]
	acc := make([]byte, hllRegisters)
	tmp := make([]byte, hllRegisters)
	var bad, corrupt bool
	for _, g := range groupSources(t, args) {
		idxs := g.idxs
		t.Do(g.key, func(cx *shard.Ctx) {
			for _, j := range idxs {
				blob, ok := cx.St.GetString(args[j], cx.NowMs, cx.Val[:0])
				cx.Val = blob
				if !ok {
					continue
				}
				if !isHLL(blob) {
					bad = true
					return
				}
				if !foldInto(acc, tmp, blob) {
					corrupt = true
					return
				}
			}
		})
		if bad {
			return resp.AppendError(nil, errNotHLL)
		}
		if corrupt {
			return resp.AppendError(nil, errCorruptHLL)
		}
	}
	out := denseFromScratch(acc)
	var werr, lerr error
	t.Do(dest, func(cx *shard.Ctx) {
		werr = cx.St.SetString(dest, out, cx.NowMs, 0, true)
		if werr == nil {
			// The install is the one mutation, framed on the destination's
			// owner inside the hop; the closure runs with no current command,
			// so the emission is relaxed-only under the transaction.
			lerr = cx.LogStrSet(dest, out, cx.St.ExpireAt(dest, cx.NowMs), false)
		}
	})
	if werr != nil {
		return resp.AppendError(nil, "ERR "+werr.Error())
	}
	if lerr != nil {
		return resp.AppendError(nil, lerr.Error())
	}
	return resp.AppendStatus(nil, "OK")
}
