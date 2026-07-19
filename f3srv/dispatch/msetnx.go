package dispatch

import (
	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/engine/f3/store"
	"github.com/tamnd/aki/f3srv/resp"
)

// MSETNX (spec 2064/f3/17 command-coverage line 235): the all-or-nothing sibling
// of MSET. It writes every key/value pair only when none of the keys already
// exist, replying 1 on the write and 0 when any key is present, in any keyspace.
// The atomicity is the point: a reader must never see a half-applied MSETNX, and
// a key that exists as a set or a hash counts as present exactly as it does for
// EXISTS, so the probe runs through keyExistsAnywhere, not the string store alone.
//
// Two routes carry it, split by co-location the same way SMOVE splits (dispatch
// SMOVE wiring). When every key hashes to one owner the co-located fast path runs
// the whole probe-then-write in that owner's batch loop, atomic by single
// ownership. When the keys span shards it rides the F17 intent barrier: DoTxn arms
// write intents on every key in ascending shard order, msetnxCross probes each key
// under the held barrier and, only if all are absent, writes each, so no point
// command touching any of the keys interleaves between the probe and the write.
//
// The wrong-number-of-arguments guard fires before any probe or lock, matching
// Redis's mSetGenericCommand: an odd argument count is malformed and never a
// partial write.

const errMsetnxArgs = "ERR wrong number of arguments for 'msetnx' command"

// msetnxKeys extracts the key arguments of an MSETNX tail, the even positions of
// its key/value pairs, for the co-location check and the cross-shard key set. An
// odd trailing argument (a malformed tail) is dropped from the key set; the
// handlers reject the command on the arity guard before either route acts on it.
func msetnxKeys(a [][]byte) [][]byte {
	keys := make([][]byte, 0, (len(a)+1)/2)
	for i := 0; i+1 < len(a); i += 2 {
		keys = append(keys, a[i])
	}
	return keys
}

// msetnxCmd answers a co-located MSETNX (every key on one owner), the single-shard
// fast path. It probes every key once through keyExistsAnywhere and, only if all
// are absent, writes each pair; a present key declines the whole command with 0
// and writes nothing. Under memory pressure a write that cannot allocate parks the
// command and the worker retries it when a drain frees room, resuming at the
// unwritten pair (ResumeIndex) so the committed prefix is not re-applied: the probe
// phase runs only on the fresh entry (ResumeIndex 0), because once a write has
// landed the command is past the decision point and must finish, not re-decide.
func msetnxCmd(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	if len(args)%2 != 0 {
		r.Err(errMsetnxArgs)
		return
	}
	start := cx.ResumeIndex()
	if start == 0 {
		for i := 0; i < len(args); i += 2 {
			if keyExistsAnywhere(cx, args[i]) {
				r.Int(0)
				return
			}
		}
	}
	for i := start; i+1 < len(args); i += 2 {
		if err := cx.St.SetString(args[i], args[i+1], cx.NowMs, 0, false); err != nil {
			if cx.ParkFullAt(err, i) {
				return
			}
			r.Err(msetnxStoreErr(err))
			return
		}
		// Each written pair fires its own set event, redis's per-key MSETNX
		// notification. The resume starts past a committed pair, so none fires twice.
		cx.NotifyKeyspaceEvent(shard.NotifyString, "set", args[i])
	}
	r.Int(1)
}

// msetnxCross runs a cross-shard MSETNX under an acquired transaction holding a
// write intent on every key. It probes each key at its owner and, only if all are
// absent, writes each pair at its owner; a present key declines with 0 and writes
// nothing. The barrier makes the two passes one atomic step from every other
// command's view, so the probe result cannot go stale before the writes. A write
// that fails (an oversize value, or memory pressure the cross path cannot park)
// stops the command and reports the error, the first one seen; the barrier still
// releases every intent on return.
func msetnxCross(t *shard.Txn, args [][]byte) []byte {
	if len(args)%2 != 0 {
		return resp.AppendError(nil, errMsetnxArgs)
	}
	present := false
	for i := 0; i < len(args) && !present; i += 2 {
		key := args[i]
		t.Do(key, func(cx *shard.Ctx) {
			if keyExistsAnywhere(cx, key) {
				present = true
			}
		})
	}
	if present {
		return resp.AppendInt(nil, 0)
	}
	var writeErr error
	for i := 0; i+1 < len(args); i += 2 {
		if writeErr != nil {
			break
		}
		key, val := args[i], args[i+1]
		t.Do(key, func(cx *shard.Ctx) {
			if err := cx.St.SetString(key, val, cx.NowMs, 0, false); err != nil {
				writeErr = err
				return
			}
			// Each written pair fires its own set event on its owner, the same
			// per-key notification the co-located path fires.
			cx.NotifyKeyspaceEvent(shard.NotifyString, "set", key)
		})
	}
	if writeErr != nil {
		return resp.AppendError(nil, msetnxStoreErr(writeErr))
	}
	return resp.AppendInt(nil, 1)
}

// msetnxStoreErr maps a string-store write error to its wire text, the dispatch
// twin of str.storeErr: an oversize value is the proto-max-bulk-len refusal, and
// any other error carries its own message under the ERR prefix.
func msetnxStoreErr(err error) string {
	if err == store.ErrTooBig {
		return "ERR string exceeds maximum allowed size (proto-max-bulk-len)"
	}
	return "ERR " + err.Error()
}
