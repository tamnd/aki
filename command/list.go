package command

import (
	"github.com/tamnd/aki/keyspace"
)

// listCommands returns the table for the core non-blocking list commands: the
// push, pop, length and range family (doc 09 §6). Index access, removal and the
// multi-key moves are separate later slices, and the blocking variants wait for
// the blocking-registry milestone.
func listCommands() []*CmdDesc {
	return []*CmdDesc{
		{Name: "lpush", Group: GroupList, Since: "1.0.0",
			Arity: -3, Flags: FlagWrite | FlagDenyOOM | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: func(ctx *Ctx) { pushList(ctx, true, false) }},
		{Name: "rpush", Group: GroupList, Since: "1.0.0",
			Arity: -3, Flags: FlagWrite | FlagDenyOOM | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: func(ctx *Ctx) { pushList(ctx, false, false) }},
		{Name: "lpushx", Group: GroupList, Since: "2.2.0",
			Arity: -3, Flags: FlagWrite | FlagDenyOOM | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: func(ctx *Ctx) { pushList(ctx, true, true) }},
		{Name: "rpushx", Group: GroupList, Since: "2.2.0",
			Arity: -3, Flags: FlagWrite | FlagDenyOOM | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: func(ctx *Ctx) { pushList(ctx, false, true) }},
		{Name: "lpop", Group: GroupList, Since: "1.0.0",
			Arity: -2, Flags: FlagWrite | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: func(ctx *Ctx) { popList(ctx, true) }},
		{Name: "rpop", Group: GroupList, Since: "1.0.0",
			Arity: -2, Flags: FlagWrite | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: func(ctx *Ctx) { popList(ctx, false) }},
		{Name: "llen", Group: GroupList, Since: "1.0.0",
			Arity: 2, Flags: FlagReadOnly | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleLLen},
		{Name: "lrange", Group: GroupList, Since: "1.0.0",
			Arity: 4, Flags: FlagReadOnly, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleLRange},
	}
}

// pushList implements LPUSH, RPUSH and their X variants. head selects the end to
// push onto; mustExist makes it a no-op returning 0 when the key is absent.
func pushList(ctx *Ctx, head, mustExist bool) {
	key := ctx.Argv[1]
	vals := ctx.Argv[2:]
	if ctx.deferPush {
		// Coll-form push in a batchable state: accumulate it onto the connection's
		// pending batch and reply nothing now. flushPushPending applies the batch on
		// the shard owners and writes the replies in pipeline order at the end of the
		// drain (or before the next non-deferrable command). Both the key and the
		// values are copied because they outlive this command: the staged write reaches
		// the shard after the drain. The whole argv is copied only when AOF is on, where
		// it backs verbatim propagation, mirroring the increment batch's alloc gate.
		var (
			argv  [][]byte
			dvals [][]byte
		)
		if ctx.d.aofEnabled() {
			argv = copyArgv(ctx.Argv)
			dvals = argv[2:]
		} else {
			dvals = make([][]byte, len(vals))
			for i, v := range vals {
				dvals[i] = append([]byte(nil), v...)
			}
		}
		ctx.sess.pushPend = append(ctx.sess.pushPend, deferredPush{
			shard:     keyspace.ShardOf(key),
			key:       append([]byte(nil), key...),
			vals:      dvals,
			argv:      argv,
			head:      head,
			mustExist: mustExist,
		})
		return
	}
	var (
		wrongTyp bool
		absent   bool
		newLen   int64
	)
	lim := ctx.encLimits()
	// sync is the full synchronous closure: it handles every form, the btree-backed
	// element write, the listpack to quicklist promotion, and the plain blob set. It
	// is the fallback the write-behind helper runs when the fast path below cannot
	// stage the result, and the path under commitAlways or with no workers running.
	// applyListPush is the shared core, also used by the batched push hand-off.
	sync := func(db *keyspace.DB) error {
		r, err := applyListPush(db, key, vals, head, mustExist, lim)
		if err != nil {
			return err
		}
		switch r.res {
		case pushResWrongType:
			wrongTyp = true
		default:
			newLen = r.newLen
			if !r.changed {
				// X push on an absent key: reply 0, no notify or signal.
				absent = true
			}
		}
		return nil
	}
	// compute is the write-behind fast path for the common case: a listpack-form
	// list (or a fresh key) whose new blob still fits inline. It splices the body
	// and reports the new value to stage, falling back to sync for the
	// btree-backed form, a promotion, or a codec error.
	compute := func(cur []byte, hdr keyspace.ValueHeader, found bool) rmwResult {
		if found && hdr.Type != keyspace.TypeList {
			wrongTyp = true
			return rmwResult{}
		}
		if !found && mustExist {
			absent = true
			return rmwResult{}
		}
		if found && hdr.IsColl() {
			return rmwResult{fallback: true}
		}
		newBody, newCount, err := listBlobPush(cur, vals, head)
		if err != nil {
			return rmwResult{fallback: true}
		}
		prev := uint8(keyspace.EncListpack)
		if found {
			prev = hdr.Encoding
		}
		enc, err := listBlobReportedEnc(lim, prev, newBody)
		if err != nil {
			return rmwResult{fallback: true}
		}
		// The same early-coll boundary as the sync path: a body that would spill to
		// overflow cannot be staged as a blob without reintroducing the whole-blob
		// rewrite, so fall back to the sync path, which moves it to coll form.
		if enc == keyspace.EncQuicklist || len(newBody) > keyspace.MaxInlineBody {
			return rmwResult{fallback: true}
		}
		newLen = int64(newCount)
		return rmwResult{body: newBody, typ: keyspace.TypeList, enc: enc, ttlMs: keepTTL(hdr, found), write: true}
	}
	if !ctx.rmwWriteBehind(key, compute, sync) {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	if absent {
		ctx.enc().WriteInteger(0)
		return
	}
	event := "rpush"
	if head {
		event = "lpush"
	}
	ctx.notify(notifyList, event, key)
	ctx.signalReady(key)
	ctx.enc().WriteInteger(newLen)
}

// popList implements LPOP and RPOP, with or without a count. head selects the
// end to pop from.
func popList(ctx *Ctx, head bool) {
	if len(ctx.Argv) > 3 {
		name := "lpop"
		if !head {
			name = "rpop"
		}
		ctx.enc().WriteError("ERR wrong number of arguments for '" + name + "' command")
		return
	}
	key := ctx.Argv[1]
	hasCount := len(ctx.Argv) == 3
	var count int64
	if hasCount {
		c, ok := parseInteger(ctx.Argv[2])
		if !ok || c < 0 {
			ctx.enc().WriteError("ERR value is not an integer or out of range")
			return
		}
		count = c
	}

	var (
		wrongTyp bool
		absent   bool
		emptied  bool
		popped   [][]byte
	)
	lim := ctx.encLimits()
	// sync is the synchronous closure: the btree-backed collection form, the
	// non-deferred policy, and the fast path's fallback all run through it. It pops
	// in place for a coll list and rewrites or deletes the blob for a listpack list.
	sync := func(db *keyspace.DB) error {
		hdr, found, err := listHeader(db, key)
		if err != nil {
			return err
		}
		if !found {
			absent = true
			return nil
		}
		if hdr.Type != keyspace.TypeList {
			wrongTyp = true
			return nil
		}
		// A btree-backed list pops from the window end in place: read and delete the
		// boundary rows and move head or tail, no whole-blob rewrite. CollUpdate tears
		// the key down when the last element goes.
		if hdr.IsColl() {
			before := int64(0)
			err := db.CollUpdate(key, keyspace.TypeList, hdr.Encoding, func(w *keyspace.CollWriter) error {
				before = int64(w.Count())
				n := 1
				if hasCount {
					n = int(min(count, before))
				}
				p, e := listTreePop(w, n, head)
				popped = p
				return e
			})
			if err != nil {
				return err
			}
			emptied = before > 0 && int64(len(popped)) == before
			return nil
		}
		elems, _, _, err := getList(db, key)
		if err != nil {
			return err
		}
		n := 1
		if hasCount {
			n = int(min(count, int64(len(elems))))
		}
		if n == 0 {
			return nil
		}
		var rest [][]byte
		if head {
			popped = elems[:n]
			rest = elems[n:]
		} else {
			tail := elems[len(elems)-n:]
			popped = make([][]byte, n)
			for i := range tail {
				popped[i] = tail[n-1-i]
			}
			rest = elems[:len(elems)-n]
		}
		if len(rest) == 0 {
			emptied = true
			_, err := db.Delete(key)
			return err
		}
		return db.Set(key, listEncode(rest), keyspace.TypeList, listEncoding(lim, rest, hdr.Encoding), keepTTL(hdr, found))
	}
	// compute is the write-behind fast path for a listpack-form list: pop from the
	// decoded body and either stage the shrunken blob or, when the list empties,
	// stage a delete. It defers to sync for the btree-backed form and any codec
	// surprise, so the slow path stays the single source of truth for those shapes.
	compute := func(cur []byte, hdr keyspace.ValueHeader, found bool) rmwResult {
		if !found {
			absent = true
			return rmwResult{}
		}
		if hdr.Type != keyspace.TypeList {
			wrongTyp = true
			return rmwResult{}
		}
		if hdr.IsColl() {
			return rmwResult{fallback: true}
		}
		elems, err := listDecode(cur)
		if err != nil {
			return rmwResult{fallback: true}
		}
		n := 1
		if hasCount {
			n = int(min(count, int64(len(elems))))
		}
		if n == 0 {
			return rmwResult{}
		}
		var rest [][]byte
		if head {
			popped = elems[:n]
			rest = elems[n:]
		} else {
			tail := elems[len(elems)-n:]
			popped = make([][]byte, n)
			for i := range tail {
				popped[i] = tail[n-1-i]
			}
			rest = elems[:len(elems)-n]
		}
		if len(rest) == 0 {
			emptied = true
			return rmwResult{del: true}
		}
		newBody := listEncode(rest)
		enc := listEncoding(lim, rest, hdr.Encoding)
		// A pop only shrinks the list, so this practically never trips, but keep the
		// fast path's invariant that it stages only inline listpack blobs.
		if enc == keyspace.EncQuicklist || len(newBody) > keyspace.MaxInlineBody {
			return rmwResult{fallback: true}
		}
		return rmwResult{body: newBody, typ: keyspace.TypeList, enc: enc, ttlMs: keepTTL(hdr, found), write: true}
	}
	if !ctx.rmwWriteBehind(key, compute, sync) {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	if len(popped) > 0 {
		event := "rpop"
		if head {
			event = "lpop"
		}
		ctx.notify(notifyList, event, key)
		if emptied {
			ctx.notify(notifyGeneric, "del", key)
		}
	}
	enc := ctx.enc()
	if absent {
		if hasCount {
			enc.WriteNullArray()
		} else {
			enc.WriteNull()
		}
		return
	}
	if hasCount {
		enc.WriteArrayLen(len(popped))
		for _, e := range popped {
			enc.WriteBulkString(e)
		}
		return
	}
	enc.WriteBulkString(popped[0])
}

// handleLLen implements LLEN: the element count, or 0 when the key is absent.
func handleLLen(ctx *Ctx) {
	key := ctx.Argv[1]

	if elems, ok := hotGetList(ctx, key); ok {
		ctx.enc().WriteInteger(int64(len(elems)))
		return
	}

	var (
		wrongTyp bool
		n        int64
	)
	if !ctx.view(func(db *keyspace.DB) error {
		ln, hdr, found, err := listLen(db, key)
		if err != nil {
			return err
		}
		if !found {
			return nil
		}
		if hdr.Type != keyspace.TypeList {
			wrongTyp = true
			return nil
		}
		n = ln
		return nil
	}) {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	ctx.enc().WriteInteger(n)
}

// handleLRange implements LRANGE: the elements in the inclusive [start, stop]
// range, with Redis negative indexing and clamping.
func handleLRange(ctx *Ctx) {
	key := ctx.Argv[1]
	start, ok1 := parseInteger(ctx.Argv[2])
	stop, ok2 := parseInteger(ctx.Argv[3])
	if !ok1 || !ok2 {
		ctx.enc().WriteError("ERR value is not an integer or out of range")
		return
	}

	// Hot blob path: stream the requested window straight off the encoded body,
	// skipping the per-element decode that dominates LRANGE on a hot list. A
	// corrupt body returns false with nothing written, falling through to the cold
	// path, which surfaces the decode error the same way.
	if body, ok := hotGetListBody(ctx, key); ok {
		if listBlobRangeReply(ctx.enc(), body, start, stop) {
			return
		}
	}

	var (
		wrongTyp bool
		out      [][]byte
	)
	if !ctx.view(func(db *keyspace.DB) error {
		hdr, found, err := listHeader(db, key)
		if err != nil {
			return err
		}
		if !found {
			return nil
		}
		if hdr.Type != keyspace.TypeList {
			wrongTyp = true
			return nil
		}
		// A btree-backed list reads only the requested window by seeking the cursor;
		// a blob decodes and slices.
		if hdr.IsColl() {
			out, err = listTreeRange(db, key, start, stop)
			return err
		}
		elems, _, _, err := getList(db, key)
		if err != nil {
			return err
		}
		out = listSlice(elems, start, stop)
		return nil
	}) {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	ctx.enc().WriteBulkArray(out)
}

// listRangeBounds resolves the LRANGE index rules to a clamped inclusive
// [lo, hi]: negative indices count from the tail, lo floors at zero, and hi caps
// at the last element. An empty range is reported by lo > hi at the call site.
func listRangeBounds(start, stop, n int64) (lo, hi int64) {
	if start < 0 {
		start += n
	}
	if stop < 0 {
		stop += n
	}
	if start < 0 {
		start = 0
	}
	if stop >= n {
		stop = n - 1
	}
	return start, stop
}

// listSlice resolves the LRANGE index rules: negative indices count from the
// tail, the range is inclusive and clamped, and an empty range yields nil.
func listSlice(elems [][]byte, start, stop int64) [][]byte {
	n := int64(len(elems))
	lo, hi := listRangeBounds(start, stop, n)
	if lo > hi || n == 0 {
		return nil
	}
	return elems[lo : hi+1]
}
