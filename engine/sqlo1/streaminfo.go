package sqlo1

// The stream root surface, doc 10's O(1) rung: XSETID rewrites the
// root's ID and counter fields, and XINFO STREAM answers from them.
// Both touch the root plus at most one tail-run read for the top-item
// wall, never a fence walk, so the cost is flat however long the
// stream grows.

import (
	"context"
	"errors"
	"math"
)

// The XSETID validation texts, Redis 8.8's exactly; storeErr adds the
// ERR prefix.
var (
	errStreamNoKey = errors.New("no such key")
	errXsetidTop   = errors.New("The ID specified in XSETID is smaller than the target stream top item")
	errXsetidAdded = errors.New("The entries_added specified in XSETID is smaller than the target stream length")
)

// streamInfo is the XINFO STREAM summary read off the root: every
// field is maintained, nothing is counted.
type streamInfo struct {
	count  uint64
	added  uint64
	groups uint32
	// geom synthesizes the radix-tree-keys reply: runs on the flat
	// fence, pages once paged, monotone and plausible since nothing
	// depends on the exact value.
	geom   int64
	last   streamID
	maxDel streamID
}

// Info reads key's root for XINFO STREAM. A missing key is Redis's
// no-such-key error, the one lookup XINFO makes.
func (x *Stream) Info(ctx context.Context, key []byte) (streamInfo, error) {
	exists, _, err := x.stateOf(ctx, key)
	if err != nil {
		return streamInfo{}, err
	}
	if !exists {
		return streamInfo{}, errStreamNoKey
	}
	r := &x.root
	geom := int64(len(x.fence))
	if r.paged {
		geom = int64(len(r.pidx))
	}
	return streamInfo{
		count:  r.count,
		added:  r.added,
		groups: r.groupCount,
		geom:   geom,
		last:   r.last,
		maxDel: r.maxDel,
	}, nil
}

// SetID is XSETID: it moves the last generated ID and optionally the
// entries-added counter and max-deleted-ID. The wall is the top item
// actually in the stream, not the current last generated ID, so an
// emptied stream accepts any ID, downward included, and a populated
// one accepts anything at or above its newest live entry. The
// entries-added floor is the live count, the root codec's own
// invariant.
func (x *Stream) SetID(ctx context.Context, key []byte, id streamID, setAdded bool, added uint64, setMaxDel bool, maxDel streamID) error {
	exists, expMs, err := x.stateOf(ctx, key)
	if err != nil {
		return err
	}
	if !exists {
		return errStreamNoKey
	}
	r := &x.root
	if r.count > 0 {
		top, err := x.topEntryID(ctx)
		if err != nil {
			return err
		}
		if id.less(top) {
			return errXsetidTop
		}
	}
	if setAdded && added < r.count {
		return errXsetidAdded
	}
	r.last = id
	if setAdded {
		r.added = added
	}
	if setMaxDel {
		r.maxDel = maxDel
	}
	if err := x.writeRoot(ctx, key); err != nil {
		return err
	}
	return x.restamp(ctx, key, expMs)
}

// topEntryID reads the newest live entry's ID, XSETID's wall: the tail
// run of the flat fence or of the last page, one record read. The
// caller guards the empty stream.
func (x *Stream) topEntryID(ctx context.Context) (streamID, error) {
	if x.root.paged {
		if err := x.loadPage(ctx, len(x.root.pidx)-1); err != nil {
			return streamID{}, err
		}
	}
	fe := x.fence[len(x.fence)-1]
	v, err := x.readRun(ctx, fe.segid)
	if err != nil {
		return streamID{}, err
	}
	var top streamID
	_, err = walkStreamRun(v, func(_ int, e streamEntry) error {
		if !e.dead {
			top = e.id
		}
		return nil
	})
	return top, err
}

// EntryPeek reads one boundary entry, the summary's first-entry and
// last-entry fields and the recorded-first-entry-id beside them. found
// is false on an empty stream; a missing key cannot reach here since
// Info gates it. fv is only valid during emit's usual scratch window,
// so emit renders immediately.
func (x *Stream) EntryPeek(ctx context.Context, key []byte, rev bool, emit func(id streamID, fv [][]byte)) (found bool, err error) {
	full := streamID{ms: math.MaxUint64, seq: math.MaxUint64}
	err = x.Range(ctx, key, streamID{}, full, 1, rev, func(int) {}, func(id streamID, fv [][]byte) {
		found = true
		emit(id, fv)
	})
	return found, err
}
