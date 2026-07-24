// The chain append loop and tail follower (spec 2064/obs1 doc 02
// sections 2.3 to 2.5), with the two amendments the chain-append lab
// measured (labs/obs1/o0b/01_chainappend, PR #899): no sleep between CAS
// retries, because the mandatory catch-up GET already paces the loop at
// one round trip, and probe-first catch-up once a 412 has proven a race,
// because a losing blind PUT wastes a full round trip before the node
// even learns it lost. On the doc 01 latency model probe-first is what
// lets the chain hold 16-node design load at all.
package obs1

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// ChainApplier consumes committed chain batches in dense seq order,
// exactly once each, our own and everyone else's alike. The lease fold
// implements this; an error poisons the appender's view of the chain and
// stops it before the tail advances past the failed batch.
type ChainApplier interface {
	ApplyChain(pos ChainPos, h Header, batch ChainBatch) error
}

// ChainAppender owns one node's view of one log domain: the last seq it
// has applied, the batch id counter, and the append loop. One appender
// per domain per process; the mutex only guards against accidental
// concurrent use, the design point is one append in flight per node
// (doc 02 section 2.6).
type ChainAppender struct {
	mu          sync.Mutex
	store       Store
	prefix      string
	dd          uint8
	writer      uint64
	incarnation uint32
	apply       ChainApplier
	tail        uint64
	nextBatch   uint64
}

// NewChainAppender starts an appender at start, the checkpoint's Through
// position or the zero value for a chain replayed from its first object.
func NewChainAppender(s Store, prefix string, dd uint8, writer uint64, incarnation uint32, start ChainPos, apply ChainApplier) (*ChainAppender, error) {
	if s == nil || apply == nil {
		return nil, fmt.Errorf("obs1: chain appender needs a store and an applier")
	}
	if writer == 0 {
		return nil, fmt.Errorf("obs1: chain appender needs a nonzero writer id")
	}
	if start.Seq > 0 && start.DD != dd {
		return nil, fmt.Errorf("obs1: chain appender for domain %d cannot start at %d/%d", dd, start.DD, start.Seq)
	}
	return &ChainAppender{
		store:       s,
		prefix:      prefix,
		dd:          dd,
		writer:      writer,
		incarnation: incarnation,
		apply:       apply,
		tail:        start.Seq,
		nextBatch:   1,
	}, nil
}

// Incarnation reports what this appender stamps on every batch, the
// half the doc 02 section 4.5 fence compares against the member table.
func (a *ChainAppender) Incarnation() uint32 {
	return a.incarnation
}

// Tail is the last position applied.
func (a *ChainAppender) Tail() ChainPos {
	a.mu.Lock()
	defer a.mu.Unlock()
	return ChainPos{DD: a.dd, Seq: a.tail}
}

// Follow reads forward from the tail, applying every batch, until the
// 404 that says the tail is reached (doc 02 section 2.3; S3 strong
// consistency makes it trustworthy).
func (a *ChainAppender) Follow(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	for {
		seq := a.tail + 1
		b, _, err := a.store.Get(ctx, chainKey(a.prefix, a.dd, seq))
		if errors.Is(err, ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		if _, err := a.applyFetched(seq, b, ChainBatch{}); err != nil {
			return err
		}
	}
}

// Append commits one batch of records to the chain and returns where it
// landed. Every batch that turns out to occupy a contended seq is applied
// on the way, so the applier's order stays dense. The loop is doc 02
// section 2.4 with the lab's amendments: after the first 412 on this
// append, every attempt GETs the target seq before PUTting, and no
// attempt ever sleeps.
func (a *ChainAppender) Append(ctx context.Context, records []ChainRecord) (ChainPos, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	batch := ChainBatch{BatchID: a.nextBatch, Incarnation: a.incarnation, Records: records}
	body, err := AppendChainBatch(nil, a.writer, batch)
	if err != nil {
		return ChainPos{}, err
	}
	a.nextBatch++
	tag := WriteTag{Writer: fmt.Sprintf("%016x", a.writer), Batch: seq16(batch.BatchID)}
	contended := false
	for {
		seq := a.tail + 1
		key := chainKey(a.prefix, a.dd, seq)
		if contended {
			got, _, gerr := a.store.Get(ctx, key)
			if gerr == nil {
				ours, aerr := a.applyFetched(seq, got, batch)
				if aerr != nil {
					return ChainPos{}, aerr
				}
				if ours {
					return ChainPos{DD: a.dd, Seq: seq}, nil
				}
				continue
			}
			if !errors.Is(gerr, ErrNotFound) {
				return ChainPos{}, gerr
			}
		}
		_, perr := a.store.PutIfAbsent(ctx, key, body, tag)
		switch {
		case perr == nil:
			if _, err := a.applyFetched(seq, body, batch); err != nil {
				return ChainPos{}, err
			}
			return ChainPos{DD: a.dd, Seq: seq}, nil
		case errors.Is(perr, ErrPrecondition):
			// Someone else won the seq, or an earlier ambiguous attempt of
			// this very batch landed and this retry collided with our own
			// object; applyFetched tells the two apart.
			contended = true
			got, _, gerr := a.store.Get(ctx, key)
			if errors.Is(gerr, ErrNotFound) {
				continue
			}
			if gerr != nil {
				return ChainPos{}, gerr
			}
			ours, aerr := a.applyFetched(seq, got, batch)
			if aerr != nil {
				return ChainPos{}, aerr
			}
			if ours {
				return ChainPos{DD: a.dd, Seq: seq}, nil
			}
		case errors.Is(perr, ErrConflict), errors.Is(perr, ErrAmbiguous):
			// The mandatory re-check: a timed-out PUT may have landed. Ours
			// means done; someone else's means apply and race for the next
			// seq; 404 means nothing landed and the same seq is still open.
			got, _, gerr := a.store.Get(ctx, key)
			if errors.Is(gerr, ErrNotFound) {
				continue
			}
			if gerr != nil {
				return ChainPos{}, gerr
			}
			ours, aerr := a.applyFetched(seq, got, batch)
			if aerr != nil {
				return ChainPos{}, aerr
			}
			if ours {
				return ChainPos{DD: a.dd, Seq: seq}, nil
			}
			contended = true
		default:
			return ChainPos{}, perr
		}
	}
}

// applyFetched parses one chain object, hands it to the applier, and
// advances the tail. It reports whether the object is the batch this
// appender was trying to land, by the doc 02 section 2.4 recognition
// pair widened with the incarnation: a rebooted node restarts its batch
// counter, so writer id and batch id alone could claim a previous life's
// object. A zero want never matches (Follow's case).
func (a *ChainAppender) applyFetched(seq uint64, b []byte, want ChainBatch) (bool, error) {
	got, h, err := ParseChainBatch(b)
	if err != nil {
		return false, fmt.Errorf("obs1: chain %s: %w", chainKey(a.prefix, a.dd, seq), err)
	}
	if err := a.apply.ApplyChain(ChainPos{DD: a.dd, Seq: seq}, h, got); err != nil {
		return false, err
	}
	a.tail = seq
	ours := want.BatchID != 0 && h.Writer == a.writer &&
		got.Incarnation == want.Incarnation && got.BatchID == want.BatchID
	return ours, nil
}

// LoadCheckpoint fetches and parses one checkpoint object.
func LoadCheckpoint(ctx context.Context, s Store, prefix string, dd uint8, seq uint64) (Checkpoint, Header, error) {
	key := chainCkptKey(prefix, dd, seq)
	b, _, err := s.Get(ctx, key)
	if err != nil {
		return Checkpoint{}, Header{}, err
	}
	c, h, err := ParseCheckpoint(b)
	if err != nil {
		return Checkpoint{}, Header{}, fmt.Errorf("obs1: checkpoint %s: %w", key, err)
	}
	return c, h, nil
}

// BootChain is the doc 02 section 2.5 cold boot for one domain: read the
// root, read the checkpoint it points at, and return an appender primed
// at the checkpoint's Through position along with the checkpoint itself.
// The caller primes its fold from the returned checkpoint's tables FIRST
// and then calls Follow to replay the chain from there to the tail; a
// root that points at no checkpoint yet returns a zero checkpoint and a
// replay from the chain's first object. Per invariant C-I7 the
// checkpoint is a summary, never authority: everything it says is
// re-derivable by replaying more chain.
func BootChain(ctx context.Context, s Store, prefix string, fallback bool, dd uint8, writer uint64, incarnation uint32, apply ChainApplier) (*ChainAppender, Checkpoint, error) {
	root, err := LoadRoot(ctx, s, prefix, fallback)
	if err != nil {
		return nil, Checkpoint{}, err
	}
	var ckpt Checkpoint
	var start ChainPos
	if root.CkptSeq != 0 {
		if root.CkptDD != uint16(dd) {
			return nil, Checkpoint{}, fmt.Errorf("obs1: root checkpoint is in domain %d, booting domain %d", root.CkptDD, dd)
		}
		ckpt, _, err = LoadCheckpoint(ctx, s, prefix, dd, root.CkptSeq)
		if err != nil {
			return nil, Checkpoint{}, err
		}
		start = ckpt.Through
	}
	a, err := NewChainAppender(s, prefix, dd, writer, incarnation, start, apply)
	if err != nil {
		return nil, Checkpoint{}, err
	}
	return a, ckpt, nil
}
