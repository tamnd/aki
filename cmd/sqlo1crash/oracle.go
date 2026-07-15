package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/rand"
	"sort"
	"time"
)

// The shadow oracle is the harness's own record of what the server must
// hold: for every key, the last acknowledged state, plus the one op that
// was in flight when the process died. An acked write is a promise; the op
// with no reply is the only place where either outcome is legal, and the
// verifier accepts exactly those two states and nothing else.

// pendingOp is the op a worker had sent but never got a reply for when the
// server was killed. del means the op was DEL; otherwise val is the SET
// payload.
type pendingOp struct {
	del bool
	val []byte
}

// keyState is one key's oracle entry. acked nil means the key is absent in
// the last acknowledged state.
type keyState struct {
	acked   []byte
	pending *pendingOp
}

// verdict classifies one key after restart.
type verdict int

const (
	// verdictMatch: the observed state equals the last acknowledged state,
	// which also covers a pending op that was not applied.
	verdictMatch verdict = iota
	// verdictPendingApplied: the observed state equals the outcome of the
	// in-flight op, the other legal answer for an unacknowledged write.
	verdictPendingApplied
	// verdictLost: an acknowledged value is gone. A durable store failed
	// its promise; a memory store is expected to do this on every kill.
	verdictLost
	// verdictCorrupt: the observed state matches neither the acknowledged
	// state nor the in-flight op. No store may ever do this: a value from
	// a past generation, a foreign key's bytes, or a resurrected delete
	// all land here.
	verdictCorrupt
)

// classify decides the verdict for one key given what GET returned after
// restart.
func classify(st keyState, observed []byte, found bool) verdict {
	if !found {
		if st.acked == nil {
			return verdictMatch
		}
		if st.pending != nil && st.pending.del {
			return verdictPendingApplied
		}
		return verdictLost
	}
	if st.acked != nil && bytes.Equal(observed, st.acked) {
		return verdictMatch
	}
	if st.pending != nil && !st.pending.del && bytes.Equal(observed, st.pending.val) {
		return verdictPendingApplied
	}
	return verdictCorrupt
}

// worker owns a disjoint slice of the keyspace and its oracle entries, so
// the load phase needs no locks and the ack order per key is total.
type worker struct {
	id      int
	keys    [][]byte
	states  []keyState
	rng     *rand.Rand
	version int
	ops     int
}

func newWorker(id, keys int, seed int64) *worker {
	w := &worker{
		id:     id,
		keys:   make([][]byte, keys),
		states: make([]keyState, keys),
		rng:    rand.New(rand.NewSource(seed)),
	}
	for i := range w.keys {
		w.keys[i] = fmt.Appendf(nil, "w%d-k%d", id, i)
	}
	return w
}

// opDeadline bounds every round trip so a worker blocked on a dead socket
// returns instead of hanging the iteration.
const opDeadline = 5 * time.Second

// run hammers the server until the connection breaks, which is how a
// worker learns the kill happened. Mix: 70% SET, 25% DEL, 5% EXPIRE with a
// far-future TTL so nothing expires inside a run and expiry never blurs
// the value oracle. Every reply is checked against the oracle live; a
// mismatch while the server is up is a correctness failure on the spot,
// not an ambiguity.
func (w *worker) run(rc *respConn) error {
	for {
		i := w.rng.Intn(len(w.keys))
		st := &w.states[i]
		p := w.rng.Float64()
		switch {
		case p < 0.70:
			w.version++
			val := fmt.Appendf(nil, "w%d-k%d-v%d", w.id, i, w.version)
			rep, err := rc.do(opDeadline, []byte("SET"), w.keys[i], val)
			if err != nil {
				st.pending = &pendingOp{val: val}
				return nil
			}
			if rep.kind != '+' || rep.s != "OK" {
				return fmt.Errorf("SET %s: reply %+v, want +OK", w.keys[i], rep)
			}
			st.acked = val
		case p < 0.95:
			want := int64(0)
			if st.acked != nil {
				want = 1
			}
			rep, err := rc.do(opDeadline, []byte("DEL"), w.keys[i])
			if err != nil {
				st.pending = &pendingOp{del: true}
				return nil
			}
			if rep.kind != ':' || rep.n != want {
				return fmt.Errorf("DEL %s: reply %+v, want :%d", w.keys[i], rep, want)
			}
			st.acked = nil
		default:
			want := int64(0)
			if st.acked != nil {
				want = 1
			}
			rep, err := rc.do(opDeadline, []byte("EXPIRE"), w.keys[i], []byte("100000"))
			if err != nil {
				// EXPIRE with a far-future TTL changes no value state, so
				// there is nothing to record as pending.
				return nil
			}
			if rep.kind != ':' || rep.n != want {
				return fmt.Errorf("EXPIRE %s: reply %+v, want :%d", w.keys[i], rep, want)
			}
		}
		w.ops++
	}
}

// digest is the keyspace checksum: sha256 over the sorted key=value lines
// of a keyspace map. Two keyspaces are identical exactly when their
// digests match, and the diff of the two maps names the divergent keys.
func digest(m map[string][]byte) string {
	lines := make([]string, 0, len(m))
	for k, v := range m {
		lines = append(lines, k+"="+string(v))
	}
	sort.Strings(lines)
	h := sha256.New()
	for _, l := range lines {
		h.Write([]byte(l))
		h.Write([]byte{'\n'})
	}
	return hex.EncodeToString(h.Sum(nil))
}
