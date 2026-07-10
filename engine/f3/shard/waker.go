package shard

import "sync/atomic"

// The consumer states the wake rule keys on (doc 03 section 9.1). A consumer
// that runs out of work stores stateSpinning, burns the spin window on plain
// loads, then stores stateParked and blocks; a producer that just published
// loads the state word (one uncontended read) and pays the wake only when it
// observes parked, claiming it with one CAS so racing producers issue one
// token between them. Under saturation the consumer is never parked and the
// producer's wake path is that single load.
const (
	stateRunning uint32 = iota
	stateSpinning
	stateParked
)

// waker pairs the state word with the park channel. The channel stands in for
// the raw futex of doc 03 section 9.1: a parked consumer blocks on a receive
// and the claiming producer sends the one token. It has capacity one so the
// send never blocks the producer.
type waker struct {
	state atomic.Uint32
	ch    chan struct{}
}

func (w *waker) init() {
	w.ch = make(chan struct{}, 1)
}

// wake is the producer side, called after work is published. The claim CAS
// makes the token exactly-once: a producer that loses the race knows another
// producer's token is already in flight. The return reports whether this call
// sent the token, so callers can count real cross-goroutine wakeups (the doc
// 08 section 9.5 counters) without charging the common single-load path.
func (w *waker) wake() bool {
	if w.state.Load() == stateParked && w.state.CompareAndSwap(stateParked, stateRunning) {
		w.ch <- struct{}{}
		return true
	}
	return false
}

// park blocks the consumer after it has stored stateParked and re-checked its
// queue: either that re-check saw the work, or the producer's state load sees
// parked and sends the token, so no publication can fall between the two.
func (w *waker) park() {
	<-w.ch
}

// unparkSelf is the consumer taking itself out of parked after its post-store
// re-check found work. When the CAS fails a producer claimed the wake first,
// so its token must be consumed or it would satisfy the next park spuriously.
func (w *waker) unparkSelf() {
	if !w.state.CompareAndSwap(stateParked, stateRunning) {
		<-w.ch
	}
}
