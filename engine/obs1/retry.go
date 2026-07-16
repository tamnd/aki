// The retry policy is data (milestone O0a slice 2): the numbers live in one
// struct the labs can sweep, and the loop that applies them is dumb.
package obs1

import (
	"context"
	"math/rand/v2"
	"time"
)

// RetryPolicy shapes the client's replay of retryable failures (see
// retryable in s3error.go for what qualifies; CAS conditions never enter
// this loop). Backoff is exponential from Base with full jitter, capped at
// Cap; a SlowDown response starts from SlowBase instead, because a throttle
// answered fast and hammering it back is the one thing that makes it worse.
type RetryPolicy struct {
	Attempts int           // total tries, first one included
	Base     time.Duration // backoff scale after the first failure
	SlowBase time.Duration // backoff scale after a SlowDown
	Cap      time.Duration // upper bound on any one sleep
}

// DefaultRetry is the starting point; the O0a connpool lab and the O5 cloud
// runs adjust it with numbers, not taste.
var DefaultRetry = RetryPolicy{
	Attempts: 4,
	Base:     20 * time.Millisecond,
	SlowBase: 200 * time.Millisecond,
	Cap:      2 * time.Second,
}

// backoff is the sleep before attempt n (n counts from 1 = first retry):
// full jitter over an exponentially growing window, so concurrent losers
// spread out instead of re-colliding (doc 01 section 9 hedging notes).
func (p RetryPolicy) backoff(n int, slow bool) time.Duration {
	base := p.Base
	if slow {
		base = p.SlowBase
	}
	window := base << (n - 1)
	if window > p.Cap || window <= 0 {
		window = p.Cap
	}
	return time.Duration(rand.Int64N(int64(window) + 1))
}

// sleep waits d or until ctx is done, whichever is first.
func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
