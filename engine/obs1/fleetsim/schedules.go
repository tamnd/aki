package fleetsim

import (
	"errors"
	"fmt"

	"github.com/tamnd/aki/engine/obs1/sim"
)

// ErrInjected roots every scripted fault so tests tell schedule damage
// from real bugs with errors.Is.
var ErrInjected = errors.New("fleetsim: injected fault")

// WriteOutage fails every mutation and leaves reads clean, the doc 02
// section 6 bucket-write-outage mode: nobody appends, so nobody folds
// anything new, nobody fences, and ownership freezes exactly where the
// chain left it.
func WriteOutage() sim.FaultFn {
	return func(op sim.Op, key string) *sim.Fault {
		if op == sim.OpGet {
			return nil
		}
		return &sim.Fault{Err: fmt.Errorf("%w: write outage on %s", ErrInjected, key)}
	}
}

// ReadOutage fails every GET and leaves writes clean: followers stall
// on their own cursors while holders keep appending, and nothing
// diverges because nobody folds what they cannot read.
func ReadOutage() sim.FaultFn {
	return func(op sim.Op, key string) *sim.Fault {
		if op != sim.OpGet {
			return nil
		}
		return &sim.Fault{Err: fmt.Errorf("%w: read outage on %s", ErrInjected, key)}
	}
}

// Storm fails every nth operation, the surface verdict of a SlowDown
// wave the wire client's capped backoff could not outlast: transient,
// scattered, and survivable inside a lease TTL. The counter makes the
// schedule deterministic; the sim serializes fault decisions under its
// own lock.
func Storm(nth int) sim.FaultFn {
	count := 0
	return func(op sim.Op, key string) *sim.Fault {
		count++
		if count%nth != 0 {
			return nil
		}
		return &sim.Fault{Err: fmt.Errorf("%w: storm dropped %s", ErrInjected, key)}
	}
}
