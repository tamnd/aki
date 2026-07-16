// Fault injection (spec 2064/obs1 doc 10, the E-sim fault schedules).
// A FaultFn is consulted once per Store operation and speaks the surface
// language: the error the caller would see after the wire client's retry
// loop gave its verdict. Schedules are plain code, so a lab can script
// SlowDown storms, outage windows, or one surgical ambiguous PUT, and a
// deterministic schedule keeps the whole run deterministic.
package sim

import "time"

// Op classifies a Store call for fault decisions.
type Op int

const (
	OpGet Op = iota + 1
	OpPut
	OpPutIfAbsent
	OpPutIfMatch
	OpDelete
	OpDeleteObjects
	OpCreateMultipart
	OpUploadPart
	OpCompleteMultipart
	OpAbortMultipart
)

// Fault is one injected outcome. Err nil with Extra set is a latency
// storm: the op succeeds, slowly. Err set fails the op with exactly that
// error (wrap the obs1 sentinels so errors.Is keeps working); Applied then
// says the mutation landed before the response was lost, the ambiguous-PUT
// shape doc 02 section 2.4's Recheck exists for. Applied on a conditional
// write still respects the condition, because a write that would have
// 412ed never lands no matter when the wire died.
type Fault struct {
	Err     error
	Applied bool
	Extra   time.Duration
}

// FaultFn decides the fate of one operation; nil means run it clean.
type FaultFn func(op Op, key string) *Fault
