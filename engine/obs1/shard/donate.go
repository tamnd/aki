package shard

import (
	"runtime"
	"sync/atomic"
)

// Worker donation (spec 2064/f3/11 section 6.5, doc 03 section 6). A tier-two
// or single-owner command whose work splits into independent read-only tasks
// can fan those tasks out to idle workers while the operands are frozen: for a
// co-located command the freeze is the coordinating owner itself, blocked in
// its handler for the command's duration, and for a cross-shard command it is
// the F17 intent barrier (intent.go). The donated tasks are pure reads over
// memory the coordinator owns, so the only synchronization is the job's claim
// and completion counters, purchased once per command instead of per read.
//
// The shape is a shared claim cursor, not per-donee queues, and that is the
// liveness argument: the coordinator never waits for a donee to pick anything
// up. It offers the job, then claims and runs tasks off the same cursor until
// the cursor is exhausted, so a donee that stays busy simply contributes
// nothing and the fan-out degrades to the sequential loop. The coordinator's
// final wait covers only claimed tasks, and a claimed task is executed
// immediately by its claimer with nothing to block on, so the wait is bounded
// by one task body. Two coordinators fanning out concurrently cannot wait on
// each other for the same reason: each finishes its own unclaimed work itself.
//
// Fairness is the doc's bound: a donee runs donated tasks only between its own
// batches and re-checks its inbound queue after every task, so donated work
// adds at most one task's latency to that worker's own tail (section 6.5). The
// half-the-pool cap is applied at offer time, and only workers observed idle
// are offered, so algebra fan-out never conscripts a worker that is serving
// point-op traffic.

// donateJob is one fan-out: n independent tasks, claimed off a shared cursor.
// run must be safe to call concurrently for distinct k, must only read state
// the coordinator has frozen (plus write its own k-indexed output slot), and
// must not block or fan out further; every consumer in the tree keeps to leaf
// kernels.
type donateJob struct {
	run  func(k int)
	n    int32
	next atomic.Int32 // claim cursor: the next unclaimed task index
	done atomic.Int32 // completed tasks; n means the job is finished
}

// step claims and runs one task, reporting whether a task was left to claim.
// The claim is the atomic add, so each k runs exactly once across every helper
// and the coordinator.
func (j *donateJob) step() bool {
	k := j.next.Add(1) - 1
	if k >= j.n {
		return false
	}
	j.run(int(k))
	j.done.Add(1)
	return true
}

// complete reports whether every task has finished. The done load is the
// acquire edge that publishes the tasks' writes to the coordinator.
func (j *donateJob) complete() bool { return j.done.Load() >= j.n }

// helpDonated runs donated tasks between the worker's own batches. The fast
// path is one relaxed pointer load per drain pass: a worker never offered a
// job returns at once and touches nothing else. A worker with a job runs
// tasks until the job is drained or its own inbound queue has traffic, which
// is the section-6.5 fairness bound (at most one donated task's latency added
// to the worker's own tail). The slot is cleared once the cursor is exhausted
// so a finished job cannot pin its closures past the command.
func (w *worker) helpDonated() int {
	j := w.donated.Load()
	if j == nil {
		return 0
	}
	n := 0
	for {
		if !j.step() {
			w.donated.CompareAndSwap(j, nil)
			return n
		}
		n++
		if w.inbound.ready() {
			return n
		}
	}
}

// donateReady reports whether a donated job is waiting, the load the idle
// re-check folds in so a worker never parks past an offered job.
func (w *worker) donateReady() bool {
	return w.donated.Load() != nil
}

// FanOut runs n independent read-only tasks, run(0) through run(n-1), and
// returns when every one has finished. It is the donation seam a handler uses
// for partition-parallel work (the set algebra group loop, the escalated draw
// resolve): the caller's shard is frozen by the caller's own occupancy, so
// tasks may read anything the command may read. Tasks for distinct k must be
// independent; each may write only its own k-indexed output.
//
// Idle workers are offered the job up to half the pool (the section-6.5 cap);
// the coordinator then claims tasks off the same cursor, so completion never
// depends on any donee showing up. Outside a runtime (a nil or bare Ctx in
// tests, or a single-shard pool) the loop runs inline, which is also the
// serial oracle the correctness tests compare against.
func (cx *Ctx) FanOut(n int, run func(k int)) {
	if n <= 0 {
		return
	}
	if cx == nil {
		for k := 0; k < n; k++ {
			run(k)
		}
		return
	}
	w := cx.w
	if n == 1 || w == nil || w.rt == nil || len(w.rt.workers) < 2 {
		for k := 0; k < n; k++ {
			run(k)
		}
		return
	}
	j := &donateJob{run: run, n: int32(n)}
	half := len(w.rt.workers) / 2
	offered := 0
	for _, d := range w.rt.workers {
		if offered >= half || offered >= n-1 {
			break
		}
		if d == w || d.wk.state.Load() == stateRunning {
			continue
		}
		if d.donated.CompareAndSwap(nil, j) {
			d.wk.wake()
			offered++
		}
	}
	for j.step() {
	}
	for !j.complete() {
		runtime.Gosched()
	}
}
