// Package reactoroffload is the lab behind handing a heavy set-algebra read off the epoll reactor loop
// to a worker goroutine (offloadSetAlgebra in f1srv/set_algebra.go) instead of running it inline on the
// loop. It isolates the loop-starvation the inline path caused: the reactor drains every command in a
// batch on the single loop goroutine, so when one connection issues a large SINTER/SUNION/SDIFF/
// SINTERCARD the loop runs that command's whole compute and writes its multibulk reply before it can
// service any other connection it owns. That is where the inline reactor SINTER dipped to 0.62x of Redis
// at the 256-member size while flat SET, whose reply is tiny, held 2.74x: the point ops on the same loop
// waited behind the heavy op's compute and reply.
//
// # What the lab models
//
// It runs one "loop" goroutine over a stream of events that mixes many light ops (a GET-shaped key hash
// plus a small reply copy, the cheap point commands that dominate a real batch) with an occasional heavy
// op (a sorted-hash two-pointer intersect of two 256-member sources that materializes the matched
// members into a reply buffer, the shape setMergeCollect emits). Every few dozen light ops one heavy op
// arrives, the frequency a set-algebra command mixed into a point-op workload takes.
//
//   - inline: the loop runs heavyWork itself, exactly as the reactor drains a command today, so every
//     light op queued behind a heavy one waits the heavy op's full compute-plus-reply time.
//   - offload: the loop hands the heavy op to a worker pool and moves straight on to the next light op,
//     so the light ops never wait behind the heavy compute; the heavy work runs on another goroutine the
//     Go scheduler places on another core.
//
// # What it measures
//
// The default ns/op is the loop goroutine's own busy time over the stream: inline it is lights + heavies,
// offload it is lights + the per-heavy handoff, so offload's loop occupancy is far lower even though the
// heavy work still happens (on the pool). The reported light-makespan-ns metric is the more direct one:
// the time from the start of the stream to when the loop finishes servicing the last light op. Inline
// that makespan carries every heavy op's compute the loop ran along the way; offload it collapses to
// roughly the light work alone, which is the loop responsiveness the offload restores. The pool drains
// before the timer stops each iteration only through the counter check in the test, not the benchmark,
// so the benchmark's loop-occupancy figure reflects a freed loop and the test proves no heavy work was
// dropped.
package reactoroffload
