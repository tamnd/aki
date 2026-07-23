package shard

import (
	"strconv"

	"github.com/tamnd/aki/obs1srv/resp"
)

// The object-tier cold read seam (spec 2064/obs1 doc 05 section 5): the
// owner side of the async cold-read machinery. The engine's ColdReader owns
// the single-flight table and the GET pool; the driver that composes both
// installs a plan through SetColdPlan, the same import-boundary shape
// WriteLog and LeaseView take, and a read handler that misses the hot store
// asks Ctx.ColdGet before it settles for absent.
//
// A definitive miss stays a definitive miss: the plan consults the group's
// keymap synchronously, so a key the object tier has never held costs zero
// GETs and the handler replies absent on the spot. Only a keymap hit parks
// the client, and the park happens before the fetch launches so a completion
// can never race the slot into a half-parked state.
//
// Epoch retirement lands at delivery, on the owner: a completion for a group
// a foreign grant demoted while the GET flew is not served, the client fails
// over with the doc 07 MOVED redirect instead, and the value is dropped. The
// lease view is the ownership fact and the owner is where it lives, which is
// why the reader stays epoch-blind and the check does not.

// ColdHit is one completed fetch in shard terms: self-contained value bytes
// the shard owns outright (never a view), or an absence. An absent hit is
// the fingerprint-collision case and the tombstone case folded together;
// both read as no such key.
type ColdHit struct {
	Found bool
	Value []byte
}

// ColdLaunch starts the fetch a ColdPlan hit registered. done is invoked
// exactly once, from any goroutine, and must be safe to call after the
// runtime begins shutting down.
type ColdLaunch func(done func(ColdHit, error))

// ColdPlan consults the object-tier index for key on the owner goroutine.
// ok false is the definitive zero-GET miss, or no cold tier at all; ok true
// hands back the launcher for the record's fetch.
type ColdPlan func(key []byte) (ColdLaunch, bool)

// SetColdPlan installs the cold-read plan. Wire-once before Start, like the
// other runtime seams.
func (r *Runtime) SetColdPlan(fn ColdPlan) { r.coldPlan = fn }

// coldReadStalled is the delivery-time failure reply: the fetch itself
// failed (a GET error, a decode error, or the reader closing under the
// intent). The doc 05 section 10 taxonomy keeps read errors loud and
// retryable, never silently absent.
const coldReadStalled = "ERR store: cold read failed"

// ColdGet resolves a hot-store miss against the object tier. It reports
// false when the key is definitively absent (no plan wired, or the keymap
// says the object tier never held it), in which case the handler owns the
// absent reply. On true the command is parked and the reply arrives later
// through the CompleteBlocked loopback: the fetched value as a bulk string,
// absent as a null bulk, a fetch failure as an error, and a group demoted
// mid-flight as the doc 07 MOVED redirect. Owner goroutine only, valid only
// during the handler call, and the caller must write no reply after a true
// return.
func (cx *Ctx) ColdGet(key []byte, r Reply) bool {
	w := cx.w
	if w == nil || w.rt == nil || w.rt.coldPlan == nil {
		return false
	}
	launch, ok := w.rt.coldPlan(key)
	if !ok {
		return false
	}
	conn := cx.curConn
	seq := cx.curSeq
	rt := w.rt
	shardID := w.id
	slot := HashSlot(key)
	g := rt.groups
	if g < 1 {
		g = DefaultSlotGroups
	}
	group := uint16(groupOfSlot(slot, g))
	w.coldParks++
	r.Park()
	launch(func(h ColdHit, err error) {
		rt.PostOwner(shardID, func(cx2 *Ctx) {
			cx2.w.deliverCold(conn, seq, slot, group, h, err)
		})
	})
	return true
}

// deliverCold serves one cold completion on the owner: the epoch-retirement
// check first, then the reply shape. It builds the reply in a fresh slice
// because the parking batch's arena is long gone.
func (w *worker) deliverCold(conn *Conn, seq uint32, slot int, group uint16, h ColdHit, err error) {
	if w.leases != nil {
		if ep, ok := w.leases.Demoted(group); ok {
			w.coldDropped++
			conn.CompleteBlocked(seq, resp.AppendError(nil, "MOVED "+strconv.Itoa(slot)+" "+ep))
			return
		}
	}
	var rep []byte
	switch {
	case err != nil:
		w.coldErrs++
		rep = resp.AppendError(nil, coldReadStalled)
	case !h.Found:
		rep = resp.AppendNull(nil)
	default:
		w.coldServes++
		rep = resp.AppendBulk(nil, h.Value)
	}
	conn.CompleteBlocked(seq, rep)
}

// ColdParks is the cumulative number of commands this shard parked on a cold
// fetch; ColdServes, ColdDropped, and ColdErrs split the completions into
// values served, demoted-group drops, and fetch failures. Zero on a bare
// Ctx. Owner goroutine only.
func (cx *Ctx) ColdParks() uint64 {
	if cx.w == nil {
		return 0
	}
	return cx.w.coldParks
}

// ColdServes reports the values served off the object tier.
func (cx *Ctx) ColdServes() uint64 {
	if cx.w == nil {
		return 0
	}
	return cx.w.coldServes
}

// ColdDropped reports completions retired because their group demoted while
// the GET flew.
func (cx *Ctx) ColdDropped() uint64 {
	if cx.w == nil {
		return 0
	}
	return cx.w.coldDropped
}

// ColdErrs reports fetch failures delivered as errors.
func (cx *Ctx) ColdErrs() uint64 {
	if cx.w == nil {
		return 0
	}
	return cx.w.coldErrs
}
