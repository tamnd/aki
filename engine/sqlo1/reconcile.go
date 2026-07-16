package sqlo1

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
)

// Rule W3 helpers (doc 06 section 5). A store that elides count-only
// root frames under rule W2 must rebuild those roots at replay by
// diffing segment pre-images against the replayed post-images and
// folding the delta into the last durable root image. The store stays
// payload-blind everywhere else; these three functions are the one
// sanctioned window into collection payloads, and only into the header
// fields reconciliation needs.
//
// A collection type earns W2 elision by teaching ReconcileRef its root
// sub byte and laying its records out so these functions apply: the
// segment payload leads with the doc 06 section 2.4 header (u16 entry
// count, u16 reserved, u64 min_expire_ms) and the segmented root
// carries count at offset 16 and min_expire_ms at offset 32 per
// section 2.2. The segmented hash is the only claimant today; a type
// that is not listed here must never see its root frames elided, and
// the store guards that by framing any root ReconcileRef does not
// recognize in full.

// ReconcileRef reports whether a root payload participates in W3
// reconciliation and returns the rooth its segments carry in their
// subkeys. Not-ok means the payload is an inline root or a type that
// has not claimed reconciliation, and its frames must stay full.
func ReconcileRef(rootValue []byte) (rooth uint64, ok bool) {
	if len(rootValue) < hashSegRootHdrLen || (rootValue[0] != hashSubSeg && rootValue[0] != setSubSeg) {
		return 0, false
	}
	return binary.LittleEndian.Uint64(rootValue[8:]), true
}

// SegCounts reads the entry count and min_expire_ms out of a segment
// payload header. Not-ok means the payload is too short to be a
// reconcilable segment; replay treats such a record as carrying no
// countable entries, which is what makes records that share the
// RecSeg envelope without the header (rope chunks) harmless to the
// walk. The unrecognized ones can never patch a root because the
// patch target must pass ReconcileRef first.
func SegCounts(segValue []byte) (n int, minExpireMs int64, ok bool) {
	if len(segValue) < hashSegHdrLen {
		return 0, 0, false
	}
	return int(binary.LittleEndian.Uint16(segValue)),
		int64(binary.LittleEndian.Uint64(segValue[4:])), true
}

// ReconcileFence lists the segids a reconcilable root references.
// Replay uses it to classify a plane's tail window past its last
// durable root image: segment frames confined to fenced segids are a
// count-only window and patch through ReconcileRoot, while a frame
// for an unfenced segid or a segment delete means a structural change
// lost its root frame to the crash. No count repair can make a stale
// fence route reads to a relaid segment set, so recovery rolls such a
// plane back to its last rooted batch instead of patching. A paged
// root keeps its fence in page records, so this answers not-ok for it
// and replay goes through ReconcilePages plus FencePageSegids instead.
func ReconcileFence(rootValue []byte) ([]uint64, bool) {
	r, err := decodeHashSegRoot(rootValue, nil, nil)
	if err != nil || r.paged {
		return nil, false
	}
	ids := make([]uint64, len(r.fence))
	for i, e := range r.fence {
		ids[i] = e.segid
	}
	return ids, true
}

// ReconcilePages lists the fence pageids a paged reconcilable root
// references. Not-paged means the root keeps its fence inline and
// ReconcileFence applies; an error means the payload does not decode
// as a segmented root at all, which replay treats the same way
// ReconcileFence's not-ok is treated for count patching: the frame
// must have been full, so nothing needs rebuilding.
func ReconcilePages(rootValue []byte) (pageids []uint64, paged bool, err error) {
	r, err := decodeHashSegRoot(rootValue, nil, nil)
	if err != nil {
		return nil, false, err
	}
	if !r.paged {
		return nil, false, nil
	}
	ids := make([]uint64, len(r.pidx))
	for i, e := range r.pidx {
		ids[i] = e.pageid
	}
	return ids, true, nil
}

// FencePageSegids lists the segids one fence page record references.
// Replay resolves each pageid from ReconcilePages to a page image (the
// latest tail image at or before the root frame, else the committed
// record) and unions these lists into the paged root's fenced set. The
// page decodes without a nextSegid bound because the walk has no root
// header in hand; the root that referenced the page already vouched
// for its pageid, and a corrupt segid only widens the fenced set the
// same way a corrupt fence entry would, which the segment keep scan
// tolerates.
func FencePageSegids(pageValue []byte) ([]uint64, error) {
	ents, err := decodeHashFencePage(pageValue, nil, 0)
	if err != nil {
		return nil, err
	}
	ids := make([]uint64, len(ents))
	for i, e := range ents {
		ids[i] = e.segid
	}
	return ids, nil
}

// ReconcileRoot patches a durable root image with the entry-count
// delta and lowest post-image min_expire the replay walk accumulated
// past it. The count adjusts exactly; min_expire only lowers, because
// a count-only window can add or tighten TTLs but a raise would need
// the full-image knowledge W2 already frames (doc 06, H-I6 allows
// stale-early and never stale-late). The input image is validated
// before patching and a delta that would empty or overflow the root
// is an error: a durable segmented root always has fields, so an
// impossible count means the tail and the data file disagree and
// recovery must fail loudly rather than write a lie.
func ReconcileRoot(rootValue []byte, dn int64, minExpireMs int64) ([]byte, error) {
	r, err := decodeHashSegRoot(rootValue, nil, nil)
	if err != nil {
		return nil, err
	}
	if r.count > math.MaxInt64 {
		return nil, fmt.Errorf("sqlo1: reconciling a root with count %d out of int64 range", r.count)
	}
	count := int64(r.count) + dn
	if count <= 0 {
		return nil, fmt.Errorf("sqlo1: reconciled count %d%+d leaves %d, a durable segmented root cannot be empty", r.count, dn, count)
	}
	out := bytes.Clone(rootValue)
	binary.LittleEndian.PutUint64(out[16:], uint64(count))
	if minExpireMs > 0 && (r.minExpMs == 0 || minExpireMs < r.minExpMs) {
		binary.LittleEndian.PutUint64(out[32:], uint64(minExpireMs))
		out[1] |= hflagAnyTTL
	}
	return out, nil
}
