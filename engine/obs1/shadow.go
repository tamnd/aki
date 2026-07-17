package obs1

// Segment shadowing (spec 2064/obs1 doc 06 section 1.3): when several live
// segments of one group hold a claim about the same key, the claim from the
// segment with the highest SegSeq wins, and a winning tombstone means the
// key is absent. SegSeq is monotone per group across epochs (boot recovery
// seeds the counter above every live segment), so the comparison never
// needs the epoch. Keymap rebuild and multi-segment reads both resolve
// through here so the rule lives in one place.

// ShadowEntry is one segment's claim about a key: the whole-record frame it
// holds, or a tombstone.
type ShadowEntry struct {
	SegSeq    uint64
	Tombstone bool

	// Frame is the record's whole cold frame, nil for a tombstone.
	Frame []byte
}

// ResolveShadow picks the winning claim: the record frame of the
// highest-SegSeq entry, or nil and false when the winner is a tombstone or
// no entry exists.
func ResolveShadow(entries []ShadowEntry) ([]byte, bool) {
	var win *ShadowEntry
	for i := range entries {
		if win == nil || entries[i].SegSeq > win.SegSeq {
			win = &entries[i]
		}
	}
	if win == nil || win.Tombstone {
		return nil, false
	}
	return win.Frame, true
}
