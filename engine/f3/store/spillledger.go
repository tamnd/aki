package store

// The spill patch ledger. A spill through akiSpill returns a provisional word
// the store writes into a record's run pointer straight away, but that word
// carries a stage index, not an offset: the offset is unknown until the group
// cut. So the store cannot leave the record holding a provisional word past the
// batch, it must rewrite the pointer to the absolute log word the cut assigns.
//
// The ledger is the bridge. Every time writeRun stages a run and writePtr lands
// its provisional word at value-area offset vs, recordSpill notes (vs, index).
// At the group boundary resolveSpill cuts the batch's one segment, gets the
// absolute word per stage index back from akiSpill, and patches each recorded
// pointer in place, preserving the length and reserved capacity already written
// beside the word. After the patch no provisional word survives, so nothing on
// disk or in a later read ever meets an unresolved spill from a closed batch.
//
// This is the last store-side piece the value-log re-home needs before the
// writeRun flip: it is proven here in isolation, exercised by staging runs by
// hand and asserting the records patch, with no hot path routed onto it yet.

// spillPatch records one staged run: vs is the value-area offset of the record's
// run pointer, index is the run's position in the current batch, the index the
// resolved-word slice is keyed by.
type spillPatch struct {
	vs    uint64
	index int
}

// recordSpill notes that the provisional word for a staged run was written at
// value-area offset vs, so resolveSpill can patch that pointer once the cut
// assigns the run's offset. It is a no-op on a word that is not provisional, so
// the arena and already-published paths never enter the ledger.
func (s *Store) recordSpill(vs uint64, word uint64) {
	if !isProvisional(word) {
		return
	}
	s.spillLedger = append(s.spillLedger, spillPatch{vs: vs, index: provisionalIndex(word)})
}

// resolveSpill cuts the staged batch's one value_log segment and patches every
// recorded pointer from its provisional word to the absolute inLogBit|offset the
// cut assigned, keeping the length and capacity already stored beside each word.
// It runs at the group boundary, where the command ack already waits on the
// group fsync. An empty ledger is a no-op. The ledger is cleared whether or not
// a cut happened, so a batch never carries stale entries into the next one.
func (s *Store) resolveSpill() error {
	if len(s.spillLedger) == 0 {
		return nil
	}
	words, err := s.akispill.resolve()
	if err != nil {
		return err
	}
	for _, p := range s.spillLedger {
		if p.index >= len(words) {
			// A recorded index with no resolved word means the ledger and the
			// staged batch fell out of step, a store bug, not a data condition.
			s.spillLedger = s.spillLedger[:0]
			return errLogBroken
		}
		_, vlen, vcap := s.readPtr(p.vs)
		s.writePtr(p.vs, words[p.index], vlen, vcap)
	}
	s.spillLedger = s.spillLedger[:0]
	return nil
}
