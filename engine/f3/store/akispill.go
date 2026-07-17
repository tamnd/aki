package store

// akiSpill is the store-side batch bookkeeping that lets writeRun's synchronous
// "return a value word now" contract ride the akifile writer's "offset assigned
// at flush" reality. The scratch value log hands back an absolute offset the
// instant a value is appended, because it owns its file and the logical tail is
// where the bytes will land. The shared .aki writer assigns a value_log segment's
// offset only when the group is cut, and several shards interleave through it, so
// a shard cannot predict its offset while staging.
//
// So a spill through akiSpill returns a PROVISIONAL word: the provisional bit set
// over the value's stage index in the current batch. A reader that meets a
// provisional word before the cut resolves it from the staged pending buffer, the
// read-before-flush the scratch log served from its own buffer. At the group
// boundary, resolve cuts one value_log segment for the whole batch and returns the
// absolute log word per stage index, so the caller patches each record from its
// provisional word to inLogBit|offset. That boundary is where the command ack
// already waits on the group fsync, so publishing the offset there is free.
//
// One frame per run: writeRun can hand a two-part run (a then b) whose reader
// reads len(a)+len(b) as one contiguous blob. The scratch log stored raw bytes so
// two appends were contiguous, but akifile frames each staged value with its own
// varint length and trailing CRC, so a two-part run must be one frame. akiSpill
// concatenates a and b into one value before staging, the assembled run the
// frame's single CRC needs anyway.
//
// This is the deferred-publish mechanism proven in isolation: it is not wired into
// writeRun or the reactor, so a store built with an akifile handle still spills
// through the scratch path until a later slice routes writeRun onto it.
type akiSpill struct {
	v   *akiVlog
	buf []byte // scratch for concatenating a two-part run into one frame
}

// provisionalBit marks a run word whose value is staged in the current batch but
// not yet cut, so its absolute offset is unknown. It sits above the 48-bit offset
// and below inLogBit, and never persists: resolve replaces it with a real log word
// at the group boundary, so a provisional word never reaches disk or outlives its
// batch.
const provisionalBit = uint64(1) << 62

// provisionalIndex reports the stage index a provisional word carries.
func provisionalIndex(word uint64) int { return int(word & addrMask) }

// isProvisional reports whether a run word is an unresolved spill.
func isProvisional(word uint64) bool { return word&provisionalBit != 0 }

// newAkiSpill builds the batch bookkeeping over l.
func newAkiSpill(l *akiVlog) *akiSpill { return &akiSpill{v: l} }

// stageRun frames a run of a then b into the current batch and returns a
// provisional word. b may be nil; a non-nil b concatenates with a into one frame,
// since a run reads back as one contiguous value. The bytes are readable through
// readProvisional until the batch is flushed.
func (sp *akiSpill) stageRun(a, b []byte) uint64 {
	val := a
	if len(b) > 0 {
		sp.buf = append(sp.buf[:0], a...)
		sp.buf = append(sp.buf, b...)
		val = sp.buf
	}
	return provisionalBit | uint64(sp.v.stage(val))
}

// staged reports how many runs await the next flush, the signal a caller weighs
// against a flush threshold.
func (sp *akiSpill) staged() int { return sp.v.staged() }

// readProvisional serves the bytes of a staged run from the pending buffer before
// its segment is cut. The slice aliases the buffer, valid until the next stageRun
// or resolve.
func (sp *akiSpill) readProvisional(word uint64) ([]byte, error) {
	return sp.v.readStaged(provisionalIndex(word))
}

// resolve cuts one value_log segment for the whole batch and returns the absolute
// log word (inLogBit|offset) per stage index, so a provisional word at index i
// resolves to words[i]. An empty batch returns nil. After resolve the batch is
// reset and the returned words are durable spill pointers.
func (sp *akiSpill) resolve() ([]uint64, error) {
	ptrs, err := sp.v.flush()
	if err != nil {
		return nil, err
	}
	if len(ptrs) == 0 {
		return nil, nil
	}
	words := make([]uint64, len(ptrs))
	for i, p := range ptrs {
		words[i] = inLogBit | p.ValueOff
	}
	return words, nil
}
