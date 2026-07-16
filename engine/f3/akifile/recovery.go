package akifile

// The recovery open sequence (spec 2064/f3/07 section 6, steps 3-4): read the
// immutable prefix, pick the live meta root from the two slots, cross-check the
// shard geometry, and classify the open into one of three outcomes. This is the
// pure decision the recovery driver runs before it touches the append space; the
// tail replay and native rebuild (slice 5) build on the outcome it returns.

// OpenOutcome is how recovery must treat the file after it has picked a root.
type OpenOutcome uint8

const (
	// OpenClean is a file whose live root carries clean_shutdown: the roots are
	// trustworthy and the durable tail is the root's file_size, no replay needed.
	OpenClean OpenOutcome = iota
	// OpenCrashed is a valid root without clean_shutdown: the process died mid-run,
	// so recovery replays the append tail past the root's ckpt_log_pos to catch any
	// segments that landed after the last checkpoint.
	OpenCrashed
	// OpenScanFallback is both meta slots torn: no root to trust, so recovery falls
	// back to a full 4KiB-grid scan of the whole file. A valid recovery path, not an
	// error.
	OpenScanFallback
)

// OpenState is the result of the open sequence: the immutable prefix, the live
// root (nil in scan fallback), which slot it came from (0=A, 1=B, -1=neither),
// and the outcome that decides what recovery does next.
type OpenState struct {
	Prefix  *Prefix
	Meta    *MetaSlot
	Which   int
	Outcome OpenOutcome
}

// ReadOpenState runs the open decision against a device. It reads and validates
// the prefix (a bad magic or major stops here, recovery never guesses past it),
// reads both 128-byte meta slots from their separate sectors, and picks the live
// root. If neither slot validates, the state is a scan fallback with a nil root
// and no error, because the full scan is a legitimate recovery path. Otherwise it
// cross-checks the root's SRT shard count against the prefix (a disagreement is a
// torn SRT swap or the wrong-geometry open, ErrShardCount) and classifies the
// outcome by the root's clean_shutdown flag.
func ReadOpenState(dev Device) (*OpenState, error) {
	hb := make([]byte, PrefixSize)
	if _, err := dev.ReadAt(hb, 0); err != nil {
		return nil, err
	}
	prefix, err := ParsePrefix(hb)
	if err != nil {
		return nil, err
	}

	a := make([]byte, MetaSlotSize)
	if _, err := dev.ReadAt(a, int64(prefix.MetaSlotAOff)); err != nil {
		return nil, err
	}
	b := make([]byte, MetaSlotSize)
	if _, err := dev.ReadAt(b, int64(prefix.MetaSlotBOff)); err != nil {
		return nil, err
	}

	live, which, err := MetaLive(a, b, prefix.ChecksumKind)
	if err != nil {
		// Both slots torn: no root to trust, fall back to the full scan. This is a
		// recovery path, not a failure, so it returns a state rather than the error.
		return &OpenState{Prefix: prefix, Which: -1, Outcome: OpenScanFallback}, nil
	}

	// A live root whose SRT shard count disagrees with the prefix is a torn SRT
	// swap or a file opened under the wrong shard geometry; a zero count is an SRT
	// never written (a fresh file), which agrees by construction.
	if live.SRTShardCount != 0 && live.SRTShardCount != prefix.ShardCount {
		return nil, ErrShardCount
	}

	outcome := OpenCrashed
	if live.CleanShutdown == 1 {
		outcome = OpenClean
	}
	return &OpenState{Prefix: prefix, Meta: live, Which: which, Outcome: outcome}, nil
}
