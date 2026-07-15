package sqlo1

// The exported WAL surface for sibling packages: sqlo1b routes its
// format ops (SEAL, CKPT, TRIM) through the same sidecar transport,
// and keeping the surface thin here beats a second WAL there. The
// implementation stays in wal.go.

// WAL is the sidecar transport from doc 03 section 12.
type WAL = wal

// WALFrame is one replayed frame; Payload aliases the replay buffer
// and is only valid inside the Replay callback.
type WALFrame = walFrame

// Frame ops, doc 03 section 12.2.
const (
	WALOpPut     = walOpPut
	WALOpDel     = walOpDel
	WALOpPexpire = walOpPexpire
	WALOpGenbump = walOpGenbump
	WALOpSeal    = walOpSeal
	WALOpCkpt    = walOpCkpt
	WALOpTrim    = walOpTrim
)

// OpenWAL opens or creates a sidecar and scans its segments; see
// openWAL.
func OpenWAL(path string, dbID uint64, segSize int64) (*WAL, error) {
	return openWAL(path, dbID, segSize)
}

// WALPath derives the sidecar name from the data file path.
func WALPath(data string) string { return walPath(data) }
