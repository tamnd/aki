package sqlo1b

import "github.com/tamnd/aki/engine/sqlo1"

// The subkey codec (doc 03 section 6.3) moved to engine/sqlo1 so the
// type layer can mint rooths and build subkeys without importing this
// package. The names below keep the format-level API in one place for
// sqlo1b consumers; the definitions and their tests live with sqlo1.
type Subkey = sqlo1.Subkey

const (
	SubkindSeg   = sqlo1.SubkindSeg
	SubkindFence = sqlo1.SubkindFence
)

var (
	NewSubkey    = sqlo1.NewSubkey
	DecodeSubkey = sqlo1.DecodeSubkey
	MintRooth    = sqlo1.MintRooth
)
