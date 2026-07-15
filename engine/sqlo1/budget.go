package sqlo1

import "runtime/debug"

// Memory budget, doc 04 section 15: every structure gets a share of the
// one --memory-cap number and every share is a hard cap (R-I7), with
// GOMEMLIMIT set to the cap as the runtime backstop. The rows below are
// the doc's table computed, not tuned; the two inputs the table left
// implicit are named constants with their resolutions attached.
const (
	// avgRecordEstimate sizes the header count from the arena share. The
	// doc table divides the arena bytes by an average record it never
	// states; 512 is the doc 03 v0 test geometry's average (64 B keys,
	// values 64 B to 1 KiB). Smaller real records waste header slots,
	// larger ones strand arena bytes; neither breaks a cap.
	avgRecordEstimate = 512
	// hotEntryOverhead is the per-entry header-side cost: 48 B packed hdr
	// plus 15 B Swiss map slot at 87.5 percent load plus 4 B dirty ring
	// plus 4 B free-slot stack, all preallocated to capacity. The doc's
	// 63 B counted only hdr plus map; the ring and stack are real bytes,
	// so the budget charges them.
	hotEntryOverhead = 71
)

// Budget is the computed share table for one process at a given cap.
// Byte fields are caps handed to the structures that enforce them;
// Entries is the hot-table capacity the arena share implies.
type Budget struct {
	Cap     int64
	Arenas  int64
	Entries int
	Headers int64
	Ghosts  int64
	// ChunkCache through Slack are reservations for structures that
	// arrive with later milestones; computing them now keeps the table
	// honest about how much of the cap the hot tier may take.
	ChunkCache int64
	Directory  int64
	GroupBufs  int64
	Compaction int64
	WALBufs    int64
	Dicts      int64
	Slack      int64
}

// ComputeBudget fills the doc 04 section 15 table for cap bytes and
// shards. Percentage rows follow the table; sized rows use the named
// constants above and the drain batch geometry. The rows do not sum to
// the cap: the percentages alone take 95 and the sized rows ride on top,
// which is fine because each share is enforced where it is spent, the
// memory gate milestones judge the real RSS, and GOMEMLIMIT holds the
// total (the doc's slack row is the first thing squeezed).
func ComputeBudget(cap int64, shards int) Budget {
	b := Budget{
		Cap:        cap,
		Arenas:     cap * 55 / 100,
		ChunkCache: cap * 10 / 100,
		Directory:  cap * 15 / 100,
		GroupBufs:  int64(2*drainMaxOps*shards) * 4096,
		Compaction: int64(shards) * (2 << 20),
		WALBufs:    int64(shards) * (8 << 20),
		Dicts:      4 * (112 << 10),
		Slack:      cap * 15 / 100,
	}
	b.Entries = int(b.Arenas / avgRecordEstimate)
	b.Headers = int64(b.Entries) * hotEntryOverhead
	b.Ghosts = int64(b.Entries/16) * 16
	return b
}

// Apply sets GOMEMLIMIT to the cap. The shares above keep steady state
// well under it; the limit exists so a bug degrades into GC pressure
// instead of an OOM kill.
func (b Budget) Apply() {
	debug.SetMemoryLimit(b.Cap)
}

// NewBudgetedHotTable builds a hot table whose arenas share one hard
// byte cap (the doc gives keys and values a combined share, and one pool
// self-balances as the key-to-value ratio drifts). Headers, ghost ring,
// dirty ring, and free-slot stack are preallocated to Entries, so their
// budget rows are spent up front and never grow.
func NewBudgetedHotTable(b Budget) *HotTable {
	t := NewHotTable(b.Entries)
	shared := &arenaBudget{limit: b.Arenas}
	t.keys.budget = shared
	t.vals.budget = shared
	return t
}
