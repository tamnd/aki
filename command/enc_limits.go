package command

// encLimits is the live set of OBJECT ENCODING thresholds read from the config
// store. aki stores its own physical form for every type, so these only steer the
// encoding name a key reports, not how the value is laid out on disk. A handler
// snapshots them once with ctx.encLimits() before a write so CONFIG SET
// hash-max-listpack-entries and the rest take effect on the next write, the way
// real Redis applies them.
type encLimits struct {
	listSize    int64 // list-max-listpack-size, raw (a negative value is a size tier)
	hashEntries int64 // hash-max-listpack-entries
	hashValue   int64 // hash-max-listpack-value
	setIntset   int64 // set-max-intset-entries
	setEntries  int64 // set-max-listpack-entries
	setValue    int64 // set-max-listpack-value
	zsetEntries int64 // zset-max-listpack-entries
	zsetValue   int64 // zset-max-listpack-value
}

// Stock redis.conf defaults, used when there is no config store (offline RDB
// load, a few tests) so behavior matches a default server.
const (
	// defListSize is the compiled-in default for list-max-listpack-size. Redis
	// and Valkey ship -2, the 8KB byte tier, not a positive entry count, so a
	// list of short elements stays a single listpack well past 128 entries.
	defListSize    = -2
	defHashEntries = 128
	defHashValue   = 64
	defSetIntset   = 512
	defSetEntries  = 128
	defSetValue    = 64
	defZsetEntries = 128
	defZsetValue   = 64

	// listSafetyBytes is SIZE_SAFETY_LIMIT, the 8KB listpack byte cap Redis
	// applies even when list-max-listpack-size is a positive entry count.
	listSafetyBytes = 8192
)

// defaultEncLimits returns the stock-redis.conf threshold set.
func defaultEncLimits() encLimits {
	return encLimits{
		listSize:    defListSize,
		hashEntries: defHashEntries,
		hashValue:   defHashValue,
		setIntset:   defSetIntset,
		setEntries:  defSetEntries,
		setValue:    defSetValue,
		zsetEntries: defZsetEntries,
		zsetValue:   defZsetValue,
	}
}

// encLimits reads the current thresholds from the dispatcher's config store. A
// nil dispatcher or store falls back to the defaults. The store keeps the eight
// thresholds in atomic mirrors that CONFIG SET refreshes, so this snapshot is
// taken with eight atomic loads rather than eight RWMutex-guarded map lookups;
// under a write storm those lock reads were the top reader-counter contention in
// the profile.
func (d *Dispatcher) encLimits() encLimits {
	if d == nil || d.conf == nil {
		return defaultEncLimits()
	}
	return d.conf.encodingLimits()
}

// encLimits snapshots the thresholds for the command in flight.
func (ctx *Ctx) encLimits() encLimits {
	if ctx == nil {
		return defaultEncLimits()
	}
	return ctx.d.encLimits()
}

// listLimits resolves list-max-listpack-size into an entry-count cap and a byte
// cap, mirroring quicklistNodeLimit. A positive value is an entry count with the
// 8KB safety byte cap on top. A negative value -1..-5 selects a 4/8/16/32/64 KB
// byte cap and imposes no entry cap, so a list of short elements stays a listpack
// until the bytes cross the tier. An entries value of 0 means no entry cap. Zero
// is treated as a byte-only cap at the safety size.
func (l encLimits) listLimits() (entries, bytes int) {
	switch {
	case l.listSize > 0:
		return int(l.listSize), listSafetyBytes
	case l.listSize < 0:
		tier := -l.listSize
		if tier > 5 {
			tier = 5
		}
		return 0, 1 << (11 + tier)
	default:
		return 0, listSafetyBytes
	}
}
