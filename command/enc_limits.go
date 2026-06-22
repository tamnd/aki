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
	defListSize    = 128
	defHashEntries = 128
	defHashValue   = 64
	defSetIntset   = 512
	defSetEntries  = 128
	defSetValue    = 64
	defZsetEntries = 128
	defZsetValue   = 64

	// listMaxListpackElemBytes is the per-element byte cap aki keeps for a list
	// listpack. Redis has no list-max-listpack-value directive, so this stays a
	// constant rather than a config value.
	listMaxListpackElemBytes = 64
	// listDefaultBytes is the listpack byte cap used when list-max-listpack-size
	// is a positive entry count. The size tier only applies to negative values.
	listDefaultBytes = 8192
	// listEntryOverhead approximates a listpack entry's header and backlen when
	// estimating the blob size against the byte cap.
	listEntryOverhead = 11
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
// nil dispatcher or store falls back to the defaults.
func (d *Dispatcher) encLimits() encLimits {
	if d == nil || d.conf == nil {
		return defaultEncLimits()
	}
	return encLimits{
		listSize:    d.confInt("list-max-listpack-size", defListSize),
		hashEntries: d.confInt("hash-max-listpack-entries", defHashEntries),
		hashValue:   d.confInt("hash-max-listpack-value", defHashValue),
		setIntset:   d.confInt("set-max-intset-entries", defSetIntset),
		setEntries:  d.confInt("set-max-listpack-entries", defSetEntries),
		setValue:    d.confInt("set-max-listpack-value", defSetValue),
		zsetEntries: d.confInt("zset-max-listpack-entries", defZsetEntries),
		zsetValue:   d.confInt("zset-max-listpack-value", defZsetValue),
	}
}

// encLimits snapshots the thresholds for the command in flight.
func (ctx *Ctx) encLimits() encLimits {
	if ctx == nil {
		return defaultEncLimits()
	}
	return ctx.d.encLimits()
}

// listLimits resolves list-max-listpack-size into an entry-count cap and a byte
// cap. A positive value is an entry count and leaves the byte cap at the default
// safety size. A negative value -1..-5 selects a 4/8/16/32/64 KB byte cap and
// leaves the entry count at the default. Zero falls back to the default.
func (l encLimits) listLimits() (entries, bytes int) {
	switch {
	case l.listSize > 0:
		return int(l.listSize), listDefaultBytes
	case l.listSize < 0:
		tier := -l.listSize
		if tier > 5 {
			tier = 5
		}
		return defListSize, 1 << (11 + tier)
	default:
		return defListSize, listDefaultBytes
	}
}
