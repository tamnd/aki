package sqlo1b

// Decoded-frame cache, doc 04 section 11's batched group decode. A
// point read into a compressed group pays ParseCGroup's full payload
// decode to slice one record, so a batch whose keys cluster in one
// group, or a hot group read repeatedly, decodes the same bytes over
// and over. The cache holds a few decoded views keyed by (extent,
// group) and amortizes that: the first read decodes, the rest slice.
//
// Only non-raw frames cache. A raw frame costs nothing to re-parse,
// and the open compact group rewrites its raw image in place as it
// grows, so caching it would serve stale records; a non-raw frame
// only exists once Seal closed the group, and sealed frame groups
// never rewrite. The one way a sealed group's bytes change is the
// extent being freed and reused by a new stream, and allocStream
// drops the extent's entries right where it refreshes the eflags
// dispatch cache.
//
// Callers serialize access (the store holds its lock across reads);
// the cache carries no lock of its own.

// frameCacheSize bounds the cache. Decoded payloads run to the u16
// offset bound, so the worst case stays a few hundred KiB.
const frameCacheSize = 8

// FrameStats counts what the decode side of the read path cost.
type FrameStats struct {
	Decodes     uint64 // frame payloads decoded
	DecodeBytes uint64 // uncompressed bytes those decodes produced
	Hits        uint64 // reads served from an already-decoded view
}

type frameKey struct {
	ext uint64
	grp uint16
}

// FrameCache memoizes decoded compressed-group views.
type FrameCache struct {
	views map[frameKey]*CGroupView
	order []frameKey // insertion order, oldest first
	stats FrameStats
}

func NewFrameCache() *FrameCache {
	return &FrameCache{views: make(map[frameKey]*CGroupView, frameCacheSize)}
}

// View parses a compressed group image through the cache. Raw frames
// pass straight through uncached.
func (fc *FrameCache) View(ext uint64, grp uint16, img []byte) (*CGroupView, error) {
	if fc == nil || len(img) == 0 || img[0] == SchemeRaw {
		return ParseCGroup(img)
	}
	k := frameKey{ext, grp}
	if v, ok := fc.views[k]; ok {
		fc.stats.Hits++
		return v, nil
	}
	v, err := ParseCGroup(img)
	if err != nil {
		return nil, err
	}
	fc.stats.Decodes++
	fc.stats.DecodeBytes += uint64(len(v.payload))
	if len(fc.order) >= frameCacheSize {
		delete(fc.views, fc.order[0])
		fc.order = fc.order[1:]
	}
	fc.views[k] = v
	fc.order = append(fc.order, k)
	return v, nil
}

// DropExtent evicts every cached view of one extent. allocStream
// calls it when a freed extent reactivates under a new stream.
func (fc *FrameCache) DropExtent(ext uint64) {
	if fc == nil {
		return
	}
	kept := fc.order[:0]
	for _, k := range fc.order {
		if k.ext == ext {
			delete(fc.views, k)
			continue
		}
		kept = append(kept, k)
	}
	fc.order = kept
}

// Stats reports the counters since open.
func (fc *FrameCache) Stats() FrameStats {
	if fc == nil {
		return FrameStats{}
	}
	return fc.stats
}
