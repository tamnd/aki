package sqlo1

import "context"

// The point keyspace probes, doc 12 section KS: EXISTS, TOUCH, TYPE,
// and the batched prefilter under variadic DEL/UNLINK. They share one
// discipline: an existence-class command reads no value bytes, so it
// must not touch read stamps (TOUCH's flag is the one exception,
// touching is its whole job) and must not promote cold hits, because
// polling a key is not using it and the eviction clocks would drift
// toward whoever polls the most.

// ProbeBatch reports per-key existence, one BatchGet for every key
// the hot table cannot answer. touch refreshes the read stamp of hot
// live hits only; a cold key has no stamp until something actually
// reads it, so TOUCH counts it without warming it.
func (t *Tiered) ProbeBatch(ctx context.Context, keys [][]byte, touch bool, hits []bool) ([]bool, error) {
	t.ht.SetNow(t.nowMs())
	hits = hits[:0]
	t.missKeys = t.missKeys[:0]
	t.missPos = t.missPos[:0]
	for i, k := range keys {
		if hd, ok := t.ht.peek(k); ok {
			// Tombstones and hot-expired entries are definitive
			// misses: the hot copy outranks whatever the store holds.
			live := hd.valRef != 0 && !t.ht.expired(hd)
			if live && touch {
				t.ht.touchRead(hd)
			}
			hits = append(hits, live)
			continue
		}
		hits = append(hits, false)
		t.missKeys = append(t.missKeys, k)
		t.missPos = append(t.missPos, i)
	}
	if len(t.missKeys) == 0 {
		return hits, nil
	}
	t.stats.BatchReads++
	recs, err := t.st.BatchGet(ctx, t.missKeys)
	if err != nil {
		return hits, err
	}
	for j, rec := range recs {
		if rec.Key == nil || t.expiredRec(rec) {
			continue
		}
		hits[t.missPos[j]] = true
	}
	return hits, nil
}

// typeName maps a type tag to the name TYPE answers with.
func typeName(tag uint8) string {
	switch tag {
	case TagHash:
		return "hash"
	case TagSet:
		return "set"
	case TagZset:
		return "zset"
	case TagList:
		return "list"
	case TagStream:
		return "stream"
	}
	return "string"
}

// TypeTag answers TYPE: the key's type tag (TagString..TagStream)
// without stamps or promotion. The hot header carries the tag
// directly; a cold plain record is a string, a cold root sniffs.
func (t *Tiered) TypeTag(ctx context.Context, key []byte) (uint8, bool, error) {
	t.ht.SetNow(t.nowMs())
	if hd, ok := t.ht.peek(key); ok {
		if hd.valRef == 0 || t.ht.expired(hd) {
			return 0, false, nil
		}
		return hd.typeTag & 0x0F, true, nil
	}
	t.missKeys = append(t.missKeys[:0], key)
	t.stats.BatchReads++
	recs, err := t.st.BatchGet(ctx, t.missKeys)
	if err != nil {
		return 0, false, err
	}
	rec := recs[0]
	if rec.Key == nil || t.expiredRec(rec) {
		return 0, false, nil
	}
	if rec.Root {
		tag, _, err := sniffRoot(rec.Value)
		if err != nil {
			return 0, false, err
		}
		return tag, true, nil
	}
	return TagString, true, nil
}
