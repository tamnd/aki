package store

// TouchBucket warms the home-bucket cache line for hash h ahead of its probe,
// the stage-one prefetch of the batch drain (spec 2064/f3/03 section 3.4). Go
// has no portable prefetch intrinsic, so this is the software form: one plain
// load through the bucket, returned so the caller can fold it into a sink the
// compiler must keep live. Owner-only, like every probe; the load races
// nothing because nothing else touches the shard.
func (s *Store) TouchBucket(h uint64) uint64 {
	seg, _ := s.idx.segFor(h)
	return seg.buckets[bucketIndex(h)].slots[0]
}
