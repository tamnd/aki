# Lab m1/15: the tiny-collection arena-embed memory lever

The memory bar fails on the 2M-tiny-collection gate cell: aki charges far more
per tiny collection than redis or valkey. A tiny collection today is three
separate Go-heap objects plus map overhead, every one of them GC-scanned:

1. the per-type header struct (a set is 80 bytes, size class 80),
2. its packed data slice (a single-member listpack, ~7 bytes, size class 16),
3. the registry map entry: the copied key string bytes (class 16) and the
   amortized `map[string]*set` bucket slot.

That is the three-heap-object wall. The keyspace-unification fix stores a tiny
collection where a small string value already lives: inline in one arena record,
`[header | key | packed blob]` contiguous, discriminated by a collection kind
byte. The primitive is `engine/f3/store` `PutCollBlob` (this slice). Two wins
compound: the record is packed with no per-object allocator size-class slack,
and it lives in the arena, which is mapped outside the Go heap
(`arena_map_unix.go`), so the collection bytes leave the GC-scanned set entirely
and stop inflating the collector's pacing goal the way 2M live heap objects do.

## Method

Two arms, single-member sets (member `"hello"`), no server, no wire.

- **wall**: build the real Go-heap shape, a `map[string]*wallSet` where
  `wallSet` mirrors the real 80-byte set header at its true field widths, each
  owning its own listpack blob. Read `runtime.MemStats.HeapAlloc` before and
  after, GC forced and pinned, for the true per-collection heap charge the
  allocator books (struct class + blob class + key string + map bucket slot).
- **embed**: drive the real `store.Store` through `PutCollBlob` for the same
  collections. Report the per-record arena charge from the store's own
  accounting (`MemoryUsage`: header, aligned key, reserved value capacity), which
  lives off the Go heap, plus the measured Go-heap growth over the build, which
  is the store's keyspace index (the extendible hash), the embed design's answer
  to the wall arm's map-as-index. Their sum is the apples-to-apples
  per-collection resident charge, since the wall number already folds its map
  bucket overhead in.

## Result (GamingPC-class dev box, reps 3)

```
     count   wall B/coll   embed rec B   embed idx B   embed tot B    ratio   saved (count)
    100000         126.2          40.0          20.6          60.6    0.48x          6.3 MB
    500000         147.6          40.0          20.3          60.3    0.41x         41.6 MB
   1000000         147.6          40.0          20.3          60.3    0.41x         83.2 MB
   2000000         147.6          40.0          20.3          60.3    0.41x        166.5 MB
```

The embed charges **0.41x** the wall per tiny collection, under the memory bar's
0.50x, and saves **166.5 MB** at the gate's 2M cell. The record's 40 bytes
(`16` header + `16` aligned 12-byte key + `8` single-member blob) live off the
Go heap; only the 20.3-byte index stays on it, so the collector never carries 2M
live collection objects. The wall's 147.6 bytes are all live GC-scanned heap:
the 80-byte struct, the blob's 16-byte class, the key string, and the map slot.

## What this does and does not settle

This slice prices the storage footprint the per-type routing will inherit; it
does not wire tiny sets onto the path (that is slice 2, and it must preserve the
lazy-expiry, WRONGTYPE, encoding, idle-clock, and accounting funnel the registry
owns today). The lever is only worth the routing work because the footprint it
lands on is already proven, here, to clear the bar. The single-shard box RSS the
gate cell reads is the follow-on box confirmation once routing lands; this lab
bounds the per-collection charge that RSS is made of.
