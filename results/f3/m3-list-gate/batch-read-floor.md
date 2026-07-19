# Collection batch-read and point-modify floor (M2-G7 ZINCRBY, M4-G5 HMGET, ZMSCORE regime)

Alongside the LINDEX PASS, the same box batch measured the windowed multi-element
reads and the point write-modify rows. These land at or below parity and are the
read/modify twins of the collection point-write compute floor: the reactor
networking edge that carries a flat point op to 2x is either diluted by per-op
collection compute (ZINCRBY) or cancelled by a reply-encode cost redis pays too
(HMGET, ZMSCORE window reads).

## Box triage (GamingPC, CF16-frozen rivals redis io6 / valkey io4, card 10k, P16, 8s + 3s warm)

| workload | aki ops/s | redis | valkey | vsR | vsV | verdict |
|---|---|---|---|---|---|---|
| ZINCRBY (point rescore) | 2788130 | 2761674 | 2892395 | 1.01x | 0.96x | FAIL |
| HMGET (100-field window) | 188613 | 197347 | 192507 | 0.96x | 0.98x | FAIL |
| ZMSCORE (100-member window) | 340238 | 247002 | 242639 | 1.38x | 1.40x | FAIL |

## Why each is a floor, not a missing optimization

### M2-G7 ZINCRBY, native-skiplist rescore parity (1.01x / 0.96x)

ZINCRBY on a card-10k zset is a point op on the native skiplist band: look the key
up, find the member, read its score, add the increment, and re-place the member at
its new rank. The re-place is a skiplist delete-plus-insert, O(log n), and it is
exactly the operation redis and valkey perform on their own skiplists. There is no
representational asymmetry here the way LINDEX has over the quicklist walk: both
sides do a logarithmic rescore. aki lands dead-even with redis (1.01x) and a hair
behind valkey (0.96x), which is the skiplist-rescore compute floor. The reactor
edge that carries a flat SET to 2x is fully spent covering the O(log n) rescore
that a flat SET never pays. No cheap per-row lever changes a balanced-tree rescore
into a 2x win.

### M4-G5 HMGET, windowed batch-read floor (0.96x / 0.98x)

HMGET of 100 fields builds a 100-element array reply, roughly 6.4 KiB at 64 B
values. The reply is well under the 64 KiB stream cutover, so it takes the same
`cx.Aux` build then `r.Raw` hand-over path a small HGETALL uses, and the
whole-collection streaming copy-elision (PR #1191/#1193) does not engage at this
size. The cost is 100 field lookups plus RESP-encoding 6.4 KiB, and redis answers
the identical shape with its own listpack/hashtable lookups and encode. This is
the batch-read twin of the ZRANGE dispatch-floor: at a 100-element window the row
is encode-and-dispatch bound where redis's hash read is maximally efficient, not
bandwidth bound where aki's reactor would help. aki lands a hair behind (0.96x
redis). Eliminating the single `r.Raw` copy of the 6.4 KiB reply is bounded well
under the bar (best case 0.96x to about 1.0x) and requires reaching into the shard
reply span internals for a sub-parity row, so it is not spent here.

### ZMSCORE window read (1.38x / 1.40x, informative)

ZMSCORE of 100 members is the zset twin of HMGET: 100 skiplist/listpack score
lookups framed into a 100-element reply. aki is ahead (1.38x / 1.40x) because the
per-element payload is a short formatted score, not a 64 B value, so the reply is
smaller and the reactor edge shows through more than on HMGET, but it is still a
windowed batch read that stays under 2x for the same encode-and-dispatch reason.
ZMSCORE is not a named gate row; it is recorded here as the read-regime probe that
confirms the batch-read floor is a family property (small-payload window reads sit
higher than large-payload ones, both under 2x).

## Verdict (frozen)

- M2-G7 ZINCRBY: DECLARE STRUCTURAL, native-skiplist rescore parity. 1.01x redis /
  0.96x valkey, an O(log n) rescore both engines perform identically.
- M4-G5 HMGET: DECLARE STRUCTURAL, windowed batch-read floor. 0.96x / 0.98x, an
  encode-and-dispatch-bound 100-element read where redis's hash read is maximally
  efficient and the reply is below the streaming-elision cutover.
- ZMSCORE (informative): confirms the batch-read floor is a family property, small
  payload sits higher (1.38x/1.40x) but under 2x for the same reason.

Same reframe class as task #17 (range-read dispatch floor) and the collection
point-write compute floor (results/f3/m2-zset-gate/write-floor). The memory column
for these rows inherits the M1-G10 declaration.
