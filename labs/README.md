# labs

Self-contained micro-benchmarks that capture a lesson from an aki optimization slice.

Each sub-directory is one lesson: a small, dependency-free Go package with a
`doc.go` that states the finding in prose and a `*_test.go` that reproduces it
with `go test -bench`. The point is not to benchmark aki itself (the real
benchmarks live in aki-bench and the `f1srv` package benchmarks); it is to
isolate the mechanism behind a decision so the reasoning survives, reproducible,
after the slice that produced it is long merged.

Rules for a lab:

- No import of aki internals. A lab models the shape of the problem with plain
  Go so it stays runnable and readable on its own. When the model diverges from
  the real code, `doc.go` says how and why.
- The lesson goes in `doc.go` as the package comment: what was measured, what
  won, what the wrong assumption was, and where the real code that this informed
  lives.
- Numbers in `doc.go` are the direction and rough magnitude, machine-stamped
  (which CPU), not a contract. Re-run `go test -bench . ./labs/<name>/` to
  reproduce on your box.

## Index

- [setalgebra](setalgebra/) — buffer-then-encode beats streaming for a
  memory-bound point-probe (SINTER), and a single walk beats two walks for a
  dedup-bound union (SUNION). Also: why a contaminated CPU profile blamed the
  wrong line.
- [seteager](seteager/) — the follow-on to setintersect: if the merge needs the set
  kept in hash order, what container keeps it ordered cheaply? A single flat sorted
  array gives the best merge (5.8 ms) but an impossible ~90 us O(n) SADD; a skiplist
  gives the cheapest write (83 ns) but a 183 ms merge because in-order pointer-chasing
  throws away the sequential streaming the merge exists for. Per-partition sorted arrays
  (f1raw's doc 19 layout, member in hash(m)&(P-1)) win both: an O(n/P) insert (205 ns at
  P=256) and a merge identical to the single array (5.78 vs 5.80 ms) because two same-P
  sets intersect partition-by-partition, which also makes the merge parallel across P
  cores where the rivals run one thread. The SADD tax is folded off the reply path by the
  async folder, keeping "eager" meaning always-materialized rather than built-on-read.
- [setintersect](setintersect/) — the 2x lever for large symmetric SINTER is a
  sorted-hash merge, not a better probe. A two-pointer merge over two resident
  sorted arrays is ~3-5x faster than the random probe (12 ms vs 40 ms) because it
  reads sequentially (prefetcher-served) where the probe misses DRAM per member,
  and Redis/Valkey can only probe. It wins only if the set is kept in hash order:
  re-sorting per call is ~10x slower than the probe. The negative results that
  frame it: a per-op compact-table rebuild is a wash by construction, and doc 19's
  per-member partition routing (suspected of being the gap) is ~10% undiluted and
  ~0% under production cache pressure, so it is cleared.
