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
- [setintersect](setintersect/) — a per-operation rebuild of a compact probe
  table does not beat aki's resident shared index: the O(|B|) build cost equals
  the cache-locality it buys, so it is a wash by construction. Doc 19's per-member
  partition routing, suspected of being the real SINTER gap, is measured at ~10%
  undiluted and ~0% under production cache pressure, so it is cleared. SINTER on
  equal-size sets is DRAM-latency-bound on ~1M probes; no data-structure change
  makes that 2x, only set ordering or asymmetric sizes does.
