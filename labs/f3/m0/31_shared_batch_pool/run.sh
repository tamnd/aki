#!/usr/bin/env bash
# m0/31: allocation churn of the two-tier hop-transport node pool under a
# pipelined burst. Reports allocs/op for the pre-change L1-only cache versus the
# runtime-shared pool that backs it. See README.md.
set -euo pipefail
cd "$(git rev-parse --show-toplevel 2>/dev/null || echo "$(dirname "$0")/../../../..")"
go test -run '^$' -bench BenchmarkBatchNodeChurn -benchmem ./engine/f3/shard/
