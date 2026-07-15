#!/bin/sh
# The sqlo1 import boundary (spec 2064/sqlo1, milestone 00-S0): engine/sqlo1,
# engine/sqlo1a, engine/sqlo1b, and cmd/sqlo1srv never import engine/f3 or any
# other f-series or legacy package in this module. sqlo1 is a fresh driver and
# stays sealed off the same way f3 does. Enforced as an allowlist so a new
# path can never slip through unlisted: an sqlo1 package may depend on other
# sqlo1 packages and the standard library, nothing else in the module. The
# sqlo1 trees also carry no internal/ directories; nothing here hides from
# the rest of the repo.
set -eu

mod=$(go list -m)
bad=$(go list -deps ./engine/sqlo1/... ./engine/sqlo1a/... ./engine/sqlo1b/... ./cmd/sqlo1srv |
	grep "^$mod/" |
	grep -Ev "^$mod/(engine/sqlo1|engine/sqlo1a|engine/sqlo1b|cmd/sqlo1srv)(/|$)" || true)

if [ -n "$bad" ]; then
	echo "sqlo1 packages must not import other packages in this module:"
	echo "$bad"
	exit 1
fi

dirs=$(find engine/sqlo1 engine/sqlo1a engine/sqlo1b cmd/sqlo1srv -type d -name internal)
if [ -n "$dirs" ]; then
	echo "internal/ directories are not allowed in the sqlo1 trees:"
	echo "$dirs"
	exit 1
fi

echo "sqlo1 import boundary clean"
