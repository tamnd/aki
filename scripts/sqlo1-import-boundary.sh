#!/bin/sh
# The sqlo1 import boundary (spec 2064/sqlo1, milestone 00-S0): engine/sqlo1,
# engine/sqlo1a, engine/sqlo1b, cmd/sqlo1srv, and cmd/sqlo1crash never import
# engine/f3 or any other f-series or legacy package in this module. sqlo1 is
# a fresh driver and
# stays sealed off the same way f3 does. Enforced as an allowlist so a new
# path can never slip through unlisted: an sqlo1 package may depend on other
# sqlo1 packages and the standard library, nothing else in the module. The
# sqlo1 trees also carry no internal/ directories; nothing here hides from
# the rest of the repo.
set -eu

mod=$(go list -m)
bad=$(go list -deps ./engine/sqlo1/... ./engine/sqlo1a/... ./engine/sqlo1b/... ./cmd/sqlo1srv ./cmd/sqlo1crash ./labs/sqlo1/... |
	grep "^$mod/" |
	grep -Ev "^$mod/(engine/sqlo1|engine/sqlo1a|engine/sqlo1b|cmd/sqlo1srv|cmd/sqlo1crash|labs/sqlo1)(/|$)" || true)

if [ -n "$bad" ]; then
	echo "sqlo1 packages must not import other packages in this module:"
	echo "$bad"
	exit 1
fi

# The A1 driver freeze (results/sqlo1/drivershoot.md): sqlo1a speaks the
# ncruces native API and nothing else. database/sql must stay out of the
# sqlo1 import graphs, and the losing drivers must stay out of the module.
frozen=$(go list -deps ./engine/sqlo1/... ./engine/sqlo1a/... ./engine/sqlo1b/... ./cmd/sqlo1srv ./cmd/sqlo1crash |
	grep -E '^database/sql($|/)|^modernc\.org/|^zombiezen\.com/' || true)
if [ -n "$frozen" ]; then
	echo "the sqlo1 driver freeze forbids these imports (see results/sqlo1/drivershoot.md):"
	echo "$frozen"
	exit 1
fi
if grep -Eq 'modernc\.org/sqlite|zombiezen\.com' go.mod; then
	echo "the losing drivershoot drivers must not enter the module graph:"
	grep -E 'modernc\.org/sqlite|zombiezen\.com' go.mod
	exit 1
fi

# The A2 statement catalog (milestone 03-A2 slice 2): every query sqlo1a
# runs is a named prepared statement in engine/sqlo1a/stmt.go. A query verb
# in a string literal anywhere else in the package means someone is writing
# SQL outside the catalog, and Sprintf near a query verb means someone is
# building SQL at runtime. Tests are exempt; they poke fixtures.
loose=$(grep -rn --include='*.go' -E '["`](SELECT|INSERT|UPDATE|DELETE|REPLACE)[ \n]' engine/sqlo1a |
	grep -Ev '_test\.go:|/stmt\.go:' || true)
if [ -n "$loose" ]; then
	echo "sqlo1a query literals must live in engine/sqlo1a/stmt.go:"
	echo "$loose"
	exit 1
fi
built=$(grep -rn --include='*.go' -E 'Sprintf\(.*(SELECT|INSERT|UPDATE|DELETE|REPLACE|WHERE |FROM )' engine/sqlo1a || true)
if [ -n "$built" ]; then
	echo "sqlo1a must not build SQL at runtime:"
	echo "$built"
	exit 1
fi

dirs=$(find engine/sqlo1 engine/sqlo1a engine/sqlo1b cmd/sqlo1srv cmd/sqlo1crash labs/sqlo1 -type d -name internal)
if [ -n "$dirs" ]; then
	echo "internal/ directories are not allowed in the sqlo1 trees:"
	echo "$dirs"
	exit 1
fi

echo "sqlo1 import boundary clean"
