#!/bin/sh
# The f3 import boundary (spec 2064/f3/19 section 4.1): engine/f3, f3srv, and
# cmd/f3srv never import engine/f1raw, engine/f2raw, or any legacy package in
# this module. Ports copy code in; an import edge would let a quarantined
# structure leak back. Enforced as an allowlist so a new legacy path can never
# slip through unlisted: an f3 package may depend on other f3 packages and the
# standard library, nothing else in the module.
set -eu

mod=$(go list -m)
bad=$(go list -deps ./engine/f3/... ./f3srv/... ./cmd/f3srv |
	grep "^$mod/" |
	grep -Ev "^$mod/(engine/f3|f3srv|cmd/f3srv)(/|$)" || true)

if [ -n "$bad" ]; then
	echo "engine/f3 and f3srv must not import other packages in this module:"
	echo "$bad"
	exit 1
fi
echo "f3 import boundary clean"
