#!/bin/sh
# The obs1 import boundary (spec 2064/obs1 doc 11 section 1): engine/obs1,
# obs1srv, cmd/obs1srv, and labs/obs1 never import engine/f3 or any other
# package in this module. Ports arrive by copy, the sqlo1 rule. Enforced as
# an allowlist so a new path can never slip through unlisted: an obs1
# package may depend on other obs1 packages and the standard library,
# nothing else.
set -eu

mod=$(go list -m)
bad=$(go list -deps ./engine/obs1/... ./obs1srv/... ./cmd/obs1srv ./labs/obs1/... |
	grep "^$mod/" |
	grep -Ev "^$mod/(engine/obs1|obs1srv|cmd/obs1srv|labs/obs1)(/|$)" || true)
if [ -n "$bad" ]; then
	echo "obs1 packages must not import other packages in this module:"
	echo "$bad"
	exit 1
fi

# The object-store client is hand rolled on net/http (doc 11 section 1):
# no AWS SDK, no external module of any kind in the obs1 graph. An external
# module path has a dot in its first element (github.com, golang.org);
# stdlib packages never do.
ext=$(go list -deps ./engine/obs1/... ./obs1srv/... ./cmd/obs1srv ./labs/obs1/... |
	grep -v "^$mod" |
	grep -E '^[^/]*\.[^/]*/' || true)
if [ -n "$ext" ]; then
	echo "the obs1 import graph must stay standard library only:"
	echo "$ext"
	exit 1
fi

# W-I4 (doc 04 section 11): the diskless write path never touches a file.
# The engine root package must not import os, os/exec, syscall, or
# io/ioutil directly; the section 8 NVMe hint lives outside this package.
# Direct imports only, since the stdlib reaches os transitively.
disk=$(go list -f '{{join .Imports "\n"}}' ./engine/obs1 |
	grep -Ex 'os|os/exec|syscall|io/ioutil' || true)
if [ -n "$disk" ]; then
	echo "engine/obs1 must stay off the disk APIs (W-I4):"
	echo "$disk"
	exit 1
fi

dirs=$(find engine/obs1 obs1srv cmd/obs1srv labs/obs1 -type d -name internal)
if [ -n "$dirs" ]; then
	echo "internal/ directories are not allowed in the obs1 trees:"
	echo "$dirs"
	exit 1
fi

echo "obs1 import boundary clean"
