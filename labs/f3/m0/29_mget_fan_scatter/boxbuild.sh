#!/bin/bash
# Build f3srv from the m0-mget-fan-scatter-pool branch into /root/bin/f3srv-m0scatter.
set -e
cd /root/akiperf/aki-src
export GOFLAGS=-mod=mod PATH=$PATH:/usr/local/go/bin
git fetch origin --quiet
git checkout -q m0-mget-fan-scatter-pool
git reset --hard -q origin/m0-mget-fan-scatter-pool
echo "HEAD: $(git log -1 --format='%h %s')"
go build -o /root/bin/f3srv-m0scatter ./cmd/f3srv
echo "built: $(stat -c '%s bytes %y' /root/bin/f3srv-m0scatter)"
