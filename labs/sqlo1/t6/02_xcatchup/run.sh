#!/bin/sh
# Cold catch-up sweep: build the 10^7-entry stream once, replay it
# cold at three consumer batch depths, then the pollute interleave.
# Each replay is a fresh process, so its peak RSS is the catch-up
# footprint alone.
set -e
cd "$(dirname "$0")"
dir="${XCATCHUP_DIR:-$(mktemp -d /tmp/xcatchup.XXXXXX)}"
go build -o /tmp/xcatchup .
echo "mix,n,elen,count_or_wkeys,secs,rate,mb_s_or_p99ratio,rss_mib,x1,x2"
/tmp/xcatchup -mix build -dir "$dir" "$@"
for count in 100 1000 10000; do
  /tmp/xcatchup -mix catchup -dir "$dir" -count "$count" "$@"
done
/tmp/xcatchup -mix pollute -dir "$dir" -count 1000 "$@"
rm -rf "$dir"
