#!/bin/sh
# Full xpel sweep. Writes CSV rows to stdout; redirect to xpel.csv.
set -e
cd "$(dirname "$0")"
go build -o /tmp/xpel .

echo "mix,segmax,pcap,pending,batch,order,fence,workload,ops,ns_op,frames_op,walB_op,x1,x2,x3,x4"

# Segment cap sweep across pending populations, both fence shapes:
# the WAL bill per deliver and per ack and the fence share decide
# the caps and whether the fence pages.
for fence in inline paged; do
  for segmax in 1024 2048 4096 8192 16384; do
    for pending in 100 1000 10000 100000 1000000; do
      /tmp/xpel -mix deliver -segmax $segmax -pending $pending -fence $fence
      /tmp/xpel -mix ack -segmax $segmax -pending $pending -order fifo -fence $fence
    done
  done
done

# Random-order ACK spray, capped at 10^5 where the per-batch fence
# spread already saturates.
for fence in inline paged; do
  for segmax in 1024 4096 16384; do
    for pending in 1000 10000 100000; do
      /tmp/xpel -mix ack -segmax $segmax -pending $pending -order random -fence $fence
    done
  done
done

# Read surface and cursor sweep at the candidate caps.
for segmax in 1024 4096 16384; do
  for pending in 100 1000 10000 100000 1000000; do
    /tmp/xpel -mix scan -segmax $segmax -pending $pending
    /tmp/xpel -mix claim -segmax $segmax -pending $pending -batch 100 -fence paged
  done
done

# Batch sensitivity at the candidate cap and a big PEL.
for fence in inline paged; do
  for batch in 1 10 100 1000; do
    /tmp/xpel -mix deliver -segmax 4096 -pending 100000 -batch $batch -fence $fence
    /tmp/xpel -mix ack -segmax 4096 -pending 100000 -batch $batch -order fifo -fence $fence
  done
done

# Entry-cap probe: does pcap ever bind before the byte cap?
for pcap in 64 256 1024; do
  /tmp/xpel -mix deliver -segmax 16384 -pcap $pcap -pending 100000
done

# Codec pricing.
/tmp/xpel -mix encode -segmax 4096 -pending 1000000
