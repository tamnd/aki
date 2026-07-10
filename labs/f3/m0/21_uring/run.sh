#!/bin/sh
# Lab 21 sweep: both echo arms over the conns x pipeline grid, 64B messages.
# Run on the gate box under the campaign's server mask so the numbers sit in
# the same core budget as the A/B: taskset -c 0-7 sh run.sh
set -e
cd "$(dirname "$0")"
go build -o lab21 .
for mode in epoll uring; do
  for conns in 1 64 512; do
    for pipe in 1 16; do
      ./lab21 -mode "$mode" -conns "$conns" -pipeline "$pipe" -msg 64 -seconds 8
    done
  done
done
