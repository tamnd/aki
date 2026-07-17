# lqueue: the queue marquee

Milestone T5 lab 02 (spec 2064/sqlo1 doc 07 section 8, doc 13 fairness frame).

## Question

The headline cell: sustained LPUSH+RPOP throughput and tail latency against Redis 8.8 and Valkey 9.1 as the steady depth grows from 10 to 10^7.
PRED-SQLO1-T5-QUEUE claims the p99 stays flat in depth because a queue only ever touches its edge nodes; the rivals hold the whole list in RAM, so depth costs them memory instead of time, and the fair comparison is doc 13's: equal work with durability on both sides, then throughput, tail, and (in the suite, not here) the memory bill.

## Method

Unlike the resident-model labs (lnode, lmid) this one drives live servers over RESP, because the marquee number is end to end: parse, dispatch, list op, frame group, reply.
The aki arm serves in process over the real sqlo1b file store at the production 64 MiB WAL segment; a rival arm is any address the script points at, and the gate box starts each rival with appendonly yes to keep the durability story comparable.
Every worker connection alternates one LPUSH with one RPOP on a single queue key, the reliable-queue shape, so the depth stays within one connection count of the target and a pop never finds the queue empty; per-op latency is recorded only inside the measured window after warmup.
Elements carry their sequence number as the oracle: with one connection the pops must come back in exact push order, at any width the final drain must find exactly the elements the push/pop ledger says are left, and the harness fails loudly on any miss, order error, or ledger mismatch rather than printing a row.

## Run

    ./run.sh                       # aki arm, local depths 10 100 1000
    ADDR=host:6379 LABEL=redis88 ./run.sh    # rival arm on the gate box
    go run . -quick                # smoke
    go test ./...                  # FIFO oracle, concurrent conservation, depth ceiling pin

## Results (local, 2026-07-17, macbook, self-proof only; the rival comparison is a gate-box number)

The aki arm over the real file store, 8 connections, 200 B elements, 20 s windows: 120441 / 112545 / 107795 ops per second at depth 10 / 100 / 1000, push p99 172 / 182 / 189 us, pop p99 171 / 182 / 189 us, zero misses and clean drains at every depth.
The 10 percent slide from depth 10 to 1000 is the fence prefix-sum growing with node count on the inline fence, not an edge-cost change; the flat-in-depth claim gets its real test at 10^4 and beyond, which needs fence paging.

## The gate note

Until the fence paging slice lands, a 200 B queue caps at ~3000 elements (167 fence nodes at ~19 elements each) and the harness surfaces the refusal loudly; TestLQueueDepthCeiling pins that and documents its own deletion.
The full 10 to 10^7 depth sweep plus the Redis 8.8 and Valkey 9.1 arms run on the gamingpc gate box behind that slice, and that run fills the marquee cell and settles PRED-SQLO1-T5-QUEUE.

The sweep CSV (lqueue.csv) stays untracked, like every lab CSV.
