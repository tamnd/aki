# poolrate: 5,000 GET/s through a 256 pool at the doc 01 latency model

Milestone O0a lab 02 (spec 2064/obs1 doc 01 section 2.2, doc 11 section 5).

## Question

PRED-OBS1-O0A-POOL claims a warm pool sustains 5,000 GET/s per node from one process without client-side queuing at the doc 01 latency model.
Lab 01 answered the pool-mechanics half locally (reuse above 0.99, no cliff, 256 baked); this lab scores the claim itself on the simulator, where every GET pays the cloud latency the local box cannot show.

## Method

The pool is a slot semaphore in front of the sim, the shape MaxIdleConnsPerHost gives the wire client: a request that finds every slot busy waits, and that wait is the client-side queuing the prediction says stays at zero.
The open arm fires arrivals on a fixed schedule at a target rate and measures the slot wait; genlag_max proves the generator kept the schedule, since a lagging generator understates queuing.
The closed arm runs one worker per slot flat out, which is the ceiling the pool supports, Little's law made empirical.
The sim runs the S3Standard model (GET p50 20ms, p99 150ms, lognormal), seed 1.

The analytic frame: that lognormal has mean 29.1ms (sigma = ln(7.5)/2.3263 = 0.866), so 5,000 GET/s needs about 146 requests in flight and a 256 pool supports about 8,800 GET/s.

## Run

    ./run.sh            # full sweep into poolrate.csv, in-process, nothing to start
    go run . -quick     # smoke: one short run per arm
    go test .           # harness test, tiny counts

## Results

Full sweep in poolrate.csv, run 2026-07-16 on the M-series dev box, 10s per configuration, 4 KiB object.

Ceilings: closed-loop measures 4,197 GET/s at pool 128, 8,629 at 256, 17,180 at 512, within 2 to 5% of the Little's law numbers, linear in the pool.
GET p50 and p99 sit at 20ms and 147 to 150ms in every row, the model verbatim, so nothing in the harness pollutes the latency.

The claim: at pool 256 and 5,000 arrivals per second, in-flight peaks at 172 of 256 and the slot wait is zero (p50 0, p99 0, max 12us over 50,000 requests).
Generator lag stayed under 2.3ms, so the schedule was honest.
6,500 per second still rides free (peak 223, max wait 109us); 8,000 grazes the ceiling (peak pins at 256, max wait 1.9ms, p99 still 0); 9,500 is past it and queues hard (slot wait p50 355ms and climbing), which is the signature the prediction rules out, shown reachable.
At pool 128 the same 5,000 per second sits past that pool's 4,197 ceiling and queues at p50 620ms, so a zero wait at 256 is a property of the pool size, not of the harness.

Achieved ops_per_s reads a few percent under the target in passing rows because the clock includes the tail drain after the last scheduled arrival; the schedule itself completed on time.

## Verdict

PRED-OBS1-O0A-POOL: HIT at the simulator rung.
5,000 GET/s from one process through the baked pool of 256 runs with zero client-side queuing and 1.7x headroom to the measured 8,629 ceiling, at the doc 01 section 2.2 model.
The margin story: the claim fails below pool 146 by arithmetic and below 128 by measurement, so 256 is comfortable but not lazy.
The O5 E-cloud run re-scores this against live cloud latency after the model refit.
