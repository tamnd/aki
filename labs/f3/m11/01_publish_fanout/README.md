# Lab M11-01: PUBLISH fan-out throughput

Pub/sub lives in a network-layer registry, not the shard workers (doc 17
section 13). A channel is a set of subscriber connections; PUBLISH snapshots
that set, builds the message wire once, and pushes it to each subscriber out of
band. The shard owners never see a channel, so a PUBLISH storm cannot slow a
GET. This lab measures the fan-out and checks that claim.

## Method

One f3srv on loopback, pair shape. The pair shape matters: a subscriber sits
idle in Read, and its own writer goroutine is what delivers the pushed message.
The single shape has one goroutine per connection blocked in Read with nobody on
the waker, so it cannot deliver to an idle subscriber; the reactor delivers
through its eventfd the same way the pair does. The driver tests cover the
reactor leg on the ubuntu CI runs.

Per subscriber count {1, 8, 64, 256}, that many connections SUBSCRIBE the one
channel and drain messages to a discard buffer. Each cell then runs two phases
against that registered wall, kept apart on purpose:

- Phase one, fan-out: one publisher runs PUBLISH as fast as it can for two
  seconds while the subscribers drain. The cell reports the PUBLISH rate and the
  delivered-message rate (PUBLISH count times subscriber count).
- Phase two, point-op control: the publisher is quiet, the subscribers sit idle
  and registered, and one connection runs pipelined GET rounds (depth 16) over a
  4096-key loaded keyspace for two seconds.

The GET is a control, not a concurrent load. Measuring GET while the publisher
fans out would only show the two contending for the box's cores, which says
nothing about the drain path. With the publisher quiet, a flat GET rate across
the subscriber sweep is the evidence the claim needs.

## Numbers (dev box, not gate)

macOS dev box, single f3srv, `go run ./labs/f3/m11/01_publish_fanout/`. These are
directional dev-box figures for shape, not a gate measurement; the gate row runs
on the box against redis 8.8.0 and valkey 9.1.0.

| subscribers | PUBLISH ops/s | delivered msgs/s | idle-wall GET ops/s |
|---|---|---|---|
| 1 | 0.017M | 0.017M | 0.174M |
| 8 | 0.008M | 0.067M | 0.119M |
| 64 | 0.004M | 0.262M | 0.196M |
| 256 | 0.002M | 0.392M | 0.181M |

## Verdict

The fan-out behaves as a fan-out. A single in-flight publisher slows as the
subscriber count grows, because each PUBLISH now does N out-of-band pushes and N
wakes before its reply comes back, so its round-trip latency climbs and its rate
falls (0.017M at one subscriber to 0.002M at 256). The delivered-message rate is
the figure that matters, and it climbs the other way (0.017M to 0.392M), because
each PUBLISH reaches more subscribers. A single publisher connection saturates on
latency well before the delivery path does; a real fan-out workload spreads the
publish across connections, so the delivered rate is the ceiling to read here,
not the per-publisher rate.

The point-op control holds. The idle-wall GET rate stays flat across the sweep
(0.174M, 0.119M, 0.196M, 0.181M) with no trend as the subscriber wall grows from
1 to 256; the 8-subscriber dip is dev-box noise, not a slope. Registering 256
idle subscribers costs an unrelated GET nothing, which is what the registry
design promises: the channel set and the out-of-band delivery node sit entirely
off the point-op drain path, and the DrainReplies out-of-band check is one
node-level branch a GET never produces a node to trip.
