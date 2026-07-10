// Package dispatch is the command table and per-command routing for f3srv:
// parse once, hash once, route to the owning shard, and reorder replies per
// connection (spec 2064/f3/03).
package dispatch
