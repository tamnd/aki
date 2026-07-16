// Package resp is the wire codec for obs1srv, ported by copy from
// f3srv/resp per the 2064/obs1 doc 11 section 2 inventory: the RESP2
// parser and the presized single-pass reply builder (spec 2064/f3/03
// F19), plus the Redis-parity float formatter the score and float
// replies ride. Every file except this one is byte-identical to the
// f3 original.
package resp
