// Package bulkarray prices how f1srv frames a multi-member array reply (SINTER/SUNION/SDIFF/SMEMBERS):
// an array header followed by one RESP bulk string per member. The server writes each member with
// writeBulk, which appends five times into the connection's out buffer: the '$', the decimal length,
// the header CRLF, the payload, and the trailing CRLF. A high-overlap SINTER over 256-member sets
// returns a few hundred members, so the reply is a few hundred writeBulk calls, ~5 appends each.
//
// At the small end (a 256-member set the merge itself clears in a few hundred comparisons) the reply
// encoding is a real share of the per-command cost, which is why small-N SINTER sits near parity with
// Redis while large-N pulls ahead: the merge amortizes the fixed overhead but the per-member reply
// framing does not. This lab compares the per-member five-append writer against a fused writer that
// measures the whole array reply's byte length once, grows the buffer a single time, and fills it with
// a moving index, so the gap is exactly the append-call and repeated-grow-check overhead the fused
// form removes, with the same output bytes.
package bulkarray
