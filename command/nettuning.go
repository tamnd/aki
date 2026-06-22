package command

import "time"

// This file wires the network idle knobs from doc 24 section A.5, timeout and
// tcp-keepalive, into the running server. The networking server holds both as
// live values, so a change through CONFIG SET takes effect without a restart.

// ApplyNetworkConfig pushes timeout and tcp-keepalive to the server. The server
// command calls it once at startup after the server is attached, and CONFIG SET
// calls the per-knob setters when either value changes.
func (d *Dispatcher) ApplyNetworkConfig() {
	d.applyIdleTimeout()
	d.applyTCPKeepAlive()
}

// applyIdleTimeout sets the idle client timeout. The directive is in seconds and
// 0 disables the timeout, matching Redis.
func (d *Dispatcher) applyIdleTimeout() {
	if d.srv == nil {
		return
	}
	d.srv.SetIdleTimeout(time.Duration(d.confInt("timeout", 0)) * time.Second)
}

// applyTCPKeepAlive sets the TCP keepalive period. The directive is in seconds
// and 0 leaves the OS default. It applies to connections accepted after the
// change, the same as Redis.
func (d *Dispatcher) applyTCPKeepAlive() {
	if d.srv == nil {
		return
	}
	d.srv.SetTCPKeepAlive(time.Duration(d.confInt("tcp-keepalive", 300)) * time.Second)
}
