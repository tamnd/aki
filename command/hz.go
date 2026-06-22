package command

// Redis bounds the server cron rate to this range. CONFIG SET hz takes any
// integer, matching real Redis, but the rate the cron and INFO actually use is
// clamped here. dynamic-hz can raise the rate above the configured value, up to
// the ceiling, when many clients are connected.
const (
	minHz = 1
	maxHz = 500
	// hzClientsPerTick is the client-to-hz ratio that makes dynamic-hz double the
	// rate. Redis keeps each cron pass touching at most a couple hundred clients,
	// so once the connected count outgrows the rate by this factor it speeds up.
	hzClientsPerTick = 200
)

// configuredHz returns the base cron rate from the hz directive, clamped to the
// legal range. This is the configured_hz INFO field. CONFIG SET hz accepts any
// integer the way Redis does, but a value below 1 or above 500 is clamped here
// rather than rejected, since the directive has no effect outside that range.
func (d *Dispatcher) configuredHz() int {
	hz := int(d.confInt("hz", 10))
	if hz < minHz {
		return minHz
	}
	if hz > maxHz {
		return maxHz
	}
	return hz
}

// effectiveHz returns the rate the cron loop actually ticks at. With dynamic-hz
// on it scales the base rate up while the connected client count outgrows it, so
// a busy server runs expiry and the other cron work more often, capped at 500.
// With dynamic-hz off, or before the server is wired up, it is just the
// configured rate. This is the hz INFO field.
func (d *Dispatcher) effectiveHz() int {
	hz := d.configuredHz()
	if !d.confBool("dynamic-hz", true) || d.srv == nil {
		return hz
	}
	return scaleHz(hz, d.srv.CountClients())
}

// scaleHz raises the base rate while the client count outgrows it, doubling each
// step until the ratio falls back under the threshold or the rate hits the
// ceiling. This is the dynamic-hz math, kept pure so it tests without a live
// server.
func scaleHz(base, clients int) int {
	hz := base
	for clients/hz > hzClientsPerTick {
		hz *= 2
		if hz > maxHz {
			return maxHz
		}
	}
	return hz
}
