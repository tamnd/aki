// The latency model (spec 2064/obs1 doc 01 section 2.2): per-class
// lognormal distributions carried as two quantiles, because that is what
// the published measurements state and what the O5 E-cloud refit will
// state again. A zero model draws zero, which is what unit tests and the
// differential suite want.
package sim

import (
	"math"
	"time"
)

// Dist is one latency distribution, lognormal through its p50 and p99.
type Dist struct {
	P50 time.Duration
	P99 time.Duration
}

// z99 is the standard normal quantile at 0.99.
const z99 = 2.3263

// draw maps a standard normal z onto the distribution.
func (d Dist) draw(z float64) time.Duration {
	if d.P50 <= 0 {
		return 0
	}
	sigma := math.Log(float64(d.P99)/float64(d.P50)) / z99
	return time.Duration(float64(d.P50) * math.Exp(sigma*z))
}

// LatencyModel gives each op class its distribution: reads draw from Get,
// everything that mutates draws from Put.
type LatencyModel struct {
	Get Dist
	Put Dist
}

// S3Standard is the doc 01 section 2.2 envelope, midpoints of the stated
// ranges: GET p50 10-30ms and p99 100-200ms, PUT p50 20-50ms with the PUT
// tail assumed as heavy as the GET tail until the O5 E-cloud fit says
// otherwise (the doc 10 honesty gate re-fits this from measurements).
var S3Standard = LatencyModel{
	Get: Dist{P50: 20 * time.Millisecond, P99: 150 * time.Millisecond},
	Put: Dist{P50: 35 * time.Millisecond, P99: 260 * time.Millisecond},
}
