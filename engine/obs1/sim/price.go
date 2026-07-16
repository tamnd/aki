// The price table (spec 2064/obs1 doc 09 section 1) as data, and the
// four-line bill the labs assert. A price cut is an edit here, never a
// code change, and the doc 09 rule stands: the table's numbers flip to
// trusted only from doc 01 citations dated within a year of a gate run.
package sim

import "time"

// PriceTable prices simulated requests. Dollars, us-east-1 shapes.
type PriceTable struct {
	StorageGBMonth float64 // per GB-month held
	PutPerM        float64 // per million PUT-class requests
	GetPerM        float64 // per million GET-class requests
}

// S3StandardPrices is the doc 09 section 1 sheet, its C-tagged rows.
// Deletes, aborts, and lifecycle transitions out are free there, so the
// sim tracks them in FreeRequests and bills nothing.
var S3StandardPrices = PriceTable{
	StorageGBMonth: 0.023,
	PutPerM:        5.00,
	GetPerM:        0.40,
}

// Usage is what a run consumed. PutRequests covers PUT, conditional PUT,
// and the three billed multipart calls; GetRequests covers whole, ranged,
// and tail reads; FreeRequests covers deletes, batch deletes, and aborts.
// Counts are Store operations: the wire client's internal retries are
// invisible up here, which the doc 10 honesty gate is there to catch if
// the gap ever moves a bill.
type Usage struct {
	PutRequests  int64
	GetRequests  int64
	FreeRequests int64

	BytesUp   int64
	BytesDown int64

	BytesStored     int64 // resident now
	BytesStoredPeak int64 // high-water mark, what capacity is provisioned for
}

// Bill is the sim's share of doc 09's four lines; nodes are priced by the
// fleet labs, not by a bucket.
type Bill struct {
	Storage float64
	Puts    float64
	Gets    float64
	Total   float64
}

// Bill prices the run: requests at the table's rates, and the peak
// resident bytes held for the given duration at the storage rate.
func (p PriceTable) Bill(u Usage, held time.Duration) Bill {
	const gbMonth = float64(730 * time.Hour)
	b := Bill{
		Storage: float64(u.BytesStoredPeak) / (1 << 30) * p.StorageGBMonth * float64(held) / gbMonth,
		Puts:    float64(u.PutRequests) / 1e6 * p.PutPerM,
		Gets:    float64(u.GetRequests) / 1e6 * p.GetPerM,
	}
	b.Total = b.Storage + b.Puts + b.Gets
	return b
}
