package bench

import (
	"math"
	"testing"
)

// TestHistogramPercentileAccuracy records a known range and checks the reported
// percentiles land within the histogram's resolution of the true values.
func TestHistogramPercentileAccuracy(t *testing.T) {
	h := NewHistogram(1, int64(60)*1e9, 3)
	// Record 1..10000 ns once each. The p50 should be near 5000, p90 near 9000.
	for v := int64(1); v <= 10000; v++ {
		h.RecordValue(v)
	}
	if h.TotalCount() != 10000 {
		t.Fatalf("total count = %d want 10000", h.TotalCount())
	}
	check := func(p float64, want int64) {
		got := h.ValueAtPercentile(p)
		rel := math.Abs(float64(got-want)) / float64(want)
		if rel > 0.01 {
			t.Fatalf("p%.1f = %d want ~%d (rel err %.4f)", p, got, want, rel)
		}
	}
	check(50, 5000)
	check(90, 9000)
	check(99, 9900)
	check(100, 10000)
}

// TestHistogramMinMax checks the min and max track the extremes.
func TestHistogramMinMax(t *testing.T) {
	h := NewHistogram(1, int64(60)*1e9, 3)
	for _, v := range []int64{42, 1000, 7, 999999} {
		h.RecordValue(v)
	}
	if h.Min() > 7 {
		t.Fatalf("min = %d want <= 7", h.Min())
	}
	if h.Max() < 999999 {
		t.Fatalf("max = %d want >= 999999", h.Max())
	}
}

// TestHistogramMean checks the mean of a uniform run lands near the midpoint.
func TestHistogramMean(t *testing.T) {
	h := NewHistogram(1, int64(60)*1e9, 3)
	for v := int64(1); v <= 1000; v++ {
		h.RecordValue(v)
	}
	mean := h.Mean()
	if mean < 495 || mean > 505 {
		t.Fatalf("mean = %.2f want ~500", mean)
	}
}

// TestHistogramRecordValues checks the bulk record path used by CO correction.
func TestHistogramRecordValues(t *testing.T) {
	h := NewHistogram(1, int64(60)*1e9, 3)
	h.RecordValue(100)
	h.RecordValues(1000000000, 999) // 999 synthetic 1s stalls
	if h.TotalCount() != 1000 {
		t.Fatalf("total = %d want 1000", h.TotalCount())
	}
	// With 999 of 1000 samples at 1s, the p50 is the stall value.
	got := h.ValueAtPercentile(50)
	rel := math.Abs(float64(got-1000000000)) / 1000000000
	if rel > 0.01 {
		t.Fatalf("p50 = %d want ~1e9", got)
	}
}

// TestHistogramMerge checks two histograms fold into one.
func TestHistogramMerge(t *testing.T) {
	a := NewHistogram(1, int64(60)*1e9, 3)
	b := NewHistogram(1, int64(60)*1e9, 3)
	for v := int64(1); v <= 500; v++ {
		a.RecordValue(v)
	}
	for v := int64(501); v <= 1000; v++ {
		b.RecordValue(v)
	}
	a.Merge(b)
	if a.TotalCount() != 1000 {
		t.Fatalf("merged total = %d want 1000", a.TotalCount())
	}
	if a.Max() < 1000 {
		t.Fatalf("merged max = %d want >= 1000", a.Max())
	}
}

// TestHistogramClampsHigh checks a value above the range is counted at the top
// rather than dropped.
func TestHistogramClampsHigh(t *testing.T) {
	h := NewHistogram(1, 1000, 3)
	h.RecordValue(5000)
	if h.TotalCount() != 1 {
		t.Fatalf("count = %d want 1", h.TotalCount())
	}
}
