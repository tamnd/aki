package main

import "testing"

// TestAutoPoolBytesFor checks the cap-aware sizing rule across the cases that
// matter: no cap keeps the historical default, a cgroup cap below host RAM sizes
// to a quarter of the cap, a tiny cap is floored, and the unlimited/sentinel cases
// (limit zero, or limit at or above host RAM) fall back to the default.
func TestAutoPoolBytesFor(t *testing.T) {
	const (
		mb = 1024 * 1024
		gb = 1024 * mb
	)
	cases := []struct {
		name        string
		host, limit int64
		want        int64
	}{
		{"uncapped bare metal", 64 * gb, 0, defaultBufferPoolBytes},
		{"limit equals host (v1 sentinel clamped to host)", 64 * gb, 64 * gb, defaultBufferPoolBytes},
		{"limit above host", 16 * gb, 32 * gb, defaultBufferPoolBytes},
		{"300m cgroup over 12g host", 12 * gb, 300 * mb, 75 * mb},
		{"tiny 40m cap floored", 12 * gb, 40 * mb, minAutoBufferPoolBytes},
		{"roomy 8g container quarters", 64 * gb, 8 * gb, 2 * gb},
		{"host unknown, capped", 0, 300 * mb, 75 * mb},
		{"both unknown", 0, 0, defaultBufferPoolBytes},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := autoPoolBytesFor(c.host, c.limit); got != c.want {
				t.Errorf("autoPoolBytesFor(%d, %d) = %d, want %d", c.host, c.limit, got, c.want)
			}
		})
	}
}

// TestParseBufPoolPagesAuto checks that the "auto" setting resolves to a positive
// page count above the floor. The exact value depends on the host's cgroup state,
// so this only asserts the shape, while the sizing math is pinned by
// TestAutoPoolBytesFor.
func TestParseBufPoolPagesAuto(t *testing.T) {
	pages, err := parseBufPoolPages("auto")
	if err != nil {
		t.Fatalf("parseBufPoolPages(auto): %v", err)
	}
	if pages < 64 {
		t.Errorf("auto pool = %d pages, want at least the 64-page floor", pages)
	}
	// "AUTO" is case-insensitive.
	upper, err := parseBufPoolPages("AUTO")
	if err != nil {
		t.Fatalf("parseBufPoolPages(AUTO): %v", err)
	}
	if upper != pages {
		t.Errorf("AUTO = %d pages, auto = %d pages, want equal", upper, pages)
	}
}
