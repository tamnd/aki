package command

import "testing"

// TestHzInfoLive checks that CONFIG SET hz takes effect right away in the INFO
// server section, both the configured rate and the effective rate the cron uses.
func TestHzInfoLive(t *testing.T) {
	r, c := startData(t)

	// Default rate.
	if got := infoField(t, r, c, "server", "configured_hz"); got != "10" {
		t.Fatalf("default configured_hz = %q want 10", got)
	}

	// A live change shows up at once. With one client and dynamic-hz on, the
	// effective rate equals the configured rate.
	if got := sendLine(t, r, c, "CONFIG SET hz 50"); got != "+OK" {
		t.Fatalf("CONFIG SET hz = %q", got)
	}
	if got := infoField(t, r, c, "server", "configured_hz"); got != "50" {
		t.Fatalf("configured_hz after set = %q want 50", got)
	}
	if got := infoField(t, r, c, "server", "hz"); got != "50" {
		t.Fatalf("hz after set = %q want 50", got)
	}

	// An out-of-range value is accepted and clamped, matching Redis.
	if got := sendLine(t, r, c, "CONFIG SET hz 1000"); got != "+OK" {
		t.Fatalf("CONFIG SET hz 1000 = %q", got)
	}
	if got := infoField(t, r, c, "server", "configured_hz"); got != "500" {
		t.Fatalf("configured_hz after over-range set = %q want 500", got)
	}
}

// TestConfiguredHz checks the base cron rate tracks the hz directive and clamps
// to the legal 1..500 range instead of rejecting out-of-range values.
func TestConfiguredHz(t *testing.T) {
	d := New(Config{})
	cases := []struct {
		set  string
		want int
	}{
		{"10", 10}, // default
		{"50", 50},
		{"1", 1},
		{"500", 500},
		{"0", 1},     // below the floor clamps up
		{"-5", 1},    // negative clamps up
		{"501", 500}, // above the ceiling clamps down
		{"100000", 500},
	}
	for _, c := range cases {
		d.conf.set("hz", c.set)
		if got := d.configuredHz(); got != c.want {
			t.Errorf("configuredHz with hz=%s = %d want %d", c.set, got, c.want)
		}
	}
}

// TestEffectiveHzNoServer checks that without a wired server, or with dynamic-hz
// off, the effective rate is just the configured rate.
func TestEffectiveHzNoServer(t *testing.T) {
	d := New(Config{})
	d.conf.set("hz", "20")

	// No server attached, so no client count to scale against.
	if got := d.effectiveHz(); got != 20 {
		t.Fatalf("effectiveHz with no server = %d want 20", got)
	}

	// dynamic-hz off pins it to the configured rate even once a server is wired.
	d.conf.set("dynamic-hz", "no")
	if got := d.effectiveHz(); got != 20 {
		t.Fatalf("effectiveHz with dynamic-hz off = %d want 20", got)
	}
}

// TestScaleHz checks the dynamic-hz scaling math: the rate doubles while the
// client count outgrows it by the threshold, capped at the ceiling.
func TestScaleHz(t *testing.T) {
	cases := []struct {
		base    int
		clients int
		want    int
	}{
		{10, 0, 10},          // idle server stays at the base rate
		{10, 100, 10},        // 100/10 = 10, under the threshold
		{10, 2010, 20},       // 2010/10 = 201 over the threshold, doubles once
		{1, 201, 2},          // 201/1 over, 201/2 under, one double
		{1, 30000, 256},      // climbs by doubling until 30000/256 falls under
		{10, 5_000_000, 500}, // would exceed the ceiling, clamped to 500
	}
	for _, c := range cases {
		if got := scaleHz(c.base, c.clients); got != c.want {
			t.Errorf("scaleHz(%d, %d) = %d want %d", c.base, c.clients, got, c.want)
		}
	}
}
