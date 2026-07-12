package shard

import "testing"

// TestConnSpinBudgetCollapsesAtHighWater pins the connSpinHighWater switch: a
// connection writer spins the full calibrated window while few connections are
// live, and collapses to no spin (parks immediately) once the live count
// reaches the high-water. The decision is the spinBudget helper so the test is
// deterministic and pays no timing.
func TestConnSpinBudgetCollapsesAtHighWater(t *testing.T) {
	rt := testRuntime(1)
	c := rt.NewConn()

	saved := connSpinHighWater
	t.Cleanup(func() { connSpinHighWater = saved })
	connSpinHighWater = 4

	// No live connections registered yet: the writer spins the full window.
	if got := c.spinBudget(); got != spinIters {
		t.Fatalf("below high-water spinBudget = %d, want spinIters %d", got, spinIters)
	}

	// Drive the live count up to one below the high-water: still spinning.
	rt.ConnOpened()
	rt.ConnOpened()
	rt.ConnOpened()
	if got := rt.live.Load(); got != 3 {
		t.Fatalf("live after three ConnOpened = %d, want 3", got)
	}
	if got := c.spinBudget(); got != spinIters {
		t.Fatalf("one below high-water spinBudget = %d, want spinIters %d", got, spinIters)
	}

	// Reaching the high-water collapses the spin to 0 (park immediately).
	rt.ConnOpened()
	if got := c.spinBudget(); got != 0 {
		t.Fatalf("at high-water spinBudget = %d, want 0", got)
	}

	// Dropping back below restores the spin.
	rt.ConnClosed()
	if got := rt.live.Load(); got != 3 {
		t.Fatalf("live after ConnClosed = %d, want 3", got)
	}
	if got := c.spinBudget(); got != spinIters {
		t.Fatalf("back below high-water spinBudget = %d, want spinIters %d", got, spinIters)
	}
}

// TestSetConnSpinHighWaterClamps pins that the sweep knob never disables the
// park path with a zero or negative threshold (which would make every writer
// park immediately by comparison, or worse underflow intent).
func TestSetConnSpinHighWaterClamps(t *testing.T) {
	saved := connSpinHighWater
	t.Cleanup(func() { connSpinHighWater = saved })

	SetConnSpinHighWater(0)
	if connSpinHighWater != 1 {
		t.Fatalf("SetConnSpinHighWater(0) = %d, want clamp to 1", connSpinHighWater)
	}
	SetConnSpinHighWater(-5)
	if connSpinHighWater != 1 {
		t.Fatalf("SetConnSpinHighWater(-5) = %d, want clamp to 1", connSpinHighWater)
	}
	SetConnSpinHighWater(128)
	if connSpinHighWater != 128 {
		t.Fatalf("SetConnSpinHighWater(128) = %d, want 128", connSpinHighWater)
	}
}
