package command

import (
	"strings"
	"testing"
)

// TestInfoMemoryStaticFields checks the memory section carries the allocator and
// scripting fields from doc 20 section 1.4, with integer values.
func TestInfoMemoryStaticFields(t *testing.T) {
	r, c := startData(t)

	for _, f := range []string{
		"allocator_allocated", "allocator_active", "allocator_resident",
		"allocator_frag_bytes",
		"used_memory_vm_eval", "used_memory_scripts_eval",
		"used_memory_vm_functions", "used_memory_vm_total",
		"used_memory_functions", "used_memory_scripts",
		"number_of_cached_scripts", "number_of_functions", "number_of_libraries",
	} {
		got := infoField(t, r, c, "memory", f)
		if got == "" {
			t.Fatalf("memory field %s missing", f)
		}
	}

	// The human variants are present too.
	for _, f := range []string{"used_memory_lua_human", "used_memory_vm_total_human", "used_memory_scripts_human"} {
		if got := infoField(t, r, c, "memory", f); got == "" {
			t.Fatalf("memory field %s missing", f)
		}
	}
}

// TestInfoMemoryReportingFields checks the derived reporting fields from doc 20
// section 1.4: the percent strings, the dataset split, the allocator-rss and
// rss-overhead pairs, the fragmentation bytes, and the not-metered mem_* fields.
func TestInfoMemoryReportingFields(t *testing.T) {
	r, c := startData(t)

	// The percent fields carry the "X.YZ%" shape.
	for _, f := range []string{"used_memory_peak_perc", "used_memory_dataset_perc"} {
		got := infoField(t, r, c, "memory", f)
		if !strings.HasSuffix(got, "%") {
			t.Fatalf("%s = %q want a percent string", f, got)
		}
	}

	// The dataset split matches the MEMORY STATS view: overhead and startup are 0
	// and dataset equals used_memory.
	used := infoField(t, r, c, "memory", "used_memory")
	for _, c2 := range []struct{ k, want string }{
		{"used_memory_overhead", "0"},
		{"used_memory_startup", "0"},
		{"used_memory_dataset", used},
		{"mem_not_counted_for_evict", "0"},
		{"mem_replication_backlog", "0"},
		{"mem_total_replication_buffers", "0"},
		{"mem_clients_slaves", "0"},
		{"mem_clients_normal", "0"},
		{"mem_cluster_links", "0"},
		{"mem_aof_buffer", "0"},
	} {
		if got := infoField(t, r, c, "memory", c2.k); got != c2.want {
			t.Fatalf("%s = %q want %q", c2.k, got, c2.want)
		}
	}

	// The allocator-rss and rss-overhead pairs and the fragmentation bytes are
	// present. They derive from the same runtime counters the ratios already use.
	for _, f := range []string{
		"allocator_rss_ratio", "allocator_rss_bytes",
		"rss_overhead_ratio", "rss_overhead_bytes",
		"mem_fragmentation_bytes",
		"total_system_memory", "total_system_memory_human",
	} {
		if got := infoField(t, r, c, "memory", f); got == "" {
			t.Fatalf("memory field %s missing", f)
		}
	}
}

// TestInfoMemoryScriptCounts checks number_of_cached_scripts tracks SCRIPT LOAD
// and the function counts track FUNCTION LOAD.
func TestInfoMemoryScriptCounts(t *testing.T) {
	r, c := startData(t)

	if got := infoField(t, r, c, "memory", "number_of_cached_scripts"); got != "0" {
		t.Fatalf("number_of_cached_scripts before load = %q want 0", got)
	}
	sendArgs(t, r, c, "SCRIPT", "LOAD", "return 1")
	if got := infoField(t, r, c, "memory", "number_of_cached_scripts"); got != "1" {
		t.Fatalf("number_of_cached_scripts after load = %q want 1", got)
	}

	if got := infoField(t, r, c, "memory", "number_of_libraries"); got != "0" {
		t.Fatalf("number_of_libraries before load = %q want 0", got)
	}
	if got := sendArgs(t, r, c, "FUNCTION", "LOAD", libGetSet); got != "mylib" {
		t.Fatalf("FUNCTION LOAD = %v", got)
	}
	if got := infoField(t, r, c, "memory", "number_of_libraries"); got != "1" {
		t.Fatalf("number_of_libraries after load = %q want 1", got)
	}
	// libGetSet registers myget and myset.
	if got := infoField(t, r, c, "memory", "number_of_functions"); got != "2" {
		t.Fatalf("number_of_functions after load = %q want 2", got)
	}
}
