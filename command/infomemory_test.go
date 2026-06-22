package command

import "testing"

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
