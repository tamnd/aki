package command

import (
	"strconv"
	"strings"
	"testing"
)

// TestClusterKeyslot checks the slot mapping matches real Redis for known keys
// and that the hash-tag rule coerces tagged keys to the same slot.
func TestClusterKeyslot(t *testing.T) {
	r, c := startData(t)

	cases := []struct {
		key  string
		slot int64
	}{
		{"foo", 12182},
		{"bar", 5061},
		{"", 0},
		{"123456789", 12739},
	}
	for _, tc := range cases {
		got := sendArgs(t, r, c, "CLUSTER", "KEYSLOT", tc.key)
		if got != tc.slot {
			t.Fatalf("KEYSLOT %q = %v want %d", tc.key, got, tc.slot)
		}
	}

	// Hash tags: the substring inside the first {} decides the slot, so these two
	// land together and match the bare tag.
	a := sendArgs(t, r, c, "CLUSTER", "KEYSLOT", "{user}.following")
	b := sendArgs(t, r, c, "CLUSTER", "KEYSLOT", "{user}.followers")
	bare := sendArgs(t, r, c, "CLUSTER", "KEYSLOT", "user")
	if a != b || a != bare {
		t.Fatalf("hash tag slots differ: %v %v %v", a, b, bare)
	}

	// An empty tag falls back to the whole key.
	empty := sendArgs(t, r, c, "CLUSTER", "KEYSLOT", "{}x")
	whole := sendArgs(t, r, c, "CLUSTER", "KEYSLOT", "user")
	if empty == whole {
		t.Fatalf("empty tag should not match bare key")
	}
}

// TestClusterMyIDStable checks CLUSTER MYID is a 40-hex string and stable across
// calls.
func TestClusterMyIDStable(t *testing.T) {
	r, c := startData(t)
	id1, _ := sendArgs(t, r, c, "CLUSTER", "MYID").(string)
	id2, _ := sendArgs(t, r, c, "CLUSTER", "MYID").(string)
	if len(id1) != 40 || id1 != id2 {
		t.Fatalf("MYID not stable 40-hex: %q %q", id1, id2)
	}
}

// TestClusterInfoDisabled checks the default single-node report.
func TestClusterInfoDisabled(t *testing.T) {
	r, c := startData(t)
	info, _ := sendArgs(t, r, c, "CLUSTER", "INFO").(string)
	for _, want := range []string{
		"cluster_enabled:0",
		"cluster_state:ok",
		"cluster_slots_assigned:0",
		"cluster_known_nodes:0",
	} {
		if !strings.Contains(info, want) {
			t.Fatalf("CLUSTER INFO missing %q\n%s", want, info)
		}
	}
}

// TestClusterNodesSingleLine checks CLUSTER NODES describes the local node as
// myself,master.
func TestClusterNodesSingleLine(t *testing.T) {
	r, c := startData(t)
	nodes, _ := sendArgs(t, r, c, "CLUSTER", "NODES").(string)
	if !strings.Contains(nodes, "myself,master") {
		t.Fatalf("CLUSTER NODES missing myself,master\n%s", nodes)
	}
	id, _ := sendArgs(t, r, c, "CLUSTER", "MYID").(string)
	if !strings.HasPrefix(nodes, id) {
		t.Fatalf("CLUSTER NODES should start with node id %q\n%s", id, nodes)
	}
}

// TestClusterSlotsEmpty checks SLOTS and SHARDS are empty when no slots are owned.
func TestClusterSlotsEmpty(t *testing.T) {
	r, c := startData(t)
	slots := asArray(t, sendArgs(t, r, c, "CLUSTER", "SLOTS"))
	if len(slots) != 0 {
		t.Fatalf("CLUSTER SLOTS = %v want empty", slots)
	}
	shards := asArray(t, sendArgs(t, r, c, "CLUSTER", "SHARDS"))
	if len(shards) != 0 {
		t.Fatalf("CLUSTER SHARDS = %v want empty", shards)
	}
}

// TestClusterMutationDisabled checks slot management is refused when cluster mode
// is off.
func TestClusterMutationDisabled(t *testing.T) {
	r, c := startData(t)
	got := sendArgs(t, r, c, "CLUSTER", "ADDSLOTS", "1")
	e, ok := got.(cmdErr)
	if !ok || !strings.Contains(string(e), "cluster support disabled") {
		t.Fatalf("ADDSLOTS while disabled = %v want disabled error", got)
	}
}

// TestClusterCountKeysInSlot checks the count reflects keys hashed into a slot.
func TestClusterCountKeysInSlot(t *testing.T) {
	r, c := startData(t)
	// Two keys sharing a hash tag share a slot.
	sendArgs(t, r, c, "SET", "{grp}.a", "1")
	sendArgs(t, r, c, "SET", "{grp}.b", "2")
	slot := sendArgs(t, r, c, "CLUSTER", "KEYSLOT", "grp").(int64)

	got := sendArgs(t, r, c, "CLUSTER", "COUNTKEYSINSLOT", itoa(slot))
	if got != int64(2) {
		t.Fatalf("COUNTKEYSINSLOT %d = %v want 2", slot, got)
	}
	keys := bulkSlice(t, sendArgs(t, r, c, "CLUSTER", "GETKEYSINSLOT", itoa(slot), "10"))
	if len(keys) != 2 {
		t.Fatalf("GETKEYSINSLOT = %v want 2 keys", keys)
	}
}

// TestClusterSlotManagementEnabled checks add/del slots and the reports they
// drive once cluster mode is on.
func TestClusterSlotManagementEnabled(t *testing.T) {
	r, c := startData(t)
	if got := sendArgs(t, r, c, "CONFIG", "SET", "cluster-enabled", "yes"); got != "OK" {
		t.Fatalf("CONFIG SET cluster-enabled = %v", got)
	}

	if got := sendArgs(t, r, c, "CLUSTER", "ADDSLOTSRANGE", "0", "100"); got != "OK" {
		t.Fatalf("ADDSLOTSRANGE = %v", got)
	}
	// A slot already owned is busy.
	busy := sendArgs(t, r, c, "CLUSTER", "ADDSLOTS", "50")
	if e, ok := busy.(cmdErr); !ok || !strings.Contains(string(e), "is already busy") {
		t.Fatalf("ADDSLOTS on owned slot = %v want busy error", busy)
	}

	info, _ := sendArgs(t, r, c, "CLUSTER", "INFO").(string)
	if !strings.Contains(info, "cluster_enabled:1") || !strings.Contains(info, "cluster_slots_assigned:101") {
		t.Fatalf("CLUSTER INFO after addslots\n%s", info)
	}

	slots := asArray(t, sendArgs(t, r, c, "CLUSTER", "SLOTS"))
	if len(slots) != 1 {
		t.Fatalf("CLUSTER SLOTS = %v want one range", slots)
	}
	first := asArray(t, slots[0])
	if first[0] != int64(0) || first[1] != int64(100) {
		t.Fatalf("slot range = %v %v want 0 100", first[0], first[1])
	}

	if got := sendArgs(t, r, c, "CLUSTER", "DELSLOTSRANGE", "0", "100"); got != "OK" {
		t.Fatalf("DELSLOTSRANGE = %v", got)
	}
	slots = asArray(t, sendArgs(t, r, c, "CLUSTER", "SLOTS"))
	if len(slots) != 0 {
		t.Fatalf("CLUSTER SLOTS after del = %v want empty", slots)
	}
}

// TestClusterResetClears checks RESET clears slot assignments and HARD changes the
// node id.
func TestClusterResetClears(t *testing.T) {
	r, c := startData(t)
	sendArgs(t, r, c, "CONFIG", "SET", "cluster-enabled", "yes")
	sendArgs(t, r, c, "CLUSTER", "ADDSLOTSRANGE", "0", "10")
	id1, _ := sendArgs(t, r, c, "CLUSTER", "MYID").(string)

	if got := sendArgs(t, r, c, "CLUSTER", "RESET", "HARD"); got != "OK" {
		t.Fatalf("CLUSTER RESET HARD = %v", got)
	}
	id2, _ := sendArgs(t, r, c, "CLUSTER", "MYID").(string)
	if id1 == id2 {
		t.Fatalf("RESET HARD should change node id")
	}
	info, _ := sendArgs(t, r, c, "CLUSTER", "INFO").(string)
	if !strings.Contains(info, "cluster_slots_assigned:0") {
		t.Fatalf("RESET should clear slots\n%s", info)
	}
}

func itoa(n int64) string {
	return strconv.FormatInt(n, 10)
}
