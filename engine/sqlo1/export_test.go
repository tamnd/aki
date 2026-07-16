package sqlo1

// Test-only handles. The WAL and the recovery entry are package-internal
// API used by the server loop, but the Track A recovery-order test has to
// live in an external test package because engine/sqlo1a imports this
// package; these hand it the internals.
var (
	OpenWALForTest      = openWAL
	RecoverStoreForTest = recoverStore
	AppendPutForTest    = appendPutPayload
	AppendDelForTest    = appendDelPayload
)

const (
	WalOpPutForTest = walOpPut
	WalOpDelForTest = walOpDel
)

// SetExpireForTest stamps an absolute expire_ms on a hot key; the real
// expiry surface arrives when doc 11 wires the wheel into Tiered, and
// the sqlo1b integration test needs the stamp before that.
func (t *Tiered) SetExpireForTest(key []byte, atMs int64) {
	t.ht.setExpireMs(key, atMs)
}

// EvictAllForTest drops every resident header, forcing the next reads
// through the cold path.
func (t *Tiered) EvictAllForTest() {
	for s := range t.ht.hdrs {
		if t.ht.hdrs[s].state == stateResident {
			t.ht.evict(uint32(s), true)
		}
	}
}
