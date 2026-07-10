package shard

// The in-package test handler table. The shard layer cannot import the real
// command packages (they import shard), so the tests register a minimal
// PING/ECHO/GET/SET/INCR surface over the store, the same shape the dispatch
// table wires in production.

const (
	opPing byte = iota + 1
	opEcho
	opGet
	opSet
)

func testHandlers() []Handler {
	return []Handler{
		opPing: func(cx *Ctx, args [][]byte, r Reply) {
			if len(args) == 0 {
				r.Status("PONG")
				return
			}
			r.Bulk(args[0])
		},
		opEcho: func(cx *Ctx, args [][]byte, r Reply) {
			r.Bulk(args[0])
		},
		opGet: func(cx *Ctx, args [][]byte, r Reply) {
			v, ok := cx.St.Get(args[0], cx.Val)
			cx.Val = v
			if !ok {
				r.Null()
				return
			}
			r.Bulk(v)
		},
		opSet: func(cx *Ctx, args [][]byte, r Reply) {
			if err := cx.St.Set(args[0], args[1]); err != nil {
				r.Err("ERR " + err.Error())
				return
			}
			r.Status("OK")
		},
	}
}

func testRuntime(shards int) *Runtime {
	rt := New(shards, testArena, testSeg)
	rt.Use(testHandlers())
	return rt
}
