package resp

import "strconv"

// Pre-built replies for the most common cases, kept as package-level byte slices
// so the hot path can write them with a single copy and no per-request
// allocation (doc 06 §11.6). The networking layer writes these directly into a
// client's output buffer; callers must never mutate the returned slices.
var (
	ReplyOK         = []byte("+OK\r\n")
	ReplyPong       = []byte("+PONG\r\n")
	ReplyQueued     = []byte("+QUEUED\r\n")
	ReplyNil2       = []byte("$-1\r\n")
	ReplyNil3       = []byte("_\r\n")
	ReplyNilArray2  = []byte("*-1\r\n")
	ReplyEmptyArray = []byte("*0\r\n")
	ReplyEmptyMap3  = []byte("%0\r\n")
	ReplyEmptySet3  = []byte("~0\r\n")
	ReplyZero       = []byte(":0\r\n")
	ReplyOne        = []byte(":1\r\n")
	ReplyFalse3     = []byte("#f\r\n")
	ReplyTrue3      = []byte("#t\r\n")
	ReplyReset      = []byte("+RESET\r\n")

	// ReplyMaxClients is written to a connection that is accepted only to be
	// rejected because the server is at its maxclients limit (doc 19 §1.4).
	ReplyMaxClients = []byte("-ERR max number of clients reached\r\n")
)

// The integer pool pre-renders the small integers that dominate real traffic
// (reply counts, INCR results near zero) so WriteInteger can skip strconv for
// them. The window is -2..9999; index = n + intPoolLow.
const (
	intPoolLow  = -2
	intPoolHigh = 9999
)

var integerPool = func() [][]byte {
	p := make([][]byte, intPoolHigh-intPoolLow+1)
	for n := intPoolLow; n <= intPoolHigh; n++ {
		p[n-intPoolLow] = []byte(":" + strconv.Itoa(n) + "\r\n")
	}
	return p
}()

// pooledInteger returns the pre-rendered encoding for n, or nil if n is outside
// the pooled window. The returned slice is shared and must not be mutated.
func pooledInteger(n int64) []byte {
	if n >= intPoolLow && n <= intPoolHigh {
		return integerPool[n-intPoolLow]
	}
	return nil
}
