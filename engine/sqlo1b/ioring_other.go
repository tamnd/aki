//go:build !linux

package sqlo1b

import "os"

// Non-Linux stubs: the probe says no and setup says no, so the
// startup path lands on iopool without platform switches of its own.

// RingProbe reports ErrRingUnsupported: io_uring is Linux-only.
func RingProbe() error { return ErrRingUnsupported }

// IORing does not exist off Linux; the type is declared so callers
// can mention it without build tags, but NewIORing never returns one.
type IORing struct{}

func (*IORing) Submit([]IOReq) {}
func (*IORing) Sync(uint64)    {}
func (*IORing) Close()         {}

// NewIORing reports ErrRingUnsupported: io_uring is Linux-only.
func NewIORing(*os.File, uint32, int, chan<- IOResult) (*IORing, error) {
	return nil, ErrRingUnsupported
}
