//go:build amd64

#include "textflag.h"

// func prefetch(p unsafe.Pointer)
TEXT ·prefetch(SB), NOSPLIT, $0-8
	MOVQ p+0(FP), AX
	PREFETCHT0 (AX)
	RET
