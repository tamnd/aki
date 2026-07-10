//go:build arm64

#include "textflag.h"

// func prefetch(p unsafe.Pointer)
TEXT ·prefetch(SB), NOSPLIT, $0-8
	MOVD p+0(FP), R0
	PRFM (R0), PLDL1KEEP
	RET
