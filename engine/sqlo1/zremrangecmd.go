package sqlo1

// The ZREMRANGE family's wire half: three bound grammars, one
// rank-window removal. Each form parses through the range family's
// interval helpers, so the bounds semantics (negative indices,
// exclusive scores, the lex grammar) are byte-identical to the read
// commands they mirror.

import "context"

// zremrangeCmd serves ZREMRANGEBYRANK, ZREMRANGEBYSCORE, and
// ZREMRANGEBYLEX.
func (s *Server) zremrangeCmd(ctx context.Context, reply []byte, args [][]byte, cmd string, by int) []byte {
	if len(args) != 4 {
		return arityErr(reply, cmd)
	}
	lo, hi, errMsg, err := s.zrangeInterval(ctx, args[1], args[2], args[3], by, false)
	if err != nil {
		return storeErr(reply, err)
	}
	if errMsg != "" {
		return AppendError(reply, errMsg)
	}
	n, err := s.z.ZRemRange(ctx, args[1], lo, hi)
	if err != nil {
		return storeErr(reply, err)
	}
	return AppendInt(reply, n)
}
