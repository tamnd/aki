package command

import "strings"

// maxErrArgs bounds how many of the offending arguments the unknown-command
// error echoes back, matching Redis (it shows at most the first few).
const maxErrArgs = 3

// unknownCommandError builds the reply Redis sends for a name not in the table.
// The format is the literal Redis text, including the trailing ", " after the
// last echoed argument and the backticks around each token.
func unknownCommandError(argv [][]byte) error {
	var b strings.Builder
	b.WriteString("ERR unknown command '")
	b.Write(argv[0])
	b.WriteString("', with args beginning with: ")
	for i := 1; i < len(argv) && i <= maxErrArgs; i++ {
		b.WriteString("'")
		b.Write(argv[i])
		b.WriteString("', ")
	}
	return errString(b.String())
}

// unknownSubcmdError builds the reply for a container command called with a
// subcommand it does not have.
func unknownSubcmdError(argv [][]byte) error {
	container := strings.ToLower(string(argv[0]))
	sub := ""
	if len(argv) >= 2 {
		sub = string(argv[1])
	}
	return errString("ERR Unknown subcommand or wrong number of arguments for '" +
		sub + "'. Try " + strings.ToUpper(container) + " HELP.")
}

// arityError builds the wrong-number-of-arguments reply. A subcommand reports
// itself as "container|sub".
func arityError(cmd *CmdDesc) string {
	name := cmd.Name
	if cmd.SubName != "" {
		name = cmd.SubName
	}
	return "ERR wrong number of arguments for '" + name + "' command"
}

// errString is a plain string error so the lookup helpers can return a value
// that is already RESP-ready.
type errString string

func (e errString) Error() string { return string(e) }
