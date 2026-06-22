package command

import (
	"fmt"
	"strings"
)

// This file parses ACL rule tokens into user state and renders user state back
// into the token form ACL LIST, ACL GETUSER and the aclfile use (spec 2064 doc
// 19 sections 11 and 12). The grammar is the one ACL SETUSER accepts: status
// flags, password rules, command rules, key rules, channel rules and selectors.

// applyACLRules applies a list of rule tokens to a user in order. A selector,
// written as tokens from "(" to ")", is gathered and applied as a unit. The
// returned error carries the offending token for the SETUSER error reply.
func applyACLRules(u *aclUser, tokens []string) error {
	for i := 0; i < len(tokens); i++ {
		tok := tokens[i]
		if strings.HasPrefix(tok, "(") {
			j := i
			var parts []string
			for ; j < len(tokens); j++ {
				parts = append(parts, tokens[j])
				if strings.HasSuffix(tokens[j], ")") {
					break
				}
			}
			if j == len(tokens) {
				return fmt.Errorf("unmatched parenthesis in selector starting at '%s'", tok)
			}
			joined := strings.Join(parts, " ")
			joined = strings.TrimSpace(joined[1 : len(joined)-1])
			sel, err := parseSelector(joined)
			if err != nil {
				return err
			}
			u.selectors = append(u.selectors, sel)
			i = j
			continue
		}
		if err := applyACLRule(u, tok); err != nil {
			return err
		}
	}
	return nil
}

// parseSelector parses the inner rules of a (rules...) group into a selector.
func parseSelector(inner string) (aclSelector, error) {
	var sel aclSelector
	for _, tok := range strings.Fields(inner) {
		switch {
		case tok == "allcommands" || tok == "+@all":
			sel.cmdRules = append(sel.cmdRules, aclCmdRule{grant: true, category: "@all"})
		case tok == "nocommands" || tok == "-@all":
			sel.cmdRules = append(sel.cmdRules, aclCmdRule{grant: false, category: "@all"})
		case tok == "allkeys" || tok == "~*":
			sel.keyRules = append(sel.keyRules, aclKeyRule{pattern: "*", read: true, write: true})
		case tok == "allchannels" || tok == "&*":
			sel.chanRules = append(sel.chanRules, aclChanRule{pattern: "*"})
		case strings.HasPrefix(tok, "+") || strings.HasPrefix(tok, "-"):
			r, err := parseCmdRule(tok)
			if err != nil {
				return sel, err
			}
			sel.cmdRules = append(sel.cmdRules, r)
		case strings.HasPrefix(tok, "~") || strings.HasPrefix(tok, "%"):
			r, err := parseKeyRule(tok)
			if err != nil {
				return sel, err
			}
			sel.keyRules = append(sel.keyRules, r)
		case strings.HasPrefix(tok, "&"):
			sel.chanRules = append(sel.chanRules, aclChanRule{pattern: tok[1:]})
		default:
			return sel, fmt.Errorf("Error in ACL SETUSER modifier '%s': Syntax error", tok)
		}
	}
	return sel, nil
}

// applyACLRule applies a single non-selector rule token to a user.
func applyACLRule(u *aclUser, tok string) error {
	switch {
	case tok == "on":
		u.on = true
	case tok == "off":
		u.on = false
	case tok == "nopass":
		u.nopass = true
		u.passwords = nil
	case tok == "resetpass":
		u.nopass = false
		u.passwords = nil
	case tok == "resetkeys":
		u.keyRules = nil
	case tok == "resetchannels":
		u.chanRules = nil
	case tok == "clearselector" || tok == "clearselectors":
		u.selectors = nil
	case tok == "reset":
		u.reset()
	case tok == "allkeys" || tok == "~*":
		u.keyRules = append(u.keyRules, aclKeyRule{pattern: "*", read: true, write: true})
	case tok == "allchannels" || tok == "&*":
		u.chanRules = append(u.chanRules, aclChanRule{pattern: "*"})
	case tok == "allcommands" || tok == "+@all":
		u.cmdRules = append(u.cmdRules, aclCmdRule{grant: true, category: "@all"})
	case tok == "nocommands" || tok == "-@all":
		u.cmdRules = append(u.cmdRules, aclCmdRule{grant: false, category: "@all"})
	case strings.HasPrefix(tok, ">"):
		u.nopass = false
		u.addPassword(hashPassword(tok[1:]))
	case strings.HasPrefix(tok, "<"):
		u.removePassword(hashPassword(tok[1:]))
	case strings.HasPrefix(tok, "#"):
		h := strings.ToLower(tok[1:])
		if !validSHA256(h) {
			return fmt.Errorf("Error in ACL SETUSER modifier '%s': Invalid password hash provided. It must be exactly 64 characters and contain only lowercase hexadecimal characters", tok)
		}
		u.nopass = false
		u.addPassword(h)
	case strings.HasPrefix(tok, "!"):
		h := strings.ToLower(tok[1:])
		if !validSHA256(h) {
			return fmt.Errorf("Error in ACL SETUSER modifier '%s': Invalid password hash provided. It must be exactly 64 characters and contain only lowercase hexadecimal characters", tok)
		}
		u.removePassword(h)
	case strings.HasPrefix(tok, "+") || strings.HasPrefix(tok, "-"):
		r, err := parseCmdRule(tok)
		if err != nil {
			return err
		}
		u.cmdRules = append(u.cmdRules, r)
	case strings.HasPrefix(tok, "~") || strings.HasPrefix(tok, "%"):
		r, err := parseKeyRule(tok)
		if err != nil {
			return err
		}
		u.keyRules = append(u.keyRules, r)
	case strings.HasPrefix(tok, "&"):
		u.chanRules = append(u.chanRules, aclChanRule{pattern: tok[1:]})
	default:
		return fmt.Errorf("Error in ACL SETUSER modifier '%s': Syntax error", tok)
	}
	return nil
}

// addPassword adds a hash if it is not already present.
func (u *aclUser) addPassword(hash string) {
	for _, h := range u.passwords {
		if h == hash {
			return
		}
	}
	u.passwords = append(u.passwords, hash)
}

// removePassword drops a hash from the list if present.
func (u *aclUser) removePassword(hash string) {
	for i, h := range u.passwords {
		if h == hash {
			u.passwords = append(u.passwords[:i], u.passwords[i+1:]...)
			return
		}
	}
}

// parseCmdRule parses a "+cmd", "-cmd", "+cmd|sub" or "+@category" token.
func parseCmdRule(tok string) (aclCmdRule, error) {
	grant := tok[0] == '+'
	body := tok[1:]
	if body == "" {
		return aclCmdRule{}, fmt.Errorf("Error in ACL SETUSER modifier '%s': Syntax error", tok)
	}
	if strings.HasPrefix(body, "@") {
		return aclCmdRule{grant: grant, category: strings.ToLower(body)}, nil
	}
	body = strings.ToLower(body)
	if i := strings.IndexByte(body, '|'); i >= 0 {
		return aclCmdRule{grant: grant, cmd: body[:i], sub: body[i+1:]}, nil
	}
	return aclCmdRule{grant: grant, cmd: body}, nil
}

// parseKeyRule parses a "~pat", "%R~pat", "%W~pat" or "%RW~pat" token.
func parseKeyRule(tok string) (aclKeyRule, error) {
	if strings.HasPrefix(tok, "~") {
		return aclKeyRule{pattern: tok[1:], read: true, write: true}, nil
	}
	// %R~, %W~ or %RW~
	rest := tok[1:]
	i := strings.IndexByte(rest, '~')
	if i < 0 {
		return aclKeyRule{}, fmt.Errorf("Error in ACL SETUSER modifier '%s': Syntax error", tok)
	}
	mode := strings.ToUpper(rest[:i])
	pat := rest[i+1:]
	switch mode {
	case "R":
		return aclKeyRule{pattern: pat, read: true}, nil
	case "W":
		return aclKeyRule{pattern: pat, write: true}, nil
	case "RW":
		return aclKeyRule{pattern: pat, read: true, write: true}, nil
	default:
		return aclKeyRule{}, fmt.Errorf("Error in ACL SETUSER modifier '%s': Syntax error", tok)
	}
}

// validSHA256 reports whether s is exactly 64 lowercase hex characters.
func validSHA256(s string) bool {
	if len(s) != 64 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// describeCommands renders a user's command rules into the compact string ACL
// GETUSER returns, like "+@all" or "-@all +get +set". An empty rule set is
// reported as "-@all", the default-deny baseline.
func describeCommands(u *aclUser) string {
	if len(u.cmdRules) == 0 {
		return "-@all"
	}
	var b strings.Builder
	for i, r := range u.cmdRules {
		if i > 0 {
			b.WriteByte(' ')
		}
		b.WriteString(r.token())
	}
	return b.String()
}

// keyTokens renders a user's key rules into their token form.
func keyTokens(u *aclUser) []string {
	out := make([]string, 0, len(u.keyRules))
	for _, r := range u.keyRules {
		out = append(out, r.token())
	}
	return out
}

// channelTokens renders a user's channel rules into their "&pat" token form.
func channelTokens(u *aclUser) []string {
	out := make([]string, 0, len(u.chanRules))
	for _, r := range u.chanRules {
		out = append(out, "&"+r.pattern)
	}
	return out
}

// aclLine renders a full user definition into the single-line "user ..." form
// ACL LIST returns and the aclfile stores.
func aclLine(u *aclUser) string {
	var b strings.Builder
	b.WriteString("user ")
	b.WriteString(u.name)
	if u.on {
		b.WriteString(" on")
	} else {
		b.WriteString(" off")
	}
	if u.nopass {
		b.WriteString(" nopass")
	}
	for _, h := range u.passwords {
		b.WriteString(" #")
		b.WriteString(h)
	}
	for _, t := range keyTokens(u) {
		b.WriteByte(' ')
		b.WriteString(t)
	}
	for _, t := range channelTokens(u) {
		b.WriteByte(' ')
		b.WriteString(t)
	}
	b.WriteByte(' ')
	b.WriteString(describeCommands(u))
	for i := range u.selectors {
		b.WriteString(" (")
		b.WriteString(selectorString(&u.selectors[i]))
		b.WriteByte(')')
	}
	return b.String()
}

// selectorString renders a selector's rules into the inner form used inside the
// parentheses of an ACL line.
func selectorString(s *aclSelector) string {
	var parts []string
	for _, r := range s.keyRules {
		parts = append(parts, r.token())
	}
	for _, r := range s.chanRules {
		parts = append(parts, "&"+r.pattern)
	}
	if len(s.cmdRules) == 0 {
		parts = append(parts, "-@all")
	} else {
		for _, r := range s.cmdRules {
			parts = append(parts, r.token())
		}
	}
	return strings.Join(parts, " ")
}
