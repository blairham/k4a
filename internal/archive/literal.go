// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package archive

import (
	"errors"
	"regexp/syntax"
	"strings"
	"unicode"
	"unicode/utf8"
)

// minLiteralRunes is the shortest literal worth pushing down; anything
// shorter matches most rows and only adds cost.
const minLiteralRunes = 3

// ErrNoLiteral is returned for a pattern with no literal every match must
// contain, which an archive search refuses rather than scan every row.
var ErrNoLiteral = errors.New("pattern has no literal of 3+ characters that every match must contain")

// CandidateLiteral returns a lowercase string that every match of pattern
// contains, such that strpos(lower(col), lit) > 0 holds for every row the
// Go regex matches. It is the archive's candidate filter; precision comes
// from re-applying the regex to each returned row.
func CandidateLiteral(pattern string) (string, error) {
	re, err := syntax.Parse(pattern, syntax.Perl)
	if err != nil {
		return "", err
	}
	best := ""
	for _, run := range requiredRuns(re) {
		for _, s := range pushableRuns(run) {
			if utf8.RuneCountInString(s) > utf8.RuneCountInString(best) {
				best = s
			}
		}
	}
	if utf8.RuneCountInString(best) < minLiteralRunes {
		return "", ErrNoLiteral
	}
	return best, nil
}

// litRun is a literal every match contains, with its case-folding flag.
type litRun struct {
	runes []rune
	fold  bool
}

// requiredRuns lists literals that every match of re contains. Anything the
// walk cannot prove contributes nothing, which only costs selectivity.
func requiredRuns(re *syntax.Regexp) []litRun {
	switch re.Op {
	case syntax.OpLiteral:
		return []litRun{{runes: re.Rune, fold: re.Flags&syntax.FoldCase != 0}}
	case syntax.OpCapture, syntax.OpPlus:
		return requiredRuns(re.Sub[0])
	case syntax.OpRepeat:
		if re.Min >= 1 {
			return requiredRuns(re.Sub[0])
		}
	case syntax.OpConcat:
		var out []litRun
		for _, sub := range re.Sub {
			out = append(out, requiredRuns(sub)...)
		}
		return out
	default:
		// Alternation, optional parts, classes, anchors: nothing required.
	}
	return nil
}

// pushableRuns splits a literal at runes lower() cannot be trusted with and
// returns the lowercased pieces.
func pushableRuns(r litRun) []string {
	var out []string
	var cur strings.Builder
	flush := func() {
		if cur.Len() > 0 {
			out = append(out, cur.String())
			cur.Reset()
		}
	}
	for _, c := range r.runes {
		if !pushable(c, r.fold) {
			flush()
			continue
		}
		cur.WriteRune(unicode.ToLower(c))
	}
	flush()
	return out
}

// pushable reports whether every rune the regex accepts at this position is
// turned into unicode.ToLower(c) by Athena's lower(). Under case folding
// that needs the whole simple-fold orbit to be ASCII (or the rune to have
// no case at all): Go's (?i)k also matches U+212A KELVIN SIGN and (?i)s
// matches U+017F LONG S, which lower() leaves alone.
func pushable(c rune, fold bool) bool {
	if !fold {
		return c < utf8.RuneSelf || unicode.ToLower(c) == c
	}
	for o := unicode.SimpleFold(c); o != c; o = unicode.SimpleFold(o) {
		if o >= utf8.RuneSelf || c >= utf8.RuneSelf {
			return false
		}
	}
	return true
}
