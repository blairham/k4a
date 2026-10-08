// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package index

import (
	"fmt"
	"regexp"
	"regexp/syntax"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"

	"github.com/blevesearch/bleve/v2/analysis"
	"github.com/blevesearch/bleve/v2/analysis/lang/en"
	unicodetok "github.com/blevesearch/bleve/v2/analysis/tokenizer/unicode"
	vregexp "github.com/blevesearch/vellum/regexp"
)

// Bleve regexp queries are TERM-anchored: the pattern must match an entire
// analyzed token, not a substring of the original text. The scan
// path's semantics are grep-style — the pattern matches anywhere in the raw
// key/value/headers — so running the user's pattern against the term
// dictionary verbatim silently under-matches (`order` finds nothing in a
// topic full of `"orderId":…`).
//
// This file closes that fork. The Bleve query is treated as a CANDIDATE
// filter that must never miss (recall-complete); precision is restored by
// re-applying the exact original regex to the stored text in hitToConsumed.
// Two candidate strategies, tried in order:
//
//  1. Token-local patterns — every string the regex can match consists solely
//     of token runes, so any occurrence in the raw text lies inside a single
//     analyzed token. Candidate: the pattern itself, unanchored
//     (`(?i).*(?:pat).*`).
//  2. Mandatory-literal patterns — the regex can match across token
//     separators, but every match is guaranteed to contain some contiguous
//     run of token runes (e.g. `"orderId":41` always contains `orderid`).
//     That run lies inside a single token of any matching document, so
//     `(?i).*run.*` is recall-complete. Candidate: the longest such run.
//
// Patterns fitting neither strategy — or shadowed by the analyzer's stop-word
// filter, or unsupported by the term automaton — are declared unservable, and
// CanServe steers the dispatcher to the scan path (the correctness floor).
//
// The `(?i)` widening exists because the term dictionary is lowercased by the
// standard analyzer; the exact-case decision is the post-filter's.

// minMandatoryLiteral is the shortest mandatory literal run worth querying.
// The literal candidate discards the rest of the pattern, so a 1–2 rune run
// (`.*id.*`) would return a huge, barely-filtered candidate page whose cap
// then hides real matches; below this length we scan instead.
const minMandatoryLiteral = 3

// classScanBudget bounds how many runes of a character class we enumerate
// when proving it token-local. Classes bigger than this (e.g. `\pL`, which
// includes ideographs) are conservatively treated as not token-local.
const classScanBudget = 4096

// stopWords is the standard analyzer's English stop-word set. Tokens in this
// set are DROPPED at index time, so a pattern that could be satisfied inside
// one has no term to match — those queries must scan.
var stopWords = sync.OnceValue(func() analysis.TokenMap {
	tm := analysis.NewTokenMap()
	// LoadBytes only fails on reader errors, impossible over static bytes.
	_ = tm.LoadBytes(en.EnglishStopWords) //nolint:errcheck // static data
	return tm
})

// PatternServable reports whether the index can answer re with full
// (scan-parity) recall, and the reason when it cannot. Callers with a scan
// path get this via CanServe; this export is for surfaces that have no scan
// to fall back to (cmd/k4a-mcp) and must instead tell the caller a zero-match
// answer on this pattern is not authoritative.
func PatternServable(re *regexp.Regexp) (bool, string) {
	_, reason := indexablePattern(re)
	return reason == "", reason
}

// indexablePattern decides whether the tokenized index can answer re with
// full recall and, if so, returns the recall-complete Bleve candidate
// pattern to run. A non-empty reason means the index must not serve this
// pattern (CanServe surfaces it and the dispatcher falls back to a scan).
func indexablePattern(re *regexp.Regexp) (candidate, reason string) {
	src := re.String()
	// A flag-clearing group could re-enable case sensitivity inside the
	// (?i)-widened candidate, breaking recall against the lowercased term
	// dictionary. Rare enough to punt to the scan path wholesale.
	if strings.Contains(src, "(?-") {
		return "", "pattern clears inline regex flags"
	}
	tree, err := syntax.Parse(src, syntax.Perl)
	if err != nil {
		// Unreachable: re compiled from this exact source.
		return "", "pattern does not parse"
	}

	if tokenLocal(tree) {
		cand := "(?i).*(?:" + src + ").*"
		if why := candidateUsable(cand); why != "" {
			return "", why
		}
		return cand, ""
	}

	lit := mandatoryTokenRun(tree)
	if len([]rune(lit)) < minMandatoryLiteral {
		return "", "pattern can span token boundaries and has no selective literal"
	}
	cand := "(?i).*" + regexp.QuoteMeta(lit) + ".*"
	if why := candidateUsable(cand); why != "" {
		return "", why
	}
	return cand, ""
}

// bestEffortBlevePattern is what Query actually runs. New clients consult
// CanServe first and never reach here with an unservable pattern; older
// clients (an older k4a against a newer daemon) skip that gate, and for
// them the unanchored widening is still a strict superset of the historical
// anchored query's matches — degrade gracefully rather than preserve a
// known-wrong behavior.
func bestEffortBlevePattern(re *regexp.Regexp) string {
	if cand, reason := indexablePattern(re); reason == "" {
		return cand
	}
	wrapped := "(?i).*(?:" + re.String() + ").*"
	if _, err := vregexp.New(wrapped); err == nil {
		return wrapped
	}
	return re.String()
}

// candidateUsable checks a candidate pattern against the two ways a
// recall-complete-looking query can still miss: the stop-word filter (the
// matching token was never indexed) and the term automaton's syntax subset
// (the query would error instead of matching).
func candidateUsable(cand string) string {
	goRe, err := regexp.Compile(cand)
	if err != nil {
		return "candidate pattern does not compile"
	}
	// The candidate is .*-wrapped, so MatchString(w) asks: could the pattern
	// be satisfied inside the (dropped) stop-word token w?
	for w := range stopWords() {
		if goRe.MatchString(w) {
			return fmt.Sprintf("pattern can match inside stop word %q, which the analyzer never indexes", w)
		}
	}
	if _, err := vregexp.New(cand); err != nil {
		return "pattern unsupported by the index term automaton: " + err.Error()
	}
	return ""
}

// isTokenRune reports whether r is a rune a contiguous run of which in raw
// text always lands, intact, inside a single analyzed term -- the property
// both candidate strategies rest on. It asks the index's own tokenizer
// rather than modeling UAX#29: r must survive tokenization on its own and
// join a letter and a digit on either side. That keeps letters and digits of
// the scripts that segment into words, and leaves out the ones that do not
// (Katakana splits from Latin letters, ideographs and Hiragana segment rune
// by rune, Thai is not kept at all) and a lone "_", which is dropped.
//
// r must also lowercase to a rune its own case folding reaches: the term is
// lowercased, and the (?i) candidate only matches r's fold orbit. İ fails
// this (it lowercases to i, which (?i)İ does not match).
func isTokenRune(r rune) bool {
	if v, ok := tokenRunes.Load(r); ok {
		if b, isBool := v.(bool); isBool {
			return b
		}
	}
	ok := classifyTokenRune(r)
	tokenRunes.Store(r, ok)
	return ok
}

// tokenRunes caches isTokenRune, which runs the tokenizer five times.
var tokenRunes sync.Map

var runeTokenizer = unicodetok.NewUnicodeTokenizer()

func classifyTokenRune(r rune) bool {
	if !utf8.ValidRune(r) {
		return false
	}
	s := string(r)
	for _, probe := range []string{s, "a" + s, s + "a", "0" + s, s + "0"} {
		toks := runeTokenizer.Tokenize([]byte(probe))
		if len(toks) != 1 || string(toks[0].Term) != probe {
			return false
		}
	}
	return lowerInFoldOrbit(r)
}

// lowerInFoldOrbit reports whether unicode.ToLower(r) is r or one of the
// runes simple case folding makes equivalent to it.
func lowerInFoldOrbit(r rune) bool {
	l := unicode.ToLower(r)
	for f := r; ; {
		if f == l {
			return true
		}
		if f = unicode.SimpleFold(f); f == r {
			return false
		}
	}
}

// literalRuneIsToken reports whether every rune a literal's r can match is a
// token rune: under (?i) that is r's whole fold orbit, not r alone.
func literalRuneIsToken(r rune, fold bool) bool {
	if !isTokenRune(r) {
		return false
	}
	if !fold {
		return true
	}
	for f := unicode.SimpleFold(r); f != r; f = unicode.SimpleFold(f) {
		if !isTokenRune(f) {
			return false
		}
	}
	return true
}

// tokenLocal reports whether every string re can match consists solely of
// token runes. Zero-width assertions are rejected: they don't survive
// tokenization (and vellum refuses them anyway).
func tokenLocal(re *syntax.Regexp) bool {
	switch re.Op {
	case syntax.OpLiteral:
		for _, r := range re.Rune {
			if !literalRuneIsToken(r, re.Flags&syntax.FoldCase != 0) {
				return false
			}
		}
		return true
	case syntax.OpCharClass:
		return classTokenLocal(re.Rune)
	case syntax.OpAnyChar, syntax.OpAnyCharNotNL:
		return false
	case syntax.OpBeginLine, syntax.OpEndLine, syntax.OpBeginText, syntax.OpEndText,
		syntax.OpWordBoundary, syntax.OpNoWordBoundary:
		return false
	case syntax.OpEmptyMatch:
		return true
	case syntax.OpCapture, syntax.OpStar, syntax.OpPlus, syntax.OpQuest, syntax.OpRepeat,
		syntax.OpConcat, syntax.OpAlternate:
		for _, sub := range re.Sub {
			if !tokenLocal(sub) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

// classTokenLocal reports whether a character class (as [lo,hi] rune pairs)
// is provably a subset of the token runes, giving up past classScanBudget.
func classTokenLocal(pairs []rune) bool {
	budget := classScanBudget
	for i := 0; i+1 < len(pairs); i += 2 {
		lo, hi := pairs[i], pairs[i+1]
		if int(hi-lo)+1 > budget {
			return false
		}
		for r := lo; r <= hi; r++ {
			if !isTokenRune(r) {
				return false
			}
			budget--
		}
	}
	return true
}

// mandatoryTokenRun returns the longest contiguous run of token runes that is
// guaranteed to appear in EVERY match of re (lowercased for the case-folded
// candidate), or "" if no such run can be proven. Alternations contribute
// nothing (a literal in one branch isn't mandatory); optional subtrees
// (star/quest/{0,n}) likewise.
func mandatoryTokenRun(re *syntax.Regexp) string {
	switch re.Op {
	case syntax.OpLiteral:
		return longestTokenRun(re.Rune, re.Flags&syntax.FoldCase != 0)
	case syntax.OpCapture, syntax.OpPlus:
		return mandatoryTokenRun(re.Sub[0])
	case syntax.OpRepeat:
		if re.Min >= 1 {
			return mandatoryTokenRun(re.Sub[0])
		}
		return ""
	case syntax.OpConcat:
		best := ""
		for _, sub := range re.Sub {
			if s := mandatoryTokenRun(sub); len(s) > len(best) {
				best = s
			}
		}
		return best
	default:
		return ""
	}
}

// longestTokenRun extracts the longest run of token runes from a literal,
// lowercased to match the analyzer's term dictionary.
func longestTokenRun(runes []rune, fold bool) string {
	var best, cur []rune
	for _, r := range runes {
		if literalRuneIsToken(r, fold) {
			cur = append(cur, unicode.ToLower(r))
			continue
		}
		if len(cur) > len(best) {
			best = cur
		}
		cur = nil
	}
	if len(cur) > len(best) {
		best = cur
	}
	return string(best)
}
