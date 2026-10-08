// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package index

import (
	"regexp"
	"testing"
	"unicode/utf8"

	"github.com/blevesearch/bleve/v2/analysis"
)

// FuzzIndexablePattern checks the promise the whole index rests on: when
// indexablePattern calls a pattern servable, its candidate is recall-complete.
// A search pattern arrives from the CLI, from MCP and, at k4a-index, from the
// network; if the index answers a pattern it cannot fully serve, a search
// silently misses messages a scan would have found.
//
// The oracle is the analyzer the index really uses, taken from buildMapping,
// not a model of it: for any text the original pattern matches, some term
// that analyzer produces from that text must match the candidate, anchored to
// the whole term as Bleve's regexp query is.
func FuzzIndexablePattern(f *testing.F) {
	analyzer := buildMapping().AnalyzerNamed(textAnalyzer)
	if analyzer == nil {
		f.Fatalf("index mapping has no %s analyzer", textAnalyzer)
	}
	for _, s := range []struct{ pattern, text string }{
		{"order", `{"orderId":41,"status":"shipped"}`},
		{`(?i)"orderId":41`, `{"orderId":41}`},
		{"shipped|returned", `{"status":"returned"}`},
		{"[a-z]+_[0-9]+", "key=user_42"},
		{"ord.r", "order"},
		{"41", "x-41-y"},
		{"Straße", "die Straße"},
		{"ÀB", "àb"},
		{"aア", "aア"},
		{"日本", "日本語"},
		{"is", "this is it"},
		{"__", "x __ y"},
		{"İstanbul", "İstanbul"},
		{"stanbul", "İstanbul"},
		{"(?i)ВВ", "\u1c80\u1c80"},
		{"กข", "กข"},
		{"(?i)stop", "ſtop"},
	} {
		f.Add(s.pattern, s.text)
	}
	f.Fuzz(func(t *testing.T, pattern, text string) {
		if len(pattern) > 64 || len(text) > 256 || !utf8.ValidString(text) {
			return
		}
		re, err := regexp.Compile(pattern)
		if err != nil || !re.MatchString(text) {
			return
		}
		cand, reason := indexablePattern(re)
		if reason != "" {
			return // unservable: the dispatcher scans, which is always correct
		}
		anchored, err := regexp.Compile("^(?:" + cand + ")$")
		if err != nil {
			t.Fatalf("pattern %q: candidate %q does not compile: %v", pattern, cand, err)
		}
		terms := analyzedTerms(analyzer, text)
		for _, term := range terms {
			if anchored.MatchString(term) {
				return
			}
		}
		t.Fatalf("pattern %q matches %q and was called servable, but its candidate %q matches none of the indexed terms %q",
			pattern, text, cand, terms)
	})
}

func analyzedTerms(a analysis.Analyzer, text string) []string {
	stream := a.Analyze([]byte(text))
	out := make([]string, 0, len(stream))
	for _, tok := range stream {
		out = append(out, string(tok.Term))
	}
	return out
}
