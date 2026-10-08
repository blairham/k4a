// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package index

import (
	"regexp"
	"strings"
	"testing"

	"github.com/blevesearch/bleve/v2/analysis/analyzer/standard"
	"github.com/blevesearch/bleve/v2/registry"
)

// TestIndexablePattern pins which patterns the index may serve:
// servable patterns get a recall-complete candidate, everything else must
// report a reason so CanServe steers the dispatcher to the scan path instead
// of confidently returning zero matches.
func TestIndexablePattern(t *testing.T) {
	t.Parallel()

	servable := []struct{ pattern, wantCandidate string }{
		// The headline symptom: a plain word must match inside tokens.
		{"(?i)order", "(?i).*(?:(?i)order).*"},
		{"(?i)orderid", "(?i).*(?:(?i)orderid).*"},
		// Trailing wildcard → served via the mandatory literal run.
		{"(?i)order.*", "(?i).*order.*"},
		// Punctuated literal (the scan path's bread and butter): the run of
		// token runes inside it is guaranteed in every match.
		{`(?i)"orderId":41`, "(?i).*orderid.*"},
		// Anchors are fine via the literal branch; the post-filter enforces them.
		{"(?i)^order", "(?i).*order.*"},
		// Token-local alternation and classes serve as-is, unanchored.
		{"(?i)error|fail", "(?i).*(?:(?i)error|fail).*"},
		{"(?i)[a-f0-9]{32}", "(?i).*(?:(?i)[a-f0-9]{32}).*"},
		{`(?i)\d{6,}`, `(?i).*(?:(?i)\d{6,}).*`},
		// ExtendNumLet: underscore stays inside a token.
		{"(?i)not_found", "(?i).*(?:(?i)not_found).*"},
		// Case-sensitive queries serve too — the candidate case-folds for the
		// lowercased term dictionary, the post-filter restores exact case.
		{"orderId", "(?i).*(?:orderId).*"},
	}
	for _, tc := range servable {
		got, reason := indexablePattern(regexp.MustCompile(tc.pattern))
		if reason != "" {
			t.Errorf("indexablePattern(%q): unservable (%s), want candidate %q", tc.pattern, reason, tc.wantCandidate)
			continue
		}
		if got != tc.wantCandidate {
			t.Errorf("indexablePattern(%q) = %q, want %q", tc.pattern, got, tc.wantCandidate)
		}
	}

	unservable := []struct{ pattern, wantReasonPart string }{
		// Stop words are dropped at index time — no term to match.
		{"(?i)the", "stop word"},
		{"(?i)on", "stop word"},
		// Can span token boundaries with no selective literal to fall back on.
		{"(?i)a.b", "no selective literal"},
		{".", "no selective literal"},
		// Matches the empty string, so it would match inside any dropped token.
		{"(?i)x*", "stop word"},
		// A flag-clearing group would survive the (?i) widening.
		{"(?i)foo(?-i)BAR", "clears inline regex flags"},
	}
	for _, tc := range unservable {
		got, reason := indexablePattern(regexp.MustCompile(tc.pattern))
		if reason == "" {
			t.Errorf("indexablePattern(%q) = %q, want unservable containing %q", tc.pattern, got, tc.wantReasonPart)
			continue
		}
		if !strings.Contains(reason, tc.wantReasonPart) {
			t.Errorf("indexablePattern(%q) reason = %q, want containing %q", tc.pattern, reason, tc.wantReasonPart)
		}
	}
}

// TestBestEffortBlevePattern covers the degraded path Query takes for callers
// that skipped the CanServe gate (an older client against a newer daemon):
// unservable patterns still get the unanchored widening — a strict superset
// of the historical anchored query — never the anchored original.
func TestBestEffortBlevePattern(t *testing.T) {
	t.Parallel()

	cases := []struct{ pattern, want string }{
		{"(?i)order", "(?i).*(?:(?i)order).*"}, // servable → candidate
		{"(?i)a.b", "(?i).*(?:(?i)a.b).*"},     // unservable → widened
	}
	for _, tc := range cases {
		if got := bestEffortBlevePattern(regexp.MustCompile(tc.pattern)); got != tc.want {
			t.Errorf("bestEffortBlevePattern(%q) = %q, want %q", tc.pattern, got, tc.want)
		}
	}
}

// TestStandardAnalyzerAssumptions pins the analyzer facts pattern.go's recall
// argument rests on. If bleve's standard analyzer ever changes tokenization,
// lowercasing, or its stop-word set, this fails before a silent-recall bug
// ships.
func TestStandardAnalyzerAssumptions(t *testing.T) {
	t.Parallel()

	an, err := registry.NewCache().AnalyzerNamed(standard.Name)
	if err != nil {
		t.Fatalf("AnalyzerNamed(%q): %v", standard.Name, err)
	}
	terms := func(text string) []string {
		toks := an.Analyze([]byte(text))
		out := make([]string, 0, len(toks))
		for _, tok := range toks {
			out = append(out, string(tok.Term))
		}
		return out
	}

	// JSON payloads tokenize into lowercased word runs: punctuation splits,
	// letters+digits and underscores stay joined.
	got := terms(`{"orderId":4100000042,"status":"not_found"}`)
	want := []string{"orderid", "4100000042", "status", "not_found"}
	if len(got) != len(want) {
		t.Fatalf("terms = %q, want %q", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("terms = %q, want %q", got, want)
		}
	}

	// Stop words are DROPPED — the fact that forces the CanServe rejection.
	if got := terms("the order"); len(got) != 1 || got[0] != "order" {
		t.Errorf(`terms("the order") = %q, want just ["order"]`, got)
	}

	// The stop set pattern.go screens against must agree with the analyzer.
	if !stopWords()["the"] || !stopWords()["on"] {
		t.Error(`stopWords() is missing "the"/"on" — no longer matches the analyzer's stop set?`)
	}
	if stopWords()["order"] {
		t.Error(`stopWords() contains "order" — stop set loaded incorrectly`)
	}
}
