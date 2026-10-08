// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package main

import "testing"

func TestAllowPolicy(t *testing.T) {
	tests := []struct {
		name     string
		topic    string
		patterns []string
		want     bool
	}{
		{
			name:     "exact match",
			patterns: []string{"example.ingest.prices.v1"},
			topic:    "example.ingest.prices.v1",
			want:     true,
		},
		{
			name:     "exact miss",
			patterns: []string{"example.ingest.prices.v1"},
			topic:    "example.ingest.markets.v1",
			want:     false,
		},
		{
			name:     "glob suffix matches dotted tail",
			patterns: []string{"example.ingest.*"},
			topic:    "example.ingest.markets.v1",
			want:     true,
		},
		{
			name:     "glob does not match a different prefix",
			patterns: []string{"example.ingest.*"},
			topic:    "secret.orders.v1",
			want:     false,
		},
		{
			name:     "multiple patterns, second hits",
			patterns: []string{"a.b", "example.*"},
			topic:    "example.ingest.events.v1",
			want:     true,
		},
		{name: "empty policy denies everything", patterns: nil, topic: "anything.v1", want: false},
		{name: "whitespace-only pattern is dropped", patterns: []string{"   "}, topic: "anything.v1", want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p, err := newAllowPolicy(tc.patterns)
			if err != nil {
				t.Fatalf("newAllowPolicy: %v", err)
			}
			if got := p.allowed(tc.topic); got != tc.want {
				t.Errorf("allowed(%q) = %v, want %v", tc.topic, got, tc.want)
			}
		})
	}
}

func TestAllowPolicyRejectsBadPattern(t *testing.T) {
	if _, err := newAllowPolicy([]string{"example.[ingest"}); err == nil {
		t.Error("expected an error for a malformed glob pattern")
	}
}

func TestAllowPolicyDedupes(t *testing.T) {
	p, err := newAllowPolicy([]string{"a.*", "a.*", " a.* ", "b.*"})
	if err != nil {
		t.Fatalf("newAllowPolicy: %v", err)
	}
	if got := p.String(); got != "a.*,b.*" {
		t.Errorf("String() = %q, want %q", got, "a.*,b.*")
	}
	if p.empty() {
		t.Error("policy should not be empty")
	}
}
