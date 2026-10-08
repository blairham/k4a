// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package searchcli

import (
	"regexp"
	"testing"
	"time"

	"github.com/blairham/k4a/internal/kafka"
)

func TestScopeStringDefault(t *testing.T) {
	t.Parallel()

	// Zero value → default key+value.
	got := scopeString(0)
	if got != "key+value" {
		t.Errorf("scopeString(0) = %q, want %q", got, "key+value")
	}
}

func TestScopeStringCombinations(t *testing.T) {
	t.Parallel()

	cases := []struct {
		want string
		in   kafka.SearchScope
	}{
		{"key", kafka.ScopeKey},
		{"value", kafka.ScopeValue},
		{"key+value", kafka.ScopeKey | kafka.ScopeValue},
		{"key+value+headers", kafka.ScopeKey | kafka.ScopeValue | kafka.ScopeHeaders},
		{"key+headers", kafka.ScopeKey | kafka.ScopeHeaders},
	}
	for _, tc := range cases {
		if got := scopeString(tc.in); got != tc.want {
			t.Errorf("scopeString(%d) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestParamPatternNil(t *testing.T) {
	t.Parallel()

	if got := paramPattern(kafka.SearchParams{}); got != "" {
		t.Errorf("paramPattern(nil pattern) = %q, want empty", got)
	}
}

func TestParamPatternSet(t *testing.T) {
	t.Parallel()

	re := regexp.MustCompile("(?i)needle")
	got := paramPattern(kafka.SearchParams{Pattern: re})
	if got != "(?i)needle" {
		t.Errorf("paramPattern = %q, want %q", got, "(?i)needle")
	}
}

func TestMatchOfWithHeaders(t *testing.T) {
	t.Parallel()

	ts := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	m := matchOf(kafka.ConsumedMessage{
		Topic:     "orders",
		Partition: 3,
		Offset:    42,
		Time:      ts,
		Key:       "k",
		Value:     "v",
		Headers: []kafka.MessageHeader{
			{Key: "trace", Value: "abc"},
			{Key: "env", Value: "stg"},
		},
	})
	if m.Topic != "orders" || m.Partition != 3 || m.Offset != 42 {
		t.Errorf("identity fields wrong: %+v", m)
	}
	if !m.Timestamp.Equal(ts) {
		t.Errorf("Timestamp = %v, want %v", m.Timestamp, ts)
	}
	if len(m.Headers) != 2 || m.Headers[0].Key != "trace" || m.Headers[1].Value != "stg" {
		t.Errorf("headers not preserved: %+v", m.Headers)
	}
}

func TestMatchOfNoHeaders(t *testing.T) {
	t.Parallel()

	m := matchOf(kafka.ConsumedMessage{Topic: "x"})
	if m.Headers != nil {
		t.Errorf("empty headers should be nil, got %+v", m.Headers)
	}
}

func TestJoin(t *testing.T) {
	t.Parallel()

	cases := []struct {
		want  string
		sep   string
		parts []string
	}{
		{want: "", parts: nil, sep: "+"},
		{want: "a", parts: []string{"a"}, sep: "+"},
		{want: "a+b", parts: []string{"a", "b"}, sep: "+"},
		{want: "a+b+c", parts: []string{"a", "b", "c"}, sep: "+"},
	}
	for _, tc := range cases {
		if got := join(tc.parts, tc.sep); got != tc.want {
			t.Errorf("join(%v, %q) = %q, want %q", tc.parts, tc.sep, got, tc.want)
		}
	}
}
