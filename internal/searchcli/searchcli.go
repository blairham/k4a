// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

// Package searchcli is the shared search-execution path used by both the
// `k4a search` CLI command and the `k4a mcp` MCP server. It owns the
// summary struct that both surfaces emit so the wire format is
// guaranteed identical between the two entry points.
package searchcli

import (
	"context"
	"time"

	"github.com/blairham/k4a/internal/kafka"
	"github.com/blairham/k4a/internal/remoteindex"
	"github.com/blairham/k4a/internal/searchcache"
)

// SummarySchemaVersion is the wire-format version of Summary. Bump on
// incompatible shape changes. Consumers should check this to detect
// when they need updating; current value: 1.
const SummarySchemaVersion = 1

// Match is a single matched record. Field names are stable for wire
// compatibility — same JSON shape regardless of caller (CLI or MCP).
type Match struct {
	Timestamp time.Time  `json:"timestamp"`
	Topic     string     `json:"topic"`
	Key       string     `json:"key"`
	Value     string     `json:"value"`
	Headers   []HeaderKV `json:"headers,omitempty"`
	Partition int        `json:"partition"`
	Offset    int64      `json:"offset"`
}

// HeaderKV is one record header in the wire format. Mirrors
// kafka.MessageHeader without the dependency leakage.
type HeaderKV struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// Params is the search parameters, normalized for echo in the summary.
type Params struct {
	Since         *time.Time `json:"since,omitempty"`
	Until         *time.Time `json:"until,omitempty"`
	Pattern       string     `json:"pattern"`
	Scope         string     `json:"scope"`
	Partitions    []int32    `json:"partitions,omitempty"`
	Cap           int        `json:"cap,omitempty"`
	CaseSensitive bool       `json:"case_sensitive"`
}

// Stats is the post-scan summary stats.
type Stats struct {
	Source     string `json:"source"` // "scanned" or "cached"
	ElapsedMs  int64  `json:"elapsed_ms"`
	Scanned    int64  `json:"scanned"`
	Bytes      int64  `json:"bytes"`
	Partitions int    `json:"partitions"`
	Capped     bool   `json:"capped"`
	// Truncated means the scan could not read its whole offset range, so
	// matches are missing. Consumers treating this result as a complete
	// answer — an MCP client, an analysis script — must check it.
	Truncated bool `json:"truncated"`
}

// Summary is the single self-describing wire-format document. Same shape
// returned by `k4a search --format=summary` and by the MCP `search` tool.
type Summary struct {
	Query         string  `json:"query"`
	Topic         string  `json:"topic"`
	Error         string  `json:"error,omitempty"`
	Matches       []Match `json:"matches"`
	Params        Params  `json:"params"`
	Stats         Stats   `json:"stats"`
	SchemaVersion int     `json:"schema_version"`
}

// Run executes a single search against client and returns a complete
// Summary once the scan finishes (or errors). Streaming is hidden from
// the caller — this is the "give me one parseable result" entry point.
//
// The query parameter is the raw, pre-regex search string the user typed
// (or supplied via flags / MCP). p must have Pattern already compiled.
func Run(
	ctx context.Context,
	cache *searchcache.Cache,
	client *kafka.Client,
	remote *remoteindex.Client,
	topic string,
	query string,
	p kafka.SearchParams,
	caseSensitive bool,
) Summary {
	out := Summary{
		SchemaVersion: SummarySchemaVersion,
		Query:         query,
		Topic:         topic,
		Params: Params{
			Pattern:       paramPattern(p),
			Scope:         scopeString(p.Scope),
			Partitions:    p.Partitions,
			Cap:           p.Cap,
			CaseSensitive: caseSensitive,
		},
	}
	if !p.Since.IsZero() {
		t := p.Since
		out.Params.Since = &t
	}
	if !p.Until.IsZero() {
		t := p.Until
		out.Params.Until = &t
	}

	// Base source: HWM-validated cache → broker scan. When a shared index is
	// configured, wrap it so a covered query is answered by the warm daemon and
	// anything else falls through to this scan.
	search := func(ctx context.Context, topic string, params kafka.SearchParams) (
		<-chan kafka.ConsumedMessage, <-chan kafka.DeepSearchProgress, <-chan error,
	) {
		return cache.Search(ctx, client, topic, params)
	}
	if remote != nil {
		search = remote.Wrap(search)
	}
	matchCh, progCh, errCh := search(ctx, topic, p)

	var (
		lastProg kafka.DeepSearchProgress
		scanErr  error
	)
	for matchCh != nil || progCh != nil || errCh != nil {
		select {
		case <-ctx.Done():
			matchCh, progCh, errCh = nil, nil, nil
		case m, ok := <-matchCh:
			if !ok {
				matchCh = nil
				continue
			}
			out.Matches = append(out.Matches, matchOf(m))
		case prog, ok := <-progCh:
			if !ok {
				progCh = nil
				continue
			}
			lastProg = prog
		case e, ok := <-errCh:
			if !ok {
				errCh = nil
				continue
			}
			if e != nil {
				scanErr = e
			}
		}
	}

	out.Stats = Stats{
		Source:     lastProg.Source.String(),
		ElapsedMs:  lastProg.Elapsed.Milliseconds(),
		Scanned:    lastProg.Scanned,
		Bytes:      lastProg.Bytes,
		Partitions: lastProg.Partitions,
		Capped:     lastProg.Capped,
		Truncated:  lastProg.Truncated,
	}
	if scanErr != nil {
		out.Error = scanErr.Error()
	}
	return out
}

func paramPattern(p kafka.SearchParams) string {
	if p.Pattern == nil {
		return ""
	}
	return p.Pattern.String()
}

func scopeString(s kafka.SearchScope) string {
	if s == 0 {
		s = kafka.ScopeKey | kafka.ScopeValue
	}
	parts := make([]string, 0, 3)
	if s&kafka.ScopeKey != 0 {
		parts = append(parts, "key")
	}
	if s&kafka.ScopeValue != 0 {
		parts = append(parts, "value")
	}
	if s&kafka.ScopeHeaders != 0 {
		parts = append(parts, "headers")
	}
	return join(parts, "+")
}

// join is strings.Join inlined to avoid importing strings just for this.
func join(parts []string, sep string) string {
	switch len(parts) {
	case 0:
		return ""
	case 1:
		return parts[0]
	}
	n := len(sep) * (len(parts) - 1)
	for _, p := range parts {
		n += len(p)
	}
	out := make([]byte, 0, n)
	for i, p := range parts {
		if i > 0 {
			out = append(out, sep...)
		}
		out = append(out, p...)
	}
	return string(out)
}

func matchOf(m kafka.ConsumedMessage) Match {
	out := Match{
		Topic:     m.Topic,
		Partition: m.Partition,
		Offset:    m.Offset,
		Timestamp: m.Time,
		Key:       m.Key,
		Value:     m.Value,
	}
	if len(m.Headers) > 0 {
		out.Headers = make([]HeaderKV, 0, len(m.Headers))
		for _, h := range m.Headers {
			out.Headers = append(out.Headers, HeaderKV{Key: h.Key, Value: h.Value})
		}
	}
	return out
}
