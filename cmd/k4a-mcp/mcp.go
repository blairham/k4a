// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"

	"github.com/charmbracelet/log"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/blairham/k4a/internal/index"
	"github.com/blairham/k4a/internal/indexwire/indexv1"
	"github.com/blairham/k4a/internal/timebound"
)

// Deadlines on every gRPC call into the daemon. Without one, a wedged
// k4a-index (drain goroutine blocked holding the indexer mutex)
// hangs the tool call forever and the MCP client sees only its own opaque
// timeout. With one, the failure is fast and names the daemon. Both are
// generous multiples of the daemon's normal answer time (1–150 ms).
const (
	searchTimeout   = 30 * time.Second
	coverageTimeout = 10 * time.Second
)

// newMCPServer builds the MCP server exposing the shared index's Search/Coverage
// over the wire. It is a pure gRPC CLIENT of k4a-index (indexv1.IndexClient) —
// it holds no Kafka client, no Bleve index, no AWS creds; every query is
// answered by the daemon. That is the whole point of the split: one warm index,
// many thin MCP frontends.
func newMCPServer(client indexv1.IndexClient, version string) *mcp.Server {
	srv := mcp.NewServer(&mcp.Implementation{
		Name:    "k4a-index",
		Version: version,
	}, nil)

	mcp.AddTool(srv, &mcp.Tool{
		Name: "search",
		Description: "Search a followed Kafka topic via the shared k4a-index. Returns " +
			"matches (newest-first) plus an explicit COVERAGE frame describing what the " +
			"index authoritatively holds. Use for 'did this message ever happen', 'find " +
			"records mentioning X', or 'recent events for entity Y'. Matching is " +
			"grep-style: the pattern can hit anywhere inside the raw key/value/headers, " +
			"same as a broker scan. If served_from_index is false the topic is not " +
			"followed — fall back to a direct scan (k4a search). If coverage does not " +
			"span your time window, the index is only authoritative for the covered " +
			"slice; scan the remainder. If pattern_recall is 'best_effort' the index " +
			"cannot guarantee finding every match of this particular pattern — treat " +
			"zero matches as unverified and confirm with a direct scan.",
	}, searchHandler(client))

	mcp.AddTool(srv, &mcp.Tool{
		Name: "coverage",
		Description: "Report what the shared k4a-index holds for a topic (per-partition " +
			"offset/time ranges, gaps, following/backfilling) WITHOUT running a query. Use " +
			"to decide whether an index-backed search will be authoritative before issuing it.",
	}, coverageHandler(client))

	return srv
}

// mcpSearchInput mirrors the stdio `k4a mcp` search tool's schema so agents see
// one contract across both surfaces. jsonschema tags drive the generated schema.
type mcpSearchInput struct {
	Topic         string  `json:"topic"                    jsonschema:"name of the Kafka topic to search (required)"`
	Query         string  `json:"query,omitempty"          jsonschema:"regex pattern OR literal substring (falls back to literal on parse failure); empty matches all records in the time/partition window"`
	Since         string  `json:"since,omitempty"          jsonschema:"lower time bound; relative duration like 24h/7d or an RFC3339 timestamp"`
	Until         string  `json:"until,omitempty"          jsonschema:"upper time bound; same format as since"`
	Scope         string  `json:"scope,omitempty"          jsonschema:"match scope: key, value, key+value (default), or key+value+headers"`
	Partitions    []int32 `json:"partitions,omitempty"     jsonschema:"specific partitions to search; empty means all"`
	CaseSensitive bool    `json:"case_sensitive,omitempty" jsonschema:"if true, do not add (?i) to the regex"`
	Limit         int     `json:"limit,omitempty"          jsonschema:"maximum matches to return; 0 = server default"`
}

// mcpCoverageInput is the coverage tool's schema.
type mcpCoverageInput struct {
	Topic string `json:"topic" jsonschema:"name of the Kafka topic (required)"`
}

// mcpMatch is one record, the wire indexv1.Match flattened for JSON output.
type mcpMatch struct {
	Headers   map[string]string `json:"headers,omitempty"`
	Timestamp string            `json:"timestamp,omitempty"`
	Key       string            `json:"key,omitempty"`
	Value     string            `json:"value,omitempty"`
	Offset    int64             `json:"offset"`
	Partition int32             `json:"partition"`
}

// mcpPartitionCoverage / mcpGap / mcpCoverage are the JSON projection of the
// wire Coverage frame — the same fields the client dispatcher's index-vs-scan
// decision reads, surfaced so an agent can reason about authority.
type mcpPartitionCoverage struct {
	OldestTime string `json:"oldest_time,omitempty"` // RFC3339
	NewestTime string `json:"newest_time,omitempty"`
	Partition  int32  `json:"partition"`
	OffsetLo   int64  `json:"offset_lo"`
	OffsetHi   int64  `json:"offset_hi"`
}

type mcpGap struct {
	Reason    string `json:"reason,omitempty"`
	Partition int32  `json:"partition"`
	Start     int64  `json:"start"`
	End       int64  `json:"end"`
}

type mcpCoverage struct {
	Partitions  []mcpPartitionCoverage `json:"partitions"`
	Gaps        []mcpGap               `json:"gaps,omitempty"`
	Following   bool                   `json:"following"`
	Backfilling bool                   `json:"backfilling"`
}

// mcpSearchOutput is the search tool's structured result — matches plus the
// coverage that qualifies them, and served_from_index so the agent knows
// whether to trust the index or fall back to a scan.
//
// pattern_recall qualifies a ZERO-match answer: "full" means the index is
// guaranteed to find every record a broker scan would;
// "best_effort" means this particular pattern exceeds what the tokenized
// index can promise (pattern_recall_note says why) and absence is unproven.
// This surface has no scan to fall back to — honesty is the fallback.
type mcpSearchOutput struct {
	Coverage          *mcpCoverage `json:"coverage,omitempty"`
	Topic             string       `json:"topic"`
	PatternRecall     string       `json:"pattern_recall,omitempty"`
	PatternRecallNote string       `json:"pattern_recall_note,omitempty"`
	Matches           []mcpMatch   `json:"matches"`
	Matched           int          `json:"matched"`
	ServedFromIndex   bool         `json:"served_from_index"`
}

// mcpCoverageOutput is the coverage tool's structured result.
type mcpCoverageOutput struct {
	Coverage        *mcpCoverage `json:"coverage,omitempty"`
	Topic           string       `json:"topic"`
	ServedFromIndex bool         `json:"served_from_index"`
}

func searchHandler(client indexv1.IndexClient) mcp.ToolHandlerFor[mcpSearchInput, mcpSearchOutput] {
	return searchHandlerWithTimeout(client, searchTimeout)
}

// searchHandlerWithTimeout is searchHandler with an injectable deadline so
// tests can exercise the timeout path without waiting out the real one.
func searchHandlerWithTimeout(
	client indexv1.IndexClient, timeout time.Duration,
) mcp.ToolHandlerFor[mcpSearchInput, mcpSearchOutput] {
	return func(ctx context.Context, _ *mcp.CallToolRequest, in mcpSearchInput) (
		*mcp.CallToolResult, mcpSearchOutput, error,
	) {
		if strings.TrimSpace(in.Topic) == "" {
			return nil, mcpSearchOutput{}, fmt.Errorf("topic is required")
		}
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		start := time.Now()
		log.Info("mcp tool: search",
			"topic", in.Topic, "query", in.Query, "scope", in.Scope,
			"since", in.Since, "until", in.Until, "partitions", in.Partitions)

		req, err := buildSearchRequest(in)
		if err != nil {
			log.Error("mcp tool: search bad request", "topic", in.Topic, "err", err)
			return nil, mcpSearchOutput{}, err
		}
		recall, recallNote := patternRecall(req.GetPattern())

		stream, err := client.Search(ctx, req)
		if err != nil {
			err = attributeTimeout(err, timeout)
			log.Error("mcp tool: search index call failed", "topic", in.Topic, "err", err)
			return nil, mcpSearchOutput{}, fmt.Errorf("index search: %w", err)
		}

		out := mcpSearchOutput{
			Topic:             in.Topic,
			Matches:           []mcpMatch{},
			PatternRecall:     recall,
			PatternRecallNote: recallNote,
		}
		for {
			ev, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				err = attributeTimeout(err, timeout)
				log.Error("mcp tool: search stream error", "topic", in.Topic, "err", err)
				return nil, mcpSearchOutput{}, fmt.Errorf("index stream: %w", err)
			}
			switch e := ev.GetEvent().(type) {
			case *indexv1.SearchEvent_Coverage:
				out.Coverage = coverageToMCP(e.Coverage)
				out.ServedFromIndex = e.Coverage.GetFollowing()
			case *indexv1.SearchEvent_Match:
				out.Matches = append(out.Matches, matchToMCP(e.Match))
			case *indexv1.SearchEvent_Progress:
				out.Matched = int(e.Progress.GetMatched())
			}
		}
		if out.Matched == 0 {
			out.Matched = len(out.Matches)
		}
		log.Info("mcp tool: search done",
			"topic", in.Topic,
			"served_from_index", out.ServedFromIndex,
			"matches", out.Matched,
			"dur", time.Since(start).Round(time.Millisecond))
		return nil, out, nil
	}
}

func coverageHandler(client indexv1.IndexClient) mcp.ToolHandlerFor[mcpCoverageInput, mcpCoverageOutput] {
	return coverageHandlerWithTimeout(client, coverageTimeout)
}

// coverageHandlerWithTimeout is coverageHandler with an injectable deadline.
func coverageHandlerWithTimeout(
	client indexv1.IndexClient, timeout time.Duration,
) mcp.ToolHandlerFor[mcpCoverageInput, mcpCoverageOutput] {
	return func(ctx context.Context, _ *mcp.CallToolRequest, in mcpCoverageInput) (
		*mcp.CallToolResult, mcpCoverageOutput, error,
	) {
		if strings.TrimSpace(in.Topic) == "" {
			return nil, mcpCoverageOutput{}, fmt.Errorf("topic is required")
		}
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		log.Info("mcp tool: coverage", "topic", in.Topic)
		resp, err := client.Coverage(ctx, &indexv1.CoverageRequest{Topic: in.Topic})
		if err != nil {
			err = attributeTimeout(err, timeout)
			log.Error("mcp tool: coverage index call failed", "topic", in.Topic, "err", err)
			return nil, mcpCoverageOutput{}, fmt.Errorf("index coverage: %w", err)
		}
		cov := resp.GetCoverage()
		log.Info("mcp tool: coverage done",
			"topic", in.Topic, "following", cov.GetFollowing(), "partitions", len(cov.GetPartitions()))
		return nil, mcpCoverageOutput{
			Topic:           in.Topic,
			Coverage:        coverageToMCP(cov),
			ServedFromIndex: cov.GetFollowing(),
		}, nil
	}
}

// attributeTimeout converts a deadline expiry into an error that names the
// daemon and the likely cause, so an agent sees "k4a-index didn't answer"
// instead of a bare DEADLINE_EXCEEDED bubbled up through gRPC. Other errors
// pass through unchanged.
func attributeTimeout(err error, timeout time.Duration) error {
	if status.Code(err) == codes.DeadlineExceeded || errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf(
			"k4a-index did not respond within %s — the daemon may be wedged; check/restart its pod: %w",
			timeout, err,
		)
	}
	return err
}

// buildSearchRequest translates the MCP tool input into a wire SearchRequest.
// Regex + scope + time semantics mirror `k4a search` / the stdio MCP so the
// three surfaces behave identically.
func buildSearchRequest(in mcpSearchInput) (*indexv1.SearchRequest, error) {
	req := &indexv1.SearchRequest{
		Topic:      in.Topic,
		Partitions: in.Partitions,
		Limit:      int32(in.Limit), //nolint:gosec // search limit fits in int32
		Scopes:     parseScope(in.Scope),
	}
	if pat := strings.TrimSpace(in.Query); pat != "" {
		if !in.CaseSensitive && !strings.HasPrefix(pat, "(?") {
			pat = "(?i)" + pat
		}
		if _, err := regexp.Compile(pat); err != nil {
			// Same fallback as the CLI/TUI: treat as a literal substring.
			pat = "(?i)" + regexp.QuoteMeta(strings.TrimSpace(in.Query))
		}
		req.Pattern = pat
	}

	since, err := parseTimeBound(in.Since)
	if err != nil {
		return nil, fmt.Errorf("since: %w", err)
	}
	until, err := parseTimeBound(in.Until)
	if err != nil {
		return nil, fmt.Errorf("until: %w", err)
	}
	req.SinceMs = since
	req.UntilMs = until
	return req, nil
}

// patternRecall classifies what the index can promise for this pattern —
// the same decision the client dispatcher's CanServe gate makes (issue
// pattern recall), surfaced as data because this frontend has no scan to fall back to.
func patternRecall(pattern string) (recall, note string) {
	if pattern == "" {
		return "", "" // match-all: no pattern to under-match
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		// Unreachable: buildSearchRequest already validated or literalized it.
		return "best_effort", "pattern did not compile"
	}
	if ok, reason := index.PatternServable(re); !ok {
		return "best_effort", reason
	}
	return "full", ""
}

// Wire scope names, as the daemon's SearchRequest.scopes reads them.
const (
	scopeKey     = "key"
	scopeValue   = "value"
	scopeHeaders = "headers"
)

// parseScope maps the scope string to the wire scope list. Empty → nil, which
// the daemon reads as the key+value default.
func parseScope(s string) []string {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "key+value":
		return nil
	case scopeKey:
		return []string{scopeKey}
	case scopeValue:
		return []string{scopeValue}
	case "key+value+headers":
		return []string{scopeKey, scopeValue, scopeHeaders}
	default:
		return nil
	}
}

// parseTimeBound accepts a relative duration (24h/7d/2w) or an RFC3339
// timestamp and returns Unix millis (0 = unset).
func parseTimeBound(s string) (int64, error) {
	t, err := timebound.Parse(s, time.Now())
	if err != nil || t.IsZero() {
		return 0, err
	}
	return t.UnixMilli(), nil
}

func coverageToMCP(c *indexv1.Coverage) *mcpCoverage {
	if c == nil {
		return nil
	}
	out := &mcpCoverage{
		Following:   c.GetFollowing(),
		Backfilling: c.GetBackfilling(),
		Partitions:  make([]mcpPartitionCoverage, 0, len(c.GetPartitions())),
	}
	for _, p := range c.GetPartitions() {
		out.Partitions = append(out.Partitions, mcpPartitionCoverage{
			Partition:  p.GetPartition(),
			OffsetLo:   p.GetOffsetLo(),
			OffsetHi:   p.GetOffsetHi(),
			OldestTime: msToRFC3339(p.GetTimestampLoMs()),
			NewestTime: msToRFC3339(p.GetTimestampHiMs()),
		})
	}
	for _, g := range c.GetGaps() {
		out.Gaps = append(out.Gaps, mcpGap{
			Partition: g.GetPartition(),
			Start:     g.GetStart(),
			End:       g.GetEnd(),
			Reason:    g.GetReason(),
		})
	}
	return out
}

func matchToMCP(m *indexv1.Match) mcpMatch {
	out := mcpMatch{
		Partition: m.GetPartition(),
		Offset:    m.GetOffset(),
		Timestamp: msToRFC3339(m.GetTimestampMs()),
		Key:       m.GetKey(),
		Value:     m.GetValue(),
	}
	if hs := m.GetHeaders(); len(hs) > 0 {
		out.Headers = make(map[string]string, len(hs))
		for _, h := range hs {
			out.Headers[h.GetKey()] = h.GetValue()
		}
	}
	return out
}

func msToRFC3339(ms int64) string {
	if ms == 0 {
		return ""
	}
	return time.UnixMilli(ms).UTC().Format(time.RFC3339Nano)
}
