// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package command

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/charmbracelet/log"
	goflags "github.com/jessevdk/go-flags"

	"github.com/blairham/k4a/internal/config"
	"github.com/blairham/k4a/internal/kafka"
	"github.com/blairham/k4a/internal/searchcache"
	"github.com/blairham/k4a/internal/searchcli"
	"github.com/blairham/k4a/internal/state"
	"github.com/blairham/k4a/internal/timebound"
)

// SearchFlags defines flags for the search command. The positional argument
// is the search pattern; everything else tunes the search.
type SearchFlags struct {
	Topic      string `short:"t" long:"topic"          env:"KAFKA_TOPIC" required:"true" description:"Topic to search (required)"`
	Scope      string `          long:"scope"                                            description:"Match in: key, value, key+value, key+value+headers"        default:"key+value"`
	Since      string `          long:"since"                                            description:"Lower time bound (e.g., 1h, 24h, 7d) or RFC3339 timestamp"`
	Until      string `          long:"until"                                            description:"Upper time bound (same format as --since)"`
	Partitions string `short:"P" long:"partitions"                                       description:"Comma-separated partition IDs (default: all)"`
	Format     string `          long:"format"                                           description:"Output format: text or json"                               default:"text"`
	ConnectionFlags
	Cap           int  `          long:"cap"                                              description:"Maximum matches (0 = library default)"`
	CaseSensitive bool `          long:"case-sensitive"                                   description:"Don't add (?i) prefix to the pattern"`
	Quiet         bool `short:"q" long:"quiet"                                            description:"Suppress progress messages on stderr"`
}

// SearchCommand implements the "search" CLI subcommand. It mirrors the
// in-TUI Ctrl+F search bar with flags for non-interactive use.
type SearchCommand struct{}

// Help returns the detailed help text.
func (c *SearchCommand) Help() string {
	return `Usage: k4a search PATTERN [options]

  Run a deep search against a Kafka topic. The bare positional argument
  is the search term (regex by default; falls back to literal substring
  if the regex is invalid). Matching is grep-style — the pattern can hit
  anywhere inside the raw key/value/headers — whether the answer comes
  from an index or a broker scan. All tuning happens via flags.

  Identical to the in-TUI Ctrl+F search, just non-interactive — useful
  for scripts, CI, "did this message ever happen" one-liners.

Config:
      --context=NAME              Named context from ~/.k4a/config.yaml
      --config=FILE               Config file path

Connection:
  -b, --brokers=BROKERS           Bootstrap servers, comma-separated
  -a, --auth=METHOD               Auth method: plaintext, tls, scram, mtls, iam
      --username=USER             SASL/SCRAM username
      --password=PASS             SASL/SCRAM password # pragma: allowlist secret
      --cert=FILE                 Client certificate PEM (mTLS)
      --key=FILE                  Client private key PEM (mTLS)
      --ca=FILE                   CA certificate PEM
  -r, --region=REGION             AWS region for IAM auth
  -p, --profile=PROFILE           AWS profile for IAM auth
  -k, --insecure                  Skip TLS certificate verification

Search:
  -t, --topic=TOPIC               Topic to search (required)
      --scope=SCOPE               Match in: key, value, key+value, key+value+headers
      --since=DURATION|TIMESTAMP  Lower time bound (e.g., 1h, 24h, 7d, 2026-05-14T00:00:00Z)
      --until=DURATION|TIMESTAMP  Upper time bound
      --partitions=LIST           Comma-separated partition IDs (default: all)
      --case-sensitive            Don't add (?i) prefix to the pattern
      --cap=N                     Maximum matches before stopping

Output:
      --format=FORMAT             text (default), json (NDJSON, one match per
                                  line), or summary (single JSON document
                                  with stats + matches — best for piping
                                  into jq or an LLM)
  -q, --quiet                     Suppress progress on stderr

Examples:
  # Substring search across the last day
  k4a search "4100000017" -t example.ingest.prices.v1 --since 24h

  # Regex against specific partitions, JSON output for piping into jq
  k4a search "order-\\d{8}" -t orders --partitions 0,1,2 --format json | jq .

  # Case-sensitive header search
  k4a search "X-Trace-Id" -t requests --scope key+value+headers --case-sensitive`
}

// Synopsis returns the one-line description.
func (c *SearchCommand) Synopsis() string {
	return "Search a Kafka topic for matching records"
}

// Run executes the search command.
func (c *SearchCommand) Run(args []string) int {
	var flags SearchFlags
	parser := goflags.NewParser(&flags, goflags.Default)
	rest, err := parser.ParseArgs(args)
	if err != nil {
		if goflags.WroteHelp(err) {
			return 0
		}
		log.Error("invalid flags", "err", err)
		return 1
	}
	if len(rest) == 0 {
		log.Error("search pattern is required (positional argument)")
		return 1
	}
	pattern := strings.Join(rest, " ")
	expandEnvFlags(&flags)

	params, err := buildSearchParams(pattern, &flags)
	if err != nil {
		log.Error("invalid search parameters", "err", err)
		return 1
	}

	client, err := flags.BuildClient()
	if err != nil {
		log.Error(err.Error())
		return 1
	}
	defer client.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	return runSearch(ctx, client, &flags, params, pattern)
}

// buildSearchParams translates CLI flags into a kafka.SearchParams. The
// translation mirrors the TUI's search-bar implementation so behavior is
// identical between interactive and CLI use.
func buildSearchParams(pattern string, flags *SearchFlags) (kafka.SearchParams, error) {
	pat := strings.TrimSpace(pattern)
	if pat == "" {
		return kafka.SearchParams{}, fmt.Errorf("pattern is empty")
	}
	if !flags.CaseSensitive && !strings.HasPrefix(pat, "(?") {
		pat = "(?i)" + pat
	}
	re, err := regexp.Compile(pat)
	if err != nil {
		// Fall back to literal substring on regex parse failure — same
		// behavior as the TUI search bar.
		re = regexp.MustCompile("(?i)" + regexp.QuoteMeta(strings.TrimSpace(pattern)))
	}

	scope, err := parseScopeFlag(flags.Scope)
	if err != nil {
		return kafka.SearchParams{}, err
	}
	since, err := parseTimeFlag(flags.Since)
	if err != nil {
		return kafka.SearchParams{}, fmt.Errorf("--since: %w", err)
	}
	until, err := parseTimeFlag(flags.Until)
	if err != nil {
		return kafka.SearchParams{}, fmt.Errorf("--until: %w", err)
	}
	parts, err := parsePartitionsFlag(flags.Partitions)
	if err != nil {
		return kafka.SearchParams{}, err
	}

	return kafka.SearchParams{
		Pattern:    re,
		Scope:      scope,
		Since:      since,
		Until:      until,
		Partitions: parts,
		Cap:        flags.Cap,
	}, nil
}

func parseScopeFlag(s string) (kafka.SearchScope, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "key+value":
		return kafka.ScopeKey | kafka.ScopeValue, nil
	case "key":
		return kafka.ScopeKey, nil
	case "value":
		return kafka.ScopeValue, nil
	case "key+value+headers":
		return kafka.ScopeKey | kafka.ScopeValue | kafka.ScopeHeaders, nil
	default:
		return 0, fmt.Errorf("invalid --scope %q (use: key, value, key+value, key+value+headers)", s)
	}
}

// parseTimeFlag accepts either a relative duration (e.g. "24h", "7d") or
// an RFC3339 absolute timestamp. Empty input yields a zero time (meaning
// no bound).
func parseTimeFlag(s string) (time.Time, error) {
	return timebound.Parse(s, time.Now())
}

// parseDurationExt extends time.ParseDuration with "d" (days) and "w"
// (weeks) suffixes that humans actually use for log search.
func parseDurationExt(s string) (time.Duration, error) {
	return timebound.ParseDuration(s)
}

func parsePartitionsFlag(s string) ([]int32, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, nil
	}
	parts := strings.Split(s, ",")
	out := make([]int32, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		n, err := strconv.Atoi(p)
		if err != nil || n < 0 {
			return nil, fmt.Errorf("invalid partition %q in --partitions", p)
		}
		out = append(out, int32(n)) //nolint:gosec // partition IDs fit in int32
	}
	return out, nil
}

// runSearch executes the scan and emits results in the requested
// format. Returns 0 on success (including 0 matches), 1 on fatal scan
// error.
//
// Output format affects whether we stream matches as they arrive (text,
// json/ndjson) or buffer them all and emit a single document at the end
// (summary). The summary form is what an LLM or jq pipeline wants.
func runSearch(
	ctx context.Context,
	client *kafka.Client,
	flags *SearchFlags,
	params kafka.SearchParams,
	pattern string,
) int {
	// Use the shared Tier 2 cache so repeat queries across CLI and TUI
	// invocations are instant — same on-disk layout as the TUI.
	cache := searchcache.New(state.NewRoot(config.DefaultConfigDir()))

	// Shared index (optional accelerator). nil when no endpoint is configured,
	// and any dial error is non-fatal — search always has the scan floor.
	remote, err := flags.BuildRemoteIndex()
	if err != nil {
		log.Debug("shared index unavailable, using local search", "err", err)
	}
	if remote != nil {
		defer remote.Close() //nolint:errcheck // best-effort close
	}

	format := strings.ToLower(strings.TrimSpace(flags.Format))
	if format == "" {
		format = "text"
	}

	if format == "summary" {
		// Buffer-and-emit-once path. Reuses the shared searchcli.Run
		// helper so the wire format is identical to the MCP tool.
		return emitSearchSummary(searchcli.Run(ctx, cache, client, remote, flags.Topic, pattern, params, flags.CaseSensitive))
	}

	// Streaming path: text or NDJSON. Wrap the cache scan with the shared index
	// so a covered query is served from the warm daemon.
	search := func(ctx context.Context, topic string, p kafka.SearchParams) (
		<-chan kafka.ConsumedMessage, <-chan kafka.DeepSearchProgress, <-chan error,
	) {
		return cache.Search(ctx, client, topic, p)
	}
	if remote != nil {
		search = remote.Wrap(search)
	}
	matchCh, progCh, errCh := search(ctx, flags.Topic, params)

	res, err := drainSearch(ctx, matchCh, progCh, errCh, newSearchOutput(format))
	if err != nil {
		log.Error("writing match", "err", err)
		return 1
	}

	if !flags.Quiet {
		fmt.Fprintln(os.Stderr, searchSummaryLine(res.lastProg, res.matches))
	}
	if res.scanErr != nil {
		log.Error("search failed", "err", res.scanErr)
		return 1
	}
	return 0
}

// emitSearchSummary writes the buffered summary document to stdout, returning
// 1 if it could not be written or the search itself failed.
func emitSearchSummary(sum searchcli.Summary) int {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(sum); err != nil {
		log.Error("writing summary", "err", err)
		return 1
	}
	if sum.Error != "" {
		return 1
	}
	return 0
}

// searchDrain is what drainSearch collected from a streaming search.
type searchDrain struct {
	scanErr  error
	lastProg kafka.DeepSearchProgress
	matches  int
}

// drainSearch emits every match as it arrives and keeps the last progress and
// error until all three channels close or ctx ends. The returned error is a
// failure to write a match, which stops the drain.
func drainSearch(
	ctx context.Context,
	matchCh <-chan kafka.ConsumedMessage,
	progCh <-chan kafka.DeepSearchProgress,
	errCh <-chan error,
	encoder *searchOutput,
) (searchDrain, error) {
	var res searchDrain
	for matchCh != nil || progCh != nil || errCh != nil {
		select {
		case <-ctx.Done():
			matchCh, progCh, errCh = nil, nil, nil
		case m, ok := <-matchCh:
			if !ok {
				matchCh = nil
				continue
			}
			res.matches++
			if err := encoder.emit(m); err != nil {
				return res, err
			}
		case p, ok := <-progCh:
			if !ok {
				progCh = nil
				continue
			}
			res.lastProg = p
		case e, ok := <-errCh:
			if !ok {
				errCh = nil
				continue
			}
			if e != nil {
				res.scanErr = e
			}
		}
	}
	return res, nil
}

// searchSummaryLine is the human summary printed to stderr after a streaming
// search.
func searchSummaryLine(lastProg kafka.DeepSearchProgress, matches int) string {
	var summary string
	if lastProg.Source == kafka.SourceScan {
		// Broker scan: report what it read (msgs + bytes + wall time).
		summary = fmt.Sprintf(
			"\nscanned %s msgs (%s) in %s · %d matches",
			formatCount(lastProg.Scanned),
			formatBytes(lastProg.Bytes),
			lastProg.Elapsed.Round(time.Millisecond),
			matches,
		)
	} else {
		// Cache / local index / shared index: nothing was scanned, so the
		// msgs+bytes counters are meaningless — report source, matches, time.
		summary = fmt.Sprintf(
			"\n%s · %d matches · %s",
			lastProg.Source.String(),
			matches,
			lastProg.Elapsed.Round(time.Millisecond),
		)
	}
	if lastProg.Capped {
		summary += " (capped — narrow your search)"
	}
	if lastProg.Truncated {
		summary += " (INCOMPLETE — could not read the whole range; results are missing records)"
	}
	return summary
}

// searchOutput abstracts the per-match emission for text vs json (NDJSON).
// The summary format bypasses this since it emits a single document at end-of-run.
type searchOutput struct {
	jsonEnc *json.Encoder
	textFmt bool
}

func newSearchOutput(format string) *searchOutput {
	if format == "json" {
		return &searchOutput{jsonEnc: json.NewEncoder(os.Stdout)}
	}
	return &searchOutput{textFmt: true}
}

func (o *searchOutput) emit(m kafka.ConsumedMessage) error {
	if o.textFmt {
		_, err := fmt.Printf("[p%d o%d %s] %s\t%s\n",
			m.Partition, m.Offset,
			m.Time.Format("2006-01-02T15:04:05.000Z07:00"),
			m.Key, sanitize(m.Value))
		return err
	}
	return o.jsonEnc.Encode(m)
}

// sanitize collapses newlines and tabs so a multi-line value doesn't
// scramble the text-format output.
func sanitize(s string) string {
	s = strings.ReplaceAll(s, "\n", " ")
	return strings.ReplaceAll(s, "\t", " ")
}

// formatCount mirrors the TUI helper; formatBytes is defined in state.go
// within this package and reused.
func formatCount(n int64) string {
	switch {
	case n >= 1_000_000_000:
		return fmt.Sprintf("%.1fB", float64(n)/1_000_000_000)
	case n >= 1_000_000:
		return fmt.Sprintf("%.1fM", float64(n)/1_000_000)
	case n >= 1_000:
		return fmt.Sprintf("%.1fK", float64(n)/1_000)
	default:
		return fmt.Sprintf("%d", n)
	}
}
