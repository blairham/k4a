// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package command

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"time"

	"github.com/charmbracelet/log"
	goflags "github.com/jessevdk/go-flags"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/blairham/k4a/internal/config"
	"github.com/blairham/k4a/internal/kafka"
	"github.com/blairham/k4a/internal/remoteindex"
	"github.com/blairham/k4a/internal/searchcache"
	"github.com/blairham/k4a/internal/searchcli"
	"github.com/blairham/k4a/internal/state"
)

// MCPFlags configures the `k4a mcp` subcommand. Connection flags come
// from ConnectionFlags so the server runs against the user's configured
// cluster the same way every other subcommand does.
type MCPFlags struct {
	ConnectionFlags
}

// MCPCommand exposes k4a's search (and future ops) as MCP tools over
// stdio, so an LLM agent like Claude Code or Cursor can call them.
type MCPCommand struct {
	Version string
}

// Help returns the help text.
func (c *MCPCommand) Help() string {
	return `Usage: k4a mcp [connection flags]

  Start a Model Context Protocol stdio server. The server stays running
  until stdin closes (the typical MCP lifecycle — the agent owns the
  process). All operations run against the connection specified by the
  context / flags, just like other k4a subcommands.

  Tools exposed:
    search    Run a deep search against a topic. Same engine as the
              k4a search subcommand and the TUI search bar. Returns a
              self-describing JSON document with stats and matches.

  Wire your agent to launch k4a with this subcommand. Claude Code
  config example (claude_desktop_config.json) — substitute one of
  your configured contexts (see "k4a contexts" for the list):

    {
      "mcpServers": {
        "k4a": {
          "command": "k4a",
          "args": ["mcp", "--context", "<your-context>"]
        }
      }
    }

  Smoke-testing from a shell: the server bootstraps the Kafka client
  before reading stdin, which takes a moment (SSM lookup, IAM auth).
  If you pipe a one-shot echo the pipe closes before the server is
  ready. Keep stdin open with a trailing sleep:

    (printf '%s\n' '<request>' ; sleep 3) | k4a mcp --context <name>

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
  -k, --insecure                  Skip TLS certificate verification`
}

// Synopsis returns the one-line description.
func (c *MCPCommand) Synopsis() string {
	return "Run an MCP stdio server exposing k4a search as a tool"
}

// Run starts the MCP server. Returns 0 on graceful shutdown
// (stdin closed, SIGINT/SIGTERM), 1 on configuration error.
func (c *MCPCommand) Run(args []string) int {
	var flags MCPFlags
	parser := goflags.NewParser(&flags, goflags.Default)
	if _, err := parser.ParseArgs(args); err != nil {
		if goflags.WroteHelp(err) {
			return 0
		}
		log.Error("invalid flags", "err", err)
		return 1
	}
	expandEnvFlags(&flags)

	client, err := flags.BuildClient()
	if err != nil {
		log.Error(err.Error())
		return 1
	}
	defer client.Close()

	cache := searchcache.New(state.NewRoot(config.DefaultConfigDir()))

	// Optional shared index — a covered query is served from the warm daemon,
	// everything else falls through to the local scan.
	remote, err := flags.BuildRemoteIndex()
	if err != nil {
		log.Debug("shared index unavailable, using local search", "err", err)
	}
	if remote != nil {
		defer remote.Close() //nolint:errcheck // best-effort close
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	version := c.Version
	if version == "" {
		version = "dev"
	}
	server := mcp.NewServer(&mcp.Implementation{
		Name:    "k4a",
		Version: version,
	}, nil)

	// search tool — same engine as `k4a search` and the TUI search bar.
	mcp.AddTool(
		server,
		&mcp.Tool{
			Name: "search",
			Description: "Search a Kafka topic for records matching a regex " +
				"(or literal substring on regex parse failure). Matching is " +
				"grep-style — the pattern can hit anywhere inside the raw " +
				"key/value/headers — regardless of whether the answer is served " +
				"from an index or a broker scan; patterns an index cannot answer " +
				"with full recall automatically fall back to a scan. Returns " +
				"matches plus scan stats. Use this for 'did this message ever " +
				"happen', 'find records mentioning X', or 'recent events for " +
				"entity Y' questions about Kafka topic data.",
		},
		makeSearchHandler(cache, client, remote),
	)

	if err := server.Run(ctx, &mcp.StdioTransport{}); err != nil {
		// EOF on stdin means the agent closed the pipe — that's the
		// normal shutdown signal for a stdio MCP server. The SDK wraps
		// it; we check both the sentinel and a substring fallback for
		// older SDK versions.
		if isCleanShutdown(ctx, err) {
			return 0
		}
		log.Error("mcp server exited", "err", err)
		return 1
	}
	return 0
}

// isCleanShutdown reports whether err represents a normal MCP-server
// exit (ctx canceled by signal, or stdin closed by the agent).
func isCleanShutdown(ctx context.Context, err error) bool {
	if ctx.Err() != nil {
		return true
	}
	if errors.Is(err, io.EOF) {
		return true
	}
	// SDK wraps EOF in a string-formatted error in some paths.
	return strings.Contains(err.Error(), "EOF")
}

// mcpSearchInput is the typed parameter shape for the `search` tool.
// JSON tags + jsonschema struct tags are picked up by the MCP SDK to
// produce the tool's JSON schema automatically.
type mcpSearchInput struct {
	Topic         string  `json:"topic"                    jsonschema:"name of the Kafka topic to search (required)"`
	Query         string  `json:"query"                    jsonschema:"regex pattern OR literal substring; falls back to literal on regex parse failure"`
	Since         string  `json:"since,omitempty"          jsonschema:"lower time bound; relative duration like 24h/7d or RFC3339 timestamp"`
	Until         string  `json:"until,omitempty"          jsonschema:"upper time bound; same format as since"`
	Scope         string  `json:"scope,omitempty"          jsonschema:"match scope: key, value, key+value (default), or key+value+headers"`
	Partitions    []int32 `json:"partitions,omitempty"     jsonschema:"specific partitions to scan; empty/omitted means all partitions"`
	CaseSensitive bool    `json:"case_sensitive,omitempty" jsonschema:"if true, do not add (?i) to the regex"`
	Cap           int     `json:"cap,omitempty"            jsonschema:"maximum matches to return; 0 = library default (10000)"`
}

// makeSearchHandler builds the tool handler closure. Returns a Summary
// (same struct used by `--format=summary`) as the tool's structured
// output, which the MCP SDK automatically materializes into the
// CallToolResult.
func makeSearchHandler(
	cache *searchcache.Cache,
	client *kafka.Client,
	remote *remoteindex.Client,
) mcp.ToolHandlerFor[mcpSearchInput, searchcli.Summary] {
	return func(ctx context.Context, _ *mcp.CallToolRequest, in mcpSearchInput) (
		*mcp.CallToolResult, searchcli.Summary, error,
	) {
		if strings.TrimSpace(in.Topic) == "" {
			return nil, searchcli.Summary{}, fmt.Errorf("topic is required")
		}
		if strings.TrimSpace(in.Query) == "" {
			return nil, searchcli.Summary{}, fmt.Errorf("query is required")
		}

		params, err := buildMCPParams(in)
		if err != nil {
			return nil, searchcli.Summary{}, err
		}
		sum := searchcli.Run(ctx, cache, client, remote, in.Topic, in.Query, params, in.CaseSensitive)
		return nil, sum, nil
	}
}

// buildMCPParams is the MCP-side analog of buildSearchParams from
// search.go. The two share the same time/scope/partition parsing
// semantics so behavior is identical across surfaces.
func buildMCPParams(in mcpSearchInput) (kafka.SearchParams, error) {
	pat := strings.TrimSpace(in.Query)
	if !in.CaseSensitive && !strings.HasPrefix(pat, "(?") {
		pat = "(?i)" + pat
	}
	re, err := regexp.Compile(pat)
	if err != nil {
		// Same fallback as the CLI / TUI: treat as literal substring.
		re = regexp.MustCompile("(?i)" + regexp.QuoteMeta(strings.TrimSpace(in.Query)))
	}

	scope, err := parseScopeFlag(in.Scope)
	if err != nil {
		return kafka.SearchParams{}, err
	}

	since, err := parseTimeFlag(in.Since)
	if err != nil {
		return kafka.SearchParams{}, fmt.Errorf("since: %w", err)
	}
	until, err := parseTimeFlag(in.Until)
	if err != nil {
		return kafka.SearchParams{}, fmt.Errorf("until: %w", err)
	}

	return kafka.SearchParams{
		Pattern:    re,
		Scope:      scope,
		Since:      since,
		Until:      until,
		Partitions: in.Partitions,
		Cap:        in.Cap,
	}, nil
}

// _ ensures `time` is used in this file (it is via parseTimeFlag's
// return type) — also documents that timestamps in the wire format are
// RFC3339 as elsewhere in k4a.
var _ time.Time
