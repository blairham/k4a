# AGENTS.md — k4a

Guidance for AI coding agents (Claude Code, Cursor, Copilot, Codex, OpenCode, …) working in this repository. This is the **cross-tool single source of truth** — `CLAUDE.md` imports it, and other tools read this file directly.

## Project Overview

**k4a** is an interactive terminal UI for Kafka clusters — like k9s, but for Kafka — plus headless subcommands for the same operations. Built with Charm (bubbletea, bubbles, lipgloss) on franz-go. Connects over plaintext, TLS, SASL/SCRAM, mTLS and MSK IAM.

Two optional daemons share one search index across a team: **`k4a-index`** tails an allowlisted set of topics and answers coverage-reporting searches over gRPC, and **`k4a-mcp`** puts a Model Context Protocol server in front of it. Both ship as container images with Helm charts; the CLI ships as release archives and a Homebrew formula.

- Module: `github.com/blairham/k4a`; Apache-2.0 with a CLA (`CLA.md`).
- Go 1.26 (`go.mod`'s `go` directive and `.tool-versions` must match exactly).

## Quick Reference

```bash
make build        # dist/k4a, dist/k4a-index, dist/k4a-mcp
make install      # build k4a and copy it to ~/.local/bin
make test         # go test -race ./... (no Kafka needed)
make vet          # go vet ./...
make fmt          # gofumpt (the commit hook also runs the configured formatters)
make proto        # regenerate internal/indexwire/indexv1 from proto/ (pinned buf + plugins)
make check-proto  # fail if the generated gRPC code is stale
make helm-lint    # lint and render charts/k4a-index and charts/k4a-mcp
make docker-build # both images from source (targets index and mcp)
make fuzz         # each fuzz target for FUZZTIME (default 30s)
```

There is **no `lint` target**: golangci-lint runs as the pre-commit hook and in CI's Pre-commit job, never by hand. `make check` is what CI runs; push and read CI rather than running it locally.

## Project Structure

```
main.go              # `go install github.com/blairham/k4a@latest` / `go run .` — delegates to internal/app
cmd/
  k4a/               # Canonical CLI/TUI entry point (the release archives)
  k4a-index/         # Shared index daemon (gRPC): server, follow-set manager,
                     # tail workers, the --allow policy
  k4a-mcp/           # MCP server over the daemon (streamable HTTP)
internal/
  app/               # The one subcommand table (hashicorp/cli), shared by both entry points
  awscreds/          # AWS shared-config loading for a named profile + the SSO login hint
  command/           # CLI commands (see the subcommand list below)
  config/            # Config file loading (~/.k4a/config.yaml, kubeconfig-style)
  kafka/             # Kafka client: auth/TLS, metadata, offsets, consumer, admin, deep search
  index/             # Local Bleve index: indexer, analyzer, query/coverage, pattern rewrite,
                     # async intake, catch-up backfill, inventory
  indexwire/         # gRPC contract for the shared index (generated indexv1 + conversions)
  remoteindex/       # Client backend that dials a shared k4a-index daemon
  searchcache/       # On-disk query-result cache (Tier 2) + the search wrapper
  searchcli/         # Shared search-summary shape (CLI `--format summary` + MCP)
  state/             # ~/.k4a state root: tiers, envelopes, advisory file lock, session
  timebound/         # Search time bounds (24h / 7d / 2w / RFC3339) for every surface
  upgrade/           # Self-upgrade (`k4a upgrade`, `:upgrade`)
  version/           # Version/commit/date resolution (ldflags → build info)
  tui/               # Root bubbletea app model
    style/           # Colors, styles, view types
    views/           # Topics, TopicDetail, Groups, GroupDetail, Cluster, Messages,
                     # MessageDetail, Search, Produce, CreateTopic, CreateACL,
                     # Context, ACLs, TopicConfig, ResetOffsets, BrokerDetail
proto/               # Protobuf source for internal/indexwire/indexv1 (buf)
schemas/             # Example `k4a produce --schema` files
charts/              # Helm charts: k4a-index, k4a-mcp
docs/                # architecture/ and design/ — read before changing an area
Dockerfile           # Targets `index` and `mcp` (from source) and `*-release` (GoReleaser)
```

### Subcommands (`internal/app/app.go`)

`ui` (default), `produce`, `consume`, `verify`, `create-topic`, `create-acl`,
`delete-topic`, `describe-topic`, `topic-size`, `list-acls`, `alter-config`,
`alter-replication`, `contexts`, `use-context`, `version`, `upgrade`, `state`,
`config`, `search`, `mcp`.

An unrecognized first argument is treated as flags for `ui`, so `k4a --context
staging` still opens the TUI.

## Views

- **Topics** — list with partitions, replication, out-of-sync, message counts
- **Topic Detail** — summary cards + partition table with offsets
- **Topic Config** — editable topic configuration with default toggle
- **Groups** — consumer groups with state, members, topics, lag, coordinator
- **Group Detail** — per-partition committed offset, high watermark, and lag
- **Cluster** — broker list with controller indicator
- **Broker Detail** — broker configuration with default/read-only/sensitive flags
- **Messages** — live message tail (newest first) with follow mode
- **Message Detail** — sticky metadata/header cards + scrollable pretty-printed JSON
- **ACLs** — access control list bindings with create/delete
- **Context** — context selector for switching clusters
- **Produce** — interactive message production with progress tracking
- **Create Topic** — topic creation form (name, partitions, replication)
- **Create ACL** — ACL binding creation form
- **Reset Offsets** — consumer group offset reset (earliest/latest/specific/shift)
- **Help** — `?` toggles shortcut reference overlay

## Navigation

### Tab Switching

- `1` Topics, `2` Groups, `3` Cluster, `4` ACLs

### Global Keys

- `j`/`k` navigate, `enter` drill in, `esc` back
- `/` filter (regex), `:` command mode, `?` help
- `r` refresh, `ctrl+c` quit
- `G` go to bottom, `PgDn`/`PgUp` page navigation

### Topics View

- `enter` — tail messages from topic
- `o` — topic overview (partitions, replication, retention)
- `p` — produce messages
- `c` — create new topic
- `i` — toggle internal topics visibility
- `ctrl+d` — delete topic (with confirmation)

### Topic Detail View

- `m` — view messages
- `e` — edit topic configuration
- `p` — increase partition count
- `R` — change replication factor
- `ctrl+d` — purge messages (set retention: `1h`, `1d`, `0` for all, suffix `!` for permanent)

### Groups View

- `enter` — view group detail (per-partition lag)
- `ctrl+d` — delete consumer group

### Group Detail View

- `R` — reset offsets (form: topic, mode, value)
- `:reset` — delete selected offset

### Messages View

- `enter` — view full message detail
- `s` — **search the whole topic** (opens the search form). Saving a single
  message lives in Message Detail, also on `s`, after `enter`.
- `f` — toggle follow mode (auto-scroll); `g` jump to newest + resume follow
- `o` — jump to topic overview
- `p` / `P` — cycle the partition filter forward / backward; `a` clears it
- `h` / `l` — scroll the table horizontally / pan the value column

### Message Detail View

- `s` — save message to file (JSON)

### ACLs View

- `a` — create new ACL binding
- `ctrl+d` — delete selected ACL

### Cluster View

- `enter` — view broker detail/configuration

### Broker Detail / Topic Config

- `d` — toggle showing default configurations
- `enter` — edit config value (topic config only)

### Context View

- `enter` — switch to selected context

### Form Views (Produce, Create Topic, Create ACL, Reset Offsets)

- `tab`/`shift+tab` — next/previous field
- `enter` — submit form
- `esc` — cancel

## Command Mode

Enter with `:`, supports autocomplete:

```
:q, :quit, :exit               Quit
:topics, :groups, :cluster     Switch views
:acls                          Switch to ACLs view
:context, :ctx                 Open context selector
:produce [TOPIC]               Produce messages
:create-topic, :ct             Create topic
:create-acl, :acl              Create ACL
:reset                         Delete selected offset (Group Detail)
:reconnect                     Reload credentials / re-auth
:search, :s                    Open search for the current topic
:upgrade                       Upgrade k4a and restart
:logo, :logoless               Toggle ASCII logo (in-memory; not persisted)
```

## Filter System

- `/pattern` — case-insensitive regex filter across visible columns
- `!/pattern` or `/!pattern` — negated filter (exclude matches)
- Invalid regex falls back to literal match
- Available in: Topics, Groups, Messages, ACLs, TopicConfig, BrokerDetail

## CLI Flags

```
Connection:
  -c, --context NAME      Named context from config
      --config FILE       Config file path (default: ~/.k4a/config.yaml)
  -b, --brokers BROKERS   Bootstrap servers (comma-separated)
  -a, --auth METHOD       Auth: plaintext, tls, scram, mtls, iam

SASL/SCRAM:
      --username USER     SASL/SCRAM username  (env: KAFKA_USERNAME)
      --password PASS     SASL/SCRAM password  (env: KAFKA_PASSWORD)

mTLS:
      --cert FILE         Client certificate   (env: KAFKA_TLS_CERT)
      --key FILE          Client private key    (env: KAFKA_TLS_KEY)
      --ca FILE           CA certificate        (env: KAFKA_TLS_CA)

IAM:
  -r, --region REGION     AWS region            (env: AWS_REGION, default: us-east-1)
  -p, --profile PROFILE   AWS profile           (env: AWS_PROFILE)

Display:
  -k, --insecure          Skip TLS verification
      --logoless          Hide ASCII logo from header
      --readonly          Disable all mutating operations (read-only mode)
```

The headless subcommands share a `ConnectionFlags` struct
(`internal/command/flags.go`) that adds two shared-index flags on top of the
above:

```
      --index-endpoint HOST:PORT  Shared k4a-index daemon; overrides the
                                  context   (env: K4A_INDEX_ENDPOINT)
      --index-insecure            Dial the shared index over plaintext gRPC
```

Both fall back to the resolved context's `index-endpoint` / `index-insecure`.
Config-resolution failures deliberately degrade to "no endpoint": a missing or
broken config must never turn a search into a hard error over an optional
accelerator.

When `--context` or `--brokers` is provided, the context selector is skipped on startup.

## Config File

`~/.k4a/config.yaml` — kubeconfig-style with named contexts:

```yaml
current-context: staging
contexts:
  staging:
    ssm-brokers: /my-cluster/msk-brokers
    auth: iam
    region: us-east-1
    profile: Staging/AdministratorAccess
  production:
    brokers: broker1:9098,broker2:9098
    auth: iam
    region: us-east-1
    profile: Production/ReadOnlyAccess
    insecure: false            # per-context skip-TLS-verify (the -k equivalent)
    index-endpoint: k4a-index.example.com:9500
    index-insecure: true       # plaintext gRPC to the shared index
ui:
  logoless: true    # Hide ASCII logo (like k9s ui.logoless)
  readonly: false   # true blocks every mutating operation
index:
  enabled: false    # true opts in to the local Tier 3 rolling index
```

Full context field set (`internal/config.Context`): `brokers`, `ssm-brokers`,
`auth`, `region`, `profile`, `username`, `password`, `cert`, `key`, `ca`,
`insecure`, `index-endpoint`, `index-insecure`.

The two non-context blocks are also CLI-writable: `k4a config
show|path|get <key>|set <key> <value>` over `ui.logoless`, `ui.readonly`,
`index.enabled`.

SSM parameter resolution for broker endpoints. Env var expansion with `$VAR`.

**AWS profiles are resolved in-process** — a context's `profile:` (or `-p`) goes to `internal/awscreds`, which loads it through the AWS SDK's shared config, so an SSO session or a `credential_process` (granted, aws-vault, …) works without wrapping `k4a` in `assume --exec` or `aws-vault exec`; an ambient session still works as the default chain. The SSM broker lookup in `internal/config` reports an expired session with the command that renews it (`awscreds.LoginHint`); the MSK IAM signer in `internal/kafka/auth.go` loads credentials lazily on the first handshake, so a lapsed session surfaces as a connection error. See `docs/design/auth.md`.

## Code Conventions

- Formatting and lint run as **pre-commit hooks**, never by hand (see the
  workspace `AGENTS.md`). golangci-lint v2 is pinned in go.mod's `tool` block
  and in `.pre-commit-config.yaml`; move the two together. `.golangci.yml`
  keeps five linters (goconst, gocritic, gocognit, gocyclo, funlen) off until
  their existing findings are burned down; turning one on means fixing its
  findings in the same change.
- **Formatter**: gofumpt via `go tool gofumpt`; golines at 120 columns; gci
  import groups (standard, default, `prefix(github.com/blairham/k4a)`).
- **Struct layout**: fieldalignment-optimized (govet). Its fix reorders fields
  and can drop field comments — check them after the hook runs.
- Every hand-written `.go` file starts with the two-line SPDX header
  (`SPDX-FileCopyrightText: 2026 Blair Hamilton` /
  `SPDX-License-Identifier: Apache-2.0`); `check-license-headers` enforces it.
  The generated `*.pb.go` files carry it from `proto/indexv1/index.proto`.
- **Never hand-edit `internal/indexwire/indexv1/*.pb.go`.** Change the proto,
  run `make proto` and commit both; CI's `make check-proto` fails on stale
  output.
- **Nothing secret reaches logs, `~/.k4a` or an index**, and `k4a-index`
  never follows or serves a topic outside `--allow`. `SECURITY.md` states the
  trust model — keep it true when behavior changes.
- **Read the owning `docs/design/*.md` before changing an area**, and update
  it in the same change when the change makes it stale.
- **Comments are short and explain why, not what.** American English spelling.

## Architecture Notes

- **Batched DescribeGroups**: Consumer groups are described through franz-go's `kadm.DescribeGroups` in one batched call per refresh (`internal/kafka/client.go`), not per group — see [`docs/design/async-enrichment.md`](docs/design/async-enrichment.md).
- **Async enrichment pattern**: Expensive data (topic message counts, group lag/coordinator) is fetched asynchronously after the initial metadata load, following the `TopicCountsMsg` / `GroupEnrichmentMsg` pattern. Data is preserved across 2-second refresh cycles.
- **Batched ListOffsets**: High watermarks for lag calculation use a single batched `ListOffsets` call instead of per-partition TCP dials.
- **The index is an accelerator, never the source of truth.** `internal/index/pattern.go` decides whether a regex can be answered from analyzed terms with full recall; anything it cannot prove goes to a scan. `isTokenRune` asks the index's own tokenizer rather than modeling UAX#29, and text fields use `textAnalyzer` (`internal/index/analyzer.go`): Bleve's standard analyzer with a lowercase filter that does not corrupt terms. A change to the analyzer bumps `SchemaVersion`, which rebuilds existing indexes.
- **Coverage parity**: `internal/indexwire` round-trips exactly the fields `Bookkeeper.CanServe` reads, so a client makes the same index-vs-scan decision against a daemon's coverage as against its local index.

## CI/CD

- `.github/workflows/ci.yml` — the required checks: **Pre-commit** (the same
  pinned hooks as the local commit gate, via `blairham/go-pre-commit`),
  **Detect changed files** (skips the code jobs for prose-only PRs without
  leaving a check pending), **Build and test** (`make build`,
  `make check-proto`, `go vet`, `go test -race`), **Build image** (both
  Dockerfile targets, no push) and **Helm chart** (`make helm-lint`, which
  also proves `charts/k4a-index` still refuses to render without an
  allowlist). `codeql.yml` (security-extended) and `scorecard.yml` run
  alongside. Every action is pinned by commit SHA with a `# vX.Y.Z` comment;
  Dependabot moves the pins, the Go modules (aws-sdk, franz-go, Charm and
  gRPC as groups) and the Dockerfile bases.
- A **`v*` tag is what publishes**: `goreleaser.yml` runs GoReleaser, which
  publishes `k4a` archives for Linux, macOS and Windows (amd64, arm64), a
  cosign-signed `checksums.txt`, SLSA provenance for the archives, the
  `k4a` formula in `blairham/homebrew-tap`, and the multi-arch images
  `ghcr.io/blairham/k4a-index` and `ghcr.io/blairham/k4a-mcp`, each signed
  and attested. The notes are the tag's `CHANGELOG.md` section. The release
  refuses to publish when the CHANGELOG has no section for the tag or either
  chart's `appVersion` does not match it. The first tag is `v0.0.0`.
- The same tag runs `chart.yml`, which pushes `k4a-index` and `k4a-mcp` to
  `oci://ghcr.io/blairham/charts/...` and signs each by digest; it refuses
  unless both `version` and `appVersion` in each `Chart.yaml` match the tag.
  Rerun it alone with `gh workflow run chart.yml -f tag=<tag>`. Read
  `.claude/commands/release-tag.md` before cutting a tag.
- `osv-scanner.toml` ignores one advisory, with its reason; re-check it when
  the module graph changes.

## Key Dependencies

- `twmb/franz-go` — Pure Go Kafka client (`kgo`/`kadm`/`kmsg`; no CGO), including its SASL/SCRAM and MSK IAM mechanisms
- `aws-sdk-go-v2` — AWS shared config and credentials for IAM auth, and SSM parameter resolution
- `blevesearch/bleve/v2` — the local and shared index (scorch, pure Go)
- `google.golang.org/grpc` + `protobuf` — the k4a-index wire contract
- `modelcontextprotocol/go-sdk` — `k4a mcp` (stdio) and `k4a-mcp` (HTTP)
- `blairham/tuikit` — Shared Charm TUI widgets
- `charm.land/bubbletea`, `bubbles`, `lipgloss`, `huh` — TUI framework, components, styling, forms
- `jessevdk/go-flags` — CLI flag parsing; `hashicorp/cli` — subcommand framework
- `creativeprojects/go-selfupdate` — `k4a upgrade`

## Testing

- `go test -race ./...` needs no Kafka or network: Kafka-facing code is
  tested against franz-go's in-process fakes and pure functions, the index
  against real Bleve indexes in `t.TempDir()`. Tests never touch real user
  state — redirect `~/.k4a` with `t.TempDir()` + `t.Setenv`.
- **Fuzz** targets on input that arrives from outside the process:
  `FuzzIndexablePattern` (`internal/index/pattern_fuzz_test.go`; a search
  pattern from the CLI, MCP or a k4a-index client must never be served from
  the index unless every match is found, checked against the index's real
  analyzer), `FuzzParseDuration` / `FuzzParse` (`internal/timebound`; time
  bounds must be non-negative and exact) and `FuzzParseTopicSchema`
  (`internal/kafka/schema_fuzz_test.go`; a schema that loads must produce).
  Before the first release they found a recall gap for Katakana, Thai, İ and
  fold-equivalent letters, Bleve corrupting terms that contain İ, K, Å or Ω,
  overflowing and negative lookbacks, and a schema with an empty sample that
  crashed `produce`; their inputs stay as seeds. `make fuzz` runs them.
- Every bug fix comes with the test that would have caught it.

## Documentation

See [`docs/README.md`](docs/README.md) for the layout: `docs/architecture/` (cross-cutting overview — the layers, data-flow patterns, what's out of scope) and `docs/design/` (per-subsystem living docs, each with a `Status:` + `Code:` header pointing at the implementation). Read the relevant doc before changing the area it covers — they capture trade-offs, prior incidents, and constraints that aren't visible from the source. When a change makes a doc stale, update it in the same commit; don't defer.
