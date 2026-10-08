# k4a

[![CI](https://github.com/blairham/k4a/actions/workflows/ci.yml/badge.svg)](https://github.com/blairham/k4a/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/tag/blairham/k4a?sort=semver&label=release)](https://github.com/blairham/k4a/releases)
[![CodeQL](https://github.com/blairham/k4a/actions/workflows/codeql.yml/badge.svg)](https://github.com/blairham/k4a/actions/workflows/codeql.yml)
[![OpenSSF Scorecard](https://api.securityscorecards.dev/projects/github.com/blairham/k4a/badge)](https://scorecard.dev/viewer/?uri=github.com/blairham/k4a)
[![Go version](https://img.shields.io/github/go-mod/go-version/blairham/k4a)](go.mod)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

Interactive TUI for exploring Kafka clusters — like [k9s](https://k9scli.io), but for Kafka.

Built with [Charm](https://charm.sh) (bubbletea, bubbles, lipgloss). Supports SASL/SCRAM, mTLS, and IAM authentication for AWS MSK.

## Install

### Homebrew

```bash
brew install blairham/tap/k4a
```

### Go

```bash
go install github.com/blairham/k4a@latest
```

### From source

```bash
git clone https://github.com/blairham/k4a.git
cd k4a
make install  # builds and copies to ~/.local/bin
```

### GitHub Releases

Download pre-built binaries for Linux, macOS, and Windows from the [Releases](https://github.com/blairham/k4a/releases) page.

## Usage

```bash
# Use current-context from config
k4a

# Use a specific context (skips context selector)
k4a --context staging

# Direct connection (no config file)
k4a -b broker:9092 --auth scram --username admin --password secret

# IAM auth for AWS MSK
k4a -b broker:9098 --auth iam --region us-east-1 --profile MyProfile

# Hide the ASCII logo
k4a --logoless
```

## Configuration

k4a uses a kubeconfig-style config file at `~/.k4a/config.yaml`:

```yaml
current-context: staging
contexts:
  staging:
    ssm-brokers: /my-cluster/msk-brokers
    auth: iam
    region: us-east-1
    profile: my-profile
    # Optional shared-index accelerator — see "Shared index" below.
    index-endpoint: k4a-index.example.com:9500
    index-insecure: true    # dial the daemon over plaintext gRPC
  local:
    brokers: localhost:9092
    auth: plaintext
    insecure: false         # skip TLS certificate verification (per-context -k)
ui:
  logoless: true    # hide ASCII logo from header
  readonly: false   # true disables every mutating operation
index:
  enabled: false    # true opts in to the local rolling index (off by default)
```

The two top-level preference blocks are also writable from the CLI —
`k4a config set ui.readonly true`, `k4a config set index.enabled true` (also
`ui.logoless`); `k4a config show` / `get <key>` / `path` read them back.
Context management stays in `k4a contexts` / `k4a use-context`.

### Auth methods

| Method | Description | Required flags |
|--------|-------------|----------------|
| `plaintext` | No auth, no TLS | |
| `tls` | TLS only (no client cert) | |
| `scram` | SASL/SCRAM-SHA-512 over TLS | `--username`, `--password` |
| `mtls` | Mutual TLS with client certificate | `--cert`, `--key`, `--ca` |
| `iam` | AWS MSK IAM authentication | `--region` (default: us-east-1) |

### SSM parameter resolution

Broker endpoints can be fetched from AWS SSM Parameter Store using the `ssm-brokers` field. This is useful for MSK clusters where bootstrap brokers are stored in SSM.

### AWS profiles (granted / SSO)

A `--profile` (or a context's `profile:`) is assumed **in-process**: k4a asks the AWS SDK for that profile, which reads its SSO session from `~/.aws/config` and the token cache — and invokes `granted` automatically when granted is the profile's `credential_process`. So run k4a directly:

```bash
k4a --context staging                       # profile comes from the context
k4a -b broker:9098 --auth iam -p my-readonly-profile
```

No `assume --exec 'k4a …' <Profile>` wrapper is needed. An already-assumed ambient session still works — that's the default credential chain, which is what an unset profile falls back to.

The SDK can't open a browser, so a missing or expired SSO session is reported with the command that renews it:

```
AWS credentials expired while reading SSM parameter "/my-cluster/msk-brokers"
  the SSO session for profile my-profile is missing or expired — run
  `granted sso login my-profile`, or wire granted as the profile's
  credential_process for automatic refresh
```

After renewing, `:reconnect` in the TUI re-resolves credentials without a restart.

### Context management

```bash
k4a contexts           # list all contexts
k4a use-context prod   # switch current context
```

Contexts can also be switched from within the TUI using `:context` or `:ctx`.

### Topic sizes

Report per-topic on-disk size from broker log directories (`DescribeLogDirs`) —
handy for capacity planning without opening the TUI:

```bash
k4a topic-size                          # every topic, largest on disk first
k4a topic-size quotes-east-book         # a single topic
k4a topic-size --counts quotes-east-book  # add message count (extra ListOffsets)
k4a topic-size --json | jq '.[0]'       # machine-readable output
k4a topic-size --sort name --internal   # sort by name, include internal topics
```

Two size columns are shown: **SIZE** is the logical size (one replica's worth,
leader partitions only) and **ONDISK** is the full footprint across every
replica (≈ `SIZE × replication-factor`).

### Topic configuration

Report a topic's configuration (`DescribeConfigs`) without opening the TUI. By
default only topic-level **overrides** are shown — keys explicitly pinned on the
topic (source `DYNAMIC_TOPIC`) rather than inherited from the broker:

```bash
k4a describe-topic example.ingest.markets.v1        # what is pinned here?
k4a describe-topic --all quotes-east-book            # every key, incl. inherited
k4a describe-topic --json quotes-east-book | jq      # machine-readable output

# compare one setting across a set of topics
k4a describe-topic --config-key min.insync.replicas \
  example.ingest.markets.v1 example.ingest.prices.v1
```

The `SOURCE` column is the point of the command: `DYNAMIC_TOPIC` means someone
set the value on this topic, while `STATIC_BROKER` / `DEFAULT` means it is
inherited from the cluster configuration. `--config-key` reports a key even when
inherited, which is what makes the cross-topic comparison useful.

To change a config, `k4a alter-config` is the write side of the same pair:

```bash
# drop a hand-set override so the topic inherits the broker default again
k4a alter-config --delete min.insync.replicas \
  example.ingest.markets.v1 example.ingest.events.v1

# pin a value
k4a alter-config --set retention.ms=86400000 quotes-east-book
```

Prefer `--delete` over setting today's default explicitly when undoing a
hand-set value: it restores *inherited*, rather than freezing the current
default as a new override. Both flags are repeatable and apply to every topic
named. This is a dynamic config change — it takes effect immediately, with no
broker restart, partition reassignment or leader election.

## Search

`k4a search` runs a deep search against a topic without opening the TUI —
the same scanner and the same matching semantics as the in-TUI search
(opened with `s` from the Messages view). The positional argument is the
pattern: a regex by default, falling back to a literal substring if the
regex doesn't compile. Matching is grep-style — the pattern can hit
anywhere inside the raw key/value/headers.

```bash
k4a search "4100000017" -t example.ingest.prices.v1 --since 24h
k4a search "order-\d{8}" -t orders --partitions 0,1,2 --format json | jq .
k4a search "X-Trace-Id" -t requests --scope key+value+headers --case-sensitive
```

| Flag | Description |
|------|-------------|
| `-t`, `--topic` | Topic to search (**required**; env `KAFKA_TOPIC`) |
| `--scope` | Where to match: `key`, `value`, `key+value` (default), `key+value+headers` |
| `--since` / `--until` | Time bounds — a duration (`1h`, `24h`, `7d`, `2w`) or an RFC3339 timestamp |
| `-P`, `--partitions` | Comma-separated partition IDs (default: all) |
| `--cap` | Maximum matches before stopping (`0` = the built-in 10,000 default) |
| `--case-sensitive` | Don't prepend `(?i)` to the pattern |
| `--format` | `text` (default), `json` (NDJSON, one match per line), or `summary` (a single JSON document with stats + matches — best for `jq` or an LLM) |
| `-q`, `--quiet` | Suppress the progress/summary line on stderr |

All the connection flags (`--context`, `--brokers`, `--auth`, `--region`,
`--profile`, …) apply too.

**Completeness is reported, never assumed.** The trailing summary line
distinguishes the two ways a result can be short:

- `(capped — narrow your search)` — the match cap was hit; there are more
  matches than were returned.
- `(INCOMPLETE — could not read the whole range; results are missing
  records)` — a chunk could not be fully read, so the scan did not cover
  its whole offset range.

An incomplete or capped result is **never written to the on-disk query
cache**, so a short read can't be re-served later as if it were a clean
completion. When the answer comes from a shared index instead of a scan,
a result truncated by the index's own query limit is flagged the same
way — and the client infers truncation from receiving exactly the
requested limit, so it holds even against a daemon too old to set the
flag. Index-served searches use the same substring semantics as a scan:
the index query is a deliberately widened *candidate* filter and the
exact regex is re-applied to each hit, so an index answer and a scan
answer can't diverge; patterns the tokenized index cannot answer with
full recall are routed to a scan instead.

## Shared index

k4a builds three binaries out of this repo:

| Binary | What it is |
|---|---|
| `cmd/k4a` | The CLI + TUI you install locally (also `go run main.go`) |
| `cmd/k4a-index` | The shared always-on index daemon — tails an allowlisted set of topics into a Bleve index and serves coverage-gated queries over gRPC |
| `cmd/k4a-mcp` | An MCP frontend over the daemon, so an agent can search topics as a tool call |

The daemon is a pure accelerator: point a context's `index-endpoint` at it
and a covered query is answered in milliseconds; anything it doesn't
cover — or an unreachable daemon — transparently falls back to the local
scan. A `k4a` with no endpoint configured behaves exactly as it always has.

- [`docs/design/shared-index-service.md`](docs/design/shared-index-service.md) — the daemon's design, wire contract, and deployment
- [`docs/guides/mcp-setup.md`](docs/guides/mcp-setup.md) — configuring contexts and connecting Claude Code (or any MCP client) to it
- [`docs/guides/deploy.md`](docs/guides/deploy.md) — deploying and operating the daemon with the Helm charts (values, allowlist changes, networking)
- [`docs/README.md`](docs/README.md) — the full docs index

## Views

### Topics (`1`)

Browse all Kafka topics with partitions, replication factor, out-of-sync count, and message counts.

| Key | Action |
|-----|--------|
| `enter` | Tail messages from selected topic |
| `o` | Topic overview (partitions, offsets, retention) |
| `p` | Produce messages to topic |
| `c` | Create new topic |
| `i` | Toggle internal topics visibility |
| `ctrl+d` | Delete topic |

### Topic Detail

Partition table with offsets, replication, and retention info.

| Key | Action |
|-----|--------|
| `m` | View messages |
| `e` | Edit topic configuration |
| `p` | Increase partition count |
| `R` | Change replication factor |
| `ctrl+d` | Purge messages (set retention: `1h`, `1d`, `0` for all; suffix `!` for permanent) |

### Topic Config

Edit topic-level configuration values.

| Key | Action |
|-----|--------|
| `enter` | Edit selected config value |
| `d` | Toggle showing default/read-only configs |

### Groups (`2`)

Browse consumer groups with state, members, topic count, consumer lag, and coordinator broker.

| Key | Action |
|-----|--------|
| `enter` | View group detail (per-partition lag) |
| `ctrl+d` | Delete consumer group |

### Group Detail

Per-partition committed offset, high watermark, and lag.

| Key | Action |
|-----|--------|
| `R` | Reset consumer group offsets (form: topic, mode, value) |
| `:reset` | Delete selected offset |

### Cluster (`3`)

Broker list with ID, host, port, and controller indicator.

| Key | Action |
|-----|--------|
| `enter` | View broker configuration |

### Broker Detail

Broker configuration entries with source, read-only, and sensitive flags.

| Key | Action |
|-----|--------|
| `d` | Toggle showing default configs |

### ACLs (`4`)

Browse and manage Kafka ACL bindings.

| Key | Action |
|-----|--------|
| `a` | Create new ACL binding |
| `ctrl+d` | Delete selected ACL |

### Messages

Live message tail from a topic (newest first).

| Key | Action |
|-----|--------|
| `enter` | View full message detail |
| `s` | Search the whole topic (opens the search form) |
| `f` | Toggle follow mode (auto-scroll to newest) |
| `g` | Jump to newest and resume follow |
| `o` | Jump to topic overview |
| `p` / `P` | Cycle the partition filter forward / backward |
| `a` | Clear the partition filter (all partitions) |
| `h` / `l` | Scroll the table horizontally / pan the value column |

Saving a single message is in **Message Detail** (`enter`, then `s`) —
in this list `s` opens search.

### Message Detail

Full message content with metadata cards, headers, and scrollable pretty-printed JSON body.

| Key | Action |
|-----|--------|
| `s` | Save message to file |
| `j` / `k` | Scroll message body |

### Context Selector

Switch between configured cluster contexts.

| Key | Action |
|-----|--------|
| `enter` | Switch to selected context |

When `--context` or `--brokers` is provided on the CLI, the context selector is skipped on startup.

## Navigation

| Key | Action |
|-----|--------|
| `1` / `2` / `3` / `4` | Switch tabs: Topics, Groups, Cluster, ACLs |
| `j` / `k` | Navigate up/down |
| `G` | Go to bottom |
| `PgDn` / `PgUp` | Page down/up |
| `enter` | Select / drill in |
| `esc` | Go back / clear filter |
| `r` | Refresh active view |
| `/` | Filter (regex) |
| `:` | Command mode |
| `?` | Help overlay |
| `ctrl+c` | Quit |

## Command Mode

Enter command mode with `:`. Supports tab autocomplete.

| Command | Action |
|---------|--------|
| `:q` `:quit` `:exit` | Quit |
| `:topics` | Switch to Topics view |
| `:groups` | Switch to Groups view |
| `:cluster` | Switch to Cluster view |
| `:acls` | Switch to ACLs view |
| `:context` `:ctx` | Open context selector |
| `:produce [TOPIC]` | Produce messages |
| `:create-topic` `:ct` | Create topic |
| `:create-acl` `:acl` | Create ACL binding |
| `:reset` | Delete selected offset (Group Detail only) |
| `:reconnect` | Reload credentials / re-auth |
| `:search` `:s` | Open search for the current topic |
| `:upgrade` | Upgrade k4a and restart |
| `:logo` `:logoless` | Toggle the ASCII logo (in-memory; not persisted) |

## Filtering

Filter any list view with `/`:

- `/pattern` — case-insensitive regex filter
- `!/pattern` — negated filter (exclude matches)
- Invalid regex falls back to literal match

Available in Topics, Groups, Messages, ACLs, Topic Config, and Broker Detail views.

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

The headless subcommands (`search`, `consume`, `produce`, `verify`, …) take
the same connection flags plus two for the optional shared index:

```
Shared index:
      --index-endpoint HOST:PORT  Shared k4a-index daemon; overrides the
                                  context   (env: K4A_INDEX_ENDPOINT)
      --index-insecure            Dial the shared index over plaintext
                                  (a local or port-forwarded daemon)
```

Both fall back to the resolved context's `index-endpoint` / `index-insecure`
when unset. See [Shared index](#shared-index).

## Development

```bash
make build      # build k4a, k4a-index and k4a-mcp into dist/
make test       # run tests with race detector
make fmt        # format code (gofumpt; the commit hook runs the linters)
make check      # vet + test + check-proto + helm-lint (what CI runs)
```

Requires Go 1.26+. golangci-lint, gofumpt, goreleaser, and fieldalignment are declared in `go.mod`'s `tool` block — invoke via `go tool <name>`; no separate install needed.

## License

MIT
