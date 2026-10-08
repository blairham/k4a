# k4a docs

Documentation for k4a — an interactive TUI + CLI for exploring Kafka
clusters (topics, groups, partitions, ACLs, brokers). Built with Charm
(bubbletea, bubbles, lipgloss); supports SASL/SCRAM, mTLS, and IAM auth for
AWS MSK.

The repo also builds two optional in-cluster daemons that share the same `internal/`
packages: `k4a-index` (the shared always-on index, served over gRPC) and
`k4a-mcp` (an MCP frontend over it).

## How the docs are organized

| Folder | Purpose | When to read |
|---|---|---|
| [`architecture/`](architecture/) | Cross-cutting "how the whole thing fits together" overview — the four layers, data-flow patterns, what's out of scope | Onboarding, or before a change that spans layers |
| [`design/`](design/) | Per-subsystem living docs (one per subsystem). Each has a `Status:` and `Code:` header pointing at the implementation. | Before changing a specific subsystem |
| [`guides/`](guides/) | Task-oriented how-tos (configuration, connecting agents, deploying the daemons) | Setting up k4a, an MCP client, or the shared index |

## Architecture

- [`architecture/overview.md`](architecture/overview.md) — top-level layout,
  the four layers (CLI/config, kafka client, TUI runtime, views), data-flow
  patterns (cheap-then-expensive fetch, refresh tick, connection reuse), and
  what's intentionally out of scope.

## Design docs by subsystem

| Subsystem | Doc | Status |
|---|---|---|
| Authentication (plaintext/tls/scram/mtls/iam) | [`design/auth.md`](design/auth.md) | living document |
| Contexts (loading, SSM/env resolution) | [`design/contexts.md`](design/contexts.md) | living document |
| TUI model (root app, dispatch, refresh tick, view stack) | [`design/tui-model.md`](design/tui-model.md) | living document |
| Async row enrichment (message counts, lag, coordinator) | [`design/async-enrichment.md`](design/async-enrichment.md) | living document |
| Filter and search across a topic (deep search) | [`design/deep-search.md`](design/deep-search.md) | shipped |
| State management (session, file lock, tiering) | [`design/state-management.md`](design/state-management.md) | shipped |
| Local rolling index for fast topic search | [`design/local-index.md`](design/local-index.md) | shipped (Tiers 5-6 unbuilt) |
| Shared always-on index service (`k4a-index`) | [`design/shared-index-service.md`](design/shared-index-service.md) | shipped (Phase 5 outstanding) |

## Guides

- [`guides/mcp-setup.md`](guides/mcp-setup.md) — configure k4a's contexts
  (MSK IAM auth) and connect Claude Code (or any MCP client) to the shared
  `k4a-mcp` index over MCP, with a local `k4a mcp` stdio fallback.
- [`guides/deploy.md`](guides/deploy.md) — deploying and operating the
  `k4a-index` / `k4a-mcp` daemons with the Helm charts: images, chart
  values, how to add a followed topic, the daemon's runtime flags/envs, and
  networking.

## Conventions

- **Status / Code header** at the top of every design doc — see
  [`design/auth.md`](design/auth.md) for the canonical shape.
- **Read the relevant doc before changing the area it covers** — they capture
  trade-offs, past failures, and constraints that aren't visible from the
  source.
- **Update the doc in the same commit that changes the code.** Stale design
  docs cost more than missing ones. Docs marked `proposed` / `in progress`
  describe a target design, not current behavior — flag the gap.
