# Architecture

**Status:** living document — describes current behavior

k4a is an interactive TUI for exploring Kafka clusters. Comparable
surface to k9s (lists, detail views, command mode, filter, follow),
targeted at Kafka concepts (topics, groups, partitions, ACLs, brokers).

The repo builds **three** binaries, all sharing `internal/`:

- **`cmd/k4a`** — the CLI + TUI users install. Still a single self-contained
  binary; everything below about layers and data flow describes this one.
- **`cmd/k4a-index`** — the shared always-on index daemon. Tails an
  allowlisted topic set into a Bleve index and serves coverage-gated
  Search/Follow/Coverage over gRPC. See `design/shared-index-service.md`.
- **`cmd/k4a-mcp`** — an MCP frontend that is a thin gRPC client of the
  daemon (no Kafka client, no index, no AWS creds of its own).

The daemons reuse `internal/index` and `internal/kafka` verbatim — that is
the whole point of keeping them in this repo: one indexer implementation
means "indexed (shared)", "indexed (local)", and "scanned" cannot diverge
in what they match.

## Top-level layout

```
main.go                # local-dev entry point; delegates to internal/app
cmd/
  k4a/                 # canonical CLI/TUI entry point
  k4a-index/           # index daemon: gRPC server, follow-set manager,
                       # tail workers, allow policy
  k4a-mcp/             # MCP frontend over the daemon
internal/
  app/                 # the one subcommand table (hashicorp/cli routing)
  command/             # CLI commands (ui, produce, consume, verify, search, ...)
  config/              # ~/.k4a/config.yaml loader + SSM resolver
  kafka/               # client: auth, metadata, offsets, consumer, admin,
                       # deep search
  index/               # local Bleve index: indexer, coverage/query, pattern
                       # rewrite, async intake, catch-up, inventory
  indexwire/           # gRPC contract for the shared index (proto + conversions)
  remoteindex/         # client backend that dials a shared k4a-index daemon
  searchcache/         # on-disk query-result cache + the search wrapper
  searchcli/           # shared search-summary shape (CLI summary + MCP)
  state/               # ~/.k4a state root: tiers, envelopes, file lock, session
  upgrade/             # self-upgrade
  version/             # version/commit/date resolution
  tui/                 # root bubbletea App
    style/             # colors, view-type enum
    views/             # one file per view (Topics, Groups, Search, ...)
proto/                 # protobuf sources for internal/indexwire
charts/                # Helm charts for the two daemons
```

`internal/app` parses flags, builds an `AuthConfig`, opens a
`kafka.Client`, and hands it to `tui.NewApp`. The TUI never
re-authenticates by itself — on credential refresh it asks the client to
drop idle connections and the next call dials fresh.

## The four layers

1. **CLI / config** — `internal/command` and `internal/config` resolve
   `--context` / flags / env vars / config file into a single
   `kafka.AuthConfig` + bootstrap broker list. SSM parameter resolution
   lives here so the kafka layer never sees AWS-specific quirks.
   See `design/contexts.md`.

2. **Kafka client** — `internal/kafka` wraps `twmb/franz-go` (`kgo` for
   consume/produce, `kadm` for admin, `kmsg` for protocol enums); the
   SASL/TLS/MSK-IAM auth layer is built on franz-go's `sasl` packages
   (`internal/kafka/auth.go`). Auth methods
   (`design/auth.md`), broker/topic/group fetches, offsets, message
   consume/produce, admin ops, and the deep-search scanner live here.
   (`segmentio/*` still appears in `go.sum` as an indirect encoding
   dependency — it is not the Kafka client.)

3. **TUI runtime** — `internal/tui/app.go` is the root bubbletea model.
   It holds a `map[ViewType]View`, dispatches messages to the active
   view, and runs a 2-second refresh tick. View transitions push/pop a
   stack so `esc` always lands somewhere sensible.
   See `design/tui-model.md`.

4. **Views** — one file per view in `internal/tui/views/`. Each owns its
   own table/viewport/spinner state and exposes a small interface
   (`Init`, `Update`, `View`, `Refresh`, `Resize`). Cross-view actions
   (drill-down, produce, delete) bubble back to `App` as `(action, param)`
   tuples returned from `HandleKey`.

## Data flow patterns

**Cheap-then-expensive fetch.** Initial metadata (topic list, group list)
loads first so the table is populated; expensive enrichment (message
counts, lag, coordinator) runs asynchronously and merges into rows as it
arrives. Old enrichment values are preserved across the 2-second refresh
so columns don't flicker between numeric and "...". See
`design/async-enrichment.md`.

**Refresh tick.** The root app emits `tickMsg` every 2s and calls
`Refresh()` on the active view. Views that aren't visible don't refresh —
expensive cluster operations only happen for whatever the user is looking
at right now.

**Connection reuse with reset on error.** The kafka client holds one
auth-configured `*kgo.Client` plus its `*kadm.Client` admin wrapper. On a
refresh error the App drops idle connections so the next attempt picks up
new IAM tokens, refreshed CA pools, or DNS changes.

## Persistent state

k4a *does* keep local state — it just keeps it under a rule that makes it
disposable. Everything above the user-owned config tier is independently
deletable and rebuildable, and the cluster is always the source of truth;
`design/state-management.md` is the full model.

`internal/state` owns the `~/.k4a` root and its subsystems:

| Subsystem | On disk | What it holds |
|---|---|---|
| config | `~/.k4a/config.yaml` | user-owned; contexts + preferences (never auto-cleared) |
| session | `~/.k4a/state.json` | recent UI choices |
| cache | `~/.k4a/cache/searches/` | the on-disk query-result cache (Tier 2) |
| index | `~/.k4a/index/<cluster>/<topic>/` | the local Bleve index (Tier 3, opt-in via `index.enabled`) |

Each persisted document is wrapped in a schema-versioned envelope, so a
version mismatch or corruption is handled by rename-aside-and-rebuild
rather than a hard failure. `k4a state status` reports size and age per
subsystem, `k4a state clear [subsystem|all]` removes it, `k4a state dir`
prints the root.

## What's intentionally out of scope

- **No write-by-default surfaces.** Destructive ops (delete topic /
  group / ACL, purge, offset reset) require an explicit key (`ctrl+d`,
  `R`) and a confirmation. `ui.readonly: true` removes them entirely.
- **No cross-cluster views.** One cluster per running instance. Switch
  contexts in-app via the context selector — that tears down the client
  and dials the new one.
- **No schema registry coupling.** Message deserialization is best-effort
  JSON pretty-print; we don't fetch from a registry. Adding registry
  support would be a separate design doc, not a tweak.

## Pickup notes

- Adding a new view: create `internal/tui/views/<name>.go` implementing
  the `View` interface, add a `ViewType` constant in `style/`, register
  it in the App's `viewMap`, and decide if it deserves a top-level tab
  key (`1`-`4`) or only a drill-down. Don't forget filter wiring if the
  view has a tabular component.
- Adding a Kafka operation: put it on `*kafka.Client`. If it's slow,
  wrap it in an async enrichment message — don't block the refresh.
- Read the design docs in `docs/design/` before changing the
  corresponding subsystem; they record decisions that aren't obvious
  from the code alone.
