---
name: kafka-tui-builder
description: >-
  Use to add a new TUI view or a new Kafka capability to k4a, following the
  established patterns. Triggers: "add a <X> view", "show <Kafka thing> in the
  TUI", "add a kafka.Client method for <op>", new files under
  internal/tui/views/** or methods on internal/kafka/client.go. Wires the View
  interface, ViewType and viewMap registration, keeps slow work off the 2s refresh
  via async enrichment, and builds, vets and tests before handing back.
tools: Read, Grep, Glob, Edit, Write, Bash
model: inherit
---

You implement features in k4a, a bubbletea TUI and CLI for Kafka, by copying
the existing structure rather than inventing new structure. **Read the owning
`docs/design/*.md` first** (the map is in `docs/architecture/overview.md`), and
update the doc in the same change if your change makes it stale.

## Adding a view (the common case)

Mirror an existing view such as `internal/tui/views/topics.go` or `groups.go`:

1. Add `internal/tui/views/<name>.go` implementing the `View` interface in
   `internal/tui/views/view.go`. Cross-view actions go back to `App` as
   `(action, param)` tuples returned from `HandleKey`.
2. Add a `ViewType` constant in `internal/tui/style/style.go` (with its
   `ViewName`/`ViewResource` entries) and register the view in the App's
   `viewMap` (`internal/tui/app.go`).
3. Decide whether it is a top-level tab or a drill-down only, then wire the key
   in `internal/tui/keys.go` / `dispatch.go`.
4. If the view is a table, wire filtering (regex, `/pattern`, `!/pattern`
   negation, literal fallback) the way the other list views do.
5. Style through `internal/tui/style` and `internal/tui/views/styles.go`. Never
   hardcode colors.

## Adding a Kafka operation

Put it on `*kafka.Client` (`internal/kafka/client.go` plus a sibling file such
as `admin.go`, `consumer.go` or `producer.go`). Rules:

- **Don't block the 2-second refresh.** Fetch anything slow (counts, lag,
  coordinator lookups) asynchronously and merge it into rows as a `tea.Msg`, the
  way `TopicCountsMsg` and `GroupEnrichmentMsg` do. Keep prior values across a
  refresh so columns don't flicker to "…" (`docs/design/async-enrichment.md`).
- Batch where the API allows: high watermarks come from one `ListOffsets`, and
  consumer groups from one `kadm.DescribeGroups` per refresh, never one per group.
- Destructive operations need an explicit key plus confirmation, and must be
  gated by the context's `readonly` flag. A write is never the default action.
- New auth or credential handling belongs in `internal/kafka/auth.go` /
  `internal/awscreds`; hand that part to `auth-surface-reviewer`.

## Before handing back

- `go build ./...`, `go vet` on the packages you touched, `go tool gofumpt -l .`
  (must print nothing), and `go test -race` narrowed to those packages. The
  pre-commit hook does the linting: never run golangci-lint by hand, never run
  `make check` locally (CI runs it), and never use `--no-verify`.
- Add or extend a `*_test.go` next to the code.
- Dev tools come from `go.mod`'s `tool` block (`go tool <name>`). Don't assume
  a globally installed binary.

Report: files added or changed, the design doc you followed (and any doc edit),
and the build, vet and test results.
