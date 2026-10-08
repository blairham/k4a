# Design: TUI Model

**Status:** living document — describes current behavior
**Code:** `internal/tui/app.go`, `internal/tui/dispatch.go`, `internal/tui/views/`

## Purpose

Hold the bubbletea event loop for k4a. Route messages and key presses to
the right view, manage the view stack so navigation is reversible, and
give every view a uniform interface for refresh/render/resize.

The root model is intentionally a single struct (`App`) rather than a
hierarchy of nested models. The dispatch layer is one big switch in
`Update`. This is the bubbletea idiom and we lean into it.

## The View interface

Every screen implements:

```go
type View interface {
    Init() tea.Cmd
    Update(tea.Msg) tea.Cmd
    View() string
    Refresh() tea.Cmd
    Resize(width, height int)
}
```

Two optional sub-interfaces:

- `Stoppable` — has a `Stop()`. Only `Messages` and `Produce` implement it
  (they own goroutines tailing/producing). The dispatcher calls `Stop()`
  on every view transition to prevent leaks.
- `(action, param)` returns from `HandleKey` — string tuple bubbled back
  to `App` so views don't need to know about each other. `App` interprets
  the action (`"messages"`, `"topic_detail"`, `"produce"`,
  `"confirm_delete_topic"`, ...) and decides whether to push a view, open
  a form, or stage a confirmation.

Views own their own table/viewport state. The App owns:

- `viewMap[ViewType]View` — registry; lazily populated.
- `view` — currently active `ViewType`.
- `viewStack`, `paramStack` — drill-down history (parallel slices).
- Spinner, command input, flash/error strings, dimensions.

## Navigation: switch vs push

Two transition primitives in `dispatch.go`:

- `switchView(v)` — clears the stack. Used for top-level tab switches
  (`1`/`2`/`3`/`4`). Calls `Stop()` on stoppable views and
  `Refresh()` on the new view.
- `pushView(v)` — appends to the stack. Used for drill-downs (Topics →
  TopicDetail → Messages). `popView()` (bound to `esc`) restores the
  previous entry.

`paramStack` exists because most drill-downs carry a parameter (selected
topic name, group ID). It's a parallel slice to `viewStack` rather than a
struct so that `pushView` doesn't have to know what the param is — the
caller sets it after the push.

## Message dispatch

`Update` is one flat switch on message type. The categories:

1. **Lifecycle** — `WindowSizeMsg`, `tickMsg`, `splashDoneMsg`,
   `versionCheckMsg`, `spinner.TickMsg`. Handled directly by the App.
2. **Keys** — `tea.KeyMsg` routes to `handleKey`, which decides between
   command mode, filter mode, confirmation prompts, global keys, and
   view-specific keys (delegated to `activeView().HandleKey`).
3. **Action results** — `deleteTopicMsg`, `purgeTopicMsg`,
   `setRetentionMsg`, etc. Each clears `loading`, sets a flash or
   error, and triggers a refresh of whichever view should reflect the
   change.
4. **Refresh messages from views** — `TopicsRefreshMsg`,
   `GroupsRefreshMsg`, `TopicCountsMsg`, `GroupEnrichmentMsg`,
   `ConsumerErrorMsg`, etc. These are grouped into a single case that
   delegates to the active view via `updateActiveView`.

The grouped refresh case has one cross-cutting behavior: if any refresh
message carries an error, the App calls `client.CloseIdleConnections()`.
That's how stale IAM tokens and DNS changes get picked up — we don't
proactively re-auth, we let connection churn do it.

## The 2-second tick

`tick()` returns `tea.Tick(pollInterval, ...)`. On each fire:

- If `loading` is false, call `refreshActiveView()` (which calls
  `Refresh()` on whatever view is active).
- Re-arm the tick.

Inactive views don't tick. This matters: a 2s `FetchTopicMessageCounts`
across a busy cluster is non-trivial; firing it from the Cluster view
would be wasted work.

The tick is paused during `splashActive` and `initialLoad` because the
Init batch already has a fetch in flight — overlapping refreshes during
startup produced confusing loading states.

## Splash + initial load

Startup sequence:

1. `Init` returns a batch: Topics view's `Init` (kicks off first fetch),
   spinner tick, splash timer (3s), version check.
2. While the splash is up, the Topics fetch runs in the background.
3. `splashDoneMsg` fires after 3s. App transitions to either:
   - The Topics view directly, if `--context` or `--brokers` was on the
     CLI (`skipContextView`), or
   - The Context selector view, otherwise.
4. The first `TopicsRefreshMsg` (or `TopicCountsMsg`) clears
   `initialLoad` and `loading`, ending the spinner.

The splash exists so cold-start latency (auth handshake + first metadata
fetch) doesn't render as an empty/error table. It's purely cosmetic; if
data arrives before the timer, it's still cached and ready when the
timer fires.

## Loading state

Three booleans, deliberately separate:

- `splashActive` — startup splash is showing.
- `initialLoad` — first data fetch hasn't completed.
- `loading` — a synchronous user-initiated action is in flight (delete,
  purge, reset offsets). Rendered as a different overlay than the
  splash spinner.

We considered collapsing them but each has a distinct UX: splash is a
welcome screen, initial-load is "we're connecting", loading is "your
action is running".

## Confirmations

Destructive actions go through a two-step pattern:

1. View's `HandleKey` returns e.g. `("confirm_delete_topic", "orders")`.
2. App stores `pendingConfirm{action, param}` and renders a y/n overlay.
3. Next `tea.KeyMsg` either dispatches the underlying action (which
   produces a `deleteTopicMsg` etc.) or clears the prompt.

Why a string discriminator instead of a `func()`? Serializing the
intended action through the App's state makes it trivially testable and
keeps the rendering of the prompt independent of which action is
pending. A closure would tangle render and dispatch.

## What's intentionally NOT here

- **No tea.Model per view returning models.** Views return `tea.Cmd`,
  not `(tea.Model, tea.Cmd)`. The App is the model. This avoids the
  recursive-model-type contortions that bubbletea sample apps fall into.
- **No event bus.** Cross-view effects (delete in TopicsView refreshes
  TopicsView; purge from TopicDetail refreshes TopicDetail) are wired
  explicitly in App's `Update`. There are ~20 of these and they're easy
  to read; an event bus would hide the wiring without removing it.
- **No background workers held by App.** Goroutines live inside the
  views that need them (Messages, Produce). `Stop()` is the contract.
- **No theming layer.** Colors come from `style/` constants. Adding
  user themes is plausible but not on the roadmap.

## Pickup notes

- **Adding a view:** create `internal/tui/views/<name>.go`, add a
  `ViewType` constant in `style/`, register in `App.viewMap` (either at
  construction for top-level views, or lazily on first transition).
  Decide between `switchView` (top-level tab) and `pushView` (drill-down).
- **Adding a refresh message type:** add it to the grouped case in
  `Update`. If it should clear `initialLoad`, branch in the
  `case a.initialLoad:` block alongside `TopicsRefreshMsg` and
  `TopicCountsMsg`.
- **Long-running view operations:** implement `Stoppable`. The dispatcher
  calls `Stop` on every transition; goroutines must respect their stop
  channel promptly or transitions feel laggy.
- **Don't add fields to `App` casually.** `fieldalignment` is enforced;
  the boolean cluster at the bottom of the struct is layout-sensitive.
