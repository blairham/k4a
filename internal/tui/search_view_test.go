// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package tui

import (
	"context"
	"testing"

	"github.com/blairham/k4a/internal/kafka"
	"github.com/blairham/k4a/internal/tui/style"
	"github.com/blairham/k4a/internal/tui/views"
)

// TestSearchEditRerunsInPlace: s on the results opens the bar on the current
// query, and submitting re-runs the same view rather than stacking another
// results view that esc would have to unwind.
func TestSearchEditRerunsInPlace(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	var patterns []string
	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.width, app.height = 120, 40
	app.searchFunc = func(_ context.Context, _ string, p kafka.SearchParams) (
		<-chan kafka.ConsumedMessage, <-chan kafka.DeepSearchProgress, <-chan error,
	) {
		patterns = append(patterns, p.Pattern.String())
		msgs, prog, errs := make(chan kafka.ConsumedMessage), make(chan kafka.DeepSearchProgress), make(chan error)
		close(msgs)
		close(prog)
		close(errs)
		return msgs, prog, errs
	}

	app.handleAction(actionSearch, "orders\x00first") //nolint:errcheck
	sv := typedView[*views.SearchView](app, style.ViewSearch)
	if sv == nil || app.view != style.ViewSearch {
		t.Fatal("no search view opened")
	}
	depth := len(app.viewStack)

	typeKeys(t, app, keyPress('s'))
	if !app.prompt.Active() {
		t.Fatal("s did not open the search bar")
	}
	if got := app.prompt.Value(); got != "first" {
		t.Errorf("search bar opened on %q, want the current query", got)
	}
	app.prompt.Close()

	if msg, _ := app.promptDispatch("  "); msg == "" {
		t.Error("an empty query was accepted")
	}
	app.promptDispatch("second") //nolint:errcheck
	if got := typedView[*views.SearchView](app, style.ViewSearch); got != sv || len(app.viewStack) != depth {
		t.Error("editing the query stacked a new results view")
	}
	if sv.Query() != "second" {
		t.Errorf("Query() = %q, want second", sv.Query())
	}

	typeKeys(t, app, keyPress('R'))
	want := []string{"(?i)first", "(?i)second", "(?i)second"}
	if len(patterns) != len(want) {
		t.Fatalf("searches run = %q, want %q", patterns, want)
	}
	for i := range want {
		if patterns[i] != want[i] {
			t.Errorf("search %d = %q, want %q", i, patterns[i], want[i])
		}
	}
}
