// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package tui

import (
	"context"
	"strings"
	"testing"

	tea "charm.land/bubbletea/v2"

	"github.com/blairham/k4a/internal/kafka"
	"github.com/blairham/k4a/internal/tui/style"
	"github.com/blairham/k4a/internal/tui/views"
)

// heldSearch is a search the test finishes by hand. Every search started is
// recorded with its context and the means to end it.
type heldSearch struct {
	ctx   context.Context
	match chan kafka.ConsumedMessage
	prog  chan kafka.DeepSearchProgress
	errs  chan error
	topic string
}

func (h *heldSearch) finish() {
	h.prog <- kafka.DeepSearchProgress{Done: true, Matches: 0}
	close(h.errs)
	close(h.prog)
	close(h.match)
}

func holdSearches(app *App) *[]*heldSearch {
	var held []*heldSearch
	app.searchFunc = func(ctx context.Context, topic string, _ kafka.SearchParams) (
		<-chan kafka.ConsumedMessage, <-chan kafka.DeepSearchProgress, <-chan error,
	) {
		h := &heldSearch{
			ctx: ctx, topic: topic,
			match: make(chan kafka.ConsumedMessage), prog: make(chan kafka.DeepSearchProgress, 1),
			errs: make(chan error),
		}
		held = append(held, h)
		return h.match, h.prog, h.errs
	}
	return &held
}

// pump runs cmd and feeds what it returns back through the App until the
// chain ends, as bubbletea would.
func pump(app *App, cmd tea.Cmd) {
	for i := 0; cmd != nil && i < 1000; i++ {
		msg := cmd()
		if msg == nil {
			return
		}
		_, cmd = app.Update(msg)
	}
}

// TestSearchRunsInBackground: leaving the results does not stop the scan; the
// status bar reports it, :results brings it back, :search-stop ends it.
func TestSearchRunsInBackground(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.width, app.height = 160, 40
	held := holdSearches(app)

	app.handleAction(actionSearch, "orders\x00needle") //nolint:errcheck
	sv := typedView[*views.SearchView](app, style.ViewSearch)
	if sv == nil || app.view != style.ViewSearch {
		t.Fatal("no search view opened")
	}
	if s := app.backgroundSearchStatus(); s != "" {
		t.Errorf("status %q shown while looking at the results themselves", s)
	}

	typeKeys(t, app, escKey())
	if app.view == style.ViewSearch {
		t.Fatal("esc did not leave the results")
	}
	if (*held)[0].ctx.Err() != nil || !sv.Running() {
		t.Fatal("leaving the results stopped the scan")
	}
	status, _ := app.statusBar()
	if !strings.Contains(status, "search /needle in orders · running") || !strings.Contains(status, ":results") {
		t.Errorf("status bar = %q, want the background search's progress and how to get back", status)
	}

	if msg, _ := app.dispatchCommand("results"); msg != "" || app.view != style.ViewSearch {
		t.Fatalf(":results = %q, view %v; want the results back", msg, style.ViewName(app.view))
	}
	typeKeys(t, app, escKey())

	if msg, _ := app.dispatchCommand("search-stop"); msg != "" {
		t.Fatalf(":search-stop = %q", msg)
	}
	if (*held)[0].ctx.Err() == nil || sv.Running() {
		t.Error(":search-stop did not cancel the scan")
	}
	if !strings.Contains(app.flash, "stopped the background search /needle") {
		t.Errorf("flash = %q, want it to say what was stopped", app.flash)
	}
	if msg, _ := app.dispatchCommand("search-stop"); msg != "no search is running" {
		t.Errorf(":search-stop with nothing running = %q", msg)
	}
	if s := app.backgroundSearchStatus(); s != "" {
		t.Errorf("status %q still shown for a stopped search", s)
	}
}

// TestBackgroundSearchSaysWhenItEnds: a scan finishing behind another view
// flashes its result, so the user knows to come back.
func TestBackgroundSearchSaysWhenItEnds(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.width, app.height = 160, 40
	held := holdSearches(app)

	_, cmd := app.handleAction(actionSearch, "orders\x00needle")
	typeKeys(t, app, escKey())
	(*held)[0].finish()
	pump(app, cmd)

	if !strings.Contains(app.flash, "search /needle in orders · scanned · 0 matches") ||
		!strings.Contains(app.flash, ":results") {
		t.Errorf("flash = %q, want the finished search's result and how to see it", app.flash)
	}
}

// TestNewSearchReplacesRunningOne: one search at a time, and the one replaced
// is canceled rather than left scanning with nothing reading it.
func TestNewSearchReplacesRunningOne(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.width, app.height = 160, 40
	held := holdSearches(app)

	app.handleAction(actionSearch, "orders\x00first") //nolint:errcheck
	typeKeys(t, app, escKey())
	app.handleAction(actionSearch, "orders\x00second") //nolint:errcheck

	if len(*held) != 2 {
		t.Fatalf("%d searches started, want 2", len(*held))
	}
	if (*held)[0].ctx.Err() == nil {
		t.Error("the replaced search is still scanning")
	}
	if (*held)[1].ctx.Err() != nil {
		t.Error("the new search was canceled")
	}
}

// TestSearchCommandTakesTopicGlob: :search with a glob opens the bar for it,
// and the search it runs is a cross-topic one.
func TestSearchCommandTakesTopicGlob(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.width, app.height = 160, 40
	holdSearches(app)

	if msg, _ := app.dispatchCommand("search orders.*"); msg != "" || !app.prompt.Active() {
		t.Fatalf(":search orders.* = %q, bar open %v", msg, app.prompt.Active())
	}
	app.promptDispatch("needle") //nolint:errcheck
	sv := typedView[*views.SearchView](app, style.ViewSearch)
	if sv == nil || sv.Topic() != "orders.*" || !sv.Multi() {
		t.Fatalf("search view = %+v, want a cross-topic search of orders.*", sv)
	}
}
