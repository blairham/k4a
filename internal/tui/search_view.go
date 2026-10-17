// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package tui

import (
	"context"
	"errors"
	"strings"

	tea "charm.land/bubbletea/v2"
	"github.com/blairham/tuikit/chrome"

	"github.com/blairham/k4a/internal/tui/style"
	"github.com/blairham/k4a/internal/tui/views"
)

// rerunSearch runs the Search view's query again in place, so new records
// are picked up without leaving the results.
func (a *App) rerunSearch() tea.Cmd {
	v := typedView[*views.SearchView](a, style.ViewSearch)
	if v == nil {
		return nil
	}
	return v.Rerun()
}

// editSearch opens the search bar on the current query and re-runs the
// Search view in place with what is submitted, rather than stacking a new
// results view on top of this one.
func (a *App) editSearch() tea.Cmd {
	v := typedView[*views.SearchView](a, style.ViewSearch)
	if v == nil {
		return nil
	}
	a.promptDispatch = func(input string) (string, tea.Cmd) {
		q := strings.TrimSpace(input)
		if q == "" {
			return "search query is empty", nil
		}
		if a.session != nil {
			_ = a.session.AddRecentSearch(q) //nolint:errcheck // best-effort session persistence
		}
		v.SetQuery(q)
		return "", v.Rerun()
	}
	cmd := a.prompt.Open(v.Query(), chrome.OpenOpts{Prompt: "search>"})
	a.resizeActiveView()
	return cmd
}

// stopSearch cancels the search in flight, keeping what it found, and
// reports whether one was running.
func (a *App) stopSearch() bool {
	v := typedView[*views.SearchView](a, style.ViewSearch)
	if v == nil || !v.Cancel() {
		return false
	}
	if a.view != style.ViewSearch {
		a.flash = "stopped the background " + v.StatusLine()
	}
	return true
}

// showSearch brings the last search's results back from wherever the user
// went, running or finished. esc returns to where they were.
func (a *App) showSearch() string {
	if typedView[*views.SearchView](a, style.ViewSearch) == nil {
		return "no search to show: start one with :search"
	}
	if a.view == style.ViewSearch {
		return ""
	}
	a.pushView(a.view)
	a.view = style.ViewSearch
	a.closeAllBars()
	a.resizeActiveView()
	return ""
}

// resolveTopics lists the topics a glob selects, for a cross-topic search.
func (a *App) resolveTopics(ctx context.Context, glob string) ([]string, error) {
	if a.client == nil {
		return nil, errors.New("not connected to a cluster")
	}
	return a.client.ResolveTopics(ctx, glob)
}

// updateSearch routes a scan message to the Search view wherever the user
// is: a search keeps running in the background while they look elsewhere,
// and says so when it ends.
func (a *App) updateSearch(msg tea.Msg) tea.Cmd {
	v := typedView[*views.SearchView](a, style.ViewSearch)
	if v == nil {
		return nil
	}
	wasRunning := v.Running()
	cmd := v.Update(msg)
	if wasRunning && !v.Running() && a.view != style.ViewSearch {
		a.flash = v.StatusLine() + " — :results to view"
		a.resizeActiveView()
	}
	return cmd
}

// backgroundSearchStatus is the status-bar line for a search running behind
// another view, or "" when there is none to report.
func (a *App) backgroundSearchStatus() string {
	if a.view == style.ViewSearch {
		return ""
	}
	v := typedView[*views.SearchView](a, style.ViewSearch)
	if v == nil || !v.Running() {
		return ""
	}
	return v.StatusLine() + " — :results to view, :search-stop to stop"
}
