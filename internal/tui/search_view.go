// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package tui

import (
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
