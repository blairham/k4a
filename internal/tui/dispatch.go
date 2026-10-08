// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package tui

import (
	tea "charm.land/bubbletea/v2"
	"github.com/blairham/tuikit/viewfsm"

	"github.com/blairham/k4a/internal/tui/style"
	"github.com/blairham/k4a/internal/tui/views"
)

func (a *App) switchView(v style.ViewType) tea.Cmd {
	// Started with nothing selected, there is no client, and every data
	// view queries one. The picker is the only view that works in that
	// state — and the only one that ends it — so stay put and say why.
	// Keyed on the flag, not on a nil client: a nil client is also the
	// ordinary fixture in tests, where the returned cmd is never run.
	if a.awaitingContext && v != style.ViewContext {
		a.errFlash = "no context selected — pick one first"
		return nil
	}
	a.stopStoppableViews()
	a.viewStack = nil
	a.filterStack = nil
	a.filter = ""
	a.view = v
	a.showHelp = false
	a.closeAllBars()
	a.setActiveFilter("")
	a.resizeActiveView()

	if av := a.activeView(); av != nil {
		return av.Refresh()
	}
	return nil
}

// pushView saves v, and the filter applied to it, then starts the drilled-in
// view unfiltered. The filter belongs to the view it narrows: carried into a
// topic's messages it would make the first esc clear it instead of going back.
func (a *App) pushView(v style.ViewType) {
	a.viewStack = append(a.viewStack, v)
	a.filterStack = append(a.filterStack, a.filter)
	a.filter = ""
}

func (a *App) popView() {
	if len(a.viewStack) == 0 {
		return
	}
	// Don't call stopStoppableViews here — it would tear down the view
	// we're about to restore (e.g. Messages's tail consumer dies when
	// the user escapes back from a Search overlay), defeating the
	// purpose of having a view stack at all. Lifecycle teardown happens
	// on switchView (tab change) and at app shutdown.
	a.view = a.viewStack[len(a.viewStack)-1]
	a.viewStack = a.viewStack[:len(a.viewStack)-1]
	a.filter = a.filterStack[len(a.filterStack)-1]
	a.filterStack = a.filterStack[:len(a.filterStack)-1]
	a.closeAllBars()
	a.setActiveFilter(a.filter)
	a.resizeActiveView()
}

// closeAllBars closes every chrome bar widget. Called on view switch
// and pop to guarantee the next view doesn't inherit a stale bar.
func (a *App) closeAllBars() {
	if a.commandBar != nil {
		a.commandBar.Close()
	}
	if a.filterBar != nil {
		a.filterBar.Close()
	}
	if a.prompt != nil {
		a.prompt.Close()
	}
	if a.confirm != nil {
		a.confirm.Close()
	}
}

// stopStoppableViews stops any views that implement the Stoppable
// interface. The indexer is *not* stopped here — it's app-scoped per
// docs/design/local-index.md and persists across view navigation until
// app shutdown (see shutdown / stopAllIndexers).
func (a *App) stopStoppableViews() {
	for _, vt := range []style.ViewType{style.ViewMessages, style.ViewProduce} {
		if v, ok := a.viewMap[vt].(views.Stoppable); ok {
			v.Stop()
		}
	}
}

// shutdown is the single teardown path for app exit (ctrl-c, :quit,
// upgrade). Stops stoppable views and drains/closes every open
// indexer so writer locks are released and pending records flush
// before tea.Quit returns control to main.
func (a *App) shutdown() {
	a.stopStoppableViews()
	a.stopAllIndexers()
}

// tableHeight is the inner table/viewport height inside the bordered
// content frame, derived from chrome.ContentInnerSize so it stays in
// sync with the chrome layout reservations.
func (a *App) tableHeight() int {
	_, h := a.chrome.ContentInnerSize(
		a.width,
		a.height,
		a.filterBar != nil && a.filterBar.Active(),
		(a.commandBar != nil && a.commandBar.Active()) ||
			(a.prompt != nil && a.prompt.Active()),
		a.confirm != nil && a.confirm.Active(),
		a.errFlash != "" || a.flash != "",
	)
	return h
}

// innerWidth is the inner table/viewport width inside the bordered
// content frame. Delegates to chrome for the same reason as tableHeight.
func (a *App) innerWidth() int {
	w, _ := a.chrome.ContentInnerSize(
		a.width,
		a.height,
		a.filterBar != nil && a.filterBar.Active(),
		(a.commandBar != nil && a.commandBar.Active()) ||
			(a.prompt != nil && a.prompt.Active()),
		a.confirm != nil && a.confirm.Active(),
		a.errFlash != "" || a.flash != "",
	)
	return w
}

func (a *App) resizeActiveView() {
	if v := a.activeView(); v != nil {
		v.Resize(a.innerWidth(), a.tableHeight())
	}
}

func (a *App) refreshActiveView() tea.Cmd {
	if v := a.activeView(); v != nil {
		return v.Refresh()
	}
	return nil
}

func (a *App) updateActiveView(msg tea.Msg) tea.Cmd {
	if v := a.activeView(); v != nil {
		return v.Update(msg)
	}
	return nil
}

// updateActiveTable delegates key events to the bubbles table/viewport for navigation.
func (a *App) updateActiveTable(msg tea.Msg) tea.Cmd {
	v := a.activeView()
	if v == nil {
		return nil
	}

	// Form-based views (huh) handle keys themselves, not via table
	// navigation — and get them untranslated, so typing j or g into a
	// field types the letter.
	if fv, ok := v.(views.FormView); ok {
		if keyMsg, ok := msg.(tea.KeyMsg); ok {
			if cmd := fv.HandleKeyMsg(keyMsg); cmd != nil {
				return cmd
			}
		}
		return v.UpdateTable(msg)
	}

	// Translate vim/k9s navigation keys (j/k/h/l, g/G, ctrl+f/ctrl+b,
	// ctrl+d/ctrl+u) onto the arrow, home/end and page keys the bubbles
	// table and viewport bind. tuikit's table keymap leaves j/k unbound on
	// purpose, so without this they do nothing. View-specific keys were
	// already offered to the view by handleKey, so a view that binds one of
	// these letters still gets it first.
	if km, ok := msg.(tea.KeyMsg); ok {
		msg = viewfsm.TranslateNavKey(km)
	}
	return v.UpdateTable(msg)
}

func (a *App) activeViewHandleKey(key string) (string, string) {
	if v := a.activeView(); v != nil {
		return v.HandleKey(key)
	}
	return "", ""
}

//nolint:unparam // Cmd return reserved for views that queue follow-up work after a filter change
func (a *App) setActiveFilter(filter string) tea.Cmd {
	v := a.activeView()
	if v == nil {
		return nil
	}
	v.SetFilter(filter)
	// Some views queue follow-up work after a filter change (e.g. MessagesView
	// debounces the deep-search trigger). Drain that here so the Cmd reaches
	// the bubbletea loop.
	if pf, ok := v.(interface{ PendingFilterCmd() tea.Cmd }); ok {
		return pf.PendingFilterCmd()
	}
	return nil
}

// refreshMsgMatchesView returns true when the refresh message belongs to the
// currently active view.  Stale messages from a previous view (e.g. a
// ContextRefreshMsg arriving after we switched to Topics) must not clear the
// loading spinner.
func (a *App) refreshMsgMatchesView(msg tea.Msg) bool {
	switch msg.(type) {
	case views.TopicsRefreshMsg, views.TopicCountsMsg:
		return a.view == style.ViewTopics
	case views.TopicDetailRefreshMsg:
		return a.view == style.ViewTopicDetail
	case views.GroupsRefreshMsg, views.GroupEnrichmentMsg:
		return a.view == style.ViewGroups
	case views.GroupDetailRefreshMsg:
		return a.view == style.ViewGroupDetail
	case views.ConsumerErrorMsg:
		return a.view == style.ViewMessages
	case views.ClusterRefreshMsg:
		return a.view == style.ViewCluster
	case views.ContextRefreshMsg:
		return a.view == style.ViewContext
	case views.ACLsRefreshMsg:
		return a.view == style.ViewACLs
	case views.TopicConfigRefreshMsg:
		return a.view == style.ViewTopicConfig
	case views.ResetOffsetsTopicsMsg, views.ResetOffsetsResultMsg:
		return a.view == style.ViewResetOffsets
	case views.BrokerDetailRefreshMsg:
		return a.view == style.ViewBrokerDetail
	case views.ProduceProgressMsg, views.ProduceDoneMsg:
		return a.view == style.ViewProduce
	case views.CreateTopicResultMsg:
		return a.view == style.ViewCreateTopic
	case views.CreateACLResultMsg:
		return a.view == style.ViewCreateACL
	}
	return true
}

// refreshMsgHasError returns true if a data refresh message carries an error.
func refreshMsgHasError(msg tea.Msg) bool {
	switch m := msg.(type) {
	case views.TopicsRefreshMsg:
		return m.Err != nil
	case views.GroupsRefreshMsg:
		return m.Err != nil
	case views.ClusterRefreshMsg:
		return m.Err != nil
	case views.TopicDetailRefreshMsg:
		return m.Err != nil
	case views.ACLsRefreshMsg:
		return m.Err != nil
	case views.TopicConfigRefreshMsg:
		return m.Err != nil
	case views.BrokerDetailRefreshMsg:
		return m.Err != nil
	case views.GroupDetailRefreshMsg:
		return m.Err != nil
	case views.ConsumerErrorMsg:
		return m.Err != nil
	}
	return false
}
