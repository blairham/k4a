// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package tui

import (
	"slices"

	"github.com/blairham/tuikit/chrome"

	"github.com/blairham/k4a/internal/tui/style"
)

// binding is one key a view acts on. Every binding is listed in that view's
// help overlay; the hint ones also fill its hint bar, which has room for
// only a few.
type binding struct {
	key, desc string
	hint      bool
}

// Shortcut keys and descriptions that recur across views.
const (
	keyEnter = "<enter>"
	keyEsc   = "<esc>"
	keySlash = "</>"
	keyHelp  = "<?>"
	keyCtrlD = "<ctrl-d>"
	keyCtrlF = "<ctrl-f>"
	keyCtrlB = "<ctrl-b>"
	keyP     = "<p>"
	keyS     = "<s>"
	keyR     = "<R>"
	keyD     = "<d>"
	keyO     = "<o>"
	keyA     = "<a>"
	keyE     = "<e>"
	keyC     = "<c>"

	descBack     = "Back"
	descFilter   = "Filter"
	descHelp     = "Help"
	descDelete   = "Delete"
	descMessages = "Messages"
	descOverview = "Overview"
	descDefaults = "Defaults"
	descPurge    = "Purge"
	descExport   = "Export"
)

// copyBindings copy the selected or displayed message's value, key, or the
// whole record as JSON, as shown (decoded or raw).
var copyBindings = []binding{
	{keyC, "Copy Value", false},
	{"<K>", "Copy Key", false},
	{"<C>", "Copy JSON", false},
}

// viewBindings is each view's own keys. A key here must be handled by the
// view (TestBindingsAreHandled), and a key the view handles must be here
// (TestHandledKeysAreBound), so the hint bar and help cannot drift from the
// code again.
var viewBindings = map[style.ViewType][]binding{
	style.ViewTopics: {
		{keyEnter, descMessages, true},
		{keyO, descOverview, true},
		{keyP, "Produce", true},
		{"<c>", "Create", true},
		{"<i>", "Internal", true},
		{keyCtrlD, descDelete, true},
	},
	style.ViewTopicDetail: {
		{"<m>", descMessages, true},
		{"<e>", "Config", true},
		{keyP, "Partitions", true},
		{keyR, "Replicas", true},
		{keyCtrlD, descPurge, true},
		{keyA, "Reassign", true},
		{keyEsc, descBack, false},
	},
	style.ViewTopicConfig: {
		{keyEnter, "Edit", true},
		{keyD, descDefaults, true},
		{keyEsc, descBack, true},
	},
	style.ViewGroups: {
		{keyEnter, "Detail", true},
		{keyCtrlD, descDelete, true},
	},
	style.ViewGroupDetail: {
		{keyR, "Reset", true},
		{"<:reset>", descDelete, true},
		{keyEsc, descBack, true},
	},
	style.ViewCluster: {
		{keyEnter, "Detail", true},
		{keyEsc, descBack, false},
	},
	style.ViewBrokerDetail: {
		{keyEnter, "Edit", true},
		{keyD, descDefaults, true},
		{keyEsc, descBack, true},
	},
	style.ViewMessages: {
		{keyEnter, "View", true},
		{keyS, "Search", true},
		{keyO, descOverview, true},
		{"<f>", "Follow", true},
		{keyCtrlD, descPurge, true},
		{"<g>", "Newest", false},
		{keyP, "Next Partition", false},
		{"<P>", "Prev Partition", false},
		{keyA, "All Partitions", false},
		{"<h>", "Scroll Left", false},
		{"<l>", "Scroll Right", false},
		{keyE, descExport, false},
		copyBindings[0], copyBindings[1], copyBindings[2],
		{keyEsc, descBack, false},
	},
	style.ViewMessageDetail: {
		{keyS, "Save", true},
		{keyC, "Copy Value", true},
		copyBindings[1], copyBindings[2],
		{"<n>", "Next", true},
		{"<N>", "Previous", false},
		{keyCtrlF, "PgDn", true},
		{keyCtrlB, "PgUp", true},
		{keyEsc, descBack, true},
	},
	style.ViewContext: {
		{keyEnter, "Switch", true},
		{keyEsc, descBack, true},
	},
	style.ViewACLs: {
		{keyA, "Create", true},
		{keyE, "Edit", true},
		{keyCtrlD, descDelete, true},
	},
	style.ViewQuotas: {
		{keyEnter, "Edit", true},
		{keyA, "Add", true},
		{keyCtrlD, "Remove", true},
	},
	style.ViewSearch: {
		{keyEnter, "View", true},
		{keyS, "Edit Query", true},
		{keyR, "Re-run", true},
		{keyE, descExport, true},
		{keyEsc, descBack, false},
	},
	style.ViewProduce:      formBindings,
	style.ViewCreateTopic:  formBindings,
	style.ViewCreateACL:    formBindings,
	style.ViewResetOffsets: formBindings,
}

// formBindings serves every form view. While a form is being filled in it
// owns every key but esc, so the global shortcuts are not offered there.
var formBindings = []binding{
	{"<tab>", "Next", true},
	{"<shift-tab>", "Previous", false},
	{keyEnter, "Submit", true},
	{keyEsc, descBack, true},
}

// formViews are the views whose keys go to a form rather than to the
// global shortcuts.
var formViews = map[style.ViewType]bool{
	style.ViewProduce:      true,
	style.ViewCreateTopic:  true,
	style.ViewCreateACL:    true,
	style.ViewResetOffsets: true,
}

// noRefreshViews do not advertise <r>: their Refresh is a no-op (a live
// tail, a one-shot scan, a single message), or, in a form, r is typed
// into the field.
var noRefreshViews = map[style.ViewType]bool{
	style.ViewMessages:      true,
	style.ViewMessageDetail: true,
	style.ViewSearch:        true,
	style.ViewProduce:       true,
	style.ViewCreateTopic:   true,
	style.ViewCreateACL:     true,
	style.ViewResetOffsets:  true,
}

// filterViews are the views whose SetFilter filters something.
var filterViews = map[style.ViewType]bool{
	style.ViewTopics:       true,
	style.ViewTopicConfig:  true,
	style.ViewGroups:       true,
	style.ViewMessages:     true,
	style.ViewACLs:         true,
	style.ViewBrokerDetail: true,
	style.ViewSearch:       true,
	style.ViewQuotas:       true,
}

// tabKeys are the view-switch hotkeys, valid everywhere but in a form.
var tabKeys = []chrome.Shortcut{
	{Key: "<1>", Desc: "Topics"},
	{Key: "<2>", Desc: "Groups"},
	{Key: "<3>", Desc: "Cluster"},
	{Key: "<4>", Desc: "ACLs"},
}

// tabShortcuts is the hint bar's view-switch column for view v.
func tabShortcuts(v style.ViewType) []chrome.Shortcut {
	if formViews[v] {
		return nil
	}
	return tabKeys
}

// actionShortcuts is the action column for view v: its hint bindings plus
// the always-on actions it supports, ordered k9s-style by sortShortcuts
// (digit keys first, rest alphabetical by description).
func actionShortcuts(v style.ViewType) []chrome.Shortcut {
	var actions []chrome.Shortcut
	for _, b := range viewBindings[v] {
		if b.hint {
			actions = append(actions, chrome.Shortcut{Key: b.key, Desc: b.desc})
		}
	}
	if filterViews[v] {
		actions = append(actions, chrome.Shortcut{Key: keySlash, Desc: descFilter})
	}
	if !noRefreshViews[v] {
		actions = append(actions, chrome.Shortcut{Key: "<r>", Desc: "Refresh"})
	}
	if !formViews[v] {
		actions = append(actions,
			chrome.Shortcut{Key: keyHelp, Desc: descHelp},
			chrome.Shortcut{Key: "<:q>", Desc: "Quit"},
		)
	}
	sortShortcuts(actions)
	return actions
}

// helpPanel is the help overlay for view v: only the keys that work there.
// Multi-character `:command` aliases are left to the `:` palette, except a
// command that is the only way to reach a view's action (`:reset`). Each
// section is sorted by sortHelpEntries: view-switch digit keys first in
// numerical order, then everything else alphabetical by description (k9s
// convention).
func helpPanel(v style.ViewType) chrome.HelpPanel {
	resource := make([]chrome.HelpEntry, 0, len(tabKeys)+len(viewBindings[v]))
	if !formViews[v] {
		for _, s := range tabKeys {
			resource = append(resource, chrome.HelpEntry(s))
		}
	}
	for _, b := range viewBindings[v] {
		if b.key == keyEsc {
			continue // GENERAL lists it
		}
		resource = append(resource, chrome.HelpEntry{Key: b.key, Desc: b.desc})
	}

	general := []chrome.HelpEntry{{Key: keyEsc, Desc: "Back/Close"}}
	if !formViews[v] {
		general = append(general,
			chrome.HelpEntry{Key: "<:cmd>", Desc: "Command mode"},
			chrome.HelpEntry{Key: keyHelp, Desc: descHelp},
		)
	}
	if filterViews[v] {
		general = append(general, chrome.HelpEntry{Key: keySlash, Desc: descFilter})
	}
	if !noRefreshViews[v] {
		general = append(general, chrome.HelpEntry{Key: "<r>", Desc: "Refresh"})
	}

	panel := chrome.HelpPanel{Sections: []chrome.HelpSection{
		{Title: "RESOURCE", Entries: resource},
		{Title: "GENERAL", Entries: general},
	}}
	if nav := navigationHelp(v); len(nav) > 0 {
		panel.Sections = append(panel.Sections, chrome.HelpSection{Title: "NAVIGATION", Entries: nav})
	}
	for i := range panel.Sections {
		sortHelpEntries(panel.Sections[i].Entries)
	}
	return panel
}

// navigationHelp is tuikit's chrome.NavigationHelp wording, minus the
// [ ] - history keys k4a does not bind and any key view v binds itself.
// ctrl-f/ctrl-b, not ctrl-d/ctrl-u: ctrl-d is Delete/Purge in several views.
func navigationHelp(v style.ViewType) []chrome.HelpEntry {
	if formViews[v] {
		return nil
	}
	nav := []chrome.HelpEntry{
		{Key: "<j>", Desc: "Down"},
		{Key: "<k>", Desc: "Up"},
		{Key: "<g>", Desc: "Goto Top"},
		{Key: "<shift-g>", Desc: "Goto Bottom"},
		{Key: keyCtrlF, Desc: "Page Down"},
		{Key: keyCtrlB, Desc: "Page Up"},
	}
	return slices.DeleteFunc(nav, func(e chrome.HelpEntry) bool {
		return slices.ContainsFunc(viewBindings[v], func(b binding) bool { return b.key == e.Key })
	})
}
