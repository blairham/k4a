// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package tui

import (
	"fmt"
	"sort"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	"github.com/blairham/tuikit/chrome"

	"github.com/blairham/k4a/internal/tui/style"
	"github.com/blairham/k4a/internal/tui/views"
)

func (a *App) tick() tea.Cmd {
	return tea.Tick(pollInterval, func(t time.Time) tea.Msg {
		return tickMsg(t)
	})
}

// View renders the full app layout. The chrome (top section, bars,
// help overlay, status bar, breadcrumb footer, screen background) is
// owned by tuikit/chrome — we just populate a Frame.
func (a *App) View() tea.View {
	if a.width == 0 || a.splashActive {
		v := tea.NewView(a.renderSplash())
		v.AltScreen = true
		return v
	}

	frame := chrome.Frame{
		Width:       a.width,
		Height:      a.height,
		InfoLines:   a.renderInfoPanel(),
		Shortcuts:   a.renderShortcuts(),
		Content:     a.renderContent(),
		Breadcrumb:  a.breadcrumb(),
		HelpVisible: a.showHelp,
		Help:        a.helpPanel(),
	}
	switch {
	case a.errFlash != "":
		frame.StatusBar = a.errFlash
		frame.StatusBarLevel = chrome.LevelError
	case a.flash != "":
		frame.StatusBar = a.flash
		frame.StatusBarLevel = chrome.LevelInfo
	}
	switch {
	case a.confirm.Active():
		frame.Confirm = a.confirm.Prompt()
	case a.prompt.Active():
		frame.Command = a.prompt.Input()
	case a.filterBar.Active():
		frame.Filter = a.filterBar.Input()
	case a.commandBar.Active():
		frame.Command = a.commandBar.Input()
	}

	v := tea.NewView(a.chrome.Render(frame))
	v.AltScreen = true
	return v
}

func (a *App) renderSplash() string {
	logo := style.Logo

	// Build the splash content lines.
	lines := make([]string, 0, 1+len(logoBigLines)+3)
	lines = append(lines, "")
	for _, l := range logoBigLines {
		lines = append(lines, logo.Render(l))
	}
	// a.loader.View() renders the spinner beside its current tip; index 0
	// ("Connecting...") shows here until the first rotation.
	lines = append(lines, "", a.loader.View(), "")

	block := strings.Join(lines, "\n")

	// Center the block on screen.
	w := a.width
	if w == 0 {
		w = 80
	}
	h := a.height
	if h == 0 {
		h = 24
	}

	centered := lipgloss.NewStyle().
		Width(w).
		Height(h).
		Align(lipgloss.Center, lipgloss.Center).
		Background(style.ColorBg).
		Foreground(style.ColorSteelBlue).
		Render(block)

	return centered
}

func (a *App) renderInfoPanel() []string {
	label := style.InfoLabel
	value := style.InfoValue

	ctx := a.connInfo.Context
	if ctx == "" {
		ctx = "direct"
	}

	brokers := a.connInfo.Brokers
	if len(brokers) > 31 {
		brokers = brokers[:28] + "..."
	}

	ctxLine := label.Render("Context:  ") + value.Render(ctx)
	if a.readonly {
		ctxLine += " " + lipgloss.NewStyle().Foreground(style.ColorOrange).Bold(true).Render("[RO]")
	}

	return []string{
		ctxLine,
		label.Render("Auth:     ") + value.Render(a.connInfo.Auth),
		label.Render("Region:   ") + value.Render(a.connInfo.Region),
		label.Render("Brokers:  ") + value.Render(brokers),
		a.chrome.VersionLine("K4a Rev:", a.version, a.latestVersion),
	}
}

// renderShortcuts returns the shortcut grid for the active view,
// k9s-style: view-switch hotkeys (<1>..<4>) stacked in the leftmost
// column, per-view actions plus always-on actions stacked in the
// second column. Layout is owned by [chrome.Chrome.ShortcutGrid].
func (a *App) renderShortcuts() []string {
	viewKeys := []chrome.Shortcut{
		{Key: "<1>", Desc: "Topics"},
		{Key: "<2>", Desc: "Groups"},
		{Key: "<3>", Desc: "Cluster"},
		{Key: "<4>", Desc: "ACLs"},
	}

	var actions []chrome.Shortcut //nolint:prealloc // each case below assigns a fresh literal
	switch a.view {
	case style.ViewTopics:
		actions = []chrome.Shortcut{
			{Key: "<enter>", Desc: "Messages"},
			{Key: "<o>", Desc: "Overview"},
			{Key: "<p>", Desc: "Produce"},
			{Key: "<ctrl-d>", Desc: "Delete"},
		}
	case style.ViewTopicDetail:
		actions = []chrome.Shortcut{
			{Key: "<m>", Desc: "Messages"},
			{Key: "<e>", Desc: "Config"},
			{Key: "<p>", Desc: "Partitions"},
			{Key: "<R>", Desc: "Replicas"},
		}
	case style.ViewTopicConfig:
		actions = []chrome.Shortcut{
			{Key: "<enter>", Desc: "Edit"},
			{Key: "<d>", Desc: "Defaults"},
			{Key: "</>", Desc: "Filter"},
			{Key: "<esc>", Desc: "Back"},
		}
	case style.ViewGroups:
		actions = []chrome.Shortcut{
			{Key: "<enter>", Desc: "Detail"},
			{Key: "<ctrl-d>", Desc: "Delete"},
			{Key: "</>", Desc: "Filter"},
			{Key: "<?>", Desc: "Help"},
		}
	case style.ViewMessages:
		actions = []chrome.Shortcut{
			{Key: "<enter>", Desc: "View"},
			{Key: "<s>", Desc: "Search"},
			{Key: "<ctrl-d>", Desc: "Purge"},
			{Key: "<f>", Desc: "Follow"},
		}
	case style.ViewMessageDetail:
		actions = []chrome.Shortcut{
			{Key: "<s>", Desc: "Save"},
			{Key: "<ctrl-f>", Desc: "PgDn"},
			{Key: "<ctrl-b>", Desc: "PgUp"},
			{Key: "<esc>", Desc: "Back"},
		}
	case style.ViewContext:
		actions = []chrome.Shortcut{
			{Key: "<enter>", Desc: "Switch"},
			{Key: "<esc>", Desc: "Back"},
			{Key: "<ctrl-f>", Desc: "PgDn"},
			{Key: "<ctrl-b>", Desc: "PgUp"},
		}
	case style.ViewACLs:
		actions = []chrome.Shortcut{
			{Key: "<a>", Desc: "Create"},
			{Key: "<ctrl-d>", Desc: "Delete"},
			{Key: "</>", Desc: "Filter"},
			{Key: "<?>", Desc: "Help"},
		}
	case style.ViewGroupDetail:
		actions = []chrome.Shortcut{
			{Key: "<R>", Desc: "Reset"},
			{Key: "<:reset>", Desc: "Delete"},
			{Key: "</>", Desc: "Filter"},
			{Key: "<esc>", Desc: "Back"},
		}
	case style.ViewBrokerDetail:
		actions = []chrome.Shortcut{
			{Key: "<d>", Desc: "Defaults"},
			{Key: "<esc>", Desc: "Back"},
			{Key: "</>", Desc: "Filter"},
			{Key: "<?>", Desc: "Help"},
		}
	case style.ViewProduce, style.ViewCreateTopic, style.ViewCreateACL, style.ViewResetOffsets:
		actions = []chrome.Shortcut{
			{Key: "<tab>", Desc: "Next"},
			{Key: "<enter>", Desc: "Submit"},
			{Key: "<esc>", Desc: "Back"},
			{Key: "<?>", Desc: "Help"},
		}
	default:
		actions = []chrome.Shortcut{
			{Key: "<enter>", Desc: "Select"},
			{Key: "<esc>", Desc: "Back"},
			{Key: "</>", Desc: "Filter"},
			{Key: "<?>", Desc: "Help"},
		}
	}

	// Always-on actions land in the action column alongside the
	// per-view set; sortShortcuts then orders the merged set k9s-style
	// (digit keys first, rest alphabetical by description).
	actions = append(
		actions,
		chrome.Shortcut{Key: "<r>", Desc: "Refresh"},
		chrome.Shortcut{Key: "<:q>", Desc: "Quit"},
	)
	sortShortcuts(actions)

	return a.chrome.ShortcutGrid(viewKeys, actions)
}

// renderContent assembles the bordered content area (table or help)
// for [chrome.Frame.Content]. The help overlay path is owned by
// chrome via Frame.HelpVisible+Help — when help is on we return ""
// so chrome.Render swaps in its own help overlay.
func (a *App) renderContent() string {
	if a.showHelp {
		return "" // chrome.Render uses Frame.Help instead
	}

	_, innerH := a.chrome.ContentInnerSize(
		a.width,
		a.height,
		a.filterBar.Active(),
		a.commandBar.Active() || a.prompt.Active(),
		a.confirm.Active(),
		a.errFlash != "" || a.flash != "",
	)
	box := a.chrome.BorderedContent(a.renderActiveView(), a.width, innerH)
	return chrome.InjectBorderTitle(box, a.renderResourceTitle(), a.chrome.Theme)
}

// breadcrumb converts the view stack into chrome footer crumbs. The
// last crumb (the active view) is marked Leaf so chrome can color it
// distinctively.
func (a *App) breadcrumb() []chrome.Crumb {
	crumbs := make([]chrome.Crumb, 0, len(a.viewStack)+1)
	for _, v := range a.viewStack {
		crumbs = append(crumbs, chrome.Crumb{Label: style.ViewName(v)})
	}
	crumbs = append(crumbs, chrome.Crumb{Label: style.ViewName(a.view), Leaf: true})
	return crumbs
}

// helpPanel returns the help overlay content for the chrome to render
// when [chrome.Frame.HelpVisible] is true. The panel is static across
// views — k9s and tuikit consumers do the same.
//
// Entries omit multi-character `:command` aliases (e.g. `:produce`,
// `:ct`, `:reset`) — those are reachable through the `:` palette and
// don't belong in the keystroke reference. Each section is sorted via
// sortHelpEntries: view-switch digit keys first in numerical order,
// then everything else alphabetical by description (k9s convention).
func (a *App) helpPanel() chrome.HelpPanel {
	panel := chrome.HelpPanel{
		Sections: []chrome.HelpSection{
			{
				Title: "RESOURCE",
				Entries: []chrome.HelpEntry{
					{Key: "<1>", Desc: "Topics"},
					{Key: "<2>", Desc: "Groups"},
					{Key: "<3>", Desc: "Cluster"},
					{Key: "<4>", Desc: "ACLs"},
					{Key: "<enter>", Desc: "Messages"},
					{Key: "<o>", Desc: "Topic Overview"},
					{Key: "<e>", Desc: "Topic Config"},
					{Key: "<p>", Desc: "Produce"},
					{Key: "<c>", Desc: "Create Topic"},
					{Key: "<a>", Desc: "Create ACL"},
					{Key: "<i>", Desc: "Toggle Internal"},
					{Key: "<p>", Desc: "Add Partitions"},
					{Key: "<s>", Desc: "Save Message"},
					{Key: "<ctrl-d>", Desc: "Delete"},
					{Key: "<ctrl-d>", Desc: "Purge Topic"},
					{Key: "<R>", Desc: "Reset Offsets"},
				},
			},
			{
				Title: "GENERAL",
				Entries: []chrome.HelpEntry{
					{Key: "<:cmd>", Desc: "Command mode"},
					{Key: "</>", Desc: "Filter"},
					{Key: "<esc>", Desc: "Back/Close"},
					{Key: "<r>", Desc: "Refresh"},
					{Key: "<?>", Desc: "Help"},
				},
			},
			{
				// tuikit's chrome.NavigationHelp wording, minus the
				// [ ] - history keys k4a does not bind. ctrl-f/ctrl-b,
				// not ctrl-d/ctrl-u: ctrl-d is Delete/Purge here.
				Title: "NAVIGATION",
				Entries: []chrome.HelpEntry{
					{Key: "<j>", Desc: "Down"},
					{Key: "<k>", Desc: "Up"},
					{Key: "<g>", Desc: "Goto Top"},
					{Key: "<shift-g>", Desc: "Goto Bottom"},
					{Key: "<ctrl-f>", Desc: "Page Down"},
					{Key: "<ctrl-b>", Desc: "Page Up"},
					{Key: "<f>", Desc: "Toggle Follow"},
				},
			},
		},
	}
	for i := range panel.Sections {
		sortHelpEntries(panel.Sections[i].Entries)
	}
	return panel
}

// isDigitKey reports whether key is a k9s-style view-switch hotkey of
// the form "<N>" where N is one or more decimal digits. Mirrors
// tuikit's chrome.isViewKey (unexported there).
func isDigitKey(key string) bool {
	if len(key) < 3 || key[0] != '<' || key[len(key)-1] != '>' {
		return false
	}
	for i := 1; i < len(key)-1; i++ {
		if key[i] < '0' || key[i] > '9' {
			return false
		}
	}
	return true
}

// sortByDigitThenDesc compares two (key, desc) pairs k9s-style:
// digit-keyed entries sort before non-digit, and within each group
// the sort key is description (alphabetical, case-insensitive) — or
// numeric key value when both are digit-keyed.
func sortByDigitThenDesc(ki, di, kj, dj string) bool {
	ai, aj := isDigitKey(ki), isDigitKey(kj)
	if ai != aj {
		return ai // digit first
	}
	if ai {
		// Both digit — sort by numeric value of the inner digits.
		return digitKeyValue(ki) < digitKeyValue(kj)
	}
	return strings.ToLower(di) < strings.ToLower(dj)
}

// digitKeyValue returns the integer value of a "<N>"-shaped key.
// Returns 0 if key isn't digit-shaped (caller should pre-check).
func digitKeyValue(key string) int {
	n := 0
	for i := 1; i < len(key)-1; i++ {
		n = n*10 + int(key[i]-'0')
	}
	return n
}

func sortShortcuts(s []chrome.Shortcut) {
	sort.SliceStable(s, func(i, j int) bool {
		return sortByDigitThenDesc(s[i].Key, s[i].Desc, s[j].Key, s[j].Desc)
	})
}

func sortHelpEntries(s []chrome.HelpEntry) {
	sort.SliceStable(s, func(i, j int) bool {
		return sortByDigitThenDesc(s[i].Key, s[i].Desc, s[j].Key, s[j].Desc)
	})
}

// activeFilter returns the filter string currently driving the view —
// either the persistent app filter or whatever's typed into the
// filter bar in real time.
func (a *App) activeFilter() string {
	if a.filterBar.Active() {
		return a.filterBar.Value()
	}
	return a.filter
}

func (a *App) renderResourceTitle() string {
	resource := style.ViewResource(a.view)
	count := a.activeViewCount()
	filter := "all"
	if f := a.activeFilter(); f != "" {
		filter = f
	}

	// Colorize each segment of the resource title.
	nameStyle := lipgloss.NewStyle().Foreground(style.ColorAqua).Bold(true)
	filterStyle := lipgloss.NewStyle().Foreground(style.ColorFuchsia).Bold(true)
	countStyle := lipgloss.NewStyle().Foreground(style.ColorPapayaWhip).Bold(true)

	var name string
	switch a.view {
	case style.ViewTopicDetail:
		if v := typedView[*views.TopicDetailView](a, style.ViewTopicDetail); v != nil {
			name = v.Topic()
		}
	case style.ViewTopicConfig:
		if v := typedView[*views.TopicConfigView](a, style.ViewTopicConfig); v != nil {
			name = v.Topic() + " " + resource
		}
	case style.ViewMessages:
		if v := typedView[*views.MessagesView](a, style.ViewMessages); v != nil {
			name = resource + "s"
			if p := v.PartitionFilter(); p >= 0 {
				name += fmt.Sprintf(" partition:%d", p)
			}
		}
	case style.ViewSearch:
		name = "matches"
		if v := typedView[*views.SearchView](a, style.ViewSearch); v != nil {
			if q := v.Query(); q != "" {
				name = fmt.Sprintf("matches /%s in %s", q, v.Topic())
			}
		}
	default:
		name = resource + "s"
	}

	title := nameStyle.Render(name) +
		nameStyle.Render("(") + filterStyle.Render(filter) + nameStyle.Render(")") +
		nameStyle.Render("[") + countStyle.Render(fmt.Sprintf("%d", count)) + nameStyle.Render("]")

	// Append a highlighted filter indicator when actively filtering (like k9s).
	if f := a.activeFilter(); f != "" {
		filterTag := lipgloss.NewStyle().
			Background(style.ColorSeaGreen).
			Foreground(style.ColorBg).
			Bold(true).
			Padding(0, 1).
			Render("</" + f + ">")
		title += " " + filterTag
	}

	return title
}

func (a *App) activeViewCount() int {
	if v := a.activeView(); v != nil {
		return v.Count()
	}
	return 0
}

func (a *App) activeViewLoading() bool {
	if a.loading {
		return true
	}
	if v := a.activeView(); v != nil {
		return v.Loading()
	}
	return false
}

func (a *App) renderLoadingContent() string {
	return a.loader.Centered(a.innerWidth(), a.tableHeight())
}

func (a *App) renderActiveView() string {
	if a.activeViewLoading() {
		return a.renderLoadingContent()
	}
	if v := a.activeView(); v != nil {
		return v.View()
	}
	return ""
}
