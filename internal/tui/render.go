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
		Help:        a.withDecodeHelp(helpPanel(a.view)),
	}
	frame.StatusBar, frame.StatusBarLevel = a.statusBar()
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

// statusBar is the status row: an error or a flash when there is one, else
// the progress of a search running behind the current view.
func (a *App) statusBar() (string, chrome.StatusBarLevel) {
	switch {
	case a.errFlash != "":
		return a.errFlash, chrome.LevelError
	case a.flash != "":
		return a.flash, chrome.LevelInfo
	}
	return a.backgroundSearchStatus(), chrome.LevelInfo
}

// hasStatusBar reports whether the status row is shown, which the content
// height has to leave room for.
func (a *App) hasStatusBar() bool {
	s, _ := a.statusBar()
	return s != ""
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
	return a.chrome.ShortcutGrid(tabShortcuts(a.view), a.withDecodeShortcut(actionShortcuts(a.view)))
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
		a.hasStatusBar(),
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
