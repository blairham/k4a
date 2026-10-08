// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package views

import (
	"fmt"
	"sort"

	"charm.land/bubbles/v2/table"
	tea "charm.land/bubbletea/v2"

	"github.com/blairham/k4a/internal/config"
	"github.com/blairham/k4a/internal/tui/style"
)

// ContextRefreshMsg carries the context list.
type ContextRefreshMsg struct {
	Contexts []contextEntry
}

type contextEntry struct {
	Name    string
	Auth    string
	Brokers string
	Current bool
}

// ContextView shows configured contexts for selection.
type ContextView struct {
	cfg     *config.Config
	current string
	entries []contextEntry
	table   table.Model
}

// NewContextView creates a new context view.
func NewContextView(cfg *config.Config, currentContext string) *ContextView {
	cols := []table.Column{
		{Title: "", Width: 2},
		{Title: "CONTEXT", Width: 20},
		{Title: "AUTH", Width: 10},
		{Title: "BROKERS", Width: 60},
	}

	t := table.New(
		table.WithColumns(cols),
		table.WithFocused(true),
		table.WithStyles(tableStyles()),
		table.WithKeyMap(tableKeyMap()),
	)

	return &ContextView{cfg: cfg, current: currentContext, table: t}
}

// Init returns the initial command.
func (v *ContextView) Init() tea.Cmd { return v.refresh() }

// Update handles messages.
func (v *ContextView) Update(msg tea.Msg) tea.Cmd {
	if msg, ok := msg.(ContextRefreshMsg); ok {
		v.entries = msg.Contexts
		v.rebuildRows()
	}
	return nil
}

// UpdateTable delegates to the bubbles table.
func (v *ContextView) UpdateTable(msg tea.Msg) tea.Cmd {
	var cmd tea.Cmd
	v.table, cmd = v.table.Update(msg)
	return cmd
}

// Resize updates table dimensions.
func (v *ContextView) Resize(width, height int) {
	v.table.SetWidth(width)
	v.table.SetHeight(height)
	v.table.SetStyles(tableStylesWithWidth(width))
}

// Count returns the visible row count.
func (v *ContextView) Count() int { return len(v.table.Rows()) }

// HandleKey processes keys — enter selects a context.
func (v *ContextView) HandleKey(key string) (string, string) {
	if key == KeyEnter {
		row := v.table.SelectedRow()
		if row != nil {
			return "switch_context", row[1] // column 1 is the context name
		}
	}
	return "", ""
}

// SetFilter is a no-op for context view.
func (v *ContextView) SetFilter(_ string) {}

// View renders the view.
func (v *ContextView) View() string {
	header := fmt.Sprintf(
		"  %s %s\n",
		style.InfoLabel.Render("Select a context:"),
		style.InfoValue.Render("press enter to switch"),
	)
	return header + fixSelectedRow(v.table.View())
}

// Loading returns false — context view has no loading state.
func (v *ContextView) Loading() bool { return false }

// Refresh returns a command to refresh.
func (v *ContextView) Refresh() tea.Cmd { return v.refresh() }

func (v *ContextView) refresh() tea.Cmd {
	cfg := v.cfg
	current := v.current
	return func() tea.Msg {
		entries := make([]contextEntry, 0, len(cfg.Contexts))
		for name, ctx := range cfg.Contexts {
			brokers := ctx.Brokers
			if brokers == "" && ctx.SSMBrokers != "" {
				brokers = fmt.Sprintf("ssm:%s", ctx.SSMBrokers)
			}
			entries = append(entries, contextEntry{
				Name:    name,
				Auth:    ctx.Auth,
				Brokers: brokers,
				Current: name == current,
			})
		}
		sort.Slice(entries, func(i, j int) bool {
			return entries[i].Name < entries[j].Name
		})
		return ContextRefreshMsg{Contexts: entries}
	}
}

func (v *ContextView) rebuildRows() {
	hadRows := len(v.table.Rows()) > 0
	priorCursor := v.table.Cursor()
	rows := make([]table.Row, 0, len(v.entries))
	currentIdx := 0
	for i, e := range v.entries {
		marker := ""
		if e.Current {
			marker = "*"
			currentIdx = i
		}
		brokers := e.Brokers
		if len(brokers) > 58 {
			brokers = brokers[:55] + "..."
		}
		rows = append(rows, table.Row{marker, e.Name, e.Auth, brokers})
	}
	setTableRows(&v.table, rows)
	// Seed the cursor on the current context the first time we populate. After
	// that the periodic tick refresh must not yank the user's selection back to
	// the `*` row while they're navigating.
	if hadRows && priorCursor >= 0 && priorCursor < len(rows) {
		v.table.SetCursor(priorCursor)
	} else {
		v.table.SetCursor(currentIdx)
	}
}
