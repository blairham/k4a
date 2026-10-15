// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package views

import (
	"context"
	"fmt"

	"charm.land/bubbles/v2/table"
	tea "charm.land/bubbletea/v2"

	"github.com/blairham/k4a/internal/kafka"
	"github.com/blairham/k4a/internal/tui/style"
)

// QuotasRefreshMsg carries refreshed client quota data.
type QuotasRefreshMsg struct {
	Err    error
	Quotas []kafka.ClientQuota
}

// Keys the quotas view binds, and the actions they ask the App to run. Each
// action's param is the selected row's index in Visible.
const (
	KeyAddQuota = "a"

	ActionAddQuota    = "add_quota"
	ActionEditQuota   = "edit_quota"
	ActionDeleteQuota = "delete_quota"
)

// QuotasView lists client quotas (DescribeClientQuotas), one row per entity
// and quota key.
type QuotasView struct {
	err     error
	client  *kafka.Client
	filter  string
	all     []kafka.ClientQuota
	visible []kafka.ClientQuota
	table   table.Model
	loading bool
}

var quotaFixedCols = []table.Column{
	{Title: "QUOTA", Width: 28},
	{Title: colValue, Width: 16},
}

// NewQuotasView creates a new client quotas view.
func NewQuotasView(client *kafka.Client) *QuotasView {
	t := table.New(
		table.WithColumns(buildQuotaColumns(100)),
		table.WithFocused(true),
		table.WithStyles(tableStyles()),
		table.WithKeyMap(tableKeyMap()),
	)
	return &QuotasView{client: client, table: t, loading: true}
}

// buildQuotaColumns gives ENTITY the width the fixed columns leave.
func buildQuotaColumns(totalWidth int) []table.Column {
	fixed := 0
	for _, c := range quotaFixedCols {
		fixed += c.Width + 2
	}
	cols := make([]table.Column, 0, len(quotaFixedCols)+1)
	cols = append(cols, table.Column{Title: "ENTITY", Width: max(totalWidth-fixed-2, 20)})
	return append(cols, quotaFixedCols...)
}

// Init returns the initial command.
func (v *QuotasView) Init() tea.Cmd { return v.refresh() }

// Update handles messages.
func (v *QuotasView) Update(msg tea.Msg) tea.Cmd {
	if msg, ok := msg.(QuotasRefreshMsg); ok {
		v.loading = false
		if msg.Err != nil {
			v.err = kafka.FormatUserError(msg.Err)
			return nil
		}
		v.err = nil
		v.all = msg.Quotas
		v.rebuildRows()
	}
	return nil
}

// UpdateTable delegates to the bubbles table.
func (v *QuotasView) UpdateTable(msg tea.Msg) tea.Cmd {
	var cmd tea.Cmd
	v.table, cmd = v.table.Update(msg)
	return cmd
}

// Resize updates table dimensions and reapplies styles.
func (v *QuotasView) Resize(width, height int) {
	v.table.SetColumns(buildQuotaColumns(width))
	v.table.SetWidth(width)
	v.table.SetHeight(height)
	v.table.SetStyles(tableStylesWithWidth(width))
}

// Count returns the visible row count.
func (v *QuotasView) Count() int { return len(v.table.Rows()) }

// HandleKey processes non-table keys.
func (v *QuotasView) HandleKey(key string) (action, param string) {
	if key == KeyAddQuota {
		return ActionAddQuota, ""
	}
	idx := v.table.Cursor()
	if idx < 0 || idx >= len(v.visible) {
		return "", ""
	}
	switch key {
	case KeyEnter:
		return ActionEditQuota, fmt.Sprintf("%d", idx)
	case keyCtrlD:
		return ActionDeleteQuota, fmt.Sprintf("%d", idx)
	}
	return "", ""
}

// Visible returns the quota at row idx as shown, or false.
func (v *QuotasView) Visible(idx int) (kafka.ClientQuota, bool) {
	if idx < 0 || idx >= len(v.visible) {
		return kafka.ClientQuota{}, false
	}
	return v.visible[idx], true
}

// SetFilter sets the filter and rebuilds rows.
func (v *QuotasView) SetFilter(filter string) {
	v.filter = filter
	v.rebuildRows()
}

// View renders the view.
func (v *QuotasView) View() string {
	if v.err != nil {
		return style.Error.Render(fmt.Sprintf("  Error: %v", v.err))
	}
	if len(v.all) == 0 {
		return style.Muted.Render("  No client quotas set. Press <a> to add one.")
	}
	return fixSelectedRow(v.table.View())
}

func (v *QuotasView) rebuildRows() {
	var rows []table.Row
	v.visible = nil
	f := parseFilter(v.filter)
	for _, q := range v.all {
		entity, value := q.Entity.String(), kafka.FormatQuotaValue(q.Value)
		if !f.Empty() && !f.MatchesAny(entity, q.Key, value) {
			continue
		}
		rows = append(rows, table.Row{entity, q.Key, value})
		v.visible = append(v.visible, q)
	}
	setTableRows(&v.table, rows)
}

// Loading returns true until the first data fetch completes.
func (v *QuotasView) Loading() bool { return v.loading }

// Refresh returns a command to refresh.
func (v *QuotasView) Refresh() tea.Cmd { return v.refresh() }

func (v *QuotasView) refresh() tea.Cmd {
	return func() tea.Msg {
		quotas, err := v.client.FetchClientQuotas(context.Background())
		return QuotasRefreshMsg{Quotas: quotas, Err: err}
	}
}
