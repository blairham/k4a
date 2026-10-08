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

// GroupDetailRefreshMsg carries refreshed group detail.
type GroupDetailRefreshMsg struct {
	Detail *kafka.GroupDetail
	Err    error
}

// GroupDetailView shows per-partition lag for a consumer group.
type GroupDetailView struct {
	err     error
	client  *kafka.Client
	detail  *kafka.GroupDetail
	groupID string
	table   table.Model
	loading bool
}

// NewGroupDetailView creates a new group detail view.
func NewGroupDetailView(client *kafka.Client, groupID string) *GroupDetailView {
	cols := []table.Column{
		{Title: "TOPIC", Width: 30},
		{Title: "PARTITION", Width: 10},
		{Title: "COMMITTED", Width: 12},
		{Title: "HIGH WM", Width: 12},
		{Title: "LAG", Width: 10},
	}

	t := table.New(
		table.WithColumns(cols),
		table.WithFocused(true),
		table.WithStyles(tableStyles()),
		table.WithKeyMap(tableKeyMap()),
	)

	return &GroupDetailView{client: client, groupID: groupID, table: t, loading: true}
}

// Init returns the initial command.
func (v *GroupDetailView) Init() tea.Cmd { return v.refresh() }

// Update handles messages.
func (v *GroupDetailView) Update(msg tea.Msg) tea.Cmd {
	switch msg := msg.(type) {
	case GroupDetailRefreshMsg:
		v.loading = false
		if msg.Err != nil {
			v.err = kafka.FormatUserError(msg.Err)
			return nil
		}
		v.err = nil
		v.detail = msg.Detail
		v.rebuildRows()
	}
	return nil
}

// UpdateTable delegates to the bubbles table.
func (v *GroupDetailView) UpdateTable(msg tea.Msg) tea.Cmd {
	var cmd tea.Cmd
	v.table, cmd = v.table.Update(msg)
	return cmd
}

// Resize updates table dimensions and reapplies styles.
func (v *GroupDetailView) Resize(width, height int) {
	v.table.SetWidth(width)
	v.table.SetHeight(height)
	v.table.SetStyles(tableStylesWithWidth(width))
}

// Count returns the visible row count.
func (v *GroupDetailView) Count() int { return len(v.table.Rows()) }

// HandleKey processes non-table keys.
func (v *GroupDetailView) HandleKey(key string) (string, string) {
	if key == "R" {
		return "reset_offsets", v.groupID
	}
	return "", ""
}

// SetFilter is a no-op for detail view.
func (v *GroupDetailView) SetFilter(_ string) {}

// View renders the view.
func (v *GroupDetailView) View() string {
	if v.err != nil {
		return style.Error.Render(fmt.Sprintf("  Error: %v", v.err))
	}
	return fixSelectedRow(v.table.View())
}

func (v *GroupDetailView) rebuildRows() {
	if v.detail == nil {
		return
	}
	rows := make([]table.Row, 0, len(v.detail.Offsets))
	for _, o := range v.detail.Offsets {
		rows = append(rows, table.Row{
			o.Topic,
			fmt.Sprintf("%d", o.Partition),
			fmt.Sprintf("%d", o.CommittedOffset),
			fmt.Sprintf("%d", o.HighWatermark),
			fmt.Sprintf("%d", o.Lag),
		})
	}
	setTableRows(&v.table, rows)
}

// GetSelectedOffset returns the offset info for the currently highlighted row.
func (v *GroupDetailView) GetSelectedOffset() *kafka.GroupOffsetInfo {
	if v.detail == nil {
		return nil
	}
	idx := v.table.Cursor()
	if idx < 0 || idx >= len(v.detail.Offsets) {
		return nil
	}
	return &v.detail.Offsets[idx]
}

// GroupID returns the consumer group ID.
func (v *GroupDetailView) GroupID() string { return v.groupID }

// Loading returns true until the first data fetch completes.
func (v *GroupDetailView) Loading() bool { return v.loading }

// Refresh returns a command to refresh.
func (v *GroupDetailView) Refresh() tea.Cmd { return v.refresh() }

func (v *GroupDetailView) refresh() tea.Cmd {
	groupID := v.groupID
	return func() tea.Msg {
		detail, err := v.client.FetchGroupDetail(context.Background(), groupID)
		return GroupDetailRefreshMsg{Detail: detail, Err: err}
	}
}
