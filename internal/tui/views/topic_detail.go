// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package views

import (
	"context"
	"fmt"
	"strings"

	"charm.land/bubbles/v2/table"
	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"

	"github.com/blairham/k4a/internal/kafka"
	"github.com/blairham/k4a/internal/tui/style"
)

// TopicDetailRefreshMsg carries refreshed topic detail.
type TopicDetailRefreshMsg struct {
	Detail *kafka.TopicDetail
	Err    error
}

// TopicDetailView shows partition details for a topic.
type TopicDetailView struct {
	err     error
	client  *kafka.Client
	detail  *kafka.TopicDetail
	topic   string
	table   table.Model
	width   int
	loading bool
}

// NewTopicDetailView creates a new topic detail view.
func NewTopicDetailView(client *kafka.Client, topic string) *TopicDetailView {
	cols := []table.Column{
		{Title: "PARTITION ID", Width: 14},
		{Title: "REPLICAS", Width: 20},
		{Title: "FIRST OFFSET", Width: 14},
		{Title: "NEXT OFFSET", Width: 14},
		{Title: "MESSAGE COUNT", Width: 15},
	}

	t := table.New(
		table.WithColumns(cols),
		table.WithFocused(true),
		table.WithStyles(tableStyles()),
		table.WithKeyMap(tableKeyMap()),
	)

	return &TopicDetailView{
		client:  client,
		topic:   topic,
		table:   t,
		loading: true,
	}
}

// Topic returns the topic name.
func (v *TopicDetailView) Topic() string { return v.topic }

// Init returns the initial command.
func (v *TopicDetailView) Init() tea.Cmd { return v.refresh() }

// Update handles messages.
func (v *TopicDetailView) Update(msg tea.Msg) tea.Cmd {
	switch msg := msg.(type) {
	case TopicDetailRefreshMsg:
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
func (v *TopicDetailView) UpdateTable(msg tea.Msg) tea.Cmd {
	var cmd tea.Cmd
	v.table, cmd = v.table.Update(msg)
	return cmd
}

// Resize updates table dimensions and reapplies styles.
func (v *TopicDetailView) Resize(width, height int) {
	v.width = width
	v.table.SetWidth(width)
	// Reserve lines for summary cards (top border + labels + values + bottom border + blank = 5).
	tableHeight := height - 5
	if tableHeight < 1 {
		tableHeight = 1
	}
	v.table.SetHeight(tableHeight)
	v.table.SetStyles(tableStylesWithWidth(width))
}

// Count returns the visible row count.
func (v *TopicDetailView) Count() int { return len(v.table.Rows()) }

// HandleKey processes non-table keys.
func (v *TopicDetailView) HandleKey(key string) (string, string) {
	switch key {
	case "m":
		return "messages", v.topic
	case "e":
		return "topic_config", v.topic
	case "p":
		return "increase_partitions", v.topic
	case "R":
		return "change_replication", v.topic
	case keyCtrlD:
		return "confirm_purge_topic", v.topic
	}
	return "", ""
}

// PartitionCount returns the current partition count, or 0 if data hasn't loaded.
func (v *TopicDetailView) PartitionCount() int {
	if v.detail == nil {
		return 0
	}
	return len(v.detail.Partitions)
}

// ReplicationFactor returns the current replication factor, or 0 if data hasn't loaded.
func (v *TopicDetailView) ReplicationFactor() int {
	if v.detail == nil {
		return 0
	}
	return v.detail.ReplicationFactor
}

// SetFilter is a no-op for partition view.
func (v *TopicDetailView) SetFilter(_ string) {}

// View renders the summary cards + partition table.
func (v *TopicDetailView) View() string {
	if v.err != nil {
		return style.Error.Render(fmt.Sprintf("  Error: %v", v.err))
	}

	var sb strings.Builder
	sb.WriteString(v.renderSummary())
	sb.WriteString("\n")
	sb.WriteString(fixSelectedRow(v.table.View()))
	return sb.String()
}

func (v *TopicDetailView) renderSummary() string {
	if v.detail == nil {
		return ""
	}

	d := v.detail

	labelStyle := lipgloss.NewStyle().Foreground(style.ColorGray)
	valueStyle := lipgloss.NewStyle().Foreground(style.ColorWhite).Bold(true)
	borderColor := lipgloss.NewStyle().Foreground(style.ColorBorder)

	type card struct {
		vStyle lipgloss.Style
		label  string
		value  string
	}

	topicType := "External"
	if d.Internal {
		topicType = "Internal"
	}

	items := []card{
		{label: "Partitions", value: fmt.Sprintf("%d", len(d.Partitions)), vStyle: valueStyle},
		{label: "Replication", value: fmt.Sprintf("%d", d.ReplicationFactor), vStyle: valueStyle},
		{label: "URP", value: fmt.Sprintf("%d", d.URP), vStyle: urpValueStyle(d.URP)},
		{label: "In Sync", value: fmt.Sprintf("%d of %d", d.TotalISR, d.TotalReplicas), vStyle: valueStyle},
		{label: "Type", value: topicType, vStyle: valueStyle},
		{label: "Cleanup", value: d.CleanupPolicy, vStyle: valueStyle},
		{label: "Retention", value: formatRetention(d.RetentionMs, d.RetentionBytes), vStyle: valueStyle},
		{label: "Messages", value: formatCount(d.TotalMessages), vStyle: valueStyle},
	}

	n := len(items)
	// Total visual width of the card grid:
	// ┌──┬──┬──┐  = 1 (┌) + n cells + (n-1) separators (┬) + 1 (┐)
	// So: totalWidth = n*cellWidth + n + 1
	// Solving: cellWidth = (v.width - n - 1) / n
	avail := v.width - n - 1
	if avail < n {
		avail = n
	}
	cellWidth := avail / n
	remainder := avail - cellWidth*n

	sep := borderColor.Render("│")

	var labels, values strings.Builder
	var topBorder, botBorder strings.Builder
	topBorder.WriteString(borderColor.Render("┌"))
	botBorder.WriteString(borderColor.Render("└"))
	labels.WriteString(sep)
	values.WriteString(sep)

	for i, item := range items {
		w := cellWidth
		if i < remainder {
			w++
		}

		contentW := w - 2 // padding(0, 1) takes 2 chars
		padded := lipgloss.NewStyle().Width(w).MaxWidth(w).Padding(0, 1)
		labels.WriteString(padded.Render(labelStyle.Render(truncate(item.label, contentW))))
		values.WriteString(padded.Render(item.vStyle.Render(truncate(item.value, contentW))))

		if i < n-1 {
			labels.WriteString(sep)
			values.WriteString(sep)
			topBorder.WriteString(borderColor.Render(strings.Repeat("─", w) + "┬"))
			botBorder.WriteString(borderColor.Render(strings.Repeat("─", w) + "┴"))
		} else {
			labels.WriteString(sep)
			values.WriteString(sep)
			topBorder.WriteString(borderColor.Render(strings.Repeat("─", w) + "┐"))
			botBorder.WriteString(borderColor.Render(strings.Repeat("─", w) + "┘"))
		}
	}

	return topBorder.String() + "\n" +
		labels.String() + "\n" +
		values.String() + "\n" +
		botBorder.String()
}

func urpValueStyle(urp int) lipgloss.Style {
	if urp > 0 {
		return lipgloss.NewStyle().Foreground(style.ColorRed).Bold(true)
	}
	return lipgloss.NewStyle().Foreground(style.ColorGreen).Bold(true)
}

func (v *TopicDetailView) rebuildRows() {
	if v.detail == nil {
		return
	}
	rows := make([]table.Row, 0, len(v.detail.Partitions))
	for _, p := range v.detail.Partitions {
		// Format replicas with leader first (leader is highlighted in the UI).
		replicaStrs := make([]string, 0, len(p.Replicas))
		for _, r := range p.Replicas {
			replicaStrs = append(replicaStrs, fmt.Sprintf("%d", r))
		}

		rows = append(rows, table.Row{
			fmt.Sprintf("%d", p.ID),
			strings.Join(replicaStrs, ", "),
			fmt.Sprintf("%d", p.FirstOffset),
			fmt.Sprintf("%d", p.LastOffset),
			formatCount(p.Messages),
		})
	}
	setTableRows(&v.table, rows)
}

// Loading returns true until the first data fetch completes.
func (v *TopicDetailView) Loading() bool { return v.loading }

// Refresh returns a command to refresh.
func (v *TopicDetailView) Refresh() tea.Cmd { return v.refresh() }

func (v *TopicDetailView) refresh() tea.Cmd {
	topic := v.topic
	return func() tea.Msg {
		detail, err := v.client.FetchTopicDetail(context.Background(), topic)
		return TopicDetailRefreshMsg{Detail: detail, Err: err}
	}
}
