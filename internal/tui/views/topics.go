// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

// Package views implements the individual TUI views.
package views

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"charm.land/bubbles/v2/table"
	tea "charm.land/bubbletea/v2"

	"github.com/blairham/k4a/internal/kafka"
	"github.com/blairham/k4a/internal/tui/style"
)

// KeyEnter is the string representation of the enter key.
const KeyEnter = "enter"

// TopicsRefreshMsg carries refreshed topic data.
type TopicsRefreshMsg struct {
	Err    error
	Topics []kafka.TopicInfo
}

// TopicCountsMsg carries async message counts.
type TopicCountsMsg struct {
	Counts map[string]int64
}

// TopicsView shows the list of topics.
type TopicsView struct {
	err          error
	client       *kafka.Client
	indexLookup  func(topic string) bool
	filter       string
	allTopics    []kafka.TopicInfo
	visible      []kafka.TopicInfo
	table        table.Model
	loading      bool
	showInternal bool
}

// Fixed-width columns (right side). NAME gets whatever is left.
var topicFixedCols = []table.Column{
	{Title: "PARTITIONS", Width: 12},
	{Title: "REPLICATION", Width: 13},
	{Title: "OUT OF SYNC", Width: 13},
	{Title: "MESSAGES", Width: 12},
	{Title: "INDEX", Width: 7},
}

// NewTopicsView creates a new topics view.
func NewTopicsView(client *kafka.Client) *TopicsView {
	cols := buildTopicColumns(80)

	t := table.New(
		table.WithColumns(cols),
		table.WithFocused(true),
		table.WithStyles(tableStyles()),
		table.WithKeyMap(tableKeyMap()),
	)

	return &TopicsView{
		client:  client,
		table:   t,
		loading: true,
	}
}

// buildTopicColumns calculates columns with NAME filling remaining width.
func buildTopicColumns(totalWidth int) []table.Column {
	fixedWidth := 0
	for _, c := range topicFixedCols {
		fixedWidth += c.Width + 2 // +2 for cell padding
	}
	nameWidth := totalWidth - fixedWidth - 2 // -2 for NAME cell padding
	if nameWidth < 20 {
		nameWidth = 20
	}

	cols := make([]table.Column, 0, len(topicFixedCols)+1)
	cols = append(cols, table.Column{Title: "NAME", Width: nameWidth})
	cols = append(cols, topicFixedCols...)
	return cols
}

// Init returns the initial command to load topics.
func (v *TopicsView) Init() tea.Cmd {
	return v.refresh()
}

// Update handles data refresh messages.
func (v *TopicsView) Update(msg tea.Msg) tea.Cmd {
	switch msg := msg.(type) {
	case TopicsRefreshMsg:
		if msg.Err != nil {
			v.loading = false
			v.err = kafka.FormatUserError(msg.Err)
			return nil
		}
		v.err = nil
		// Preserve existing message counts from previous fetch.
		oldCounts := make(map[string]int64)
		for _, t := range v.allTopics {
			if t.Messages >= 0 {
				oldCounts[t.Name] = t.Messages
			}
		}
		v.allTopics = msg.Topics
		for i := range v.allTopics {
			if count, ok := oldCounts[v.allTopics[i].Name]; ok {
				v.allTopics[i].Messages = count
			}
		}
		v.rebuildRows()
		// Kick off async message count fetch.
		return v.fetchCounts()
	case TopicCountsMsg:
		v.loading = false
		// Merge counts into existing topics.
		for i := range v.allTopics {
			if count, ok := msg.Counts[v.allTopics[i].Name]; ok {
				v.allTopics[i].Messages = count
			}
		}
		v.rebuildRows()
		return nil
	}
	return nil
}

// UpdateTable delegates key/mouse events to the bubbles table.
func (v *TopicsView) UpdateTable(msg tea.Msg) tea.Cmd {
	var cmd tea.Cmd
	v.table, cmd = v.table.Update(msg)
	return cmd
}

// Resize updates the table dimensions, recalculates NAME column width, and reapplies styles.
func (v *TopicsView) Resize(width, height int) {
	v.table.SetColumns(buildTopicColumns(width))
	v.table.SetWidth(width)
	v.table.SetHeight(height)
	v.table.SetStyles(tableStylesWithWidth(width))
}

// Count returns the visible row count.
func (v *TopicsView) Count() int {
	return len(v.table.Rows())
}

// HandleKey processes non-table keys, returning navigation actions.
func (v *TopicsView) HandleKey(key string) (action, param string) {
	switch key {
	case "i":
		v.showInternal = !v.showInternal
		v.rebuildRows()
	case KeyEnter:
		// Enter goes to messages (most common action).
		if row := v.table.SelectedRow(); row != nil {
			idx := v.table.Cursor()
			if idx >= 0 && idx < len(v.visible) {
				return "messages", v.visible[idx].Name
			}
		}
	case "o":
		// Overview/detail for the selected topic.
		if row := v.table.SelectedRow(); row != nil {
			idx := v.table.Cursor()
			if idx >= 0 && idx < len(v.visible) {
				return "topic_detail", v.visible[idx].Name
			}
		}
	case "p":
		// Produce messages to the selected topic.
		if row := v.table.SelectedRow(); row != nil {
			idx := v.table.Cursor()
			if idx >= 0 && idx < len(v.visible) {
				return "produce", v.visible[idx].Name
			}
		}
	case "c":
		// Create a new topic.
		return "create_topic", ""
	case keyCtrlD:
		// Delete the selected topic (with confirmation).
		if row := v.table.SelectedRow(); row != nil {
			idx := v.table.Cursor()
			if idx >= 0 && idx < len(v.visible) {
				return "confirm_delete_topic", v.visible[idx].Name
			}
		}
	}
	return "", ""
}

// SetFilter sets the filter and rebuilds rows.
func (v *TopicsView) SetFilter(filter string) {
	v.filter = filter
	v.rebuildRows()
}

// View renders the topics view.
func (v *TopicsView) View() string {
	if v.err != nil {
		return style.Error.Render(fmt.Sprintf("  Error: %v", v.err))
	}
	return fixSelectedRow(v.table.View())
}

func (v *TopicsView) rebuildRows() {
	sort.Slice(v.allTopics, func(i, j int) bool {
		return v.allTopics[i].Name < v.allTopics[j].Name
	})

	var rows []table.Row
	v.visible = nil

	f := parseFilter(v.filter)

	for _, t := range v.allTopics {
		if !v.showInternal && t.Internal {
			continue
		}
		if !f.Empty() && !f.MatchesAny(t.Name) {
			continue
		}

		msgs := "..."
		if t.Messages >= 0 {
			msgs = formatCount(t.Messages)
		}
		indexed := ""
		if v.indexLookup != nil && v.indexLookup(t.Name) {
			indexed = "✓"
		}
		rows = append(rows, table.Row{
			t.Name,
			fmt.Sprintf("%d", t.Partitions),
			fmt.Sprintf("%d", t.ReplicationFactor),
			fmt.Sprintf("%d", t.OutOfSync),
			msgs,
			indexed,
		})
		v.visible = append(v.visible, t)
	}

	setTableRows(&v.table, rows)
}

func (v *TopicsView) refresh() tea.Cmd {
	return func() tea.Msg {
		topics, err := v.client.FetchTopics(context.Background())
		return TopicsRefreshMsg{Topics: topics, Err: err}
	}
}

func (v *TopicsView) fetchCounts() tea.Cmd {
	names := make([]string, 0, len(v.allTopics))
	for _, t := range v.allTopics {
		if !t.Internal {
			names = append(names, t.Name)
		}
	}
	return func() tea.Msg {
		counts, err := v.client.FetchTopicMessageCounts(context.Background(), names)
		if err != nil {
			return TopicsRefreshMsg{Err: err}
		}
		return TopicCountsMsg{Counts: counts}
	}
}

// SetIndexLookup wires a predicate that reports whether a topic has a
// local index. Called from rebuildRows to render the INDEX column. A
// nil lookup (or a default zero-value view) renders an empty column.
func (v *TopicsView) SetIndexLookup(fn func(topic string) bool) {
	v.indexLookup = fn
	v.rebuildRows()
}

// Loading returns true until the first data fetch completes.
func (v *TopicsView) Loading() bool { return v.loading }

// Refresh returns a command to refresh topics.
func (v *TopicsView) Refresh() tea.Cmd {
	return v.refresh()
}

// formatCount formats large numbers with K/M/B suffixes.
func formatCount(n int64) string {
	switch {
	case n >= 1_000_000_000:
		return fmt.Sprintf("%.1fB", float64(n)/1_000_000_000)
	case n >= 1_000_000:
		return fmt.Sprintf("%.1fM", float64(n)/1_000_000)
	case n >= 1_000:
		return fmt.Sprintf("%.1fK", float64(n)/1_000)
	default:
		return fmt.Sprintf("%d", n)
	}
}

func formatRetention(ms, bytes int64) string {
	var parts []string
	if ms >= 0 {
		parts = append(parts, formatRetentionMs(ms))
	}
	if bytes >= 0 {
		parts = append(parts, formatRetentionBytes(bytes))
	}
	if len(parts) == 0 {
		return "∞"
	}
	return strings.Join(parts, " / ")
}

func formatRetentionMs(ms int64) string {
	const (
		msPerMinute = 60 * 1000
		msPerHour   = 60 * msPerMinute
		msPerDay    = 24 * msPerHour
	)
	switch {
	case ms <= 0:
		return "∞"
	case ms%msPerDay == 0:
		return fmt.Sprintf("%dd", ms/msPerDay)
	case ms%msPerHour == 0:
		return fmt.Sprintf("%dh", ms/msPerHour)
	case ms%msPerMinute == 0:
		return fmt.Sprintf("%dm", ms/msPerMinute)
	default:
		return fmt.Sprintf("%dms", ms)
	}
}

func formatRetentionBytes(b int64) string {
	const (
		kb = 1024
		mb = 1024 * kb
		gb = 1024 * mb
	)
	switch {
	case b < 0:
		return "∞"
	case b >= gb:
		return fmt.Sprintf("%.1fGB", float64(b)/float64(gb))
	case b >= mb:
		return fmt.Sprintf("%.1fMB", float64(b)/float64(mb))
	case b >= kb:
		return fmt.Sprintf("%.1fKB", float64(b)/float64(kb))
	default:
		return fmt.Sprintf("%dB", b)
	}
}
