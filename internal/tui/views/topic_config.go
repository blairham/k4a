// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package views

import (
	"context"
	"fmt"
	"sort"

	"charm.land/bubbles/v2/table"
	tea "charm.land/bubbletea/v2"

	"github.com/blairham/k4a/internal/kafka"
	"github.com/blairham/k4a/internal/tui/style"
)

// TopicConfigRefreshMsg carries refreshed topic config data.
type TopicConfigRefreshMsg struct {
	Err     error
	Configs []kafka.ConfigEntry
}

// TopicConfigView shows topic configuration entries.
type TopicConfigView struct {
	err          error
	client       *kafka.Client
	topic        string
	filter       string
	allConfigs   []kafka.ConfigEntry
	visible      []kafka.ConfigEntry
	table        table.Model
	width        int
	loading      bool
	showDefaults bool
}

var configFixedCols = []table.Column{
	{Title: "SOURCE", Width: 24},
	{Title: "RO", Width: 4},
	{Title: "SENSITIVE", Width: 10},
}

// NewTopicConfigView creates a new topic config view.
func NewTopicConfigView(client *kafka.Client, topic string) *TopicConfigView {
	t := table.New(
		table.WithColumns(buildConfigColumns(100)),
		table.WithFocused(true),
		table.WithStyles(tableStyles()),
		table.WithKeyMap(tableKeyMap()),
	)
	return &TopicConfigView{client: client, topic: topic, table: t, loading: true}
}

// buildConfigColumns calculates columns with NAME and VALUE sharing remaining width.
func buildConfigColumns(totalWidth int) []table.Column {
	fixedWidth := 0
	for _, c := range configFixedCols {
		fixedWidth += c.Width + 2
	}
	remaining := totalWidth - fixedWidth - 4 // 2 for each flex column gap
	if remaining < 40 {
		remaining = 40
	}
	nameWidth := remaining * 2 / 5
	valueWidth := remaining - nameWidth

	cols := make([]table.Column, 0, len(configFixedCols)+2)
	cols = append(
		cols,
		table.Column{Title: colName, Width: nameWidth},
		table.Column{Title: "VALUE", Width: valueWidth},
	)
	cols = append(cols, configFixedCols...)
	return cols
}

// Topic returns the topic name.
func (v *TopicConfigView) Topic() string { return v.topic }

// Init returns the initial command.
func (v *TopicConfigView) Init() tea.Cmd { return v.refresh() }

// Update handles messages.
func (v *TopicConfigView) Update(msg tea.Msg) tea.Cmd {
	if msg, ok := msg.(TopicConfigRefreshMsg); ok {
		v.loading = false
		if msg.Err != nil {
			v.err = kafka.FormatUserError(msg.Err)
			return nil
		}
		v.err = nil
		v.allConfigs = msg.Configs
		sort.Slice(v.allConfigs, func(i, j int) bool {
			return v.allConfigs[i].Name < v.allConfigs[j].Name
		})
		v.rebuildRows()
	}
	return nil
}

// UpdateTable delegates to the bubbles table.
func (v *TopicConfigView) UpdateTable(msg tea.Msg) tea.Cmd {
	var cmd tea.Cmd
	v.table, cmd = v.table.Update(msg)
	return cmd
}

// Resize updates table dimensions and reapplies styles.
func (v *TopicConfigView) Resize(width, height int) {
	v.width = width
	v.table.SetColumns(buildConfigColumns(width))
	v.table.SetWidth(width)
	v.table.SetHeight(height)
	v.table.SetStyles(tableStylesWithWidth(width))
}

// Count returns the visible row count.
func (v *TopicConfigView) Count() int { return len(v.table.Rows()) }

// HandleKey processes non-table keys.
func (v *TopicConfigView) HandleKey(key string) (action, param string) {
	switch key {
	case KeyEnter:
		idx := v.table.Cursor()
		if idx >= 0 && idx < len(v.visible) {
			entry := v.visible[idx]
			if !entry.ReadOnly && !entry.Sensitive {
				return "edit_topic_config", entry.Name
			}
		}
	case "d":
		v.showDefaults = !v.showDefaults
		v.rebuildRows()
	}
	return "", ""
}

// SelectedConfig returns the config entry for the given name, or nil.
func (v *TopicConfigView) SelectedConfig(name string) *kafka.ConfigEntry {
	for i := range v.allConfigs {
		if v.allConfigs[i].Name == name {
			return &v.allConfigs[i]
		}
	}
	return nil
}

// SetFilter sets the filter and rebuilds rows.
func (v *TopicConfigView) SetFilter(filter string) {
	v.filter = filter
	v.rebuildRows()
}

// View renders the view.
func (v *TopicConfigView) View() string {
	if v.err != nil {
		return style.Error.Render(fmt.Sprintf("  Error: %v", v.err))
	}
	if len(v.allConfigs) == 0 {
		return style.Muted.Render("  No configuration entries.")
	}
	return fixSelectedRow(v.table.View())
}

func (v *TopicConfigView) rebuildRows() {
	var rows []table.Row
	v.visible = nil
	f := parseFilter(v.filter)

	for _, e := range v.allConfigs {
		if !v.showDefaults && e.IsDefault {
			continue
		}
		if !f.Empty() && !f.MatchesAny(e.Name, e.Value) {
			continue
		}

		ro := ""
		if e.ReadOnly {
			ro = "Y"
		}
		sens := ""
		if e.Sensitive {
			sens = "Y"
		}

		rows = append(rows, table.Row{
			e.Name,
			e.Value,
			e.Source,
			fmt.Sprintf("%*s", (4+len(ro))/2, ro),
			fmt.Sprintf("%*s", (10+len(sens))/2, sens),
		})
		v.visible = append(v.visible, e)
	}
	setTableRows(&v.table, rows)
}

// Loading returns true until the first data fetch completes.
func (v *TopicConfigView) Loading() bool { return v.loading }

// Refresh returns a command to refresh.
func (v *TopicConfigView) Refresh() tea.Cmd { return v.refresh() }

func (v *TopicConfigView) refresh() tea.Cmd {
	topic := v.topic
	return func() tea.Msg {
		configs, err := v.client.FetchTopicConfigs(context.Background(), topic)
		return TopicConfigRefreshMsg{Configs: configs, Err: err}
	}
}
