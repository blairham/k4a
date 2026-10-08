// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package views

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"charm.land/bubbles/v2/table"
	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"

	"github.com/blairham/k4a/internal/kafka"
	"github.com/blairham/k4a/internal/tui/style"
)

// BrokerDetailRefreshMsg carries refreshed broker config data.
type BrokerDetailRefreshMsg struct {
	Err     error
	Configs []kafka.ConfigEntry
}

// BrokerDetailView shows broker configuration entries.
type BrokerDetailView struct {
	err          error
	client       *kafka.Client
	filter       string
	allConfigs   []kafka.ConfigEntry
	visible      []kafka.ConfigEntry
	broker       kafka.BrokerInfo
	table        table.Model
	width        int
	controller   bool
	loading      bool
	showDefaults bool
}

// NewBrokerDetailView creates a new broker detail view.
func NewBrokerDetailView(client *kafka.Client, broker kafka.BrokerInfo, controller bool) *BrokerDetailView {
	t := table.New(
		table.WithColumns(buildConfigColumns(100)),
		table.WithFocused(true),
		table.WithStyles(tableStyles()),
		table.WithKeyMap(tableKeyMap()),
	)
	return &BrokerDetailView{client: client, broker: broker, controller: controller, table: t, loading: true}
}

// Init returns the initial command.
func (v *BrokerDetailView) Init() tea.Cmd { return v.refresh() }

// Update handles messages.
func (v *BrokerDetailView) Update(msg tea.Msg) tea.Cmd {
	switch msg := msg.(type) {
	case BrokerDetailRefreshMsg:
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
func (v *BrokerDetailView) UpdateTable(msg tea.Msg) tea.Cmd {
	var cmd tea.Cmd
	v.table, cmd = v.table.Update(msg)
	return cmd
}

// Resize updates table dimensions and reapplies styles.
func (v *BrokerDetailView) Resize(width, height int) {
	v.width = width
	v.table.SetColumns(buildConfigColumns(width))
	v.table.SetWidth(width)
	// Reserve lines for summary card.
	tableHeight := height - 5
	if tableHeight < 1 {
		tableHeight = 1
	}
	v.table.SetHeight(tableHeight)
	v.table.SetStyles(tableStylesWithWidth(width))
}

// Count returns the visible row count.
func (v *BrokerDetailView) Count() int { return len(v.table.Rows()) }

// HandleKey processes non-table keys.
func (v *BrokerDetailView) HandleKey(key string) (string, string) {
	if key == "d" {
		v.showDefaults = !v.showDefaults
		v.rebuildRows()
	}
	return "", ""
}

// SetFilter sets the filter and rebuilds rows.
func (v *BrokerDetailView) SetFilter(filter string) {
	v.filter = filter
	v.rebuildRows()
}

// View renders the summary card + config table.
func (v *BrokerDetailView) View() string {
	if v.err != nil {
		return style.Error.Render(fmt.Sprintf("  Error: %v", v.err))
	}

	var sb strings.Builder
	sb.WriteString(v.renderSummary())
	sb.WriteString("\n")

	if len(v.allConfigs) == 0 {
		sb.WriteString(style.Muted.Render("  No configuration entries."))
	} else {
		sb.WriteString(fixSelectedRow(v.table.View()))
	}
	return sb.String()
}

func (v *BrokerDetailView) renderSummary() string {
	labelStyle := lipgloss.NewStyle().Foreground(style.ColorGray)
	valueStyle := lipgloss.NewStyle().Foreground(style.ColorWhite).Bold(true)
	borderColor := lipgloss.NewStyle().Foreground(style.ColorBorder)

	type card struct {
		label string
		value string
	}

	ctrl := "No"
	if v.controller {
		ctrl = "Yes"
	}

	items := []card{
		{label: "Broker ID", value: fmt.Sprintf("%d", v.broker.ID)},
		{label: "Host", value: v.broker.Host},
		{label: "Port", value: fmt.Sprintf("%d", v.broker.Port)},
		{label: "Controller", value: ctrl},
		{label: "Configs", value: fmt.Sprintf("%d", len(v.allConfigs))},
	}

	n := len(items)
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
		values.WriteString(padded.Render(valueStyle.Render(truncate(item.value, contentW))))

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

func (v *BrokerDetailView) rebuildRows() {
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
func (v *BrokerDetailView) Loading() bool { return v.loading }

// Refresh returns a command to refresh.
func (v *BrokerDetailView) Refresh() tea.Cmd { return v.refresh() }

func (v *BrokerDetailView) refresh() tea.Cmd {
	brokerID := fmt.Sprintf("%d", v.broker.ID)
	return func() tea.Msg {
		configs, err := v.client.FetchBrokerConfigs(context.Background(), brokerID)
		return BrokerDetailRefreshMsg{Configs: configs, Err: err}
	}
}
