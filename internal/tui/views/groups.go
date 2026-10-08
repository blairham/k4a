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

// pendingValue is the placeholder shown while enrichment data is loading.
const pendingValue = "..."

// GroupsRefreshMsg carries refreshed group data.
type GroupsRefreshMsg struct {
	Err    error
	Groups []kafka.GroupInfo
}

// GroupEnrichmentMsg carries async enrichment data for groups.
type GroupEnrichmentMsg struct {
	Enrichment map[string]kafka.GroupEnrichment
}

// GroupsView shows consumer groups.
type GroupsView struct {
	err       error
	client    *kafka.Client
	filter    string
	allGroups []kafka.GroupInfo
	visible   []kafka.GroupInfo
	table     table.Model
	loading   bool
}

// NewGroupsView creates a new groups view.
func NewGroupsView(client *kafka.Client) *GroupsView {
	cols := []table.Column{
		{Title: "GROUP ID", Width: 45},
		{Title: "STATE", Width: 12},
		{Title: "MEMBERS", Width: 9},
		{Title: "TOPICS", Width: 8},
		{Title: "LAG", Width: 12},
		{Title: "COORDINATOR", Width: 13},
	}

	t := table.New(
		table.WithColumns(cols),
		table.WithFocused(true),
		table.WithStyles(tableStyles()),
		table.WithKeyMap(tableKeyMap()),
	)

	return &GroupsView{
		client:  client,
		table:   t,
		loading: true,
	}
}

// Init returns the initial command.
func (v *GroupsView) Init() tea.Cmd { return v.refresh() }

// Update handles messages.
func (v *GroupsView) Update(msg tea.Msg) tea.Cmd {
	switch msg := msg.(type) {
	case GroupsRefreshMsg:
		v.loading = false
		if msg.Err != nil {
			v.err = kafka.FormatUserError(msg.Err)
			return nil
		}
		v.err = nil

		// Preserve existing enrichment data from previous fetch.
		oldEnrichment := make(map[string]kafka.GroupEnrichment)
		for _, g := range v.allGroups {
			if g.Topics >= 0 || g.TotalLag >= 0 || g.Coordinator >= 0 {
				oldEnrichment[g.GroupID] = kafka.GroupEnrichment{
					Topics:      g.Topics,
					TotalLag:    g.TotalLag,
					Coordinator: g.Coordinator,
				}
			}
		}
		v.allGroups = msg.Groups
		for i := range v.allGroups {
			if e, ok := oldEnrichment[v.allGroups[i].GroupID]; ok {
				v.allGroups[i].Topics = e.Topics
				v.allGroups[i].TotalLag = e.TotalLag
				v.allGroups[i].Coordinator = e.Coordinator
			}
		}
		v.rebuildRows()
		return v.fetchEnrichment()

	case GroupEnrichmentMsg:
		for i := range v.allGroups {
			if e, ok := msg.Enrichment[v.allGroups[i].GroupID]; ok {
				v.allGroups[i].Topics = e.Topics
				v.allGroups[i].TotalLag = e.TotalLag
				v.allGroups[i].Coordinator = e.Coordinator
			}
		}
		v.rebuildRows()
		return nil
	}
	return nil
}

// UpdateTable delegates to the bubbles table.
func (v *GroupsView) UpdateTable(msg tea.Msg) tea.Cmd {
	var cmd tea.Cmd
	v.table, cmd = v.table.Update(msg)
	return cmd
}

// Resize updates table dimensions and reapplies styles.
func (v *GroupsView) Resize(width, height int) {
	v.table.SetWidth(width)
	v.table.SetHeight(height)
	v.table.SetStyles(tableStylesWithWidth(width))
}

// Count returns the visible row count.
func (v *GroupsView) Count() int { return len(v.table.Rows()) }

// HandleKey processes non-table keys.
func (v *GroupsView) HandleKey(key string) (string, string) {
	idx := v.table.Cursor()
	switch key {
	case KeyEnter:
		if row := v.table.SelectedRow(); row != nil {
			if idx >= 0 && idx < len(v.visible) {
				return "group_detail", v.visible[idx].GroupID
			}
		}
	case keyCtrlD:
		if idx >= 0 && idx < len(v.visible) {
			return "confirm_delete_group", v.visible[idx].GroupID
		}
	}
	return "", ""
}

// SetFilter sets the filter and rebuilds rows.
func (v *GroupsView) SetFilter(filter string) {
	v.filter = filter
	v.rebuildRows()
}

// View renders the view.
func (v *GroupsView) View() string {
	if v.err != nil {
		return style.Error.Render(fmt.Sprintf("  Error: %v", v.err))
	}
	return fixSelectedRow(v.table.View())
}

func (v *GroupsView) rebuildRows() {
	sort.Slice(v.allGroups, func(i, j int) bool {
		return v.allGroups[i].GroupID < v.allGroups[j].GroupID
	})

	var rows []table.Row
	v.visible = nil

	f := parseFilter(v.filter)

	for _, g := range v.allGroups {
		if !f.Empty() && !f.MatchesAny(g.GroupID) {
			continue
		}

		topics := pendingValue
		if g.Topics >= 0 {
			topics = fmt.Sprintf("%d", g.Topics)
		}

		lag := pendingValue
		if g.TotalLag >= 0 {
			lag = formatCount(g.TotalLag)
		}

		coordinator := pendingValue
		if g.Coordinator >= 0 {
			coordinator = fmt.Sprintf("%d", g.Coordinator)
		}

		rows = append(rows, table.Row{
			g.GroupID,
			g.State,
			fmt.Sprintf("%d", g.Members),
			topics,
			lag,
			coordinator,
		})
		v.visible = append(v.visible, g)
	}

	setTableRows(&v.table, rows)
}

// Loading returns true until the first data fetch completes.
func (v *GroupsView) Loading() bool { return v.loading }

// Refresh returns a command to refresh.
func (v *GroupsView) Refresh() tea.Cmd { return v.refresh() }

func (v *GroupsView) refresh() tea.Cmd {
	return func() tea.Msg {
		groups, err := v.client.FetchGroups(context.Background())
		return GroupsRefreshMsg{Groups: groups, Err: err}
	}
}

func (v *GroupsView) fetchEnrichment() tea.Cmd {
	ids := make([]string, 0, len(v.allGroups))
	for _, g := range v.allGroups {
		ids = append(ids, g.GroupID)
	}
	return func() tea.Msg {
		return GroupEnrichmentMsg{Enrichment: v.client.FetchGroupsEnrichment(context.Background(), ids)}
	}
}
