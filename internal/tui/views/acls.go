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

	"github.com/blairham/k4a/internal/kafka"
	"github.com/blairham/k4a/internal/tui/style"
)

// ACLsRefreshMsg carries refreshed ACL data.
type ACLsRefreshMsg struct {
	Err  error
	ACLs []kafka.ACLEntry
}

// ACLsView shows the list of ACL bindings.
type ACLsView struct {
	err     error
	client  *kafka.Client
	filter  string
	all     []kafka.ACLEntry
	visible []kafka.ACLEntry
	table   table.Model
	loading bool
}

var aclFixedCols = []table.Column{
	{Title: "RESOURCE", Width: 10},
	{Title: colName, Width: 28},
	{Title: "PATTERN", Width: 10},
	{Title: "HOST", Width: 16},
	{Title: "OPERATION", Width: 16},
	{Title: "PERMISSION", Width: 11},
}

// NewACLsView creates a new ACLs view.
func NewACLsView(client *kafka.Client) *ACLsView {
	t := table.New(
		table.WithColumns(buildACLColumns(100)),
		table.WithFocused(true),
		table.WithStyles(tableStyles()),
		table.WithKeyMap(tableKeyMap()),
	)
	return &ACLsView{client: client, table: t, loading: true}
}

// buildACLColumns calculates columns with PRINCIPAL filling remaining width.
func buildACLColumns(totalWidth int) []table.Column {
	fixedWidth := 0
	for _, c := range aclFixedCols {
		fixedWidth += c.Width + 2
	}
	principalWidth := totalWidth - fixedWidth - 2
	if principalWidth < 20 {
		principalWidth = 20
	}
	cols := make([]table.Column, 0, len(aclFixedCols)+1)
	cols = append(cols, table.Column{Title: "PRINCIPAL", Width: principalWidth})
	cols = append(cols, aclFixedCols...)
	return cols
}

// Init returns the initial command.
func (v *ACLsView) Init() tea.Cmd { return v.refresh() }

// Update handles messages.
func (v *ACLsView) Update(msg tea.Msg) tea.Cmd {
	if msg, ok := msg.(ACLsRefreshMsg); ok {
		v.loading = false
		if msg.Err != nil {
			v.err = kafka.FormatUserError(msg.Err)
			return nil
		}
		v.err = nil
		v.all = msg.ACLs
		sortACLs(v.all)
		v.rebuildRows()
	}
	return nil
}

// UpdateTable delegates to the bubbles table.
func (v *ACLsView) UpdateTable(msg tea.Msg) tea.Cmd {
	var cmd tea.Cmd
	v.table, cmd = v.table.Update(msg)
	return cmd
}

// Resize updates table dimensions and reapplies styles.
func (v *ACLsView) Resize(width, height int) {
	v.table.SetColumns(buildACLColumns(width))
	v.table.SetWidth(width)
	v.table.SetHeight(height)
	v.table.SetStyles(tableStylesWithWidth(width))
}

// Count returns the visible row count.
func (v *ACLsView) Count() int { return len(v.table.Rows()) }

// HandleKey processes non-table keys.
func (v *ACLsView) HandleKey(key string) (action, param string) {
	switch key {
	case "a":
		return "create_acl", ""
	case keyCtrlD:
		idx := v.table.Cursor()
		if idx >= 0 && idx < len(v.visible) {
			return "confirm_delete_acl", aclKey(v.visible[idx])
		}
	}
	return "", ""
}

// SetFilter sets the filter and rebuilds rows.
func (v *ACLsView) SetFilter(filter string) {
	v.filter = filter
	v.rebuildRows()
}

// SelectedACL returns the ACL matching the given key, or nil if not found.
// The key is the value produced by aclKey — used by the app-level confirm flow
// to locate the target across refreshes without passing the struct around.
func (v *ACLsView) SelectedACL(key string) *kafka.ACLEntry {
	for i := range v.all {
		if aclKey(v.all[i]) == key {
			return &v.all[i]
		}
	}
	return nil
}

// View renders the view.
func (v *ACLsView) View() string {
	if v.err != nil {
		return style.Error.Render(fmt.Sprintf("  Error: %v", v.err))
	}
	if len(v.all) == 0 {
		return style.Muted.Render("  No ACLs configured. Press <a> or :acl to create one.")
	}
	return fixSelectedRow(v.table.View())
}

func (v *ACLsView) rebuildRows() {
	var rows []table.Row
	v.visible = nil
	f := parseFilter(v.filter)

	for _, e := range v.all {
		if !f.Empty() && !f.MatchesAny(
			e.Principal, e.ResourceType, e.ResourceName, e.PatternType,
			e.Host, e.Operation, e.Permission,
		) {
			continue
		}
		rows = append(rows, table.Row{
			e.Principal,
			e.ResourceType,
			e.ResourceName,
			e.PatternType,
			e.Host,
			e.Operation,
			e.Permission,
		})
		v.visible = append(v.visible, e)
	}
	setTableRows(&v.table, rows)
}

// Loading returns true until the first data fetch completes.
func (v *ACLsView) Loading() bool { return v.loading }

// Refresh returns a command to refresh.
func (v *ACLsView) Refresh() tea.Cmd { return v.refresh() }

func (v *ACLsView) refresh() tea.Cmd {
	return func() tea.Msg {
		acls, err := v.client.FetchACLs(context.Background())
		return ACLsRefreshMsg{ACLs: acls, Err: err}
	}
}

// aclKey returns a stable identifier for an ACL entry. All seven fields are
// needed because Kafka treats the tuple as the ACL's identity.
func aclKey(e kafka.ACLEntry) string {
	return strings.Join([]string{
		e.Principal, e.ResourceType, e.ResourceName, e.PatternType,
		e.Host, e.Operation, e.Permission,
	}, "\x00")
}

// sortACLs sorts by principal, then resource type, then resource name — mirrors
// the web UI default order.
func sortACLs(acls []kafka.ACLEntry) {
	sort.SliceStable(acls, func(i, j int) bool {
		a, b := acls[i], acls[j]
		if a.Principal != b.Principal {
			return a.Principal < b.Principal
		}
		if a.ResourceType != b.ResourceType {
			return a.ResourceType < b.ResourceType
		}
		if a.ResourceName != b.ResourceName {
			return a.ResourceName < b.ResourceName
		}
		if a.Operation != b.Operation {
			return a.Operation < b.Operation
		}
		return a.Permission < b.Permission
	})
}
