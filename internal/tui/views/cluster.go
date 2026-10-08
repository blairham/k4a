// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package views

import (
	"context"
	"fmt"
	"time"

	"charm.land/bubbles/v2/table"
	tea "charm.land/bubbletea/v2"

	"github.com/blairham/k4a/internal/kafka"
	"github.com/blairham/k4a/internal/tui/style"
)

// ClusterRefreshMsg carries refreshed cluster data.
type ClusterRefreshMsg struct {
	Cluster *kafka.ClusterInfo
	Err     error
	Latency string
}

// ClusterView shows cluster/broker information.
type ClusterView struct {
	err     error
	client  *kafka.Client
	cluster *kafka.ClusterInfo
	latency string
	table   table.Model
	loading bool
}

// NewClusterView creates a new cluster view.
func NewClusterView(client *kafka.Client) *ClusterView {
	cols := []table.Column{
		{Title: "BROKER ID", Width: 12},
		{Title: "HOST", Width: 50},
		{Title: "PORT", Width: 8},
		{Title: "CONTROLLER", Width: 12},
	}

	t := table.New(
		table.WithColumns(cols),
		table.WithFocused(true),
		table.WithStyles(tableStyles()),
		table.WithKeyMap(tableKeyMap()),
	)

	return &ClusterView{client: client, table: t, loading: true}
}

// Init returns the initial command.
func (v *ClusterView) Init() tea.Cmd { return v.refresh() }

// Update handles messages.
func (v *ClusterView) Update(msg tea.Msg) tea.Cmd {
	if msg, ok := msg.(ClusterRefreshMsg); ok {
		v.loading = false
		if msg.Err != nil {
			v.err = kafka.FormatUserError(msg.Err)
			return nil
		}
		v.err = nil
		v.cluster = msg.Cluster
		v.latency = msg.Latency
		v.rebuildRows()
	}
	return nil
}

// UpdateTable delegates to the bubbles table.
func (v *ClusterView) UpdateTable(msg tea.Msg) tea.Cmd {
	var cmd tea.Cmd
	v.table, cmd = v.table.Update(msg)
	return cmd
}

// Resize updates table dimensions and reapplies styles.
func (v *ClusterView) Resize(width, height int) {
	v.table.SetWidth(width)
	v.table.SetHeight(height)
	v.table.SetStyles(tableStylesWithWidth(width))
}

// Count returns the visible row count.
func (v *ClusterView) Count() int { return len(v.table.Rows()) }

// HandleKey processes non-table keys.
func (v *ClusterView) HandleKey(key string) (string, string) {
	if key == KeyEnter && v.cluster != nil {
		idx := v.table.Cursor()
		if idx >= 0 && idx < len(v.cluster.Brokers) {
			return "broker_detail", fmt.Sprintf("%d", v.cluster.Brokers[idx].ID)
		}
	}
	return "", ""
}

// BrokerByID returns the broker and controller flag for a given ID, or nil.
func (v *ClusterView) BrokerByID(id int) (*kafka.BrokerInfo, bool) {
	if v.cluster == nil {
		return nil, false
	}
	for i := range v.cluster.Brokers {
		if v.cluster.Brokers[i].ID == id {
			return &v.cluster.Brokers[i], v.cluster.Brokers[i].ID == v.cluster.ControllerID
		}
	}
	return nil, false
}

// SetFilter is a no-op for cluster view.
func (v *ClusterView) SetFilter(_ string) {}

// View renders the view.
func (v *ClusterView) View() string {
	if v.err != nil {
		return style.Error.Render(fmt.Sprintf("  Error: %v", v.err))
	}
	var header string
	if v.cluster != nil {
		clusterID := v.cluster.ClusterID
		if clusterID == "" {
			clusterID = "unknown"
		}
		brokerCount := len(v.cluster.Brokers)
		latency := v.latency
		if latency == "" {
			latency = "..."
		}
		dot := style.DotGreen.Render("●")
		if v.err != nil {
			dot = style.DotRed.Render("●")
		}
		header = fmt.Sprintf(
			"  %s %s  %s %d  %s %s\n",
			style.InfoLabel.Render("Cluster:"),
			style.InfoValue.Render(clusterID),
			style.InfoLabel.Render("Brokers:"),
			brokerCount,
			style.InfoLabel.Render("Latency:"),
			dot+" "+style.InfoValue.Render(latency),
		)
	}
	return header + fixSelectedRow(v.table.View())
}

func (v *ClusterView) rebuildRows() {
	if v.cluster == nil {
		return
	}
	rows := make([]table.Row, 0, len(v.cluster.Brokers))
	for _, b := range v.cluster.Brokers {
		controller := ""
		if b.ID == v.cluster.ControllerID {
			controller = "✓"
		}
		rows = append(rows, table.Row{
			fmt.Sprintf("%d", b.ID),
			b.Host,
			fmt.Sprintf("%d", b.Port),
			controller,
		})
	}
	setTableRows(&v.table, rows)
}

// Loading returns true until the first data fetch completes.
func (v *ClusterView) Loading() bool { return v.loading }

// Refresh returns a command to refresh.
func (v *ClusterView) Refresh() tea.Cmd { return v.refresh() }

func (v *ClusterView) refresh() tea.Cmd {
	return func() tea.Msg {
		ctx := context.Background()
		latency, pingErr := v.client.Ping(ctx)
		_ = pingErr // best-effort — cluster fetch below will surface errors
		cluster, err := v.client.FetchCluster(ctx)
		return ClusterRefreshMsg{
			Cluster: cluster,
			Err:     err,
			Latency: latency.Round(time.Millisecond).String(),
		}
	}
}
