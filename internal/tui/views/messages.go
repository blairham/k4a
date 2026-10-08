// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package views

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"charm.land/bubbles/v2/table"
	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"

	"github.com/blairham/k4a/internal/kafka"
	"github.com/blairham/k4a/internal/tui/style"
)

const maxMessages = 1000

// MessageMsg carries a single consumed message.
type MessageMsg kafka.ConsumedMessage

// ConsumerReadyMsg signals that the initial message fetch has completed.
type ConsumerReadyMsg struct{}

// ConsumerErrorMsg carries a fatal error from the consumer (e.g. credential failure).
type ConsumerErrorMsg struct{ Err error }

// MessagesView shows a live tail of messages from a topic.
type MessagesView struct {
	err             error
	cancel          context.CancelFunc
	msgCh           <-chan kafka.ConsumedMessage
	errCh           <-chan error
	readyCh         <-chan struct{}
	client          *kafka.Client
	topic           string
	filter          string
	partitions      []int
	messages        []kafka.ConsumedMessage
	table           table.Model
	valueOffset     int
	height          int
	width           int
	xOffset         int
	partitionFilter int
	follow          bool
	loading         bool
}

// Fixed-width columns for messages. VALUE gets remaining space.
var msgFixedCols = []table.Column{
	{Title: "OFFSET", Width: 10},
	{Title: "PARTITION", Width: 10},
	{Title: "TIMESTAMP", Width: 26},
	{Title: "KEY", Width: 20},
}

func buildMsgColumns(totalWidth, xOffset int) []table.Column {
	cols := make([]table.Column, 0, len(msgFixedCols)+1)
	consumed := xOffset
	for _, c := range msgFixedCols {
		colTotal := c.Width + 2 // +2 for cell padding
		switch {
		case consumed >= colTotal:
			// Column fully scrolled off — hide it.
			consumed -= colTotal
			cols = append(cols, table.Column{Title: c.Title, Width: 0})
		case consumed > 0:
			// Column partially scrolled — shrink it.
			remaining := c.Width - consumed
			if remaining < 0 {
				remaining = 0
			}
			consumed = 0
			cols = append(cols, table.Column{Title: c.Title, Width: remaining})
		default:
			cols = append(cols, c)
		}
	}
	// VALUE gets all remaining space. Floor at 0 (not 20) — clamping
	// VALUE to 20 when totalWidth was small caused sum(col.Width+2) >
	// totalWidth, which the bubbles table's outer viewport then
	// wrapped to two visible lines per row. Users with narrow
	// terminals saw blank rows between data rows. If the user wants
	// more VALUE visibility they can scroll horizontally with the
	// existing xOffset, or widen their terminal.
	fixedWidth := 0
	for _, c := range cols {
		if c.Width > 0 {
			fixedWidth += c.Width + 2
		}
	}
	valueWidth := totalWidth - fixedWidth - 2
	if valueWidth < 0 {
		valueWidth = 0
	}
	cols = append(cols, table.Column{Title: "VALUE", Width: valueWidth})
	return cols
}

// maxFixedColsWidth returns the total width of all fixed columns (including padding).
func maxFixedColsWidth() int {
	total := 0
	for _, c := range msgFixedCols {
		total += c.Width + 2
	}
	return total
}

// NewMessagesView creates a new messages view.
func NewMessagesView(client *kafka.Client, topic string) *MessagesView {
	cols := buildMsgColumns(80, 0)

	t := table.New(
		table.WithColumns(cols),
		table.WithFocused(true),
		table.WithStyles(tableStyles()),
		table.WithKeyMap(tableKeyMap()),
	)

	return &MessagesView{
		client:          client,
		topic:           topic,
		partitionFilter: -1,
		follow:          true,
		table:           t,
		loading:         true,
	}
}

// Init starts consuming messages.
func (v *MessagesView) Init() tea.Cmd {
	ctx, cancel := context.WithCancel(context.Background())
	v.cancel = cancel

	// Seed partition list from metadata so all partitions are available
	// for filtering even before messages arrive.
	if ids, err := v.client.FetchPartitionIDs(ctx, v.topic); err == nil {
		v.partitions = ids
	}

	v.msgCh, v.errCh, v.readyCh = v.client.Consume(ctx, v.topic)

	return tea.Batch(
		v.waitForMessage(),
		v.waitForReady(),
		v.waitForError(),
	)
}

// Stop cancels the consumer.
func (v *MessagesView) Stop() {
	if v.cancel != nil {
		v.cancel()
	}
}

// Update handles messages.
func (v *MessagesView) Update(msg tea.Msg) tea.Cmd {
	switch msg := msg.(type) {
	case ConsumerErrorMsg:
		v.loading = false
		v.err = kafka.FormatUserError(msg.Err)
		return nil
	case ConsumerReadyMsg:
		v.loading = false
		return nil
	case MessageMsg:
		v.loading = false
		m := kafka.ConsumedMessage(msg)
		v.messages = append(v.messages, m)
		v.trackPartition(m.Partition)

		// Drain any additional pending messages from the channel to batch them.
		v.drainPending()

		// Cap the in-memory buffer to maxMessages.
		if len(v.messages) > maxMessages {
			v.messages = v.messages[len(v.messages)-maxMessages:]
		}
		v.rebuildRows()
		if v.follow {
			v.table.GotoTop()
		}
		return v.waitForMessage()
	}
	return nil
}

// drainPending reads all immediately available messages from the channel
// without blocking, to batch initial fetch results into a single render.
func (v *MessagesView) drainPending() {
	for {
		select {
		case msg, ok := <-v.msgCh:
			if !ok {
				return
			}
			v.messages = append(v.messages, msg)
			v.trackPartition(msg.Partition)
		default:
			return
		}
	}
}

// UpdateTable delegates to the bubbles table.
func (v *MessagesView) UpdateTable(msg tea.Msg) tea.Cmd {
	var cmd tea.Cmd
	v.table, cmd = v.table.Update(msg)
	return cmd
}

// Resize updates table dimensions, recalculates VALUE column, and reapplies styles.
func (v *MessagesView) Resize(width, height int) {
	v.width = width
	v.height = height
	v.table.SetColumns(buildMsgColumns(width, v.xOffset))
	v.table.SetWidth(width)
	v.table.SetHeight(height)
	v.table.SetStyles(tableStylesWithWidth(width))
}

// Count returns the visible (filtered) message count.
func (v *MessagesView) Count() int { return len(v.table.Rows()) }

// Topic returns the topic this view is consuming.
func (v *MessagesView) Topic() string { return v.topic }

// Filter returns the current in-memory regex filter, or "" when none is set.
func (v *MessagesView) Filter() string { return v.filter }

// HandleKey processes non-table keys.
func (v *MessagesView) HandleKey(key string) (string, string) {
	switch key {
	case KeyEnter:
		// Get selected message (rows are reversed, so cursor 0 = newest = last in v.messages).
		idx := v.table.Cursor()
		msgIdx := len(v.messages) - 1 - idx
		if msgIdx >= 0 && msgIdx < len(v.messages) {
			return "message_detail", fmt.Sprintf("%d", msgIdx)
		}
	case "s":
		// In the messages list, `s` opens search. Saving a single
		// message is available from the message-detail view (also
		// bound to `s`) after drilling in with Enter.
		return "search", v.topic

	case "o":
		// Switch to topic overview.
		return "topic_detail", v.topic
	case "p", "P", "a":
		v.handlePartitionKey(key)
	default:
		v.handleNavKey(key)
	}
	return "", ""
}

// handlePartitionKey steps the partition filter forward (p) or back (P), or
// clears it (a).
func (v *MessagesView) handlePartitionKey(key string) {
	switch key {
	case "p":
		v.cyclePartitionForward()
	case "P":
		v.cyclePartitionBackward()
	case "a":
		if v.partitionFilter < 0 {
			return
		}
		v.partitionFilter = -1
	}
	v.rebuildRows()
	if v.follow {
		v.table.GotoTop()
	}
}

// handleNavKey handles follow mode and scrolling. Moving off the newest row
// leaves follow mode; jumping back to it rejoins.
func (v *MessagesView) handleNavKey(key string) {
	switch key {
	case "f":
		v.follow = !v.follow
		if v.follow {
			v.table.GotoTop()
		}
	case "g", "home":
		v.follow = true
		v.table.GotoTop()
	case "G", "end":
		v.follow = false
	case "up", "down", "j", "k", "pgup", "pgdown", "ctrl+f", "ctrl+b":
		v.follow = false
	case "h", "left":
		if v.valueOffset > 0 {
			v.valueOffset--
			v.rebuildRows()
		} else if v.xOffset > 0 {
			v.xOffset--
			v.applyColumns()
		}
	case "l", "right":
		maxColOffset := maxFixedColsWidth()
		if v.xOffset < maxColOffset {
			v.xOffset++
			v.applyColumns()
		} else {
			v.valueOffset++
			v.rebuildRows()
		}
	}
}

// GetMessage returns a message by index.
func (v *MessagesView) GetMessage(idx int) *kafka.ConsumedMessage {
	if idx >= 0 && idx < len(v.messages) {
		return &v.messages[idx]
	}
	return nil
}

// Loading returns true until the first message is received.
func (v *MessagesView) Loading() bool { return v.loading }

// Refresh is a no-op — messages are consumed via streaming, not refresh.
func (v *MessagesView) Refresh() tea.Cmd { return nil }

// SetFilter narrows the in-memory messages by regex over key + value.
// Filter is purely local; a full-topic scan is available via Ctrl+F.
func (v *MessagesView) SetFilter(filter string) {
	v.filter = filter
	v.rebuildRows()
}

// View renders the view.
func (v *MessagesView) View() string {
	if v.err != nil {
		return style.Error.Render(fmt.Sprintf("  Error: %v", v.err))
	}
	if v.table.Height() != v.height {
		v.table.SetHeight(v.height)
	}

	if !v.loading && len(v.table.Rows()) == 0 {
		var msg string
		switch {
		case v.filter != "":
			msg = fmt.Sprintf(
				"No matches for /%s in the message buffer — press Ctrl+F to search the whole topic",
				v.filter,
			)
		default:
			msg = "No messages in this topic. New messages will appear here."
		}
		return lipgloss.NewStyle().
			Width(v.width).
			Height(v.height).
			Align(lipgloss.Center, lipgloss.Center).
			Render(style.Muted.Render(msg))
	}
	return fixSelectedRow(v.table.View())
}

// applyColumns recalculates columns with the current scroll offset.
func (v *MessagesView) applyColumns() {
	v.table.SetColumns(buildMsgColumns(v.width, v.xOffset))
}

func (v *MessagesView) rebuildRows() {
	rows := make([]table.Row, 0, len(v.messages))
	f := parseFilter(v.filter)
	// Reverse order: newest first.
	for i := len(v.messages) - 1; i >= 0; i-- {
		m := v.messages[i]
		// Apply partition filter.
		if v.partitionFilter >= 0 && m.Partition != v.partitionFilter {
			continue
		}
		// Apply regex filter across key and value. A leading "!" negates.
		if !f.Empty() && !f.MatchesAny(m.Key, m.Value) {
			continue
		}
		value := strings.ReplaceAll(m.Value, "\n", " ")
		value = strings.ReplaceAll(value, "\t", " ")
		// Apply value scroll offset.
		if v.valueOffset > 0 && v.valueOffset < len(value) {
			value = value[v.valueOffset:]
		} else if v.valueOffset >= len(value) && value != "" {
			value = ""
		}
		rows = append(rows, table.Row{
			fmt.Sprintf("%d", m.Offset),
			fmt.Sprintf("%d", m.Partition),
			m.Time.Format("1/2/2006, 15:04:05.000"),
			m.Key,
			value,
		})
	}
	setTableRows(&v.table, rows)
}

// PartitionFilter returns the current partition filter, or -1 for all.
func (v *MessagesView) PartitionFilter() int { return v.partitionFilter }

func (v *MessagesView) trackPartition(p int) {
	if !slices.Contains(v.partitions, p) {
		v.partitions = append(v.partitions, p)
		slices.Sort(v.partitions)
	}
}

func (v *MessagesView) cyclePartitionForward() {
	if len(v.partitions) == 0 {
		return
	}
	if v.partitionFilter < 0 {
		v.partitionFilter = v.partitions[0]
		return
	}
	idx := slices.Index(v.partitions, v.partitionFilter)
	if idx < 0 || idx >= len(v.partitions)-1 {
		v.partitionFilter = v.partitions[0]
	} else {
		v.partitionFilter = v.partitions[idx+1]
	}
}

func (v *MessagesView) cyclePartitionBackward() {
	if len(v.partitions) == 0 {
		return
	}
	if v.partitionFilter < 0 {
		v.partitionFilter = v.partitions[len(v.partitions)-1]
		return
	}
	idx := slices.Index(v.partitions, v.partitionFilter)
	if idx <= 0 {
		v.partitionFilter = v.partitions[len(v.partitions)-1]
	} else {
		v.partitionFilter = v.partitions[idx-1]
	}
}

func (v *MessagesView) waitForReady() tea.Cmd {
	ch := v.readyCh
	if ch == nil {
		return nil
	}
	return func() tea.Msg {
		<-ch
		return ConsumerReadyMsg{}
	}
}

func (v *MessagesView) waitForError() tea.Cmd {
	ch := v.errCh
	if ch == nil {
		return nil
	}
	return func() tea.Msg {
		err, ok := <-ch
		if !ok || err == nil {
			return nil
		}
		return ConsumerErrorMsg{Err: err}
	}
}

func (v *MessagesView) waitForMessage() tea.Cmd {
	ch := v.msgCh
	if ch == nil {
		return nil
	}
	return func() tea.Msg {
		msg, ok := <-ch
		if !ok {
			return nil
		}
		return MessageMsg(msg)
	}
}

// formatElapsed renders a duration for scan progress: sub-second as "Nms",
// otherwise "Ns" (one decimal under 10s, integer otherwise) or "MmSs".
func formatElapsed(d time.Duration) string {
	switch {
	case d < time.Second:
		return fmt.Sprintf("%dms", d.Milliseconds())
	case d < 10*time.Second:
		return fmt.Sprintf("%.1fs", d.Seconds())
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	default:
		m := int(d / time.Minute)
		s := int((d % time.Minute) / time.Second)
		return fmt.Sprintf("%dm%02ds", m, s)
	}
}
