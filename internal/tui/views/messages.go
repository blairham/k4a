// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package views

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"charm.land/bubbles/v2/table"
	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"

	"github.com/blairham/k4a/internal/kafka"
	"github.com/blairham/k4a/internal/timebound"
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
	decoder         Decoder
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
	// seek, when set, replaced the live tail: the view reads from that
	// point and keeps the first maxMessages records (capped) instead of the
	// newest. nil is the live tail.
	seek *kafka.Seek
	// gen counts consumers started by this view, so a record still in
	// flight from one a seek replaced is dropped rather than mixed in.
	gen int
	// raw shows registry-decoded keys and values as they were before
	// decoding; toggled with x.
	raw    bool
	capped bool
}

// genMessageMsg is a consumed record tagged with the consumer generation
// that read it.
type genMessageMsg struct {
	msg kafka.ConsumedMessage
	gen int
}

// Decoder decodes a message's registry-framed key and value in place
// (schemareg.Registry). Views take it as an interface so they need no
// registry to be tested.
type Decoder interface {
	DecodeMessage(ctx context.Context, m *kafka.ConsumedMessage)
}

// decodeWait bounds how long one record waits for a schema the registry has
// not answered for yet. The lookup carries on in the background, so later
// records with that schema decode; this one shows raw with a reason rather
// than stalling the tail behind a slow registry.
const decodeWait = 3 * time.Second

// KeyToggleRaw switches Messages and Message Detail between decoded and raw
// keys and values.
const KeyToggleRaw = "x"

// KeySeek opens the Messages seek prompt; ActionSeek is what it asks the app
// for, with the topic as its param.
const (
	KeySeek    = "t"
	ActionSeek = "seek_messages"
)

// seekLive is the seek prompt's word for the live tail.
const seekLive = "live"

// ParseSeek reads the seek prompt: "live" (or nothing) for the live tail,
// "b" / "beginning" / "start" for the log start, a number for an offset in
// every partition, or a time internal/timebound accepts (1h, 7d, RFC3339).
func ParseSeek(input string, now time.Time) (*kafka.Seek, error) {
	in := strings.ToLower(strings.TrimSpace(input))
	switch in {
	case "", seekLive:
		return nil, nil //nolint:nilnil // nil is the live tail, not a missing seek
	case "b", "beginning", "start":
		return &kafka.Seek{Mode: kafka.SeekStart}, nil
	}
	if n, err := strconv.ParseInt(in, 10, 64); err == nil {
		if n < 0 {
			return nil, fmt.Errorf("offset %d is negative", n)
		}
		return &kafka.Seek{Mode: kafka.SeekOffset, Offset: n}, nil
	}
	t, err := timebound.Parse(strings.TrimSpace(input), now)
	if err != nil {
		return nil, fmt.Errorf("seek to an offset, a time (1h, 7d, RFC3339), b or live: %w", err)
	}
	return &kafka.Seek{Mode: kafka.SeekTime, Time: t}, nil
}

// Fixed-width columns for messages. VALUE gets remaining space.
var msgFixedCols = []table.Column{
	{Title: "OFFSET", Width: 10},
	{Title: "PARTITION", Width: 10},
	{Title: "TIMESTAMP", Width: 26},
	{Title: "KEY", Width: 20},
}

func buildMsgColumns(totalWidth, xOffset int) []table.Column {
	return buildColumns(msgFixedCols, totalWidth, xOffset)
}

// buildColumns lays out fixed columns scrolled by xOffset, then a VALUE
// column that takes the rest of totalWidth.
func buildColumns(fixed []table.Column, totalWidth, xOffset int) []table.Column {
	cols := make([]table.Column, 0, len(fixed)+1)
	consumed := xOffset
	for _, c := range fixed {
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

// SetDecoder makes the view decode registry-framed payloads. A nil decoder
// (no registry configured) leaves every payload as consumed.
func (v *MessagesView) SetDecoder(d Decoder) { v.decoder = d }

// Raw reports whether decoded payloads are being shown raw.
func (v *MessagesView) Raw() bool { return v.raw }

// Init starts consuming messages.
func (v *MessagesView) Init() tea.Cmd {
	ctx, cancel := context.WithCancel(context.Background())
	v.cancel = cancel

	// Seed partition list from metadata so all partitions are available
	// for filtering even before messages arrive.
	if ids, err := v.client.FetchPartitionIDs(ctx, v.topic); err == nil {
		v.partitions = ids
	}

	if v.seek != nil {
		v.msgCh, v.errCh, v.readyCh = v.client.ConsumeSeek(ctx, v.topic, *v.seek)
	} else {
		v.msgCh, v.errCh, v.readyCh = v.client.Consume(ctx, v.topic)
	}
	v.msgCh = decodeStream(ctx, v.msgCh, v.decoder)

	return tea.Batch(
		v.waitForMessage(),
		v.waitForReady(),
		v.waitForError(),
	)
}

// SeekTo restarts the view at seek, or on the live tail when seek is nil:
// the old consumer stops, the buffer clears, and records the old consumer
// still had in flight are dropped (see gen).
func (v *MessagesView) SeekTo(seek *kafka.Seek) tea.Cmd {
	v.Stop()
	v.gen++
	v.seek = seek
	v.capped = false
	v.follow = seek == nil
	v.messages = nil
	v.err = nil
	v.loading = true
	v.rebuildRows()
	v.table.GotoTop()
	return v.Init()
}

// Seek returns the active seek, or nil on the live tail.
func (v *MessagesView) Seek() *kafka.Seek { return v.seek }

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
	case genMessageMsg:
		if msg.gen != v.gen {
			return nil
		}
		return v.addMessage(msg.msg)
	case MessageMsg:
		return v.addMessage(kafka.ConsumedMessage(msg))
	}
	return nil
}

// addMessage buffers m and whatever else is already waiting, then keeps
// the newest maxMessages on the live tail, or the first maxMessages after
// a seek, where the point is what follows the seek, not what is newest.
func (v *MessagesView) addMessage(m kafka.ConsumedMessage) tea.Cmd {
	if v.capped {
		return nil
	}
	v.loading = false
	v.messages = append(v.messages, m)
	v.trackPartition(m.Partition)

	// Drain any additional pending messages from the channel to batch them.
	v.drainPending()

	if len(v.messages) > maxMessages {
		if v.seek != nil {
			v.messages = v.messages[:maxMessages]
			v.capped = true
			v.Stop()
		} else {
			v.messages = v.messages[len(v.messages)-maxMessages:]
		}
	}
	v.rebuildRows()
	if v.follow {
		v.table.GotoTop()
	}
	if v.capped {
		return nil
	}
	return v.waitForMessage()
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
		// The param indexes VisibleMessages, the rows as shown: with a
		// filter on, the cursor is not an index into v.messages.
		if idx := v.table.Cursor(); idx >= 0 && idx < len(v.table.Rows()) {
			return "message_detail", fmt.Sprintf("%d", idx)
		}
	case KeyCopyValue, KeyCopyKey, KeyCopyRecord:
		if v.SelectedMessage() != nil {
			return ActionCopy, key
		}
	case KeyExport:
		return ActionExport, v.topic
	case "s":
		// In the messages list, `s` opens search. Saving a single
		// message is available from the message-detail view (also
		// bound to `s`) after drilling in with Enter.
		return "search", v.topic

	case "o":
		// Switch to topic overview.
		return "topic_detail", v.topic
	case KeySeek:
		return ActionSeek, v.topic
	case keyCtrlD:
		// The hint bar offers Purge here as it does on topic detail; the
		// app routes the action to the retention prompt (and refuses it
		// in readonly mode), so the view only names it.
		return "confirm_purge_topic", v.topic
	case "p", "P", "a":
		v.handlePartitionKey(key)
	case KeyToggleRaw:
		v.raw = !v.raw
		v.rebuildRows()
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

// VisibleMessages returns the messages as the table shows them: newest
// first, with the partition and / filters applied. It is a copy, so the
// tail can keep appending while a caller holds it.
func (v *MessagesView) VisibleMessages() []kafka.ConsumedMessage {
	f := parseFilter(v.filter)
	out := make([]kafka.ConsumedMessage, 0, len(v.messages))
	for i := len(v.messages) - 1; i >= 0; i-- {
		if v.shows(v.messages[i], f) {
			out = append(out, v.messages[i])
		}
	}
	return out
}

// SelectedMessage returns the message under the cursor, or nil.
func (v *MessagesView) SelectedMessage() *kafka.ConsumedMessage {
	visible := v.VisibleMessages()
	if idx := v.table.Cursor(); idx >= 0 && idx < len(visible) {
		return &visible[idx]
	}
	return nil
}

// shows reports whether m passes the partition and / filters.
func (v *MessagesView) shows(m kafka.ConsumedMessage, f rowFilter) bool {
	if v.partitionFilter >= 0 && m.Partition != v.partitionFilter {
		return false
	}
	return f.Empty() || f.MatchesAny(m.KeyDecoding.Shown(m.Key, v.raw), m.ValueDecoding.Shown(m.Value, v.raw))
}

// Loading returns true until the first message is received.
func (v *MessagesView) Loading() bool { return v.loading }

// Refresh is a no-op — messages are consumed via streaming, not refresh.
func (v *MessagesView) Refresh() tea.Cmd { return nil }

// SetFilter narrows the in-memory messages by regex over key + value.
// Filter is purely local; a full-topic scan is `s` (or :search).
func (v *MessagesView) SetFilter(filter string) {
	v.filter = filter
	v.rebuildRows()
}

// View renders the view.
func (v *MessagesView) View() string {
	if v.err != nil {
		return style.Error.Render(fmt.Sprintf("  Error: %v", v.err))
	}
	status := v.seekStatus()
	height := v.height
	if status != "" {
		height--
	}
	if v.table.Height() != height {
		v.table.SetHeight(height)
	}

	if !v.loading && len(v.table.Rows()) == 0 {
		var msg string
		switch {
		case v.filter != "":
			msg = fmt.Sprintf(
				"No matches for /%s in the message buffer — press s to search the whole topic",
				v.filter,
			)
		case v.seek != nil:
			msg = fmt.Sprintf("No records from %s yet. New ones will appear here; t then live returns to the tail.", v.seek)
		default:
			msg = "No messages in this topic. New messages will appear here."
		}
		return lipgloss.NewStyle().
			Width(v.width).
			Height(v.height).
			Align(lipgloss.Center, lipgloss.Center).
			Render(style.Muted.Render(msg))
	}
	if status != "" {
		return style.Muted.Render(status) + "\n" + fixSelectedRow(v.table.View())
	}
	return fixSelectedRow(v.table.View())
}

// seekStatus is the line above a seeked view saying where it started and
// how to get back to the live tail; empty on the live tail.
func (v *MessagesView) seekStatus() string {
	if v.seek == nil {
		return ""
	}
	s := "  from " + v.seek.String()
	if v.capped {
		s += fmt.Sprintf(" · first %d records, stopped", maxMessages)
	}
	return s + " · t then live returns to the tail"
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
		if !v.shows(m, f) {
			continue
		}
		key := m.KeyDecoding.Shown(m.Key, v.raw)
		shown := m.ValueDecoding.Shown(m.Value, v.raw)
		value := strings.ReplaceAll(shown, "\n", " ")
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
			key,
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
	gen := v.gen
	return func() tea.Msg {
		msg, ok := <-ch
		if !ok {
			return nil
		}
		return genMessageMsg{msg: msg, gen: gen}
	}
}

// decodeStream decodes each message from in on its own goroutine, so a
// registry lookup never runs on the UI loop. With no decoder it returns in.
func decodeStream(ctx context.Context, in <-chan kafka.ConsumedMessage, d Decoder) <-chan kafka.ConsumedMessage {
	if d == nil || in == nil {
		return in
	}
	out := make(chan kafka.ConsumedMessage, cap(in))
	go func() {
		defer close(out)
		for m := range in {
			dctx, cancel := context.WithTimeout(ctx, decodeWait)
			d.DecodeMessage(dctx, &m)
			cancel()
			select {
			case out <- m:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
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
