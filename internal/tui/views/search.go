// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package views

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"charm.land/bubbles/v2/table"
	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"

	"github.com/blairham/k4a/internal/kafka"
	"github.com/blairham/k4a/internal/tui/style"
)

// SearchMatchMsg carries one match streamed by the active scan.
type SearchMatchMsg struct {
	Msg    kafka.ConsumedMessage
	ScanID int
}

// SearchProgressMsg carries a periodic scan-progress snapshot.
type SearchProgressMsg struct {
	Prog   kafka.DeepSearchProgress
	ScanID int
}

// SearchErrMsg carries a fatal scan error.
type SearchErrMsg struct {
	Err    error
	ScanID int
}

// SearchTerminalMsg signals that the scan is fully drained: all three of its
// channels have closed and no further matches or progress can arrive.
type SearchTerminalMsg struct{ ScanID int }

// searchChannel identifies one of the scan's three result channels.
type searchChannel int

const (
	searchChanMatches searchChannel = iota
	searchChanProgress
	searchChanErrors
)

// SearchChanClosedMsg reports that ONE of the scan's channels closed. The scan
// is not over until all three have: the producer closes them in the order
// errors, progress, matches (LIFO defers), so treating the first close as the
// end drops matches still buffered in the others.
type SearchChanClosedMsg struct {
	ScanID int
	Which  searchChannel
}

// searchState tracks the view state machine.
type searchState int

const (
	searchStateRunning searchState = iota
	searchStateDone
	searchStateCapped
	// searchStateTruncated means the scan could not read its whole range, so
	// the results shown are missing records the window asked for. It must be
	// visually distinct from Done — a short read that looks complete is what
	// made the chunked-scan bug dangerous rather than merely annoying.
	searchStateTruncated
	searchStateErrored
)

// SearchFunc runs a deep search and returns the standard three-channel
// shape. The TUI passes in either *kafka.Client.DeepSearch directly or a
// cache-wrapped variant; SearchView doesn't care which.
type SearchFunc func(
	ctx context.Context,
	topic string,
	params kafka.SearchParams,
) (<-chan kafka.ConsumedMessage, <-chan kafka.DeepSearchProgress, <-chan error)

// SearchView streams results from an active deep search.
// The query itself is collected by the App's command bar in search mode;
// SearchView is only the results pane.
type SearchView struct {
	scanErr   error
	cancel    context.CancelFunc
	matchCh   <-chan kafka.ConsumedMessage
	progCh    <-chan kafka.DeepSearchProgress
	errCh     <-chan error
	runSearch SearchFunc
	topic     string
	query     string
	matches   []kafka.ConsumedMessage
	table     table.Model
	progress  kafka.DeepSearchProgress
	state     searchState
	scanID    int
	xOffset   int
	width     int
	height    int
}

// NewSearchView starts a deep search for query against topic and returns a
// view that streams matches into a results table. query may be plain text
// or a regex; if it doesn't parse as a valid regex, it's matched as a
// literal substring (via regexp.QuoteMeta). The actual scan is delegated
// to the supplied SearchFunc — the App injects a cache-wrapped one so
// hits are instant.
func NewSearchView(run SearchFunc, topic, query string) *SearchView {
	v := &SearchView{
		runSearch: run,
		topic:     topic,
		query:     query,
		state:     searchStateRunning,
	}

	cols := buildMsgColumns(80, 0)
	v.table = table.New(
		table.WithColumns(cols),
		table.WithFocused(true),
		table.WithStyles(tableStyles()),
		table.WithKeyMap(tableKeyMap()),
	)
	return v
}

// Init kicks off the scan. The query was already supplied to NewSearchView,
// so there's no form state to navigate first.
func (v *SearchView) Init() tea.Cmd { return v.start() }

// Update handles streamed scan events. Every non-terminal handler re-issues
// waitForNextEvent so the channel-polling loop keeps running; only the
// terminal message stops it.
func (v *SearchView) Update(msg tea.Msg) tea.Cmd {
	switch m := msg.(type) {
	case SearchMatchMsg:
		if m.ScanID != v.scanID {
			return v.waitForNextEvent()
		}
		v.matches = append(v.matches, m.Msg)
		v.appendRow(m.Msg)
		return v.waitForNextEvent()
	case SearchProgressMsg:
		if m.ScanID != v.scanID {
			return v.waitForNextEvent()
		}
		v.applyProgress(m.Prog)
		return v.waitForNextEvent()
	case SearchErrMsg:
		if m.ScanID != v.scanID {
			return v.waitForNextEvent()
		}
		v.state = searchStateErrored
		v.scanErr = m.Err
		return v.waitForNextEvent()
	case SearchChanClosedMsg:
		if m.ScanID != v.scanID {
			return nil
		}
		return v.channelClosed(m.Which)
	case SearchTerminalMsg:
		if m.ScanID != v.scanID {
			return nil
		}
		if v.state == searchStateRunning {
			v.state = searchStateDone
		}
		return nil
	}
	return nil
}

// applyProgress records a progress frame and moves the state on: capped and
// truncated are sticky warnings, and a done frame finishes a running scan.
func (v *SearchView) applyProgress(prog kafka.DeepSearchProgress) {
	v.progress = prog
	switch {
	case prog.Capped:
		v.state = searchStateCapped
	case prog.Truncated && v.state != searchStateErrored:
		v.state = searchStateTruncated
	}
	if prog.Done && v.state == searchStateRunning {
		v.state = searchStateDone
	}
}

// channelClosed forgets the closed channel and keeps listening while any of
// the others is still open; once all are closed, a running scan is done.
func (v *SearchView) channelClosed(which searchChannel) tea.Cmd {
	switch which {
	case searchChanMatches:
		v.matchCh = nil
	case searchChanProgress:
		v.progCh = nil
	case searchChanErrors:
		v.errCh = nil
	}
	if v.matchCh != nil || v.progCh != nil || v.errCh != nil {
		// Other channels still have data to drain — keep listening.
		return v.waitForNextEvent()
	}
	if v.state == searchStateRunning {
		v.state = searchStateDone
	}
	return nil
}

// UpdateTable delegates table navigation.
func (v *SearchView) UpdateTable(msg tea.Msg) tea.Cmd {
	var cmd tea.Cmd
	v.table, cmd = v.table.Update(msg)
	return cmd
}

// Resize updates table dimensions. The table is sized by View() at render
// time so it can reclaim the two header rows once the scan finishes.
func (v *SearchView) Resize(width, height int) {
	v.width = width
	v.height = height
	v.table.SetColumns(buildMsgColumns(width, v.xOffset))
	v.table.SetWidth(width)
	v.table.SetStyles(tableStylesWithWidth(width))
}

// headerVisible reports whether the two-line search header should render.
// We show it while the scan is in flight, on error, and whenever the result
// set is incomplete — capped and truncated both mean "what you see is not the
// whole answer", and that warning is the most important thing on the screen.
// Only a clean, complete scan hides the header to reclaim the two rows; there
// the results table really does tell the whole story.
func (v *SearchView) headerVisible() bool {
	switch v.state {
	case searchStateRunning, searchStateErrored, searchStateCapped, searchStateTruncated:
		return true
	case searchStateDone:
		return false
	}
	return false
}

// Count returns the visible match count.
func (v *SearchView) Count() int { return len(v.matches) }

// Query returns the original search query string the user typed.
func (v *SearchView) Query() string { return v.query }

// Topic returns the topic being searched.
func (v *SearchView) Topic() string { return v.topic }

// HandleKey processes non-table keys for the search results view.
func (v *SearchView) HandleKey(key string) (string, string) {
	if key == KeyEnter {
		idx := v.table.Cursor()
		if idx >= 0 && idx < len(v.matches) {
			return "search_message_detail", fmt.Sprintf("%d", idx)
		}
	}
	return "", ""
}

// GetMatch returns the match at idx, or false if out of range.
func (v *SearchView) GetMatch(idx int) (kafka.ConsumedMessage, bool) {
	if idx < 0 || idx >= len(v.matches) {
		return kafka.ConsumedMessage{}, false
	}
	return v.matches[idx], true
}

// Loading is false — progress is rendered via the header.
func (v *SearchView) Loading() bool { return false }

// SetFilter narrows the rendered results in-memory.
func (v *SearchView) SetFilter(_ string) {}

// Refresh is a no-op for search results. The App ticks every active view's
// Refresh on a poll interval to keep live data current; for a one-shot
// scan that would mean restarting the search on every tick. Users re-run a
// search by re-entering the search bar (Ctrl+F).
func (v *SearchView) Refresh() tea.Cmd { return nil }

// Stop cancels any in-flight scan.
func (v *SearchView) Stop() {
	if v.cancel != nil {
		v.cancel()
		v.cancel = nil
	}
}

// View renders the search results, with a bordered sub-box holding the
// two-line search header while a scan is running (or has errored), and
// a full-height table once it's finished.
func (v *SearchView) View() string {
	if v.headerVisible() {
		header := v.renderHeaderBox()
		hh := lipgloss.Height(header)
		v.table.SetHeight(max(v.height-hh, 0))
		return header + "\n" + fixSelectedRow(v.table.View())
	}
	v.table.SetHeight(v.height)
	return fixSelectedRow(v.table.View())
}

// renderHeaderBox wraps the two-line search header in a rounded border
// so it reads as its own sub-section above the table.
func (v *SearchView) renderHeaderBox() string {
	width := v.width
	if width <= 0 {
		width = 80
	}
	// Box has 1 char left/right border + 1 char left/right padding.
	innerWidth := max(width-4, 1)

	dim := lipgloss.NewStyle().Foreground(style.ColorGray)
	bold := lipgloss.NewStyle().Foreground(style.ColorLightSkyBlue).Bold(true)
	inner := lipgloss.NewStyle().Width(innerWidth)

	line1 := bold.Render("search: ") + dim.Render("/"+v.query+" · "+v.topic)
	line2 := dim.Render(v.progressFragments())

	body := inner.Render(line1) + "\n" + inner.Render(line2)
	return lipgloss.NewStyle().
		Width(width-2).
		BorderStyle(lipgloss.RoundedBorder()).
		BorderForeground(style.ColorBorder).
		Padding(0, 1).
		Render(body)
}

// SelectedMessage returns the currently highlighted match, if any.
func (v *SearchView) SelectedMessage() (kafka.ConsumedMessage, bool) {
	if len(v.matches) == 0 {
		return kafka.ConsumedMessage{}, false
	}
	idx := v.table.Cursor()
	if idx < 0 || idx >= len(v.matches) {
		return kafka.ConsumedMessage{}, false
	}
	return v.matches[idx], true
}

func (v *SearchView) progressFragments() string {
	switch v.state {
	case searchStateRunning:
		var pct int64
		if v.progress.Total > 0 {
			pct = 100 * v.progress.Scanned / v.progress.Total
		}
		return fmt.Sprintf(
			"running · %d%% · %s msgs · %s · %s · %d/%d partitions · %d matches",
			pct,
			formatCount(v.progress.Scanned),
			formatBytes(v.progress.Bytes),
			formatElapsed(v.progress.Elapsed),
			v.progress.DonePartitions, v.progress.Partitions,
			v.progress.Matches,
		)
	case searchStateCapped:
		return fmt.Sprintf(
			"stopped at %d matches · %s · narrow your search",
			v.progress.Matches, formatElapsed(v.progress.Elapsed),
		)
	case searchStateTruncated:
		return fmt.Sprintf(
			"INCOMPLETE · %d matches · %s · could not read the whole range — results are missing records",
			v.progress.Matches, formatElapsed(v.progress.Elapsed),
		)
	case searchStateDone:
		return fmt.Sprintf(
			"%s · %d matches · %s msgs · %s · %s",
			v.progress.Source.String(), // "scanned" or "cached" — set by the scanner/wrapper
			v.progress.Matches,
			formatCount(v.progress.Scanned),
			formatBytes(v.progress.Bytes),
			formatElapsed(v.progress.Elapsed),
		)
	case searchStateErrored:
		if v.scanErr != nil {
			return "error: " + v.scanErr.Error()
		}
		return "error"
	}
	return ""
}

func (v *SearchView) start() tea.Cmd {
	re, err := compileSearchQuery(v.query)
	if err != nil {
		v.state = searchStateErrored
		v.scanErr = err
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	v.cancel = cancel
	v.scanID++
	v.state = searchStateRunning

	matchCh, progCh, errCh := v.runSearch(ctx, v.topic, kafka.SearchParams{
		Pattern: re,
		Scope:   kafka.ScopeKey | kafka.ScopeValue,
	})
	v.matchCh = matchCh
	v.progCh = progCh
	v.errCh = errCh
	return v.waitForNextEvent()
}

func (v *SearchView) waitForNextEvent() tea.Cmd {
	// A nil channel blocks forever in a select, which is exactly what we want
	// for one that has already closed — but if every channel is nil there is
	// nothing left to wait for and selecting would deadlock the Cmd goroutine.
	if v.matchCh == nil && v.progCh == nil && v.errCh == nil {
		return nil
	}
	matchCh := v.matchCh
	progCh := v.progCh
	errCh := v.errCh
	id := v.scanID
	return func() tea.Msg {
		select {
		case m, ok := <-matchCh:
			if !ok {
				return SearchChanClosedMsg{ScanID: id, Which: searchChanMatches}
			}
			return SearchMatchMsg{ScanID: id, Msg: m}
		case p, ok := <-progCh:
			if !ok {
				return SearchChanClosedMsg{ScanID: id, Which: searchChanProgress}
			}
			return SearchProgressMsg{ScanID: id, Prog: p}
		case err, ok := <-errCh:
			if !ok {
				return SearchChanClosedMsg{ScanID: id, Which: searchChanErrors}
			}
			if err == nil {
				// Not expected — the scanner only sends real errors. Keep
				// listening rather than returning a nil Msg, which would
				// dispatch nothing and strand the view mid-scan.
				return SearchChanClosedMsg{ScanID: id, Which: searchChanErrors}
			}
			return SearchErrMsg{ScanID: id, Err: err}
		}
	}
}

// appendRow appends a single match row to the table without rebuilding the
// existing rows. With thousands of matches streaming in, a full rebuild per
// match dominates render time and causes input lag.
func (v *SearchView) appendRow(m kafka.ConsumedMessage) {
	value := strings.ReplaceAll(m.Value, "\n", " ")
	value = strings.ReplaceAll(value, "\t", " ")
	row := table.Row{
		fmt.Sprintf("%d", m.Offset),
		fmt.Sprintf("%d", m.Partition),
		m.Time.Format("1/2/2006, 15:04:05.000"),
		m.Key,
		value,
	}
	setTableRows(&v.table, append(v.table.Rows(), row))
}

// compileSearchQuery interprets the bar input. It's tried as a regex first;
// if invalid, the input is matched as a literal substring (via QuoteMeta).
// Case-insensitive by default — users searching "ORDER" usually want "order"
// to match too. To search case-sensitively, prefix the regex with `(?-i)`.
func compileSearchQuery(q string) (*regexp.Regexp, error) {
	q = strings.TrimSpace(q)
	if q == "" {
		return nil, fmt.Errorf("search query is empty")
	}
	pat := q
	if !strings.HasPrefix(pat, "(?") {
		pat = "(?i)" + pat
	}
	if re, err := regexp.Compile(pat); err == nil {
		return re, nil
	}
	// Invalid regex — treat the raw input as a literal substring.
	literal := "(?i)" + regexp.QuoteMeta(q)
	return regexp.Compile(literal)
}
