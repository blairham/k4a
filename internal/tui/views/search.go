// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package views

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync/atomic"

	"charm.land/bubbles/v2/table"
	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"

	"github.com/blairham/k4a/internal/jsonpath"
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
	// searchStateCanceled means the user stopped the scan: what is shown is
	// whatever arrived before, and says so.
	searchStateCanceled
)

// scanSeq numbers scans across every SearchView. Per-view numbering let a
// replaced view's late messages (scan 1) pass as the new view's scan 1.
var scanSeq atomic.Int64

func nextScanID() int { return int(scanSeq.Add(1)) }

// SearchFunc runs a deep search and returns the standard three-channel
// shape. The TUI passes in either *kafka.Client.DeepSearch directly or a
// cache-wrapped variant; SearchView doesn't care which.
type SearchFunc func(
	ctx context.Context,
	topic string,
	params kafka.SearchParams,
) (<-chan kafka.ConsumedMessage, <-chan kafka.DeepSearchProgress, <-chan error)

// TopicResolver lists the topics a glob selects, for a cross-topic search.
type TopicResolver func(ctx context.Context, glob string) ([]string, error)

// SearchView streams results from an active deep search.
// The query itself is collected by the App's command bar in search mode;
// SearchView is only the results pane. Its topic may be a glob
// (kafka.IsTopicGlob), which searches every topic it selects; see
// search_topics.go.
type SearchView struct {
	scanErr   error
	cancel    context.CancelFunc
	matchCh   <-chan kafka.ConsumedMessage
	progCh    <-chan kafka.DeepSearchProgress
	errCh     <-chan error
	runSearch SearchFunc
	resolve   TopicResolver
	perTopic  map[string]*topicCoverage
	topic     string
	query     string
	filter    string
	matches   []kafka.ConsumedMessage
	visible   []int    // indexes into matches that pass filter, in table order
	topics    []string // what a glob resolved to, in display order
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

	cols := v.columns(80)
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
	// A message from an earlier scan came from that scan's own reader; the
	// current scan has its reader already, so a stale one starts nothing.
	case SearchMatchMsg:
		if m.ScanID != v.scanID {
			return nil
		}
		v.matches = append(v.matches, m.Msg)
		if f := parseFilter(v.filter); f.Empty() || f.MatchesAny(m.Msg.Key, m.Msg.Value) {
			v.visible = append(v.visible, len(v.matches)-1)
			v.appendRow(m.Msg)
		}
		return v.waitForNextEvent()
	case SearchProgressMsg:
		if m.ScanID != v.scanID {
			return nil
		}
		if v.Multi() {
			v.topicProgress(m.Prog)
		} else if v.state != searchStateCanceled {
			v.applyProgress(m.Prog)
		}
		return v.waitForNextEvent()
	case SearchErrMsg:
		if m.ScanID != v.scanID {
			return nil
		}
		v.applyErr(m.Err)
		return v.waitForNextEvent()
	case SearchTopicsMsg:
		if m.ScanID != v.scanID {
			return nil
		}
		return v.topicsResolved(m)
	case SearchChanClosedMsg:
		if m.ScanID != v.scanID {
			return nil
		}
		return v.channelClosed(m.Which)
	case SearchTerminalMsg:
		if m.ScanID != v.scanID {
			return nil
		}
		v.finish()
		return nil
	}
	return nil
}

// applyErr records a scan error. After the user stopped the scan, the
// cancellation surfacing as an error is expected and not shown; in a
// cross-topic search, one topic's error belongs to that topic.
func (v *SearchView) applyErr(err error) {
	if v.state == searchStateCanceled {
		return
	}
	var te *kafka.TopicError
	if v.Multi() && errors.As(err, &te) {
		v.topicErr(te)
		return
	}
	v.state = searchStateErrored
	v.scanErr = err
}

// finish moves a running scan to its end state once every channel closed.
func (v *SearchView) finish() {
	if v.state != searchStateRunning {
		return
	}
	if v.Multi() {
		v.finishTopics()
		return
	}
	v.state = searchStateDone
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
	v.finish()
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
	v.table.SetColumns(v.columns(width))
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
	if v.Multi() {
		return true // the per-topic coverage line is the answer's provenance
	}
	switch v.state {
	case searchStateRunning, searchStateErrored, searchStateCapped, searchStateTruncated,
		searchStateCanceled:
		return true
	case searchStateDone:
		return false
	}
	return false
}

// Count returns the visible match count.
func (v *SearchView) Count() int { return len(v.visible) }

// Query returns the original search query string the user typed.
func (v *SearchView) Query() string { return v.query }

// Topic returns the topic being searched.
func (v *SearchView) Topic() string { return v.topic }

// Keys the search results view binds beyond enter and export.
const (
	KeyRerunSearch = "R"
	KeyEditSearch  = "s"
	KeyStopSearch  = "x"
)

// Actions the search results view asks the App to run.
const (
	ActionRerunSearch = "search_rerun"
	ActionEditSearch  = "search_edit"
	ActionStopSearch  = "search_stop"
)

// Running reports whether the scan is still in flight.
func (v *SearchView) Running() bool { return v.state == searchStateRunning }

// Cancel stops a running scan and keeps what it found, marked as partial.
// It reports whether there was a scan to stop.
func (v *SearchView) Cancel() bool {
	if v.state != searchStateRunning {
		return false
	}
	v.Stop()
	v.state = searchStateCanceled
	return true
}

// StatusLine is the one-line state of the search, for the App to show while
// the scan runs in the background behind another view.
func (v *SearchView) StatusLine() string {
	return fmt.Sprintf("search /%s in %s · %s", v.query, v.topic, v.progressFragments())
}

// HandleKey processes non-table keys for the search results view.
func (v *SearchView) HandleKey(key string) (string, string) {
	switch key {
	case KeyEnter:
		idx := v.table.Cursor()
		if idx >= 0 && idx < len(v.visible) {
			return "search_message_detail", fmt.Sprintf("%d", idx)
		}
	case KeyExport:
		return ActionExport, v.topic
	case KeyCopyValue, KeyCopyKey, KeyCopyRecord:
		if _, ok := v.SelectedMessage(); ok {
			return ActionCopy, key
		}
	case KeyRerunSearch:
		return ActionRerunSearch, v.topic
	case KeyEditSearch:
		return ActionEditSearch, v.topic
	case KeyStopSearch:
		if v.Running() {
			return ActionStopSearch, v.topic
		}
	}
	return "", ""
}

// SetQuery replaces the query the next Rerun searches for.
func (v *SearchView) SetQuery(q string) { v.query = q }

// Rerun cancels the scan in flight, if any, and starts the query again from
// an empty result set. Messages from the old scan are dropped by scanID.
func (v *SearchView) Rerun() tea.Cmd {
	v.Stop()
	v.matches, v.visible = nil, nil
	v.topics, v.perTopic = nil, nil
	v.progress = kafka.DeepSearchProgress{}
	v.scanErr = nil
	setTableRows(&v.table, nil)
	v.table.GotoTop()
	return v.start()
}

// VisibleMatches returns the matches that pass the filter, in table order.
func (v *SearchView) VisibleMatches() []kafka.ConsumedMessage {
	out := make([]kafka.ConsumedMessage, 0, len(v.visible))
	for _, i := range v.visible {
		out = append(out, v.matches[i])
	}
	return out
}

// Matches returns a copy of the matches so far, in the order shown.
func (v *SearchView) Matches() []kafka.ConsumedMessage {
	return append([]kafka.ConsumedMessage(nil), v.matches...)
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

// SetFilter narrows the rendered results to matches whose key or value
// passes filter; the scan itself is unchanged and keeps streaming.
func (v *SearchView) SetFilter(filter string) {
	v.filter = filter
	v.visible = v.visible[:0]
	rows := make([]table.Row, 0, len(v.matches))
	f := parseFilter(filter)
	for i, m := range v.matches {
		if !f.Empty() && !f.MatchesAny(m.Key, m.Value) {
			continue
		}
		v.visible = append(v.visible, i)
		rows = append(rows, v.row(m))
	}
	setTableRows(&v.table, rows)
}

// Refresh is a no-op for search results. The App ticks every active view's
// Refresh on a poll interval to keep live data current; for a one-shot
// scan that would mean restarting the search on every tick. R re-runs it on
// demand (Rerun), and s edits the query first.
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

	where := v.topic
	if v.Multi() && len(v.topics) > 0 {
		where += fmt.Sprintf(" · %d topics", len(v.topics))
	}
	line1 := bold.Render("search: ") + dim.Render("/"+v.query+" · "+where)
	line2 := dim.Render(v.progressFragments())

	body := inner.Render(line1) + "\n" + inner.Render(line2)
	if v.Multi() {
		body += "\n" + inner.Render(dim.Render(v.coverageLine()))
	}
	return lipgloss.NewStyle().
		Width(width-2).
		BorderStyle(lipgloss.RoundedBorder()).
		BorderForeground(style.ColorBorder).
		Padding(0, 1).
		Render(body)
}

// SelectedMessage returns the currently highlighted match, if any.
func (v *SearchView) SelectedMessage() (kafka.ConsumedMessage, bool) {
	idx := v.table.Cursor()
	if idx < 0 || idx >= len(v.visible) {
		return kafka.ConsumedMessage{}, false
	}
	return v.matches[v.visible[idx]], true
}

func (v *SearchView) progressFragments() string {
	if v.Multi() {
		return v.topicsProgressFragments()
	}
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
	case searchStateCanceled:
		return fmt.Sprintf(
			"stopped · %d matches · %s · results are partial",
			len(v.matches),
			formatElapsed(v.progress.Elapsed),
		)
	}
	return ""
}

func (v *SearchView) start() tea.Cmd {
	params, err := searchParamsFor(v.query)
	if err != nil {
		v.state = searchStateErrored
		v.scanErr = err
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	v.cancel = cancel
	v.scanID = nextScanID()
	v.state = searchStateRunning
	if v.Multi() {
		return v.startTopics(ctx, params)
	}

	matchCh, progCh, errCh := v.runSearch(ctx, v.topic, params)
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
	setTableRows(&v.table, append(v.table.Rows(), v.row(m)))
}

// row is one match as a results-table row; a cross-topic search leads with
// the topic, since the same offset means nothing without it.
func (v *SearchView) row(m kafka.ConsumedMessage) table.Row {
	r := matchRow(m)
	if v.Multi() {
		return append(table.Row{m.Topic}, r...)
	}
	return r
}

// columns is the results table's layout for width.
func (v *SearchView) columns(width int) []table.Column {
	if v.Multi() {
		return buildColumns(searchTopicsFixedCols, width, v.xOffset)
	}
	return buildMsgColumns(width, v.xOffset)
}

// matchRow is one match as a results-table row.
func matchRow(m kafka.ConsumedMessage) table.Row {
	value := strings.ReplaceAll(m.Value, "\n", " ")
	value = strings.ReplaceAll(value, "\t", " ")
	return table.Row{
		fmt.Sprintf("%d", m.Offset),
		fmt.Sprintf("%d", m.Partition),
		m.Time.Format("1/2/2006, 15:04:05.000"),
		m.Key,
		value,
	}
}

// pathQueryPrefix marks bar input as a JSON-path predicate, the TUI spelling
// of `k4a search --path`: `path:.a.b == "x"`, or the `field:value` shorthand,
// `path:orderId:42`. It only applies when a predicate parses after it, so a
// regex search for text like "path: /api" is unaffected.
const pathQueryPrefix = "path:"

// searchParamsFor interprets the bar input: `path:.a.b == "x"` (or
// `path:a.b:x`) is a JSON-path predicate, which the index answers when it is
// an equality; anything else is a regex.
func searchParamsFor(q string) (kafka.SearchParams, error) {
	params := kafka.SearchParams{Scope: kafka.ScopeKey | kafka.ScopeValue}
	if rest, ok := strings.CutPrefix(strings.TrimSpace(q), pathQueryPrefix); ok {
		expr, err := jsonpath.Parse(rest)
		switch {
		case err == nil:
			params.Path = expr
			return params, nil
		case strings.HasPrefix(strings.TrimSpace(rest), "."):
			return params, err // meant as a path, and malformed
		}
	}
	re, err := compileSearchQuery(q)
	if err != nil {
		return params, err
	}
	params.Pattern = re
	return params, nil
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
