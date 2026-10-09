// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package tui

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/blairham/tuikit/loading"

	"github.com/blairham/k4a/internal/config"
	"github.com/blairham/k4a/internal/kafka"
	"github.com/blairham/k4a/internal/tui/style"
	"github.com/blairham/k4a/internal/tui/views"
)

// keyPress creates a KeyPressMsg for a printable rune.
func keyPress(r rune) tea.KeyPressMsg {
	return tea.KeyPressMsg{Code: r, Text: string(r)}
}

// escKey creates a KeyPressMsg for the escape key.
func escKey() tea.KeyPressMsg {
	return tea.KeyPressMsg{Code: tea.KeyEscape}
}

// ---------------------------------------------------------------------------
// tableHeight / innerWidth
// ---------------------------------------------------------------------------

func TestTableHeight(t *testing.T) {
	t.Parallel()

	app := &App{height: 40}
	h := app.tableHeight()
	// chrome reservation = TopSectionRows(6, tuikit's default ShortcutRows
	// — k9s's menu height) + 2 border + 2 footer = 10
	if h != 30 {
		t.Errorf("tableHeight() = %d, want 30", h)
	}
}

func TestTableHeightCommanding(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.height = 40
	app.commandBar.Open()
	h := app.tableHeight()
	// chrome reservation = TopSectionRows(6, the logo's height) + 2 border
	// + 2 footer + 3 (command bar) = 13
	if h != 27 {
		t.Errorf("tableHeight() with command bar open = %d, want 27", h)
	}
}

func TestTableHeightMinimum(t *testing.T) {
	t.Parallel()

	app := &App{height: 1}
	h := app.tableHeight()
	if h < 1 {
		t.Errorf("tableHeight() = %d, want >= 1", h)
	}
}

func TestInnerWidth(t *testing.T) {
	t.Parallel()

	app := &App{width: 100}
	w := app.innerWidth()
	if w != 96 {
		t.Errorf("innerWidth() = %d, want 96", w)
	}
}

func TestInnerWidthMinimum(t *testing.T) {
	t.Parallel()

	app := &App{width: 1}
	w := app.innerWidth()
	if w < 1 {
		t.Errorf("innerWidth() = %d, want >= 1", w)
	}
}

// ---------------------------------------------------------------------------
// NewApp + initial state
// ---------------------------------------------------------------------------

func TestNewApp(t *testing.T) {
	t.Parallel()

	info := ConnInfo{
		Context: "test",
		Auth:    "scram",
		Brokers: "broker:9092",
		Region:  "us-east-1",
	}

	app := NewApp(nil, info, nil, "", "dev")
	if app.view != style.ViewTopics {
		t.Errorf("initial view = %d, want ViewTopics", app.view)
	}
	if app.connInfo.Context != "test" {
		t.Errorf("connInfo.Context = %q", app.connInfo.Context)
	}
	if app.loading != true {
		t.Error("expected loading = true initially")
	}
}

// ---------------------------------------------------------------------------
// executeCommand
// ---------------------------------------------------------------------------

func TestExecuteCommandQuit(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")

	for _, cmd := range []string{"q", "quit", "exit"} {
		_, teaCmd := app.dispatchCommand(cmd)
		if teaCmd == nil {
			t.Errorf("dispatchCommand(%q) should return quit cmd", cmd)
		}
	}
}

func TestExecuteCommandNavigation(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.width = 80
	app.height = 40

	app.dispatchCommand("topics") //nolint:errcheck
	if app.view != style.ViewTopics {
		t.Errorf("after :topics, view = %d", app.view)
	}

	app.dispatchCommand("groups") //nolint:errcheck
	if app.view != style.ViewGroups {
		t.Errorf("after :groups, view = %d", app.view)
	}

	app.dispatchCommand("cluster") //nolint:errcheck
	if app.view != style.ViewCluster {
		t.Errorf("after :cluster, view = %d", app.view)
	}

	app.dispatchCommand("brokers") //nolint:errcheck
	if app.view != style.ViewCluster {
		t.Errorf("after :brokers, view = %d", app.view)
	}
}

func TestExecuteCommandFilter(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")

	// Filter command starts with /.
	_, cmd := app.dispatchCommand("/orders")
	if cmd != nil {
		t.Error("filter command should return nil cmd")
	}
}

func TestExecuteCommandUnknown(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")

	_, cmd := app.dispatchCommand("nonexistent")
	if cmd != nil {
		t.Error("unknown command should return nil cmd")
	}
}

func TestExecuteCommandLogoToggle(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.width = 200
	app.height = 40

	// App starts with logo enabled (no cfg.UI.Logoless).
	if app.logoless {
		t.Fatal("app should start with logo enabled")
	}
	if len(app.chrome.Logo) == 0 {
		t.Fatal("chrome should start with logo lines")
	}

	app.dispatchCommand("logo") //nolint:errcheck
	if !app.logoless {
		t.Error("after :logo, logoless should be true")
	}
	if len(app.chrome.Logo) != 0 {
		t.Error("after :logo, chrome.Logo should be empty")
	}

	app.dispatchCommand("logoless") //nolint:errcheck
	if app.logoless {
		t.Error("after second toggle, logoless should be false")
	}
	if len(app.chrome.Logo) == 0 {
		t.Error("after second toggle, chrome.Logo should be restored")
	}
}

// ---------------------------------------------------------------------------
// View stack (push/pop)
// ---------------------------------------------------------------------------

func TestPushPopView(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.width = 80
	app.height = 40

	if len(app.viewStack) != 0 {
		t.Fatal("expected empty view stack initially")
	}

	app.pushView(style.ViewTopics)
	if len(app.viewStack) != 1 {
		t.Fatalf("viewStack len = %d, want 1", len(app.viewStack))
	}
	if app.viewStack[0] != style.ViewTopics {
		t.Errorf("viewStack[0] = %d, want ViewTopics", app.viewStack[0])
	}

	app.pushView(style.ViewGroups)
	if len(app.viewStack) != 2 {
		t.Fatalf("viewStack len = %d, want 2", len(app.viewStack))
	}

	app.popView()
	if len(app.viewStack) != 1 {
		t.Fatalf("after pop, viewStack len = %d, want 1", len(app.viewStack))
	}
	if app.view != style.ViewGroups {
		t.Errorf("after pop, view = %d, want ViewGroups", app.view)
	}

	app.popView()
	if len(app.viewStack) != 0 {
		t.Fatalf("after second pop, viewStack len = %d, want 0", len(app.viewStack))
	}

	// Pop on empty stack should not panic.
	app.popView()
}

// ---------------------------------------------------------------------------
// handleKey — global navigation
// ---------------------------------------------------------------------------

func TestHandleKeyTabSwitch(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.width = 80
	app.height = 40

	app.handleKey(keyPress('2')) //nolint:errcheck
	if app.view != style.ViewGroups {
		t.Errorf("after '2', view = %d, want ViewGroups", app.view)
	}

	app.handleKey(keyPress('3')) //nolint:errcheck
	if app.view != style.ViewCluster {
		t.Errorf("after '3', view = %d, want ViewCluster", app.view)
	}

	app.handleKey(keyPress('1')) //nolint:errcheck
	if app.view != style.ViewTopics {
		t.Errorf("after '1', view = %d, want ViewTopics", app.view)
	}
}

func TestHandleKeyHelp(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.width = 80
	app.height = 40

	app.handleKey(keyPress('?')) //nolint:errcheck
	if !app.showHelp {
		t.Error("expected showHelp = true after '?'")
	}

	// '?' again closes help.
	app.handleKey(keyPress('?')) //nolint:errcheck
	if app.showHelp {
		t.Error("expected showHelp = false after second '?'")
	}

	// Open and close with esc.
	app.handleKey(keyPress('?')) //nolint:errcheck
	app.handleKey(escKey())      //nolint:errcheck
	if app.showHelp {
		t.Error("expected showHelp = false after esc")
	}
}

func TestHandleKeyEscGoesBack(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.width = 80
	app.height = 40

	// Push a view on the stack.
	app.pushView(style.ViewTopics)
	app.view = style.ViewGroups

	app.handleKey(escKey()) //nolint:errcheck
	if app.view != style.ViewTopics {
		t.Errorf("after esc with stack, view = %d, want ViewTopics", app.view)
	}
}

func TestHandleKeyEscOnEmptyStack(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.width = 80
	app.height = 40

	// Esc on empty stack should NOT quit — just no-op.
	_, cmd := app.handleKey(escKey())
	if cmd != nil {
		t.Error("esc on empty stack should return nil cmd (not quit)")
	}
}

func TestHandleKeyCommandMode(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.width = 80
	app.height = 40

	// Enter command mode.
	app.handleKey(keyPress(':')) //nolint:errcheck
	if !app.commandBar.Active() {
		t.Error("expected commanding = true after ':'")
	}

	// Esc exits command mode.
	app.handleKey(escKey()) //nolint:errcheck
	if app.commandBar.Active() {
		t.Error("expected commanding = false after esc in command mode")
	}
}

func TestHandleKeyFilterMode(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.width = 80
	app.height = 40

	// Enter filter mode — `/` now opens the FilterBar (separate from
	// the command palette) rather than seeding the command input with
	// a leading "/" prefix.
	app.handleKey(keyPress('/')) //nolint:errcheck
	if !app.filterBar.Active() {
		t.Error("expected filterBar.Active() = true after '/'")
	}
	if app.commandBar.Active() {
		t.Error("filter mode should not open the command bar")
	}
}

// ---------------------------------------------------------------------------
// renderInfoPanel
// ---------------------------------------------------------------------------

func TestRenderInfoPanel(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{
		Context: "staging",
		Auth:    "iam",
		Region:  "us-east-1",
		Brokers: "broker:9098",
	}, nil, "", "dev")

	lines := app.renderInfoPanel()
	if len(lines) != 5 {
		t.Fatalf("expected 5 info lines, got %d", len(lines))
	}
}

func TestRenderInfoPanelDirectContext(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{
		Context: "",
		Auth:    "plaintext",
		Brokers: "localhost:9092",
	}, nil, "", "dev")

	lines := app.renderInfoPanel()
	// Empty context should render as "direct" (embedded in styled text).
	found := false
	for _, l := range lines {
		if strings.Contains(l, "direct") {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected 'direct' in info panel when context is empty")
	}
}

func TestRenderInfoPanelLongBrokers(t *testing.T) {
	t.Parallel()

	long := strings.Repeat("broker-", 10) + ":9092"
	app := NewApp(nil, ConnInfo{Brokers: long}, nil, "", "dev")

	lines := app.renderInfoPanel()
	// Brokers should be truncated.
	for _, l := range lines {
		if strings.Contains(l, "...") {
			return
		}
	}
	t.Error("expected truncated brokers with '...'")
}

// headerHeight/footerHeight were removed in the chrome.Render
// migration — tableHeight now delegates to chrome.ContentInnerSize.
// See TestTableHeight for the replacement coverage.

// ---------------------------------------------------------------------------
// View rendering (smoke test — no panics)
// ---------------------------------------------------------------------------

func TestViewBeforeResize(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	view := app.View()
	// Before any WindowSizeMsg, width=0, should show connecting.
	if !strings.Contains(view.Content, "Connecting") {
		t.Errorf("expected 'Connecting' in initial view, got %q", view.Content)
	}
}

func TestViewAfterResize(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{Context: "test", Auth: "iam"}, nil, "", "dev")
	app.Update(tea.WindowSizeMsg{Width: 120, Height: 40}) //nolint:errcheck

	view := app.View()
	if view.Content == "" {
		t.Error("View() returned empty after resize")
	}
}

func TestViewHelp(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.Update(tea.WindowSizeMsg{Width: 120, Height: 40})           //nolint:errcheck
	app.Update(views.TopicsRefreshMsg{Topics: []kafka.TopicInfo{}}) //nolint:errcheck
	app.Update(views.TopicCountsMsg{Counts: map[string]int64{}})    //nolint:errcheck
	app.splashActive = false
	app.loading = false
	app.showHelp = true

	view := app.View()
	// Help overlay should contain navigation hints like "Help" and shortcut keys.
	if !strings.Contains(view.Content, "Help") {
		t.Error("expected help content with 'Help' text")
	}
}

// ---------------------------------------------------------------------------
// sanitizeFilename
// ---------------------------------------------------------------------------

func TestSanitizeFilename(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input string
		want  string
	}{
		{"simple-topic", "simple-topic"},
		{"my.topic.v1", "my.topic.v1"},
		{"topic/with/slashes", "topic_with_slashes"},
		{"topic with spaces", "topic_with_spaces"},
		{"topic:special!chars@here", "topic_special_chars_here"},
		{"under_score", "under_score"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			if got := sanitizeFilename(tt.input); got != tt.want {
				t.Errorf("sanitizeFilename(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// saveMessageToFile
// ---------------------------------------------------------------------------

func TestSaveMessageToFileJSON(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "orders-2-1500.json")
	msg := &kafka.ConsumedMessage{
		Topic:     "orders",
		Partition: 2,
		Offset:    1500,
		Key:       "order-123",
		Value:     `{"id":123,"status":"shipped"}`,
		Time:      time.Date(2026, 4, 19, 12, 0, 0, 0, time.UTC),
		Headers: []kafka.MessageHeader{
			{Key: "type", Value: "created"},
		},
	}

	if err := saveMessageToFile(msg, path); err != nil {
		t.Fatalf("saveMessageToFile() error: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading saved file: %v", err)
	}

	// Verify it's valid JSON.
	var parsed map[string]any
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("saved file is not valid JSON: %v", err)
	}

	// Value should be embedded as an object, not a string.
	if _, ok := parsed["value"].(map[string]any); !ok {
		t.Errorf("value should be a JSON object, got %T", parsed["value"])
	}
	if parsed["topic"] != "orders" {
		t.Errorf("topic = %v, want orders", parsed["topic"])
	}
	if parsed["key"] != "order-123" {
		t.Errorf("key = %v, want order-123", parsed["key"])
	}
}

func TestSaveMessageToFileNonJSON(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "logs-0-42.json")
	msg := &kafka.ConsumedMessage{
		Topic:     "logs",
		Partition: 0,
		Offset:    42,
		Value:     "plain text value",
		Time:      time.Now(),
	}

	if err := saveMessageToFile(msg, path); err != nil {
		t.Fatalf("saveMessageToFile() error: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading saved file: %v", err)
	}

	var parsed map[string]any
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("saved file is not valid JSON: %v", err)
	}

	// Non-JSON value should be escaped as a string.
	if _, ok := parsed["value"].(string); !ok {
		t.Errorf("non-JSON value should be a string, got %T", parsed["value"])
	}
}

func TestSaveMessageToFileCreatesDir(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "subdir", "msg.json")
	msg := &kafka.ConsumedMessage{
		Topic: "test",
		Time:  time.Now(),
	}

	if err := saveMessageToFile(msg, path); err != nil {
		t.Fatalf("saveMessageToFile() error: %v", err)
	}

	if _, err := os.Stat(path); err != nil {
		t.Errorf("file not created at %s: %v", path, err)
	}
}

func TestMessageFilename(t *testing.T) {
	t.Parallel()

	tests := []struct {
		topic string
		want  string
		part  int
		off   int64
	}{
		{topic: "orders", part: 2, off: 1500, want: "orders-2-1500.json"},
		{topic: "my/topic.v1", part: 0, off: 1, want: "my_topic.v1-0-1.json"},
	}

	for _, tt := range tests {
		msg := &kafka.ConsumedMessage{Topic: tt.topic, Partition: tt.part, Offset: tt.off}
		if got := messageFilename(msg); got != tt.want {
			t.Errorf("messageFilename(%q) = %q, want %q", tt.topic, got, tt.want)
		}
	}
}

// ---------------------------------------------------------------------------
// flash message clearing
// ---------------------------------------------------------------------------

func TestFlashClearedOnKeypress(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.width = 80
	app.height = 40
	app.loading = false
	app.flash = "Saved to test.json"

	app.handleKey(keyPress('j')) //nolint:errcheck

	if app.flash != "" {
		t.Errorf("flash should be cleared on keypress, got %q", app.flash)
	}
}

func TestErrFlashClearedOnKeypress(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.width = 80
	app.height = 40
	app.loading = false
	app.errFlash = "some error"

	app.handleKey(keyPress('j')) //nolint:errcheck

	if app.errFlash != "" {
		t.Errorf("errFlash should be cleared on keypress, got %q", app.errFlash)
	}
}

// ---------------------------------------------------------------------------
// :reset command
// ---------------------------------------------------------------------------

func TestResetCommandNotInGroupDetail(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.width = 80
	app.height = 40

	// :reset from topics view should be a no-op.
	app.dispatchCommand("reset") //nolint:errcheck
	if app.commandBar.Active() {
		t.Error("expected commanding = false when not in group detail")
	}
}

func TestResetCommandInGroupDetail(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.width = 80
	app.height = 40
	app.loading = false

	// Set up group detail view with data.
	gdv := views.NewGroupDetailView(nil, "test-group")
	gdv.Update(views.GroupDetailRefreshMsg{Detail: &kafka.GroupDetail{ //nolint:errcheck
		GroupID: "test-group",
		State:   "Empty",
		Offsets: []kafka.GroupOffsetInfo{
			{Topic: "orders", Partition: 0, CommittedOffset: 100, HighWatermark: 150, Lag: 50},
		},
	}})
	gdv.Resize(80, 30)
	app.setView(style.ViewGroupDetail, gdv)
	app.view = style.ViewGroupDetail

	app.dispatchCommand("reset") //nolint:errcheck

	// Should have opened a confirmation prompt via the chrome.Confirm
	// widget. The (action, param) it captured live in the dispatch
	// closure now — observable only through Prompt() string.
	if !app.confirm.Active() {
		t.Error("expected confirm.Active() = true for delete prompt")
	}
	prompt := app.confirm.Prompt()
	if !strings.Contains(prompt, "orders") {
		t.Errorf("confirm prompt = %q, want it to reference the topic 'orders'", prompt)
	}
}

func TestResetCommandNoDataLoaded(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.width = 80
	app.height = 40
	app.loading = false

	// Group detail view with no data loaded.
	app.setView(style.ViewGroupDetail, views.NewGroupDetailView(nil, "test-group"))
	app.view = style.ViewGroupDetail

	app.dispatchCommand("reset") //nolint:errcheck

	// No offset selected — should not prompt.
	if app.confirm.Active() {
		t.Error("expected no confirm prompt when no offset data is loaded")
	}
}

func TestExecuteConfirmDeleteOffsetDeclined(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.loading = false
	pc := pendingConfirm{action: "delete_offset", param: "my-group\x00orders\x000"}

	cmd := app.executeConfirmAction(pc, false)
	if app.loading {
		t.Error("expected loading = false after declining")
	}
	if cmd != nil {
		t.Error("declined confirm should produce no Cmd")
	}
}

func TestExecuteConfirmDeleteOffsetConfirmed(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.loading = false
	pc := pendingConfirm{action: "delete_offset", param: "my-group\x00orders\x000"}

	cmd := app.executeConfirmAction(pc, true)
	if !app.loading {
		t.Error("expected loading = true after confirming delete_offset")
	}
	if cmd == nil {
		t.Error("expected non-nil cmd after confirming delete_offset")
	}
}

// ---------------------------------------------------------------------------
// Splash screen / initialLoad — auth error clears splash
// ---------------------------------------------------------------------------

func TestInitialLoadClearsOnTopicsError(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.width = 80
	app.height = 40
	app.splashActive = false // simulate post-splash context switch
	app.initialLoad = true
	app.loading = true

	// Simulate a topics fetch that fails with an auth error.
	app.Update(views.TopicsRefreshMsg{Err: fmt.Errorf("authentication failed")}) //nolint:errcheck

	if app.initialLoad {
		t.Error("initialLoad should be false after TopicsRefreshMsg with error")
	}
	if app.loading {
		t.Error("loading should be false after TopicsRefreshMsg with error")
	}
}

func TestInitialLoadClearsOnTopicsSuccess(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.width = 80
	app.height = 40
	app.splashActive = false // simulate post-splash context switch
	app.initialLoad = true
	app.loading = true

	// Successful topics load clears initialLoad (context switch path).
	app.Update(views.TopicsRefreshMsg{Topics: []kafka.TopicInfo{ //nolint:errcheck
		{Name: "orders", Partitions: 3},
	}})

	if app.initialLoad {
		t.Error("initialLoad should be false after TopicsRefreshMsg")
	}
	if app.loading {
		t.Error("loading should be false after TopicsRefreshMsg")
	}
}

func TestInitialLoadClearsOnTopicCounts(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.width = 80
	app.height = 40
	app.splashActive = false // simulate post-splash context switch
	app.initialLoad = true
	app.loading = true

	app.Update(views.TopicCountsMsg{Counts: map[string]int64{"orders": 100}}) //nolint:errcheck

	if app.initialLoad {
		t.Error("initialLoad should be false after TopicCountsMsg")
	}
	if app.loading {
		t.Error("loading should be false after TopicCountsMsg")
	}
}

func TestSplashBlocksLoadingClear(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.width = 80
	app.height = 40
	// splashActive = true from NewApp

	// Data arrives during splash — should NOT clear loading.
	app.Update(views.TopicsRefreshMsg{Topics: []kafka.TopicInfo{ //nolint:errcheck
		{Name: "orders", Partitions: 3},
	}})

	if !app.splashActive {
		t.Error("splashActive should still be true")
	}
}

// ---------------------------------------------------------------------------
// Increase partitions — validation
// ---------------------------------------------------------------------------

func TestIncreasePartitionsValidation(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.loading = false

	// Set up topic detail view with 3 partitions.
	tdv := views.NewTopicDetailView(nil, "test-topic")
	tdv.Update(views.TopicDetailRefreshMsg{Detail: &kafka.TopicDetail{ //nolint:errcheck
		Name:       "test-topic",
		Partitions: []kafka.PartitionInfo{{ID: 0}, {ID: 1}, {ID: 2}},
	}})
	app.setView(style.ViewTopicDetail, tdv)
	app.view = style.ViewTopicDetail

	// Same count should be rejected.
	pc := pendingConfirm{action: "increase_partitions", param: "test-topic"}
	if errMsg, _ := app.executeValueAction(pc, "3"); errMsg == "" {
		t.Error("expected error for same partition count")
	}

	// Lower count should be rejected.
	if errMsg, _ := app.executeValueAction(pc, "2"); errMsg == "" {
		t.Error("expected error for lower partition count")
	}

	// Invalid input should be rejected.
	if errMsg, _ := app.executeValueAction(pc, "abc"); errMsg == "" {
		t.Error("expected error for non-numeric input")
	}

	// Valid increase should set loading.
	if _, cmd := app.executeValueAction(pc, "6"); cmd == nil {
		t.Error("expected cmd for valid increase")
	}
	if !app.loading {
		t.Error("expected loading=true for valid increase")
	}
}

// ---------------------------------------------------------------------------
// parseDuration
// ---------------------------------------------------------------------------

func TestParseDuration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		input         string
		wantMs        int64
		wantPermanent bool
		wantErr       bool
	}{
		{"1h", "1h", 3600000, false, false},
		{"30m", "30m", 1800000, false, false},
		{"1d", "1d", 86400000, false, false},
		{"0 purge all", "0", 1000, false, false},
		{"1h permanent", "1h!", 3600000, true, false},
		{"0 permanent", "0!", 0, true, false},
		{"bad input", "bad", 0, false, true},
		{"empty", "", 0, false, true},
		{"unknown suffix", "1x", 0, false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ms, permanent, err := parseDuration(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Errorf("parseDuration(%q) expected error, got nil", tt.input)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseDuration(%q) unexpected error: %v", tt.input, err)
			}
			if ms != tt.wantMs {
				t.Errorf("parseDuration(%q) ms = %d, want %d", tt.input, ms, tt.wantMs)
			}
			if permanent != tt.wantPermanent {
				t.Errorf("parseDuration(%q) permanent = %v, want %v", tt.input, permanent, tt.wantPermanent)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// formatRetentionMs
// ---------------------------------------------------------------------------

func TestFormatRetentionMs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		want string
		ms   int64
	}{
		{"1d", 86400000},
		{"1h", 3600000},
		{"1m", 60000},
		{"1500ms", 1500},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			t.Parallel()
			if got := formatRetentionMs(tt.ms); got != tt.want {
				t.Errorf("formatRetentionMs(%d) = %q, want %q", tt.ms, got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// executeCommand — view switching
// ---------------------------------------------------------------------------

func TestExecuteCommandTopics(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.width = 80
	app.height = 40

	app.dispatchCommand("topics") //nolint:errcheck
	if app.view != style.ViewTopics {
		t.Errorf("after :topics, view = %d, want ViewTopics", app.view)
	}
}

func TestExecuteCommandGroups(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.width = 80
	app.height = 40

	app.dispatchCommand("groups") //nolint:errcheck
	if app.view != style.ViewGroups {
		t.Errorf("after :groups, view = %d, want ViewGroups", app.view)
	}
}

func TestExecuteCommandCluster(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.width = 80
	app.height = 40

	app.dispatchCommand("cluster") //nolint:errcheck
	if app.view != style.ViewCluster {
		t.Errorf("after :cluster, view = %d, want ViewCluster", app.view)
	}
}

func TestExecuteCommandACLs(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.width = 80
	app.height = 40

	app.dispatchCommand("acls") //nolint:errcheck
	if app.view != style.ViewACLs {
		t.Errorf("after :acls, view = %d, want ViewACLs", app.view)
	}
}

func TestExecuteCommandQuitVariants(t *testing.T) {
	t.Parallel()

	for _, cmd := range []string{"q", "quit", "exit"} {
		t.Run(cmd, func(t *testing.T) {
			t.Parallel()
			app := NewApp(nil, ConnInfo{}, nil, "", "dev")
			_, teaCmd := app.dispatchCommand(cmd)
			if teaCmd == nil {
				t.Errorf("dispatchCommand(%q) should return quit cmd", cmd)
			}
		})
	}
}

func TestExecuteCommandCreateTopic(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.width = 80
	app.height = 40

	app.dispatchCommand("ct") //nolint:errcheck
	if app.view != style.ViewCreateTopic {
		t.Errorf("after :ct, view = %d, want ViewCreateTopic", app.view)
	}
}

func TestExecuteCommandCreateACL(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.width = 80
	app.height = 40

	app.dispatchCommand("acl") //nolint:errcheck
	if app.view != style.ViewCreateACL {
		t.Errorf("after :acl, view = %d, want ViewCreateACL", app.view)
	}
}

// ---------------------------------------------------------------------------
// Update — delete/purge/edit messages
// ---------------------------------------------------------------------------

func TestUpdateDeleteTopicMsgSuccess(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.loading = true
	app.Update(deleteTopicMsg{topic: "t1"}) //nolint:errcheck

	if app.loading {
		t.Error("loading should be false after deleteTopicMsg success")
	}
}

func TestUpdateDeleteTopicMsgError(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.loading = true
	app.Update(deleteTopicMsg{err: fmt.Errorf("delete failed"), topic: "t1"}) //nolint:errcheck

	if app.loading {
		t.Error("loading should be false after deleteTopicMsg error")
	}
	if app.errFlash == "" {
		t.Error("errFlash should be set after deleteTopicMsg error")
	}
}

func TestUpdateDeleteGroupMsgSuccess(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.loading = true
	app.Update(deleteGroupMsg{}) //nolint:errcheck

	if app.loading {
		t.Error("loading should be false after deleteGroupMsg success")
	}
}

func TestUpdateDeleteACLMsgSuccess(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.loading = true
	app.Update(deleteACLMsg{}) //nolint:errcheck

	if app.loading {
		t.Error("loading should be false after deleteACLMsg success")
	}
}

func TestUpdatePurgeTopicMsgSuccess(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.loading = true
	app.Update(purgeTopicMsg{topic: "t1"}) //nolint:errcheck

	if app.loading {
		t.Error("loading should be false after purgeTopicMsg success")
	}
	if app.flash == "" {
		t.Error("flash should be set after purgeTopicMsg success")
	}
}

func TestUpdateEditTopicConfigMsgError(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.loading = true
	app.Update(editTopicConfigMsg{err: fmt.Errorf("config failed")}) //nolint:errcheck

	if app.loading {
		t.Error("loading should be false after editTopicConfigMsg error")
	}
	if app.errFlash == "" {
		t.Error("errFlash should be set after editTopicConfigMsg error")
	}
}

func TestUpdateIncreasePartitionsMsgSuccess(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.loading = true
	app.Update(increasePartitionsMsg{}) //nolint:errcheck

	if app.loading {
		t.Error("loading should be false after increasePartitionsMsg success")
	}
	if app.flash == "" {
		t.Error("flash should be set after increasePartitionsMsg success")
	}
}

// ---------------------------------------------------------------------------
// refreshMsgMatchesView
// ---------------------------------------------------------------------------

func TestRefreshMsgMatchesView(t *testing.T) {
	t.Parallel()

	tests := []struct {
		msg     tea.Msg
		name    string
		view    style.ViewType
		altView style.ViewType
		want    bool
	}{
		{
			name:    "TopicsRefreshMsg",
			msg:     views.TopicsRefreshMsg{},
			view:    style.ViewTopics,
			want:    true,
			altView: style.ViewGroups,
		},
		{
			name:    "GroupsRefreshMsg",
			msg:     views.GroupsRefreshMsg{},
			view:    style.ViewGroups,
			want:    true,
			altView: style.ViewTopics,
		},
		{
			name:    "ClusterRefreshMsg",
			msg:     views.ClusterRefreshMsg{},
			view:    style.ViewCluster,
			want:    true,
			altView: style.ViewTopics,
		},
		{name: "ACLsRefreshMsg", msg: views.ACLsRefreshMsg{}, view: style.ViewACLs, want: true, altView: style.ViewTopics},
		{name: "TopicCountsMsg", msg: views.TopicCountsMsg{}, view: style.ViewTopics, want: true, altView: style.ViewGroups},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			app := NewApp(nil, ConnInfo{}, nil, "", "dev")
			app.view = tt.view
			if got := app.refreshMsgMatchesView(tt.msg); got != tt.want {
				t.Errorf("refreshMsgMatchesView() with matching view = %v, want %v", got, tt.want)
			}
			app.view = tt.altView
			if got := app.refreshMsgMatchesView(tt.msg); got != false {
				t.Errorf("refreshMsgMatchesView() with non-matching view = %v, want false", got)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// refreshMsgHasError
// ---------------------------------------------------------------------------

func TestRefreshMsgHasError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		msg  tea.Msg
		name string
		want bool
	}{
		{name: "TopicsRefreshMsg with error", msg: views.TopicsRefreshMsg{Err: fmt.Errorf("fail")}, want: true},
		{name: "TopicsRefreshMsg without error", msg: views.TopicsRefreshMsg{}, want: false},
		{name: "GroupsRefreshMsg with error", msg: views.GroupsRefreshMsg{Err: fmt.Errorf("fail")}, want: true},
		{name: "GroupsRefreshMsg without error", msg: views.GroupsRefreshMsg{}, want: false},
		{name: "ClusterRefreshMsg with error", msg: views.ClusterRefreshMsg{Err: fmt.Errorf("fail")}, want: true},
		{name: "ClusterRefreshMsg without error", msg: views.ClusterRefreshMsg{}, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := refreshMsgHasError(tt.msg); got != tt.want {
				t.Errorf("refreshMsgHasError() = %v, want %v", got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// splashDoneMsg handling
// ---------------------------------------------------------------------------

func TestSplashDoneWithSkipContextView(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.width = 80
	app.height = 40
	app.SetSkipContextView()

	app.Update(splashDoneMsg{}) //nolint:errcheck

	if app.splashActive {
		t.Error("splashActive should be false after splashDoneMsg")
	}
	if app.view != style.ViewTopics {
		t.Errorf("view = %d, want ViewTopics when skipContextView=true", app.view)
	}
}

func TestSplashDoneWithoutSkipContextView(t *testing.T) {
	t.Parallel()

	cfg := &config.Config{Contexts: map[string]config.Context{"test": {}}}
	app := NewApp(nil, ConnInfo{}, cfg, "", "dev")
	app.width = 80
	app.height = 40

	app.Update(splashDoneMsg{}) //nolint:errcheck

	if app.splashActive {
		t.Error("splashActive should be false after splashDoneMsg")
	}
	if app.view != style.ViewContext {
		t.Errorf("view = %d, want ViewContext when skipContextView=false", app.view)
	}
}

// ---------------------------------------------------------------------------
// Render helpers
// ---------------------------------------------------------------------------

func TestActiveViewCount(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.width = 80
	app.height = 40
	app.loading = false

	app.viewMap[style.ViewTopics].Update(views.TopicsRefreshMsg{Topics: []kafka.TopicInfo{
		{Name: "a"}, {Name: "b"}, {Name: "c"},
	}})

	if c := app.activeViewCount(); c != 3 {
		t.Errorf("activeViewCount() = %d, want 3", c)
	}
}

func TestActiveViewLoading(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.loading = true
	if !app.activeViewLoading() {
		t.Error("activeViewLoading() should be true when app.loading=true")
	}

	app.loading = false
	// TopicsView.Loading clears after TopicCountsMsg (second phase).
	app.viewMap[style.ViewTopics].Update(views.TopicsRefreshMsg{Topics: nil})
	app.viewMap[style.ViewTopics].Update(views.TopicCountsMsg{})
	if app.activeViewLoading() {
		t.Error("activeViewLoading() should be false after data arrives")
	}
}

func TestRenderActiveView(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.width = 80
	app.height = 40
	app.loading = false
	app.viewMap[style.ViewTopics].Resize(76, 31)
	app.viewMap[style.ViewTopics].Update(views.TopicsRefreshMsg{Topics: []kafka.TopicInfo{
		{Name: "orders", Partitions: 3},
	}})

	out := app.renderActiveView()
	if out == "" {
		t.Error("renderActiveView() returned empty")
	}
}

func TestRenderResourceTitle(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.width = 80
	app.height = 40
	title := app.renderResourceTitle()
	if title == "" {
		t.Error("renderResourceTitle() returned empty")
	}
}

func TestBreadcrumb(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.viewStack = []style.ViewType{style.ViewTopics, style.ViewTopicDetail}
	app.view = style.ViewMessages

	crumbs := app.breadcrumb()
	if len(crumbs) != 3 {
		t.Fatalf("breadcrumb len = %d, want 3", len(crumbs))
	}
	if crumbs[2].Label != style.ViewName(style.ViewMessages) || !crumbs[2].Leaf {
		t.Errorf("breadcrumb leaf = %+v; want %q with Leaf=true", crumbs[2], style.ViewName(style.ViewMessages))
	}
	if crumbs[0].Leaf || crumbs[1].Leaf {
		t.Errorf("non-leaf crumbs marked as leaf: %+v", crumbs)
	}
}

func TestRenderInfoPanelShowsUpdateAvailable(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "v0.4.2")
	app.latestVersion = "v0.5.0"

	lines := app.renderInfoPanel()
	if len(lines) != 5 {
		t.Fatalf("expected 5 info lines, got %d", len(lines))
	}
	revLine := lines[4]
	if !strings.Contains(revLine, "K4a Rev:") {
		t.Errorf("rev line missing label: %q", revLine)
	}
	if !strings.Contains(revLine, "v0.4.2") {
		t.Errorf("rev line missing current version: %q", revLine)
	}
	if !strings.Contains(revLine, "⚡") {
		t.Errorf("rev line missing lightning-bolt update cue: %q", revLine)
	}
	if !strings.Contains(revLine, "v0.5.0") {
		t.Errorf("rev line missing latest version: %q", revLine)
	}
}

func TestViewRendersActiveCommandBar(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.width = 120
	app.height = 40
	app.splashActive = false
	app.commandBar.Open()

	out := app.View().Content
	// Chrome renders the command-bar prompt prefix ("🐶:") when the
	// command bar is in the frame — its presence confirms the frame
	// included Frame.Command and chrome.Render rendered the bar.
	if !strings.Contains(out, "🐶") {
		t.Errorf("View() did not render command bar prompt; got: %q", truncate(out, 200))
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

func TestActiveFilter(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")

	// With filter set directly.
	app.filter = "foo"
	if f := app.activeFilter(); f != "foo" {
		t.Errorf("activeFilter() = %q, want foo", f)
	}

	// While the filter bar is open the typed value takes precedence
	// (matches the live-filter semantics — every keystroke updates the
	// view).
	app.filterBar.OpenWith("bar")
	if f := app.activeFilter(); f != "bar" {
		t.Errorf("activeFilter() with filter bar = %q, want bar", f)
	}
}

func TestWindowSizeMsg(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.Update(tea.WindowSizeMsg{Width: 120, Height: 50}) //nolint:errcheck

	if app.width != 120 || app.height != 50 {
		t.Errorf("size = %dx%d, want 120x50", app.width, app.height)
	}
}

func TestTickMsg(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.width = 80
	app.height = 40
	app.splashActive = false

	_, cmd := app.Update(tickMsg(time.Now()))
	if cmd == nil {
		t.Error("tickMsg should return a command")
	}
}

func TestPromptSelectAllMuteOnOpen(t *testing.T) {
	t.Parallel()

	// The mute-prefill / restore-on-first-keystroke behavior moved
	// into chrome.Prompt's SelectAll option (covered by tuikit's own
	// tests). Here we just verify the k4a callsite that opens a value
	// prompt actually engages the SelectAll path.
	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.width = 80
	app.openValuePrompt(
		"purge_topic",
		"topic-foo",
		"1h",
		"retention:",
		"",
		true,
	) //nolint:errcheck // dispatch closure stored on app
	if !app.prompt.Active() {
		t.Fatal("expected prompt.Active() = true after openValuePrompt")
	}
	if app.prompt.Input().Placeholder != "1h" {
		t.Errorf("SelectAll should surface prefill via placeholder; got %q", app.prompt.Input().Placeholder)
	}
}

// ---------------------------------------------------------------------------
// handleAction
// ---------------------------------------------------------------------------

func TestHandleActionTopicDetail(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.width = 80
	app.height = 40

	app.handleAction("topic_detail", "my-topic") //nolint:errcheck

	if app.view != style.ViewTopicDetail {
		t.Errorf("view = %d, want ViewTopicDetail", app.view)
	}
	if len(app.viewStack) != 1 {
		t.Errorf("viewStack len = %d, want 1", len(app.viewStack))
	}
}

func TestHandleActionGroupDetail(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.width = 80
	app.height = 40

	app.handleAction("group_detail", "my-group") //nolint:errcheck

	if app.view != style.ViewGroupDetail {
		t.Errorf("view = %d, want ViewGroupDetail", app.view)
	}
}

func TestHandleActionContextView(t *testing.T) {
	t.Parallel()

	cfg := &config.Config{Contexts: map[string]config.Context{"test": {}}}
	app := NewApp(nil, ConnInfo{}, cfg, "", "dev")
	app.width = 80
	app.height = 40

	app.handleAction("context_view", "") //nolint:errcheck

	if app.view != style.ViewContext {
		t.Errorf("view = %d, want ViewContext", app.view)
	}
}

func TestHandleActionContextViewNilConfig(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.handleAction("context_view", "") //nolint:errcheck

	// Should not change view without config.
	if app.view == style.ViewContext {
		t.Error("should not switch to context view without config")
	}
}

func TestHandleActionCreateTopic(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.width = 80
	app.height = 40

	app.handleAction("create_topic", "") //nolint:errcheck
	if app.view != style.ViewCreateTopic {
		t.Errorf("view = %d, want ViewCreateTopic", app.view)
	}
}

func TestHandleActionProduce(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.width = 80
	app.height = 40

	app.handleAction("produce", "my-topic") //nolint:errcheck
	if app.view != style.ViewProduce {
		t.Errorf("view = %d, want ViewProduce", app.view)
	}
}

func TestHandleActionCreateACL(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.width = 80
	app.height = 40

	app.handleAction("create_acl", "") //nolint:errcheck
	if app.view != style.ViewCreateACL {
		t.Errorf("view = %d, want ViewCreateACL", app.view)
	}
}

func TestReconnectNoContext(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.reconnect() //nolint:errcheck
	if app.errFlash == "" {
		t.Error("expected errFlash when no context")
	}
}

func TestStartPurgeTopic(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.width = 80
	app.height = 40

	app.startPurgeTopic("my-topic") //nolint:errcheck
	if !app.prompt.Active() {
		t.Error("expected prompt.Active() = true after startPurgeTopic")
	}
	// Dispatch closure should be wired so a typed retention fires the
	// purge command path.
	if app.promptDispatch == nil {
		t.Error("expected promptDispatch to be set")
	}
}

// ---------------------------------------------------------------------------
// executeCommand — additional commands
// ---------------------------------------------------------------------------

func TestExecuteCommandCreateTopicVariants(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.width = 80
	app.height = 40

	for _, cmd := range []string{"ct", "create-topic"} {
		app.dispatchCommand(cmd) //nolint:errcheck
		if app.view != style.ViewCreateTopic {
			t.Errorf("after :%s, view = %d, want ViewCreateTopic", cmd, app.view)
		}
	}
}

func TestExecuteCommandCreateACLVariants(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.width = 80
	app.height = 40

	for _, cmd := range []string{"acl", "create-acl"} {
		app.dispatchCommand(cmd) //nolint:errcheck
		if app.view != style.ViewCreateACL {
			t.Errorf("after :%s, view = %d, want ViewCreateACL", cmd, app.view)
		}
	}
}

func TestExecuteCommandContext(t *testing.T) {
	t.Parallel()

	cfg := &config.Config{Contexts: map[string]config.Context{"test": {}}}
	app := NewApp(nil, ConnInfo{}, cfg, "", "dev")
	app.width = 80
	app.height = 40

	for _, cmd := range []string{"ctx", "context"} {
		app.dispatchCommand(cmd) //nolint:errcheck
		if app.view != style.ViewContext {
			t.Errorf("after :%s, view = %d, want ViewContext", cmd, app.view)
		}
	}
}

func TestExecuteCommandProduce(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.width = 80
	app.height = 40

	app.dispatchCommand("produce my-topic") //nolint:errcheck
	if app.view != style.ViewProduce {
		t.Errorf("view = %d, want ViewProduce", app.view)
	}
}

// ---------------------------------------------------------------------------
// refreshMsgMatchesView / refreshMsgHasError — more cases
// ---------------------------------------------------------------------------

func TestRefreshMsgMatchesViewAllTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		msg  tea.Msg
		view style.ViewType
		want bool
	}{
		{views.TopicsRefreshMsg{}, style.ViewTopics, true},
		{views.TopicsRefreshMsg{}, style.ViewGroups, false},
		{views.TopicCountsMsg{}, style.ViewTopics, true},
		{views.GroupsRefreshMsg{}, style.ViewGroups, true},
		{views.GroupEnrichmentMsg{}, style.ViewGroups, true},
		{views.ClusterRefreshMsg{}, style.ViewCluster, true},
		{views.TopicDetailRefreshMsg{}, style.ViewTopicDetail, true},
		{views.GroupDetailRefreshMsg{}, style.ViewGroupDetail, true},
		{views.ACLsRefreshMsg{}, style.ViewACLs, true},
		{views.TopicConfigRefreshMsg{}, style.ViewTopicConfig, true},
		{views.BrokerDetailRefreshMsg{}, style.ViewBrokerDetail, true},
		{views.ConsumerErrorMsg{}, style.ViewMessages, true},
		{views.ContextRefreshMsg{}, style.ViewContext, true},
		{views.ProduceProgressMsg{}, style.ViewProduce, true},
		{views.ProduceDoneMsg{}, style.ViewProduce, true},
		{views.CreateTopicResultMsg{}, style.ViewCreateTopic, true},
		{views.CreateACLResultMsg{}, style.ViewCreateACL, true},
		{views.ResetOffsetsTopicsMsg{}, style.ViewResetOffsets, true},
		{views.ResetOffsetsResultMsg{}, style.ViewResetOffsets, true},
	}

	for _, tt := range tests {
		app := NewApp(nil, ConnInfo{}, nil, "", "dev")
		app.view = tt.view
		got := app.refreshMsgMatchesView(tt.msg)
		if got != tt.want {
			t.Errorf("refreshMsgMatchesView(%T) with view=%d = %v, want %v", tt.msg, tt.view, got, tt.want)
		}
	}
}

func TestRefreshMsgHasErrorAllTypes(t *testing.T) {
	t.Parallel()

	err := fmt.Errorf("test error")
	tests := []struct {
		msg  tea.Msg
		name string
		want bool
	}{
		{name: "TopicsRefreshMsg with err", msg: views.TopicsRefreshMsg{Err: err}, want: true},
		{name: "TopicsRefreshMsg no err", msg: views.TopicsRefreshMsg{}, want: false},
		{name: "GroupsRefreshMsg with err", msg: views.GroupsRefreshMsg{Err: err}, want: true},
		{name: "GroupsRefreshMsg no err", msg: views.GroupsRefreshMsg{}, want: false},
		{name: "ClusterRefreshMsg with err", msg: views.ClusterRefreshMsg{Err: err}, want: true},
		{name: "TopicDetailRefreshMsg with err", msg: views.TopicDetailRefreshMsg{Err: err}, want: true},
		{name: "ACLsRefreshMsg with err", msg: views.ACLsRefreshMsg{Err: err}, want: true},
		{name: "TopicConfigRefreshMsg with err", msg: views.TopicConfigRefreshMsg{Err: err}, want: true},
		{name: "BrokerDetailRefreshMsg with err", msg: views.BrokerDetailRefreshMsg{Err: err}, want: true},
		{name: "GroupDetailRefreshMsg with err", msg: views.GroupDetailRefreshMsg{Err: err}, want: true},
		{name: "ConsumerErrorMsg with err", msg: views.ConsumerErrorMsg{Err: err}, want: true},
		{name: "unknown msg", msg: tickMsg(time.Now()), want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := refreshMsgHasError(tt.msg)
			if got != tt.want {
				t.Errorf("refreshMsgHasError(%T) = %v, want %v", tt.msg, got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// renderShortcuts — cover all view types
// ---------------------------------------------------------------------------

func TestRenderShortcutsAllViews(t *testing.T) {
	t.Parallel()

	allViews := []style.ViewType{
		style.ViewTopics, style.ViewTopicDetail, style.ViewTopicConfig,
		style.ViewGroups, style.ViewGroupDetail, style.ViewCluster,
		style.ViewMessages, style.ViewMessageDetail, style.ViewContext,
		style.ViewACLs, style.ViewBrokerDetail, style.ViewProduce,
		style.ViewCreateTopic, style.ViewCreateACL, style.ViewResetOffsets,
	}

	for _, vt := range allViews {
		app := NewApp(nil, ConnInfo{}, nil, "", "dev")
		app.width = 140
		app.height = 40
		app.view = vt
		lines := app.renderShortcuts()
		// 4 view-switch hotkeys in column 1; each case has 4
		// per-view actions plus 2 always-on (<r>, <:q>) in column 2.
		// chrome.ShortcutGrid pads to max — so 6 rows.
		if len(lines) != 6 {
			t.Errorf("renderShortcuts() for view %d returned %d lines, want 6", vt, len(lines))
		}
	}
}

// ---------------------------------------------------------------------------
// View() — bordered content, flash, errFlash, help overlay
// ---------------------------------------------------------------------------

func TestViewRendersBorderedContent(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.width = 120
	app.height = 40
	app.splashActive = false
	app.loading = false
	app.viewMap[style.ViewTopics].Update(views.TopicsRefreshMsg{Topics: []kafka.TopicInfo{{Name: "t1"}}})
	app.viewMap[style.ViewTopics].Resize(116, 31)

	out := app.View().Content
	if out == "" {
		t.Error("View() returned empty")
	}
}

func TestViewRendersFlash(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.width = 120
	app.height = 40
	app.splashActive = false
	app.flash = "success!"

	if out := app.View().Content; !strings.Contains(out, "success!") {
		t.Errorf("View() missing flash text; got: %q", truncate(out, 200))
	}
}

func TestViewRendersErrFlash(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.width = 120
	app.height = 40
	app.splashActive = false
	app.errFlash = "something broke"

	if out := app.View().Content; !strings.Contains(out, "something broke") {
		t.Errorf("View() missing errFlash text; got: %q", truncate(out, 200))
	}
}

func TestViewRendersHelpOverlay(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.width = 120
	app.height = 40
	app.splashActive = false
	app.showHelp = true

	// chrome's help overlay surfaces every section title and at least
	// one resource entry. The bold+underline header style makes lipgloss
	// wrap each LETTER in its own ANSI escape, so we strip ANSI before
	// matching the section title.
	plain := stripANSI(app.View().Content)
	if !strings.Contains(plain, "RESOURCE") || !strings.Contains(plain, "Topics") {
		t.Errorf("View() did not render help overlay; got: %q", truncate(plain, 1000))
	}
}

// stripANSI removes all CSI escapes (\x1b[...<final>) from a string so
// substring assertions on the visible text don't get tripped up by
// lipgloss's per-letter style wrapping (bold+underline section
// headers, etc.).
func stripANSI(s string) string {
	var out strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == 0x1b && i+1 < len(s) && s[i+1] == '[' {
			i += 2
			for i < len(s) && (s[i] < 0x40 || s[i] > 0x7E) {
				i++
			}
			continue
		}
		out.WriteByte(s[i])
	}
	return out.String()
}

// ---------------------------------------------------------------------------
// handleAction — reset_offsets
// ---------------------------------------------------------------------------

func TestHandleActionResetOffsets(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.width = 80
	app.height = 40

	app.handleAction("reset_offsets", "my-group") //nolint:errcheck
	if app.view != style.ViewResetOffsets {
		t.Errorf("view = %d, want ViewResetOffsets", app.view)
	}
}

// ---------------------------------------------------------------------------
// handleAction — topic_config
// ---------------------------------------------------------------------------

func TestHandleActionTopicConfig(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.width = 80
	app.height = 40

	app.handleAction("topic_config", "my-topic") //nolint:errcheck
	if app.view != style.ViewTopicConfig {
		t.Errorf("view = %d, want ViewTopicConfig", app.view)
	}
}

// ---------------------------------------------------------------------------
// handleAction — switch_context same context
// ---------------------------------------------------------------------------

func TestHandleActionSwitchContextSameContext(t *testing.T) {
	t.Parallel()

	cfg := &config.Config{Contexts: map[string]config.Context{"test": {Brokers: "localhost:9092"}}}
	app := NewApp(nil, ConnInfo{Context: "test"}, cfg, "", "dev")
	app.width = 80
	app.height = 40

	app.handleAction("switch_context", "test") //nolint:errcheck

	if !app.loading {
		t.Error("expected loading = true after switch_context to same context")
	}
	if app.view != style.ViewTopics {
		t.Errorf("view = %d, want ViewTopics", app.view)
	}
}

// ---------------------------------------------------------------------------
// handleAction — confirm_delete_topic
// ---------------------------------------------------------------------------

func TestHandleActionConfirmDeleteTopic(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.width = 80
	app.height = 40

	app.handleAction("confirm_delete_topic", "my-topic") //nolint:errcheck

	if !app.confirm.Active() {
		t.Error("expected confirm.Active() = true for delete topic prompt")
	}
	if got := app.confirm.Prompt(); !strings.Contains(got, "my-topic") {
		t.Errorf("confirm prompt = %q, want it to reference my-topic", got)
	}
}

// ---------------------------------------------------------------------------
// handleAction — confirm_delete_group
// ---------------------------------------------------------------------------

func TestHandleActionConfirmDeleteGroup(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.width = 80
	app.height = 40

	app.handleAction("confirm_delete_group", "my-group") //nolint:errcheck

	if !app.confirm.Active() {
		t.Error("expected confirm.Active() = true for delete group prompt")
	}
	if got := app.confirm.Prompt(); !strings.Contains(got, "my-group") {
		t.Errorf("confirm prompt = %q, want it to reference my-group", got)
	}
}

// ---------------------------------------------------------------------------
// applySwitchContext — success
// ---------------------------------------------------------------------------

func TestApplySwitchContextSuccess(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.width = 80
	app.height = 40

	msg := switchContextMsg{
		name: "prod",
		client: func() *kafka.Client {
			c, _ := kafka.NewClient(kafka.AuthConfig{Method: kafka.AuthPlaintext}, "new-broker:9092")
			return c
		}(),
		info: ConnInfo{Context: "prod", Auth: "iam", Brokers: "new-broker:9092"},
	}

	app.applySwitchContext(msg) //nolint:errcheck

	if app.connInfo.Context != "prod" {
		t.Errorf("connInfo.Context = %q, want prod", app.connInfo.Context)
	}
	if app.view != style.ViewTopics {
		t.Errorf("view = %d, want ViewTopics", app.view)
	}
	if !app.loading {
		t.Error("expected loading = true after successful context switch")
	}
	if app.client != msg.client {
		t.Error("client not swapped")
	}
}

// ---------------------------------------------------------------------------
// applySwitchContext — error
// ---------------------------------------------------------------------------

func TestApplySwitchContextError(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.width = 80
	app.height = 40
	app.loading = true

	msg := switchContextMsg{
		err: fmt.Errorf("connection refused"),
	}

	app.applySwitchContext(msg) //nolint:errcheck

	if app.errFlash == "" {
		t.Error("expected errFlash to be set on error")
	}
	if app.loading {
		t.Error("expected loading = false after error")
	}
}

// ---------------------------------------------------------------------------
// handleKey — esc clears filter
// ---------------------------------------------------------------------------

func TestHandleKeyEscClearsFilter(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.width = 80
	app.height = 40
	app.loading = false
	app.filter = "orders"

	app.handleKey(escKey()) //nolint:errcheck

	if app.filter != "" {
		t.Errorf("filter = %q, want empty after esc", app.filter)
	}
}

// ---------------------------------------------------------------------------
// handleKey — esc pops view
// ---------------------------------------------------------------------------

func TestHandleKeyEscPopsView(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.width = 80
	app.height = 40
	app.loading = false

	app.pushView(style.ViewTopics)
	app.view = style.ViewGroupDetail

	app.handleKey(escKey()) //nolint:errcheck

	if app.view != style.ViewTopics {
		t.Errorf("view = %d, want ViewTopics after esc pop", app.view)
	}
}

// filterRecorder wraps a real view and remembers the last filter pushed to it.
type filterRecorder struct {
	inner
	filter string
}

type inner = views.View

func (f *filterRecorder) SetFilter(filter string) {
	f.filter = filter
	f.inner.SetFilter(filter)
}

// ---------------------------------------------------------------------------
// handleKey — esc out of a drilled-in view keeps the parent's filter
// ---------------------------------------------------------------------------

// Filtering the topics list, opening a topic, then pressing esc used to clear
// the topics filter (carried into the child view) instead of going back, and a
// second esc returned to an unfiltered list.
func TestHandleKeyEscFromDrillInRestoresParentFilter(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.width = 80
	app.height = 40
	app.loading = false

	topics := &filterRecorder{inner: app.viewMap[style.ViewTopics]}
	app.viewMap[style.ViewTopics] = topics
	app.onFilterChange("orders")

	app.openView(style.ViewMessages, nil) //nolint:errcheck
	if app.filter != "" {
		t.Fatalf("filter in drilled-in view = %q, want empty", app.filter)
	}

	app.handleKey(escKey()) //nolint:errcheck

	if app.view != style.ViewTopics {
		t.Fatalf("view = %d, want ViewTopics after one esc", app.view)
	}
	if app.filter != "orders" {
		t.Errorf("app filter = %q, want %q restored", app.filter, "orders")
	}
	if topics.filter != "orders" {
		t.Errorf("topics view filter = %q, want %q re-applied", topics.filter, "orders")
	}
}

// ---------------------------------------------------------------------------
// handleKey — refresh
// ---------------------------------------------------------------------------

func TestHandleKeyRefresh(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.width = 80
	app.height = 40
	app.loading = false

	_, cmd := app.handleKey(keyPress('r'))
	if cmd == nil {
		t.Error("expected non-nil cmd from 'r' refresh key")
	}
}

// ---------------------------------------------------------------------------
// Update — versionCheckMsg
// ---------------------------------------------------------------------------

func TestUpdateVersionCheckMsg(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.Update(versionCheckMsg{latest: "v2.0.0"}) //nolint:errcheck

	if app.latestVersion != "v2.0.0" {
		t.Errorf("latestVersion = %q, want v2.0.0", app.latestVersion)
	}
}

// ---------------------------------------------------------------------------
// Update — loading.TickMsg
// ---------------------------------------------------------------------------

func TestUpdateLoadingTickMsg(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.width = 80
	app.height = 40

	first := app.loader.Tip()
	// tuikit/loading rotates the tip every 30 ticks (defaultRotateEvery).
	for range 30 {
		app.Update(loading.TickMsg{Time: time.Now()}) //nolint:errcheck
	}

	if got := app.loader.Tip(); got == first {
		t.Errorf("loader tip did not rotate after 30 ticks (still %q)", got)
	}
}

// ---------------------------------------------------------------------------
// renderLoadingContent
// ---------------------------------------------------------------------------

func TestRenderLoadingContent(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{Context: "test", Auth: "iam"}, nil, "", "dev")
	app.width = 80
	app.height = 40
	app.loading = true

	content := app.renderLoadingContent()
	if content == "" {
		t.Error("renderLoadingContent() returned empty")
	}
}

// ---------------------------------------------------------------------------
// renderSplash
// ---------------------------------------------------------------------------

func TestRenderSplash(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.width = 80
	app.height = 40

	splash := app.renderSplash()
	if splash == "" {
		t.Error("renderSplash() returned empty")
	}
	if !strings.Contains(splash, "Connecting") {
		t.Error("expected splash to contain 'Connecting'")
	}
}

// ---------------------------------------------------------------------------
// executePendingAction — delete_topic
// ---------------------------------------------------------------------------

func TestExecutePendingDeleteTopicConfirmed(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.loading = false
	pc := pendingConfirm{action: "delete_topic", param: "my-topic"}

	cmd := app.executeConfirmAction(pc, true)
	if !app.loading {
		t.Error("expected loading = true after confirming delete_topic")
	}
	if cmd == nil {
		t.Error("expected non-nil cmd after confirming delete_topic")
	}
}

func TestExecutePendingDeleteTopicDeclined(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.loading = false
	pc := pendingConfirm{action: "delete_topic", param: "my-topic"}

	cmd := app.executeConfirmAction(pc, false)
	if app.loading {
		t.Error("expected loading = false after declining delete_topic")
	}
	if cmd != nil {
		t.Error("expected nil cmd after declining delete_topic")
	}
}

// ---------------------------------------------------------------------------
// executePendingAction — delete_acl
// ---------------------------------------------------------------------------

func TestExecutePendingDeleteACLConfirmed(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.loading = false
	pc := pendingConfirm{action: "delete_acl", param: "some-acl-key"}

	// aclsView is nil so doDeleteACL returns a cmd that yields an error msg.
	// We just verify it does not panic and sets loading.
	cmd := app.executeConfirmAction(pc, true)
	if !app.loading {
		t.Error("expected loading = true after confirming delete_acl")
	}
	if cmd == nil {
		t.Error("expected non-nil cmd after confirming delete_acl")
	}
}

// ---------------------------------------------------------------------------
// executePendingAction — delete_group
// ---------------------------------------------------------------------------

func TestExecutePendingDeleteGroupConfirmed(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.loading = false
	pc := pendingConfirm{action: "delete_group", param: "my-group"}

	cmd := app.executeConfirmAction(pc, true)
	if !app.loading {
		t.Error("expected loading = true after confirming delete_group")
	}
	if cmd == nil {
		t.Error("expected non-nil cmd after confirming delete_group")
	}
}

// ---------------------------------------------------------------------------
// executePendingAction — save_message
// ---------------------------------------------------------------------------

func TestExecutePendingSaveMessage(t *testing.T) {
	t.Parallel()

	msg := &kafka.ConsumedMessage{
		Topic:     "orders",
		Partition: 0,
		Offset:    42,
		Key:       "key-1",
		Value:     `{"id":1}`,
		Time:      time.Now(),
	}

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.width = 80
	app.height = 40

	// Set up MessageDetailView so resolveMessage can find the message.
	app.setView(style.ViewMessageDetail, views.NewMessageDetailView(msg))
	app.view = style.ViewMessageDetail

	dir := t.TempDir()
	path := filepath.Join(dir, "test-msg.json")

	pc := pendingConfirm{action: "save_message", param: "detail"}
	app.executeValueAction(pc, path) //nolint:errcheck

	if app.flash == "" {
		t.Error("expected flash to be set after saving message")
	}
	if app.errFlash != "" {
		t.Errorf("unexpected errFlash: %s", app.errFlash)
	}

	// Verify file was created.
	if _, err := os.Stat(path); err != nil {
		t.Errorf("saved file not found: %v", err)
	}
}

// ---------------------------------------------------------------------------
// executePendingAction — alter_topic_config
// ---------------------------------------------------------------------------

func TestExecutePendingAlterTopicConfigValid(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.loading = false
	pc := pendingConfirm{action: "alter_topic_config", param: "my-topic\x00retention.ms"}

	_, cmd := app.executeValueAction(pc, "86400000")
	if !app.loading {
		t.Error("expected loading = true after valid alter_topic_config")
	}
	if cmd == nil {
		t.Error("expected non-nil cmd after valid alter_topic_config")
	}
}

// ---------------------------------------------------------------------------
// executePendingAction — purge_topic
// ---------------------------------------------------------------------------

func TestExecutePendingPurgeTopicValid(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.loading = false
	pc := pendingConfirm{action: "purge_topic", param: "my-topic"}

	_, cmd := app.executeValueAction(pc, "1h")
	if !app.loading {
		t.Error("expected loading = true after valid purge_topic")
	}
	if cmd == nil {
		t.Error("expected non-nil cmd for purge_topic 1h")
	}
}

func TestExecutePendingPurgeTopicPermanent(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.loading = false
	pc := pendingConfirm{action: "purge_topic", param: "my-topic"}

	_, cmd := app.executeValueAction(pc, "1h!")
	if !app.loading {
		t.Error("expected loading = true after purge_topic permanent")
	}
	if cmd == nil {
		t.Error("expected non-nil cmd for purge_topic 1h!")
	}
}

func TestExecutePendingPurgeTopicAll(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.loading = false
	pc := pendingConfirm{action: "purge_topic", param: "my-topic"}

	_, cmd := app.executeValueAction(pc, "0")
	if !app.loading {
		t.Error("expected loading = true after purge_topic 0")
	}
	if cmd == nil {
		t.Error("expected non-nil cmd for purge_topic 0")
	}
}

func TestExecutePendingPurgeTopicPermanentZero(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.loading = false
	pc := pendingConfirm{action: "purge_topic", param: "my-topic"}

	_, cmd := app.executeValueAction(pc, "0!")
	if !app.loading {
		t.Error("expected loading = true after purge_topic 0!")
	}
	if cmd == nil {
		t.Error("expected non-nil cmd for purge_topic 0!")
	}
}

func TestExecutePendingPurgeTopicInvalid(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.loading = false
	pc := pendingConfirm{action: "purge_topic", param: "my-topic"}

	errMsg, _ := app.executeValueAction(pc, "bad")
	if errMsg == "" {
		t.Error("expected errMsg for invalid purge duration")
	}
	if app.loading {
		t.Error("expected loading = false after invalid purge_topic")
	}
}

// ---------------------------------------------------------------------------
// executePendingAction — increase_partitions
// ---------------------------------------------------------------------------

func TestExecutePendingIncreasePartitionsInvalid(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.loading = false

	tdv := views.NewTopicDetailView(nil, "test-topic")
	tdv.Update(views.TopicDetailRefreshMsg{Detail: &kafka.TopicDetail{ //nolint:errcheck
		Name:       "test-topic",
		Partitions: []kafka.PartitionInfo{{ID: 0}, {ID: 1}, {ID: 2}},
	}})
	app.setView(style.ViewTopicDetail, tdv)
	app.view = style.ViewTopicDetail

	pc := pendingConfirm{action: "increase_partitions", param: "test-topic"}

	// Non-numeric input.
	if errMsg, _ := app.executeValueAction(pc, "abc"); errMsg == "" {
		t.Error("expected errMsg for non-numeric partition count")
	}
}

func TestExecutePendingIncreasePartitionsTooLow(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.loading = false

	tdv := views.NewTopicDetailView(nil, "test-topic")
	tdv.Update(views.TopicDetailRefreshMsg{Detail: &kafka.TopicDetail{ //nolint:errcheck
		Name:       "test-topic",
		Partitions: []kafka.PartitionInfo{{ID: 0}, {ID: 1}, {ID: 2}},
	}})
	app.setView(style.ViewTopicDetail, tdv)
	app.view = style.ViewTopicDetail

	pc := pendingConfirm{action: "increase_partitions", param: "test-topic"}

	// Lower than current (3 partitions).
	errMsg, _ := app.executeValueAction(pc, "2")
	if errMsg == "" {
		t.Error("expected errMsg for partition count lower than current")
	}
	if app.loading {
		t.Error("expected loading = false after too-low partition count")
	}
}

// handleCommandKey was the legacy in-app polymorphic command-bar
// handler. All four tests it carried (Esc closes, Enter persists,
// typing forwards, SelectAll mute) are now covered by tuikit's
// CommandBar / Prompt / FilterBar / Confirm tests in
// github.com/blairham/tuikit/chrome — k4a no longer owns that
// behavior, so the tests don't have anything app-local to assert.

// ---------------------------------------------------------------------------
// handleAction — broker_detail
// ---------------------------------------------------------------------------

func TestHandleActionBrokerDetail(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.width = 80
	app.height = 40

	// Set up ClusterView with broker data.
	cv := views.NewClusterView(nil)
	cv.Update(views.ClusterRefreshMsg{Cluster: &kafka.ClusterInfo{
		ClusterID:    "test-cluster",
		ControllerID: 1,
		Brokers: []kafka.BrokerInfo{
			{ID: 1, Host: "broker-1", Port: 9092},
			{ID: 2, Host: "broker-2", Port: 9092},
		},
	}})
	app.setView(style.ViewCluster, cv)

	app.handleAction("broker_detail", "1") //nolint:errcheck

	if app.view != style.ViewBrokerDetail {
		t.Errorf("view = %d, want ViewBrokerDetail", app.view)
	}
}

func TestHandleActionBrokerDetailNilCluster(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.width = 80
	app.height = 40

	// No ClusterView set up — should be a no-op.
	prevView := app.view
	app.handleAction("broker_detail", "1") //nolint:errcheck

	if app.view != prevView {
		t.Errorf("view should not change when ClusterView is nil, got %d", app.view)
	}
}

// ---------------------------------------------------------------------------
// updateActiveTable — FormView vs regular
// ---------------------------------------------------------------------------

func TestUpdateActiveTableFormView(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.width = 80
	app.height = 40

	// ProduceView implements FormView.
	pv := views.NewProduceView(nil, "test-topic")
	app.setView(style.ViewProduce, pv)
	app.view = style.ViewProduce
	app.resizeActiveView()

	// Should not panic — dispatches through FormView.HandleKeyMsg.
	cmd := app.updateActiveTable(tea.KeyPressMsg{Code: 'a', Text: "a"})
	_ = cmd // may or may not be nil depending on the form state
}

func TestUpdateActiveTableNonFormView(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.width = 80
	app.height = 40

	// TopicsView is a regular table view, not a FormView.
	app.viewMap[style.ViewTopics].Update(views.TopicsRefreshMsg{Topics: []kafka.TopicInfo{
		{Name: "t1"}, {Name: "t2"},
	}})
	app.viewMap[style.ViewTopics].Resize(76, 31)
	app.view = style.ViewTopics

	// Should dispatch via UpdateTable (j = move down).
	cmd := app.updateActiveTable(tea.KeyPressMsg{Code: 'j', Text: "j"})
	_ = cmd // table update may return nil
}

// TestVimNavKeysMoveTheTableCursor pins that j/k and ctrl+f/ctrl+b reach
// the table as movement. tuikit's table keymap deliberately leaves j/k
// unbound and expects the app to translate them, so without the
// translation in updateActiveTable they silently do nothing.
func TestVimNavKeysMoveTheTableCursor(t *testing.T) {
	t.Parallel()

	selected := func(app *App) string {
		_, name := app.viewMap[style.ViewTopics].HandleKey(views.KeyEnter)
		return name
	}
	press := func(app *App, key tea.KeyPressMsg) {
		app.handleKey(key) //nolint:errcheck // model return is the app itself
	}

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.width = 80
	app.height = 40
	app.splashActive = false
	app.viewMap[style.ViewTopics].Update(views.TopicsRefreshMsg{Topics: []kafka.TopicInfo{
		{Name: "t1"}, {Name: "t2"}, {Name: "t3"},
	}})
	app.viewMap[style.ViewTopics].Resize(76, 31)
	app.view = style.ViewTopics

	if got := selected(app); got != "t1" {
		t.Fatalf("initial selection = %q, want t1", got)
	}
	press(app, tea.KeyPressMsg{Code: 'j', Text: "j"})
	if got := selected(app); got != "t2" {
		t.Fatalf("after j: selection = %q, want t2", got)
	}
	press(app, tea.KeyPressMsg{Code: 'k', Text: "k"})
	if got := selected(app); got != "t1" {
		t.Fatalf("after k: selection = %q, want t1", got)
	}
	press(app, tea.KeyPressMsg{Code: 'f', Mod: tea.ModCtrl})
	if got := selected(app); got != "t3" {
		t.Fatalf("after ctrl+f: selection = %q, want t3", got)
	}
	press(app, tea.KeyPressMsg{Code: 'b', Mod: tea.ModCtrl})
	if got := selected(app); got != "t1" {
		t.Fatalf("after ctrl+b: selection = %q, want t1", got)
	}
}

// ---------------------------------------------------------------------------
// View rendering paths
// ---------------------------------------------------------------------------

func TestViewFullRender(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{Context: "test", Auth: "iam"}, nil, "", "dev")
	app.Update(tea.WindowSizeMsg{Width: 120, Height: 40}) //nolint:errcheck

	// Load topics data to get past the splash/loading state.
	app.Update(views.TopicsRefreshMsg{Topics: []kafka.TopicInfo{ //nolint:errcheck
		{Name: "orders", Partitions: 3},
	}})
	app.Update(views.TopicCountsMsg{Counts: map[string]int64{"orders": 100}}) //nolint:errcheck
	app.splashActive = false
	app.loading = false

	view := app.View()
	if view.Content == "" {
		t.Error("View() returned empty content for full render")
	}
	// Should contain view structure elements.
	if !strings.Contains(view.Content, "orders") {
		t.Error("expected 'orders' topic in rendered output")
	}
}

func TestViewSplashRender(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.width = 120
	app.height = 40
	app.splashActive = true

	view := app.View()
	if view.Content == "" {
		t.Error("View() returned empty content for splash")
	}
	if !strings.Contains(view.Content, "Connecting") {
		t.Error("expected 'Connecting' in splash view")
	}
}

// ---------------------------------------------------------------------------
// renderActiveView — data loaded
// ---------------------------------------------------------------------------

func TestRenderActiveViewWithData(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.width = 120
	app.height = 40
	app.loading = false

	app.viewMap[style.ViewTopics].Resize(app.innerWidth(), app.tableHeight())
	app.viewMap[style.ViewTopics].Update(views.TopicsRefreshMsg{Topics: []kafka.TopicInfo{
		{Name: "orders", Partitions: 3},
		{Name: "events", Partitions: 6},
	}})
	app.viewMap[style.ViewTopics].Update(views.TopicCountsMsg{Counts: map[string]int64{
		"orders": 100,
		"events": 500,
	}})

	out := app.renderActiveView()
	if out == "" {
		t.Error("renderActiveView() returned empty with data loaded")
	}
}

// ---------------------------------------------------------------------------
// renderActiveView — loading state
// ---------------------------------------------------------------------------

func TestRenderActiveViewLoadingState(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{Context: "test", Auth: "iam"}, nil, "", "dev")
	app.width = 100
	app.height = 40
	app.loading = true

	out := app.renderActiveView()
	if out == "" {
		t.Error("renderActiveView() returned empty in loading state")
	}
}

// ---------------------------------------------------------------------------
// activeViewCount — nil/unregistered view
// ---------------------------------------------------------------------------

func TestActiveViewCountNilView(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.view = style.ViewType(99) // unregistered view type

	if c := app.activeViewCount(); c != 0 {
		t.Errorf("activeViewCount() = %d, want 0 for unregistered view", c)
	}
}

// ---------------------------------------------------------------------------
// activeViewLoading — nil/unregistered view
// ---------------------------------------------------------------------------

func TestActiveViewLoadingNilView(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.loading = false
	app.view = style.ViewType(99)

	if app.activeViewLoading() {
		t.Error("activeViewLoading() should return false for unregistered view when app.loading=false")
	}

	app.loading = true
	if !app.activeViewLoading() {
		t.Error("activeViewLoading() should return true when app.loading=true regardless of view")
	}
}

// ---------------------------------------------------------------------------
// renderResourceTitle — TopicDetail view
// ---------------------------------------------------------------------------

func TestRenderResourceTitleTopicDetail(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.width = 80
	app.height = 40

	tdv := views.NewTopicDetailView(nil, "orders")
	app.setView(style.ViewTopicDetail, tdv)
	app.view = style.ViewTopicDetail

	title := app.renderResourceTitle()
	if !strings.Contains(title, "orders") {
		t.Errorf("renderResourceTitle() = %q, expected to contain 'orders'", title)
	}
}

// ---------------------------------------------------------------------------
// renderResourceTitle — Messages view
// ---------------------------------------------------------------------------

func TestRenderResourceTitleMessages(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.width = 80
	app.height = 40

	mv := views.NewMessagesView(nil, "orders")
	app.setView(style.ViewMessages, mv)
	app.view = style.ViewMessages

	title := app.renderResourceTitle()
	// Messages view renders resource title with "message" in it.
	lower := strings.ToLower(title)
	if !strings.Contains(lower, "message") {
		t.Errorf("renderResourceTitle() = %q, expected to contain 'message'", title)
	}
}

// ---------------------------------------------------------------------------
// renderResourceTitle — with active filter
// ---------------------------------------------------------------------------

func TestRenderResourceTitleWithFilter(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.width = 80
	app.height = 40
	app.filter = "test"

	title := app.renderResourceTitle()
	if !strings.Contains(title, "test") {
		t.Errorf("renderResourceTitle() = %q, expected to contain filter 'test'", title)
	}
}

// ---------------------------------------------------------------------------
// handleKey — ctrl+c quits
// ---------------------------------------------------------------------------

func TestHandleKeyCtrlCQuit(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.width = 80
	app.height = 40

	_, cmd := app.handleKey(tea.KeyPressMsg{Code: tea.KeyEscape, Mod: tea.ModCtrl, Text: "ctrl+c"})
	// ctrl+c should return tea.Quit.
	if cmd == nil {
		t.Error("expected non-nil cmd (tea.Quit) from ctrl+c")
	}
}

// ---------------------------------------------------------------------------
// handleKey — '4' switches to ACLs
// ---------------------------------------------------------------------------

func TestHandleKeyNumberSwitchesView(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.width = 80
	app.height = 40

	app.handleKey(keyPress('4')) //nolint:errcheck
	if app.view != style.ViewACLs {
		t.Errorf("after '4', view = %d, want ViewACLs (%d)", app.view, style.ViewACLs)
	}
}

// ---------------------------------------------------------------------------
// stopStoppableViews — should not panic
// ---------------------------------------------------------------------------

func TestStopStoppableViews(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.width = 80
	app.height = 40

	// Set up a MessagesView which implements Stoppable.
	mv := views.NewMessagesView(nil, "test-topic")
	app.setView(style.ViewMessages, mv)

	// Should not panic even though the consumer hasn't been started.
	app.stopStoppableViews()
}

// ---------------------------------------------------------------------------
// executeCommand — :reset in GroupDetail
// ---------------------------------------------------------------------------

func TestExecuteCommandResetInGroupDetail(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.width = 80
	app.height = 40
	app.loading = false

	gdv := views.NewGroupDetailView(nil, "test-group")
	gdv.Update(views.GroupDetailRefreshMsg{Detail: &kafka.GroupDetail{
		GroupID: "test-group",
		State:   "Empty",
		Offsets: []kafka.GroupOffsetInfo{
			{Topic: "orders", Partition: 0, CommittedOffset: 100, HighWatermark: 150, Lag: 50},
		},
	}})
	gdv.Resize(80, 30)
	app.setView(style.ViewGroupDetail, gdv)
	app.view = style.ViewGroupDetail

	app.dispatchCommand("reset") //nolint:errcheck

	if !app.confirm.Active() {
		t.Error("expected confirm.Active() = true after :reset on a populated group detail")
	}
}

// ---------------------------------------------------------------------------
// executeCommand — :reconnect with context
// ---------------------------------------------------------------------------

func TestExecuteCommandReconnectWithContext(t *testing.T) {
	t.Parallel()

	cfg := &config.Config{Contexts: map[string]config.Context{"test": {Brokers: "localhost:9092"}}}
	app := NewApp(nil, ConnInfo{Context: "test"}, cfg, "", "dev")
	app.width = 80
	app.height = 40

	app.dispatchCommand("reconnect") //nolint:errcheck

	if !strings.Contains(app.flash, "reconnecting") {
		t.Errorf("flash = %q, expected to contain 'reconnecting'", app.flash)
	}
}

// ---------------------------------------------------------------------------
// Update — deleteOffsetMsg success
// ---------------------------------------------------------------------------

func TestUpdateDeleteOffsetMsgSuccess(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.loading = true
	app.Update(deleteOffsetMsg{}) //nolint:errcheck

	if app.loading {
		t.Error("loading should be false after deleteOffsetMsg")
	}
	if app.flash != "Offset deleted" {
		t.Errorf("flash = %q, want 'Offset deleted'", app.flash)
	}
}

// ---------------------------------------------------------------------------
// Update — setRetentionMsg success
// ---------------------------------------------------------------------------

func TestUpdateSetRetentionMsgSuccess(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.loading = true
	app.Update(setRetentionMsg{topic: "t", retentionMs: 86400000}) //nolint:errcheck

	if app.loading {
		t.Error("loading should be false after setRetentionMsg")
	}
	if !strings.Contains(app.flash, "retention") {
		t.Errorf("flash = %q, expected to contain 'retention'", app.flash)
	}
}

// ---------------------------------------------------------------------------
// Update — resetRetentionMsg success
// ---------------------------------------------------------------------------

func TestUpdateResetRetentionMsgSuccess(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.loading = true
	app.Update(resetRetentionMsg{topic: "t"}) //nolint:errcheck

	if app.loading {
		t.Error("loading should be false after resetRetentionMsg")
	}
	if !strings.Contains(app.flash, "retention override removed") {
		t.Errorf("flash = %q, expected to contain 'retention override removed'", app.flash)
	}
}

// ---------------------------------------------------------------------------
// Update — PasteMsg in commanding mode
// ---------------------------------------------------------------------------

func TestUpdatePasteMsg(t *testing.T) {
	t.Parallel()

	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.width = 80
	app.height = 40
	app.commandBar.Open()

	app.Update(tea.PasteMsg{Content: "hello"}) //nolint:errcheck

	val := app.commandBar.Input().Value()
	if !strings.Contains(val, "hello") {
		t.Errorf("commandBar value = %q, expected to contain 'hello'", val)
	}
}
