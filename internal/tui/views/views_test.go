// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package views

import (
	"fmt"
	"strings"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"

	"github.com/blairham/k4a/internal/kafka"
)

const (
	actionMessages    = "messages"
	actionTopicDetail = "topic_detail"
	actionGroupDetail = "group_detail"
	actionSaveMessage = "save_message"

	testTopic     = "my-topic"
	testGroup     = "my-group"
	testTopicTail = "test-topic"
)

// ---------------------------------------------------------------------------
// parseFilter / matchesFilter
// ---------------------------------------------------------------------------

func TestParseFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input      string
		wantEmpty  bool
		wantNegate bool
	}{
		{"foo", false, false},
		{"!foo", false, true},
		{"", true, false},
		{"!", true, false},
		{"order.*event", false, false},
		{"!internal", false, true},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			f := parseFilter(tt.input)
			if f.Empty() != tt.wantEmpty {
				t.Errorf("parseFilter(%q).Empty() = %v, want %v", tt.input, f.Empty(), tt.wantEmpty)
			}
			// Negation can't be probed directly through tuikit/table.RowFilter
			// (the negate field is package-private). Instead, feed in a string
			// the regex would NOT match and check whether the filter returns
			// true. A non-empty negated filter returns true on no-match;
			// a non-empty positive filter returns false on no-match.
			if !tt.wantEmpty {
				gotNegate := f.MatchesAny("\x00impossible-marker\x00")
				if gotNegate != tt.wantNegate {
					t.Errorf("parseFilter(%q) negate behavior = %v, want %v",
						tt.input, gotNegate, tt.wantNegate)
				}
			}
		})
	}
}

func TestRowFilterMatches(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		filter string
		text   string
		want   bool
	}{
		{"literal", "world", "hello world", true},
		{"literal miss", "foo", "hello world", false},
		{"case insensitive", "HELLO", "hello world", true},
		{"regex dot star", "order.*event", "order-events", true},
		{"regex dot star miss", "event.*order", "order-events", false},
		{"regex alternation", "foo|bar", "something bar here", true},
		{"negate hit", "!world", "hello world", false},
		{"negate miss", "!foo", "hello world", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			f := parseFilter(tt.filter)
			if got := f.MatchesAny(tt.text); got != tt.want {
				t.Errorf("parseFilter(%q).MatchesAny(%q) = %v, want %v",
					tt.filter, tt.text, got, tt.want)
			}
		})
	}
}

func TestRowFilterMatchesAny(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		filter string
		fields []string
		want   bool
	}{
		{"hit first", "alice", []string{"alice", "bob"}, true},
		{"hit second", "bob", []string{"alice", "bob"}, true},
		{"miss all", "eve", []string{"alice", "bob"}, false},
		{"negate hit", "!alice", []string{"alice", "bob"}, false},
		{"negate miss", "!eve", []string{"alice", "bob"}, true},
		{"regex any", "ali.*", []string{"alice", "bob"}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			f := parseFilter(tt.filter)
			if got := f.MatchesAny(tt.fields...); got != tt.want {
				t.Errorf("parseFilter(%q).MatchesAny(%v) = %v, want %v",
					tt.filter, tt.fields, got, tt.want)
			}
		})
	}
}

func TestRowFilterInvalidRegexFallback(t *testing.T) {
	t.Parallel()

	// Invalid regex should fall back to literal match.
	f := parseFilter("[invalid")
	if f.Empty() {
		t.Fatal("expected non-empty filter for invalid regex")
	}
	// Should match the literal string "[invalid".
	if !f.MatchesAny("something [invalid here") {
		t.Error("expected literal fallback to match")
	}
}

// ---------------------------------------------------------------------------
// formatCount
// ---------------------------------------------------------------------------

func TestFormatCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		want string
		n    int64
	}{
		{"0", 0},
		{"1", 1},
		{"999", 999},
		{"1.0K", 1000},
		{"1.5K", 1500},
		{"1000.0K", 999_999},
		{"1.0M", 1_000_000},
		{"1.5M", 1_500_000},
		{"1.0B", 1_000_000_000},
		{"2.5B", 2_500_000_000},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%d", tt.n), func(t *testing.T) {
			t.Parallel()
			if got := formatCount(tt.n); got != tt.want {
				t.Errorf("formatCount(%d) = %q, want %q", tt.n, got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// formatBytes
// ---------------------------------------------------------------------------

func TestFormatBytes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		want string
		b    int64
	}{
		{"0 bytes", 0},
		{"512 bytes", 512},
		{"1023 bytes", 1023},
		{"1.0 KB", 1024},
		{"1.5 KB", 1536},
		{"1.0 MB", 1024 * 1024},
		{"1.5 MB", 1024*1024 + 512*1024},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%d", tt.b), func(t *testing.T) {
			t.Parallel()
			if got := formatBytes(tt.b); got != tt.want {
				t.Errorf("formatBytes(%d) = %q, want %q", tt.b, got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// prettyJSON
// ---------------------------------------------------------------------------

func TestPrettyJSONValid(t *testing.T) {
	t.Parallel()

	result := prettyJSON(`{"name":"k4a","version":1}`, 80)
	if result == `{"name":"k4a","version":1}` {
		t.Error("expected pretty-printed output, got raw input back")
	}
	// Should contain indented content (not a raw one-liner).
	if len(result) < 20 {
		t.Errorf("output too short: %q", result)
	}
}

func TestPrettyJSONInvalid(t *testing.T) {
	t.Parallel()

	raw := "not json at all"
	if got := prettyJSON(raw, 80); got != raw {
		t.Errorf("expected raw fallback, got %q", got)
	}
}

func TestPrettyJSONEmpty(t *testing.T) {
	t.Parallel()

	raw := ""
	if got := prettyJSON(raw, 80); got != raw {
		t.Errorf("expected empty string fallback, got %q", got)
	}
}

// ---------------------------------------------------------------------------
// keyDisplay
// ---------------------------------------------------------------------------

func TestKeyDisplay(t *testing.T) {
	t.Parallel()

	// Non-empty returns as-is.
	if got := keyDisplay("my-key"); got != "my-key" {
		t.Errorf("keyDisplay(my-key) = %q", got)
	}

	// Empty returns a styled "(null)" — check it's non-empty.
	if got := keyDisplay(""); got == "" {
		t.Error("keyDisplay(\"\") should return non-empty styled string")
	}
}

// ---------------------------------------------------------------------------
// buildTopicColumns / buildMsgColumns
// ---------------------------------------------------------------------------

func TestBuildTopicColumns(t *testing.T) {
	t.Parallel()

	cols := buildTopicColumns(120)
	if len(cols) != 6 {
		t.Fatalf("expected 6 columns, got %d", len(cols))
	}
	if cols[0].Title != "NAME" {
		t.Errorf("first column should be NAME, got %q", cols[0].Title)
	}
	// NAME should get remaining width.
	if cols[0].Width < 20 {
		t.Errorf("NAME width %d is too small", cols[0].Width)
	}
}

func TestBuildTopicColumnsNarrow(t *testing.T) {
	t.Parallel()

	// With very narrow width, NAME should clamp to minimum 20.
	cols := buildTopicColumns(10)
	if cols[0].Width != 20 {
		t.Errorf("expected NAME min width 20, got %d", cols[0].Width)
	}
}

func TestBuildMsgColumns(t *testing.T) {
	t.Parallel()

	cols := buildMsgColumns(120, 0)
	if len(cols) != 5 {
		t.Fatalf("expected 5 columns, got %d", len(cols))
	}
	if cols[len(cols)-1].Title != "VALUE" {
		t.Errorf("last column should be VALUE, got %q", cols[len(cols)-1].Title)
	}
}

func TestBuildMsgColumnsNeverExceedsTotalWidth(t *testing.T) {
	t.Parallel()

	// sum(col.Width+2) must be <= totalWidth for any terminal that
	// can actually fit the fixed columns. Otherwise the bubbles
	// table's outer viewport wraps each row to two visible lines —
	// leaving blank rows between data rows.
	//
	// At totalWidth < maxFixedColsWidth() the fixed columns alone
	// already exceed the width; that's a separate (existing) horizontal-
	// scroll-needed scenario.
	floor := maxFixedColsWidth() + 2 // + VALUE column padding
	for _, total := range []int{floor, floor + 5, 96, 120, 160, 200} {
		cols := buildMsgColumns(total, 0)
		sum := 0
		for _, c := range cols {
			if c.Width > 0 {
				sum += c.Width + 2
			}
		}
		if sum > total {
			t.Errorf("buildMsgColumns(%d): sum %d exceeds total width", total, sum)
		}
	}
}

func TestBuildMsgColumnsNarrowFloorsValueAtZero(t *testing.T) {
	t.Parallel()

	// At narrow widths the VALUE column collapses to 0 — the fixed
	// columns still render, and the user can widen the terminal or
	// scroll. Previously this clamped to 20, which then overflowed
	// the table and caused per-row wrapping.
	cols := buildMsgColumns(10, 0)
	last := cols[len(cols)-1]
	if last.Width != 0 {
		t.Errorf("expected VALUE width 0 at narrow total, got %d", last.Width)
	}
}

// ---------------------------------------------------------------------------
// TopicsView
// ---------------------------------------------------------------------------

func TestTopicsViewUpdateRefresh(t *testing.T) {
	t.Parallel()

	v := NewTopicsView(nil)

	topics := []kafka.TopicInfo{
		{Name: "orders", Partitions: 6, ReplicationFactor: 3, OutOfSync: 0, Messages: -1},
		{Name: "events", Partitions: 3, ReplicationFactor: 3, OutOfSync: 1, Messages: -1},
		{Name: "_internal", Partitions: 1, ReplicationFactor: 1, OutOfSync: 0, Messages: -1, Internal: true},
	}

	// Simulate refresh message.
	v.Update(TopicsRefreshMsg{Topics: topics}) //nolint:errcheck

	// Internal topics hidden by default.
	if v.Count() != 2 {
		t.Errorf("Count() = %d, want 2 (internal hidden)", v.Count())
	}
}

func TestTopicsViewUpdateCountsMerge(t *testing.T) {
	t.Parallel()

	v := NewTopicsView(nil)

	topics := []kafka.TopicInfo{
		{Name: "orders", Partitions: 6, Messages: -1},
		{Name: "events", Partitions: 3, Messages: -1},
	}
	v.Update(TopicsRefreshMsg{Topics: topics}) //nolint:errcheck

	// Send counts message.
	v.Update(TopicCountsMsg{Counts: map[string]int64{"orders": 1000, "events": 500}}) //nolint:errcheck

	// Verify counts were merged.
	rows := v.table.Rows()
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	// Rows are sorted alphabetically: events first, orders second.
	if rows[0][4] != "500" {
		t.Errorf("events messages = %q, want %q", rows[0][4], "500")
	}
	if rows[1][4] != "1.0K" {
		t.Errorf("orders messages = %q, want %q", rows[1][4], "1.0K")
	}
}

func TestTopicsViewCountPreservation(t *testing.T) {
	t.Parallel()

	v := NewTopicsView(nil)

	// Initial load with counts.
	topics := []kafka.TopicInfo{
		{Name: "orders", Partitions: 6, Messages: -1},
	}
	v.Update(TopicsRefreshMsg{Topics: topics})                         //nolint:errcheck
	v.Update(TopicCountsMsg{Counts: map[string]int64{"orders": 5000}}) //nolint:errcheck
	v.Update(TopicsRefreshMsg{Topics: []kafka.TopicInfo{topics[0]}})   //nolint:errcheck

	// After re-refresh, old counts should be preserved.
	rows := v.table.Rows()
	if rows[0][4] != "5.0K" {
		t.Errorf("after re-refresh, orders messages = %q, want %q", rows[0][4], "5.0K")
	}
}

func TestTopicsViewFilter(t *testing.T) {
	t.Parallel()

	v := NewTopicsView(nil)

	topics := []kafka.TopicInfo{
		{Name: "orders", Partitions: 6},
		{Name: "events", Partitions: 3},
		{Name: "order-events", Partitions: 2},
	}
	v.Update(TopicsRefreshMsg{Topics: topics}) //nolint:errcheck

	v.SetFilter("order")
	if v.Count() != 2 {
		t.Errorf("filtered Count() = %d, want 2", v.Count())
	}

	v.SetFilter("events")
	if v.Count() != 2 {
		t.Errorf("filtered Count() = %d, want 2", v.Count())
	}

	v.SetFilter("")
	if v.Count() != 3 {
		t.Errorf("unfiltered Count() = %d, want 3", v.Count())
	}
}

func TestTopicsViewFilterNegate(t *testing.T) {
	t.Parallel()

	v := NewTopicsView(nil)
	v.Update(TopicsRefreshMsg{Topics: []kafka.TopicInfo{ //nolint:errcheck
		{Name: "orders"},
		{Name: "events"},
		{Name: "order-events"},
	}})

	v.SetFilter("!order")
	if v.Count() != 1 {
		t.Errorf("negated filter: Count() = %d, want 1 (only events)", v.Count())
	}
}

func TestTopicsViewFilterRegex(t *testing.T) {
	t.Parallel()

	v := NewTopicsView(nil)
	v.Update(TopicsRefreshMsg{Topics: []kafka.TopicInfo{ //nolint:errcheck
		{Name: "orders"},
		{Name: "events"},
		{Name: "order-events"},
	}})

	v.SetFilter("order.*event")
	if v.Count() != 1 {
		t.Errorf("regex filter: Count() = %d, want 1 (only order-events)", v.Count())
	}
}

func TestTopicsViewFilterCaseInsensitive(t *testing.T) {
	t.Parallel()

	v := NewTopicsView(nil)
	v.Update(TopicsRefreshMsg{Topics: []kafka.TopicInfo{ //nolint:errcheck
		{Name: "OrderEvents"},
	}})

	v.SetFilter("orderevents")
	if v.Count() != 1 {
		t.Errorf("case-insensitive filter: Count() = %d, want 1", v.Count())
	}
}

func TestTopicsViewToggleInternal(t *testing.T) {
	t.Parallel()

	v := NewTopicsView(nil)
	v.Update(TopicsRefreshMsg{Topics: []kafka.TopicInfo{ //nolint:errcheck
		{Name: "orders"},
		{Name: "_internal", Internal: true},
	}})

	if v.Count() != 1 {
		t.Errorf("internal hidden: Count() = %d, want 1", v.Count())
	}

	// Toggle internal on.
	v.HandleKey("i")
	if v.Count() != 2 {
		t.Errorf("internal shown: Count() = %d, want 2", v.Count())
	}

	// Toggle back off.
	v.HandleKey("i")
	if v.Count() != 1 {
		t.Errorf("internal hidden again: Count() = %d, want 1", v.Count())
	}
}

func TestTopicsViewHandleKeyEnter(t *testing.T) {
	t.Parallel()

	v := NewTopicsView(nil)
	v.Update(TopicsRefreshMsg{Topics: []kafka.TopicInfo{ //nolint:errcheck
		{Name: testTopicTail},
	}})
	v.Resize(80, 20)

	action, param := v.HandleKey(KeyEnter)
	if action != actionMessages || param != testTopicTail {
		t.Errorf("enter: action=%q param=%q, want messages/test-topic", action, param)
	}
}

func TestTopicsViewHandleKeyOverview(t *testing.T) {
	t.Parallel()

	v := NewTopicsView(nil)
	v.Update(TopicsRefreshMsg{Topics: []kafka.TopicInfo{ //nolint:errcheck
		{Name: testTopicTail},
	}})
	v.Resize(80, 20)

	action, param := v.HandleKey("o")
	if action != actionTopicDetail || param != testTopicTail {
		t.Errorf("o: action=%q param=%q, want topic_detail/test-topic", action, param)
	}
}

func TestTopicsViewUpdateError(t *testing.T) {
	t.Parallel()

	v := NewTopicsView(nil)
	v.Update(TopicsRefreshMsg{Err: fmt.Errorf("connection failed")}) //nolint:errcheck

	view := v.View()
	if view == "" {
		t.Error("expected error message in view")
	}
}

func TestTopicsViewAuthErrorFromCounts(t *testing.T) {
	t.Parallel()

	v := NewTopicsView(nil)

	// Load topics normally first.
	v.Update(TopicsRefreshMsg{Topics: []kafka.TopicInfo{ //nolint:errcheck
		{Name: "orders", Partitions: 6, Messages: -1},
	}})

	// Simulate what fetchCounts does when it hits an auth error:
	// it sends a TopicsRefreshMsg with the raw error; the view formats it.
	v.Update(TopicsRefreshMsg{Err: fmt.Errorf("request failed: expired token")}) //nolint:errcheck

	if v.err == nil {
		t.Fatal("err should be set after auth error from counts")
	}
	if !strings.Contains(v.err.Error(), "authentication failed") {
		t.Errorf("error should contain user-friendly message, got %q", v.err.Error())
	}

	view := v.View()
	if view == "" {
		t.Error("expected error in view")
	}
}

func TestTopicsViewConnectionError(t *testing.T) {
	t.Parallel()

	v := NewTopicsView(nil)
	v.Update(TopicsRefreshMsg{Err: fmt.Errorf("dial tcp: lookup broker.example.com: no such host")}) //nolint:errcheck

	if v.err == nil {
		t.Fatal("err should be set")
	}
	if !strings.Contains(v.err.Error(), "connection failed") {
		t.Errorf("error should contain connection message, got %q", v.err.Error())
	}
}

// ---------------------------------------------------------------------------
// GroupsView
// ---------------------------------------------------------------------------

func TestGroupsViewUpdateRefresh(t *testing.T) {
	t.Parallel()

	v := NewGroupsView(nil)

	groups := []kafka.GroupInfo{
		{GroupID: "consumer-1", State: "Stable", Members: 3},
		{GroupID: "consumer-2", State: "Empty", Members: 0},
	}
	v.Update(GroupsRefreshMsg{Groups: groups}) //nolint:errcheck

	if v.Count() != 2 {
		t.Errorf("Count() = %d, want 2", v.Count())
	}
}

func TestGroupsViewFilter(t *testing.T) {
	t.Parallel()

	v := NewGroupsView(nil)
	v.Update(GroupsRefreshMsg{Groups: []kafka.GroupInfo{ //nolint:errcheck
		{GroupID: "order-consumer"},
		{GroupID: "event-consumer"},
		{GroupID: "order-processor"},
	}})

	v.SetFilter("order")
	if v.Count() != 2 {
		t.Errorf("filtered Count() = %d, want 2", v.Count())
	}
}

func TestGroupsViewFilterNegate(t *testing.T) {
	t.Parallel()

	v := NewGroupsView(nil)
	v.Update(GroupsRefreshMsg{Groups: []kafka.GroupInfo{ //nolint:errcheck
		{GroupID: "order-consumer"},
		{GroupID: "event-consumer"},
		{GroupID: "order-processor"},
	}})

	v.SetFilter("!order")
	if v.Count() != 1 {
		t.Errorf("negated filter: Count() = %d, want 1 (only event-consumer)", v.Count())
	}
}

func TestGroupsViewHandleKeyEnter(t *testing.T) {
	t.Parallel()

	v := NewGroupsView(nil)
	v.Update(GroupsRefreshMsg{Groups: []kafka.GroupInfo{ //nolint:errcheck
		{GroupID: testGroup},
	}})
	v.Resize(80, 20)

	action, param := v.HandleKey(KeyEnter)
	if action != actionGroupDetail || param != testGroup {
		t.Errorf("enter: action=%q param=%q, want group_detail/my-group", action, param)
	}
}

func TestGroupsViewUpdateError(t *testing.T) {
	t.Parallel()

	v := NewGroupsView(nil)
	v.Update(GroupsRefreshMsg{Err: fmt.Errorf("failed")}) //nolint:errcheck

	view := v.View()
	if view == "" {
		t.Error("expected error message in view")
	}
}

// ---------------------------------------------------------------------------
// ClusterView
// ---------------------------------------------------------------------------

func TestClusterViewUpdate(t *testing.T) {
	t.Parallel()

	v := NewClusterView(nil)
	v.Update(ClusterRefreshMsg{Cluster: &kafka.ClusterInfo{ //nolint:errcheck
		ClusterID:    "test-cluster",
		ControllerID: 1,
		Brokers: []kafka.BrokerInfo{
			{ID: 0, Host: "broker-0", Port: 9092},
			{ID: 1, Host: "broker-1", Port: 9092},
			{ID: 2, Host: "broker-2", Port: 9092},
		},
	}})

	if v.Count() != 3 {
		t.Errorf("Count() = %d, want 3", v.Count())
	}

	// Controller broker (ID=1) should have checkmark.
	rows := v.table.Rows()
	if rows[1][3] != "✓" {
		t.Errorf("broker 1 controller = %q, want ✓", rows[1][3])
	}
	// Non-controller should be empty.
	if rows[0][3] != "" {
		t.Errorf("broker 0 controller = %q, want empty", rows[0][3])
	}
}

func TestClusterViewUpdateError(t *testing.T) {
	t.Parallel()

	v := NewClusterView(nil)
	v.Update(ClusterRefreshMsg{Err: fmt.Errorf("failed")}) //nolint:errcheck

	view := v.View()
	if view == "" {
		t.Error("expected error message in view")
	}
}

// ---------------------------------------------------------------------------
// GroupDetailView
// ---------------------------------------------------------------------------

func TestGroupDetailViewUpdate(t *testing.T) {
	t.Parallel()

	v := NewGroupDetailView(nil, testGroup)
	v.Update(GroupDetailRefreshMsg{Detail: &kafka.GroupDetail{ //nolint:errcheck
		GroupID: testGroup,
		State:   "Stable",
		Offsets: []kafka.GroupOffsetInfo{
			{Topic: "orders", Partition: 0, CommittedOffset: 100, HighWatermark: 150, Lag: 50},
			{Topic: "orders", Partition: 1, CommittedOffset: 200, HighWatermark: 200, Lag: 0},
		},
		TotalLag: 50,
	}})

	if v.Count() != 2 {
		t.Errorf("Count() = %d, want 2", v.Count())
	}

	rows := v.table.Rows()
	if rows[0][4] != "50" {
		t.Errorf("partition 0 lag = %q, want 50", rows[0][4])
	}
	if rows[1][4] != "0" {
		t.Errorf("partition 1 lag = %q, want 0", rows[1][4])
	}
}

// ---------------------------------------------------------------------------
// TopicDetailView
// ---------------------------------------------------------------------------

func TestTopicDetailViewUpdate(t *testing.T) {
	t.Parallel()

	v := NewTopicDetailView(nil, testTopic)
	if v.Topic() != testTopic {
		t.Errorf("Topic() = %q", v.Topic())
	}

	v.Update(TopicDetailRefreshMsg{Detail: &kafka.TopicDetail{ //nolint:errcheck
		Name:              testTopic,
		ReplicationFactor: 3,
		CleanupPolicy:     "compact",
		Partitions: []kafka.PartitionInfo{
			{ID: 0, Replicas: []int{1, 2, 3}, ISR: []int{1, 2, 3}, FirstOffset: 0, LastOffset: 1000, Messages: 1000},
			{ID: 1, Replicas: []int{1, 2, 3}, ISR: []int{1, 2}, FirstOffset: 0, LastOffset: 500, Messages: 500},
		},
	}})

	if v.Count() != 2 {
		t.Errorf("Count() = %d, want 2", v.Count())
	}
}

func TestTopicDetailViewHandleKeyPartitions(t *testing.T) {
	t.Parallel()

	v := NewTopicDetailView(nil, testTopic)

	action, param := v.HandleKey("p")
	if action != "increase_partitions" || param != testTopic {
		t.Errorf("p: action=%q param=%q, want increase_partitions/my-topic", action, param)
	}
}

func TestTopicDetailViewPartitionCount(t *testing.T) {
	t.Parallel()

	v := NewTopicDetailView(nil, testTopic)

	// Before data loads, should return 0.
	if v.PartitionCount() != 0 {
		t.Errorf("PartitionCount() = %d, want 0 before data", v.PartitionCount())
	}

	v.Update(TopicDetailRefreshMsg{Detail: &kafka.TopicDetail{ //nolint:errcheck
		Name: testTopic,
		Partitions: []kafka.PartitionInfo{
			{ID: 0}, {ID: 1}, {ID: 2},
		},
	}})

	if v.PartitionCount() != 3 {
		t.Errorf("PartitionCount() = %d, want 3", v.PartitionCount())
	}
}

func TestTopicDetailViewHandleKeyMessages(t *testing.T) {
	t.Parallel()

	v := NewTopicDetailView(nil, testTopic)

	action, param := v.HandleKey("m")
	if action != actionMessages || param != testTopic {
		t.Errorf("m: action=%q param=%q, want messages/my-topic", action, param)
	}

	action, param = v.HandleKey("j")
	if action != "" || param != "" {
		t.Errorf("j: action=%q param=%q, want empty", action, param)
	}
}

// ---------------------------------------------------------------------------
// MessagesView
// ---------------------------------------------------------------------------

func TestMessagesViewUpdate(t *testing.T) {
	t.Parallel()

	v := NewMessagesView(nil, testTopicTail)

	// Simulate receiving messages.
	v.Update(MessageMsg(kafka.ConsumedMessage{ //nolint:errcheck
		Topic:     testTopicTail,
		Partition: 0,
		Offset:    100,
		Key:       "key1",
		Value:     `{"hello":"world"}`,
		Time:      time.Date(2026, 1, 15, 10, 30, 0, 0, time.UTC),
	}))

	if v.Count() != 1 {
		t.Errorf("Count() = %d, want 1", v.Count())
	}

	// Rows are reversed (newest first).
	rows := v.table.Rows()
	if rows[0][0] != "100" {
		t.Errorf("offset = %q, want 100", rows[0][0])
	}
}

func TestMessagesViewMaxMessages(t *testing.T) {
	t.Parallel()

	v := NewMessagesView(nil, testTopicTail)

	// Add more than maxMessages.
	for i := range maxMessages + 100 {
		v.Update(MessageMsg(kafka.ConsumedMessage{ //nolint:errcheck
			Topic:  testTopicTail,
			Offset: int64(i),
			Time:   time.Now(),
		}))
	}

	if v.Count() != maxMessages {
		t.Errorf("Count() = %d, want %d (max)", v.Count(), maxMessages)
	}
}

func TestMessagesViewGetMessage(t *testing.T) {
	t.Parallel()

	v := NewMessagesView(nil, testTopicTail)
	v.Update(MessageMsg(kafka.ConsumedMessage{ //nolint:errcheck
		Topic:  testTopicTail,
		Offset: 42,
		Key:    "test-key",
		Time:   time.Now(),
	}))

	msg := v.GetMessage(0)
	if msg == nil {
		t.Fatal("GetMessage(0) returned nil")
	}
	if msg.Offset != 42 {
		t.Errorf("Offset = %d, want 42", msg.Offset)
	}

	// Out of bounds.
	if v.GetMessage(-1) != nil {
		t.Error("GetMessage(-1) should return nil")
	}
	if v.GetMessage(999) != nil {
		t.Error("GetMessage(999) should return nil")
	}
}

func TestMessagesViewHandleKey(t *testing.T) {
	t.Parallel()

	v := NewMessagesView(nil, testTopicTail)
	v.Update(MessageMsg(kafka.ConsumedMessage{ //nolint:errcheck
		Topic:  testTopicTail,
		Offset: 1,
		Time:   time.Now(),
	}))
	v.Resize(80, 20)

	// Follow mode is on by default.
	if !v.follow {
		t.Error("expected follow mode on by default")
	}

	// Toggle follow off.
	v.HandleKey("f")
	if v.follow {
		t.Error("expected follow off after 'f'")
	}

	// Toggle follow on.
	v.HandleKey("f")
	if !v.follow {
		t.Error("expected follow on after second 'f'")
	}

	// Scroll keys disable follow.
	v.HandleKey("down")
	if v.follow {
		t.Error("expected follow off after 'down'")
	}

	// 'g' enables follow.
	v.HandleKey("g")
	if !v.follow {
		t.Error("expected follow on after 'g'")
	}

	// 'G' disables follow.
	v.HandleKey("G")
	if v.follow {
		t.Error("expected follow off after 'G'")
	}

	// The vim line keys scroll too, so they disable follow like the
	// arrows they translate to.
	for _, key := range []string{"j", "k"} {
		v.HandleKey("g")
		v.HandleKey(key)
		if v.follow {
			t.Errorf("expected follow off after %q", key)
		}
	}

	// 'o' returns topic_detail action.
	action, param := v.HandleKey("o")
	if action != actionTopicDetail || param != testTopicTail {
		t.Errorf("o: action=%q param=%q, want topic_detail/test-topic", action, param)
	}
}

func TestMessagesViewHandleKeySearch(t *testing.T) {
	t.Parallel()

	v := NewMessagesView(nil, testTopicTail)
	v.Update(MessageMsg(kafka.ConsumedMessage{ //nolint:errcheck
		Topic:  testTopicTail,
		Offset: 42,
		Time:   time.Now(),
	}))
	v.Resize(80, 20)

	action, param := v.HandleKey("s")
	if action != "search" {
		t.Errorf("s: action=%q, want search", action)
	}
	if param != testTopicTail {
		t.Errorf("s: param=%q, want %q (topic)", param, testTopicTail)
	}
}

// TestMessagesViewHandleKeyPurge pins ctrl+d to the purge action the hint
// bar advertises. It used to fall through to navigation and do nothing.
func TestMessagesViewHandleKeyPurge(t *testing.T) {
	t.Parallel()

	v := NewMessagesView(nil, testTopicTail)
	v.Resize(80, 20)

	action, param := v.HandleKey("ctrl+d")
	if action != "confirm_purge_topic" {
		t.Errorf("ctrl+d: action=%q, want confirm_purge_topic", action)
	}
	if param != testTopicTail {
		t.Errorf("ctrl+d: param=%q, want %q (topic)", param, testTopicTail)
	}
}

func TestMessagesViewConsumerError(t *testing.T) {
	t.Parallel()

	v := NewMessagesView(nil, testTopicTail)
	v.loading = true

	// Raw auth error from client — view formats it via FormatUserError.
	v.Update(ConsumerErrorMsg{Err: fmt.Errorf("SASLAuthenticationFailed: expired token")}) //nolint:errcheck

	if v.loading {
		t.Error("loading should be false after ConsumerErrorMsg")
	}
	if v.err == nil {
		t.Fatal("err should be set after ConsumerErrorMsg")
	}

	view := v.View()
	if view == "" {
		t.Error("expected error message in view")
	}
	if !strings.Contains(view, "authentication failed") {
		t.Errorf("view should contain user-friendly auth message, got %q", view)
	}
}

func TestMessagesViewConsumerConnectionError(t *testing.T) {
	t.Parallel()

	v := NewMessagesView(nil, testTopicTail)
	v.loading = true

	v.Update(ConsumerErrorMsg{Err: fmt.Errorf("dial tcp: lookup broker.example.com: no such host")}) //nolint:errcheck

	if v.err == nil {
		t.Fatal("err should be set")
	}
	view := v.View()
	if !strings.Contains(view, "connection failed") {
		t.Errorf("view should contain connection error message, got %q", view)
	}
}

func TestMessagesViewConsumerErrorStopsLoading(t *testing.T) {
	t.Parallel()

	v := NewMessagesView(nil, testTopicTail)
	v.loading = true

	// Send a regular message first, then a connection error.
	v.Update(MessageMsg(kafka.ConsumedMessage{ //nolint:errcheck
		Topic:  testTopicTail,
		Offset: 1,
		Time:   time.Now(),
	}))
	v.Update(ConsumerErrorMsg{Err: fmt.Errorf("dial tcp: connection refused")}) //nolint:errcheck

	if v.err == nil {
		t.Fatal("err should be set")
	}
	// Error view takes precedence — no table rendered.
	view := v.View()
	if !strings.Contains(view, "connection failed") {
		t.Errorf("expected connection error in view, got %q", view)
	}
}

// ---------------------------------------------------------------------------
// MessageDetailView
// ---------------------------------------------------------------------------

func TestMessageDetailViewBasic(t *testing.T) {
	t.Parallel()

	msg := &kafka.ConsumedMessage{
		Topic:     "test",
		Partition: 0,
		Offset:    42,
		Key:       "my-key",
		Value:     `{"hello":"world"}`,
		Time:      time.Now(),
		Headers: []kafka.MessageHeader{
			{Key: "trace-id", Value: "abc123"},
		},
	}

	v := NewMessageDetailView(msg)
	if v.Count() != 1 {
		t.Errorf("Count() = %d, want 1", v.Count())
	}

	// Init returns nil.
	if cmd := v.Init(); cmd != nil {
		t.Error("Init() should return nil")
	}

	// Non-save keys are no-ops.
	action, param := v.HandleKey(KeyEnter)
	if action != "" || param != "" {
		t.Errorf("HandleKey(enter): action=%q param=%q, want empty", action, param)
	}
}

func TestMessageDetailViewSaveKey(t *testing.T) {
	t.Parallel()

	msg := &kafka.ConsumedMessage{
		Topic: "test",
		Value: `{"hello":"world"}`,
		Time:  time.Now(),
	}
	v := NewMessageDetailView(msg)

	action, param := v.HandleKey("s")
	if action != actionSaveMessage || param != "" {
		t.Errorf("HandleKey(s): action=%q param=%q, want save_message/\"\"", action, param)
	}
}

func TestMessageDetailViewGetMessage(t *testing.T) {
	t.Parallel()

	msg := &kafka.ConsumedMessage{
		Topic:  "test",
		Offset: 99,
		Time:   time.Now(),
	}
	v := NewMessageDetailView(msg)

	got := v.GetMessage()
	if got == nil {
		t.Fatal("GetMessage() returned nil")
	}
	if got.Offset != 99 {
		t.Errorf("GetMessage().Offset = %d, want 99", got.Offset)
	}
}

func TestMessageDetailViewResize(t *testing.T) {
	t.Parallel()

	msg := &kafka.ConsumedMessage{
		Topic: "test",
		Value: `{"key":"value"}`,
		Time:  time.Now(),
	}

	v := NewMessageDetailView(msg)
	v.Resize(100, 40)

	view := v.View()
	if view == "Loading..." {
		t.Error("after Resize, should not show Loading")
	}
}

// ---------------------------------------------------------------------------
// tableStyles
// ---------------------------------------------------------------------------

func TestTableStyles(t *testing.T) {
	t.Parallel()

	s := tableStyles()
	// Header should have bold enabled.
	if !s.Header.GetBold() {
		t.Error("expected bold headers")
	}
}

func TestTableStylesWithWidth(t *testing.T) {
	t.Parallel()

	s := tableStylesWithWidth(100)
	if w := s.Selected.GetMaxWidth(); w != 100 {
		t.Errorf("selected max width = %d, want 100", w)
	}
}

// ---------------------------------------------------------------------------
// ACLsView
// ---------------------------------------------------------------------------

func sampleACLs() []kafka.ACLEntry {
	return []kafka.ACLEntry{
		{
			Principal: "User:bob", ResourceType: "Topic", ResourceName: "events",
			PatternType: "Literal", Host: "*", Operation: "Read", Permission: "Allow",
		},
		{
			Principal: "User:alice", ResourceType: "Topic", ResourceName: "orders",
			PatternType: "Literal", Host: "*", Operation: "Write", Permission: "Allow",
		},
		{
			Principal: "User:alice", ResourceType: "Group", ResourceName: "order-consumer",
			PatternType: "Literal", Host: "*", Operation: "Read", Permission: "Allow",
		},
	}
}

func TestACLsViewUpdateRefresh(t *testing.T) {
	t.Parallel()

	v := NewACLsView(nil)
	v.Update(ACLsRefreshMsg{ACLs: sampleACLs()}) //nolint:errcheck

	if v.Count() != 3 {
		t.Errorf("Count() = %d, want 3", v.Count())
	}
}

func TestACLsViewSortByPrincipal(t *testing.T) {
	t.Parallel()

	v := NewACLsView(nil)
	v.Update(ACLsRefreshMsg{ACLs: sampleACLs()}) //nolint:errcheck

	// alice comes before bob; within alice, Group before Topic.
	rows := v.table.Rows()
	if len(rows) < 3 {
		t.Fatalf("rows = %d, want 3", len(rows))
	}
	if rows[0][0] != "User:alice" || rows[0][1] != "Group" {
		t.Errorf("row 0 = %v, want alice/Group first", rows[0])
	}
	if rows[1][0] != "User:alice" || rows[1][1] != "Topic" {
		t.Errorf("row 1 = %v, want alice/Topic", rows[1])
	}
	if rows[2][0] != "User:bob" {
		t.Errorf("row 2 = %v, want bob last", rows[2])
	}
}

func TestACLsViewFilter(t *testing.T) {
	t.Parallel()

	v := NewACLsView(nil)
	v.Update(ACLsRefreshMsg{ACLs: sampleACLs()}) //nolint:errcheck

	v.SetFilter("alice")
	if v.Count() != 2 {
		t.Errorf("filter=alice: Count() = %d, want 2", v.Count())
	}

	v.SetFilter("orders")
	if v.Count() != 1 {
		t.Errorf("filter=orders: Count() = %d, want 1", v.Count())
	}

	v.SetFilter("WRITE") // case-insensitive
	if v.Count() != 1 {
		t.Errorf("filter=WRITE: Count() = %d, want 1", v.Count())
	}
}

func TestACLsViewFilterNegate(t *testing.T) {
	t.Parallel()

	v := NewACLsView(nil)
	v.Update(ACLsRefreshMsg{ACLs: sampleACLs()}) //nolint:errcheck

	v.SetFilter("!alice")
	if v.Count() != 1 {
		t.Errorf("negated filter: Count() = %d, want 1 (only bob)", v.Count())
	}
}

func TestACLsViewEmpty(t *testing.T) {
	t.Parallel()

	v := NewACLsView(nil)
	v.Update(ACLsRefreshMsg{ACLs: nil}) //nolint:errcheck

	if v.Count() != 0 {
		t.Errorf("Count() = %d, want 0", v.Count())
	}
	if got := v.View(); got == "" {
		t.Error("expected an empty-state message in the view")
	}
}

func TestACLsViewUpdateError(t *testing.T) {
	t.Parallel()

	v := NewACLsView(nil)
	v.Update(ACLsRefreshMsg{Err: fmt.Errorf("boom")}) //nolint:errcheck
	if got := v.View(); got == "" {
		t.Error("expected error in view")
	}
}

func TestACLsViewHandleKeyDelete(t *testing.T) {
	t.Parallel()

	v := NewACLsView(nil)
	v.Update(ACLsRefreshMsg{ACLs: sampleACLs()}) //nolint:errcheck
	v.Resize(120, 20)

	action, param := v.HandleKey("ctrl+d")
	if action != "confirm_delete_acl" || param == "" {
		t.Errorf("ctrl+d: action=%q param=%q, want confirm_delete_acl with non-empty param", action, param)
	}

	if entry := v.SelectedACL(param); entry == nil {
		t.Error("SelectedACL returned nil for the key just produced by ctrl+d")
	}
}

func TestACLsViewHandleKeyCreate(t *testing.T) {
	t.Parallel()

	v := NewACLsView(nil)
	action, param := v.HandleKey("a")
	if action != "create_acl" || param != "" {
		t.Errorf("a: action=%q param=%q, want create_acl/\"\"", action, param)
	}
}

// ---------------------------------------------------------------------------
// GroupsView — delete
// ---------------------------------------------------------------------------

func TestGroupsViewHandleKeyDelete(t *testing.T) {
	t.Parallel()

	v := NewGroupsView(nil)
	v.Update(GroupsRefreshMsg{Groups: []kafka.GroupInfo{
		{GroupID: "order-consumer", State: "Stable", Members: 3},
		{GroupID: "payments", State: "Empty", Members: 0},
	}}) //nolint:errcheck
	v.Resize(120, 20)

	action, param := v.HandleKey("ctrl+d")
	if action != "confirm_delete_group" {
		t.Errorf("ctrl+d: action=%q, want confirm_delete_group", action)
	}
	if param != "order-consumer" {
		t.Errorf("ctrl+d: param=%q, want order-consumer", param)
	}
}

func TestBuildACLColumnsNarrow(t *testing.T) {
	t.Parallel()

	cols := buildACLColumns(40)
	// Principal should floor at 20 even when width is too small.
	if cols[0].Title != "PRINCIPAL" || cols[0].Width != 20 {
		t.Errorf("narrow: cols[0] = %+v, want PRINCIPAL width=20", cols[0])
	}
}

// ---------------------------------------------------------------------------
// TopicConfigView
// ---------------------------------------------------------------------------

func sampleConfigs() []kafka.ConfigEntry {
	return []kafka.ConfigEntry{
		{
			Name:      "retention.ms",
			Value:     "604800000",
			Source:    "DYNAMIC_TOPIC",
			ReadOnly:  false,
			IsDefault: false,
			Sensitive: false,
		},
		{Name: "cleanup.policy", Value: "delete", Source: "DEFAULT", ReadOnly: false, IsDefault: true, Sensitive: false},
		{Name: "segment.bytes", Value: "1073741824", Source: "DEFAULT", ReadOnly: false, IsDefault: true, Sensitive: false},
		{
			Name:      "min.insync.replicas",
			Value:     "2",
			Source:    "DYNAMIC_TOPIC",
			ReadOnly:  false,
			IsDefault: false,
			Sensitive: false,
		},
	}
}

func TestTopicConfigViewRefresh(t *testing.T) {
	t.Parallel()

	v := NewTopicConfigView(nil, testTopicTail)
	v.Update(TopicConfigRefreshMsg{Configs: sampleConfigs()}) //nolint:errcheck

	// By default, showDefaults=false, so only non-default configs show.
	if v.Count() != 2 {
		t.Errorf("Count() = %d, want 2 (non-defaults only)", v.Count())
	}
}

func TestTopicConfigViewShowDefaults(t *testing.T) {
	t.Parallel()

	v := NewTopicConfigView(nil, testTopicTail)
	v.Update(TopicConfigRefreshMsg{Configs: sampleConfigs()}) //nolint:errcheck
	v.Resize(120, 20)

	// Toggle defaults on.
	v.HandleKey("d")
	if v.Count() != 4 {
		t.Errorf("Count() after toggle = %d, want 4", v.Count())
	}

	// Toggle defaults off.
	v.HandleKey("d")
	if v.Count() != 2 {
		t.Errorf("Count() after second toggle = %d, want 2", v.Count())
	}
}

func TestTopicConfigViewFilter(t *testing.T) {
	t.Parallel()

	v := NewTopicConfigView(nil, testTopicTail)
	v.Update(TopicConfigRefreshMsg{Configs: sampleConfigs()}) //nolint:errcheck

	// Toggle defaults on so all configs are visible.
	v.HandleKey("d")

	v.SetFilter("retention")
	if v.Count() != 1 {
		t.Errorf("filter=retention: Count() = %d, want 1", v.Count())
	}
}

func TestTopicConfigViewHandleKeyEdit(t *testing.T) {
	t.Parallel()

	v := NewTopicConfigView(nil, testTopicTail)
	v.Update(TopicConfigRefreshMsg{Configs: sampleConfigs()}) //nolint:errcheck
	v.Resize(120, 20)

	action, param := v.HandleKey("enter")
	if action != "edit_topic_config" {
		t.Errorf("enter: action=%q, want edit_topic_config", action)
	}
	if param == "" {
		t.Error("enter: param should be the config name")
	}
}

func TestTopicConfigViewSelectedConfig(t *testing.T) {
	t.Parallel()

	v := NewTopicConfigView(nil, testTopicTail)
	v.Update(TopicConfigRefreshMsg{Configs: sampleConfigs()}) //nolint:errcheck

	if entry := v.SelectedConfig("retention.ms"); entry == nil {
		t.Error("SelectedConfig(retention.ms) returned nil")
	}
	if entry := v.SelectedConfig("nonexistent"); entry != nil {
		t.Errorf("SelectedConfig(nonexistent) = %v, want nil", entry)
	}
}

func TestTopicConfigViewTopic(t *testing.T) {
	t.Parallel()

	v := NewTopicConfigView(nil, testTopic)
	if v.Topic() != testTopic {
		t.Errorf("Topic() = %q, want my-topic", v.Topic())
	}
}

// ---------------------------------------------------------------------------
// ResetOffsetsView
// ---------------------------------------------------------------------------

func TestResetOffsetsViewInit(t *testing.T) {
	t.Parallel()

	v := NewResetOffsetsView(nil, testGroup)
	if v.Count() != 0 {
		t.Errorf("Count() = %d, want 0", v.Count())
	}
	if got := v.View(); got == "" {
		t.Error("expected loading message")
	}
}

func TestResetOffsetsViewTopicsLoaded(t *testing.T) {
	t.Parallel()

	v := NewResetOffsetsView(nil, testGroup)
	v.Update(ResetOffsetsTopicsMsg{Topics: []string{"orders", "events"}}) //nolint:errcheck

	// After topics load, should be in form state.
	got := v.View()
	if got == "" {
		t.Error("expected form view after topics loaded")
	}
}

func TestResetOffsetsViewNoTopics(t *testing.T) {
	t.Parallel()

	v := NewResetOffsetsView(nil, testGroup)
	v.Update(ResetOffsetsTopicsMsg{Topics: nil}) //nolint:errcheck

	got := v.View()
	if got == "" {
		t.Error("expected message when no topics found")
	}
}

func TestResetOffsetsViewError(t *testing.T) {
	t.Parallel()

	v := NewResetOffsetsView(nil, testGroup)
	v.Update(ResetOffsetsTopicsMsg{Err: fmt.Errorf("boom")}) //nolint:errcheck

	got := v.View()
	if got == "" {
		t.Error("expected error message")
	}
}

func TestResetOffsetsViewResult(t *testing.T) {
	t.Parallel()

	v := NewResetOffsetsView(nil, testGroup)
	v.Update(ResetOffsetsResultMsg{Err: nil}) //nolint:errcheck

	got := v.View()
	if got == "" {
		t.Error("expected result message")
	}
}

// ---------------------------------------------------------------------------
// GroupDetailView — reset offsets key
// ---------------------------------------------------------------------------

func TestGroupDetailViewGetSelectedOffset(t *testing.T) {
	t.Parallel()

	v := NewGroupDetailView(nil, testGroup)

	// Before data loads, should return nil.
	if v.GetSelectedOffset() != nil {
		t.Error("GetSelectedOffset() should return nil before data loads")
	}

	v.Update(GroupDetailRefreshMsg{Detail: &kafka.GroupDetail{ //nolint:errcheck
		GroupID: testGroup,
		State:   "Empty",
		Offsets: []kafka.GroupOffsetInfo{
			{Topic: "orders", Partition: 0, CommittedOffset: 100, HighWatermark: 150, Lag: 50},
			{Topic: "orders", Partition: 1, CommittedOffset: 200, HighWatermark: 200, Lag: 0},
		},
	}})
	v.Resize(120, 20)

	offset := v.GetSelectedOffset()
	if offset == nil {
		t.Fatal("GetSelectedOffset() returned nil after data load")
	}
	if offset.Topic != "orders" || offset.Partition != 0 {
		t.Errorf("GetSelectedOffset() = %s:%d, want orders:0", offset.Topic, offset.Partition)
	}
}

func TestGroupDetailViewGroupID(t *testing.T) {
	t.Parallel()

	v := NewGroupDetailView(nil, testGroup)
	if v.GroupID() != testGroup {
		t.Errorf("GroupID() = %q, want my-group", v.GroupID())
	}
}

func TestGroupDetailViewHandleKeyReset(t *testing.T) {
	t.Parallel()

	v := NewGroupDetailView(nil, testGroup)
	action, param := v.HandleKey("R")
	if action != "reset_offsets" || param != testGroup {
		t.Errorf("R: action=%q param=%q, want reset_offsets/my-group", action, param)
	}
}

// ---------------------------------------------------------------------------
// BrokerDetailView
// ---------------------------------------------------------------------------

func sampleBrokerConfigs() []kafka.ConfigEntry {
	return []kafka.ConfigEntry{
		{
			Name:      "log.retention.hours",
			Value:     "168",
			Source:    "STATIC_BROKER",
			ReadOnly:  true,
			IsDefault: false,
			Sensitive: false,
		},
		{Name: "num.partitions", Value: "1", Source: "DEFAULT", ReadOnly: false, IsDefault: true, Sensitive: false},
		{Name: "log.dirs", Value: "/kafka-logs", Source: "STATIC_BROKER", ReadOnly: true, IsDefault: false, Sensitive: false},
	}
}

func TestBrokerDetailViewRefresh(t *testing.T) {
	t.Parallel()

	broker := kafka.BrokerInfo{ID: 1, Host: "broker-1", Port: 9092}
	v := NewBrokerDetailView(nil, broker, true)
	v.Update(BrokerDetailRefreshMsg{Configs: sampleBrokerConfigs()}) //nolint:errcheck

	// By default, showDefaults=false, so only non-default configs show.
	if v.Count() != 2 {
		t.Errorf("Count() = %d, want 2", v.Count())
	}
}

func TestBrokerDetailViewToggleDefaults(t *testing.T) {
	t.Parallel()

	broker := kafka.BrokerInfo{ID: 1, Host: "broker-1", Port: 9092}
	v := NewBrokerDetailView(nil, broker, false)
	v.Update(BrokerDetailRefreshMsg{Configs: sampleBrokerConfigs()}) //nolint:errcheck
	v.Resize(120, 20)

	v.HandleKey("d")
	if v.Count() != 3 {
		t.Errorf("Count() after toggle = %d, want 3", v.Count())
	}
}

func TestBrokerDetailViewFilter(t *testing.T) {
	t.Parallel()

	broker := kafka.BrokerInfo{ID: 1, Host: "broker-1", Port: 9092}
	v := NewBrokerDetailView(nil, broker, false)
	v.Update(BrokerDetailRefreshMsg{Configs: sampleBrokerConfigs()}) //nolint:errcheck

	v.HandleKey("d") // show all
	v.SetFilter("log")
	if v.Count() != 2 {
		t.Errorf("filter=log: Count() = %d, want 2", v.Count())
	}
}

func TestBrokerDetailViewSummary(t *testing.T) {
	t.Parallel()

	broker := kafka.BrokerInfo{ID: 1, Host: "broker-1", Port: 9092}
	v := NewBrokerDetailView(nil, broker, true)
	v.Update(BrokerDetailRefreshMsg{Configs: sampleBrokerConfigs()}) //nolint:errcheck
	v.Resize(120, 20)

	got := v.View()
	if got == "" {
		t.Error("expected non-empty view")
	}
}

// ---------------------------------------------------------------------------
// ClusterView — broker detail key
// ---------------------------------------------------------------------------

func TestClusterViewHandleKeyEnter(t *testing.T) {
	t.Parallel()

	v := NewClusterView(nil)
	v.Update(ClusterRefreshMsg{
		Cluster: &kafka.ClusterInfo{
			ClusterID:    "test-cluster",
			Brokers:      []kafka.BrokerInfo{{ID: 1, Host: "broker-1", Port: 9092}},
			ControllerID: 1,
		},
	}) //nolint:errcheck
	v.Resize(120, 20)

	action, param := v.HandleKey("enter")
	if action != "broker_detail" || param != "1" {
		t.Errorf("enter: action=%q param=%q, want broker_detail/1", action, param)
	}
}

// ---------------------------------------------------------------------------
// ContextView
// ---------------------------------------------------------------------------

func TestContextViewUpdate(t *testing.T) {
	t.Parallel()

	v := NewContextView(nil, "")
	v.Update(ContextRefreshMsg{Contexts: []contextEntry{
		{Name: "staging", Auth: "iam", Brokers: "broker-1:9098"},
		{Name: "production", Auth: "iam", Brokers: "broker-2:9098"},
	}}) //nolint:errcheck

	if v.Count() != 2 {
		t.Errorf("Count() = %d, want 2", v.Count())
	}
}

func TestContextViewHandleKeyEnter(t *testing.T) {
	t.Parallel()

	v := NewContextView(nil, "")
	v.Update(ContextRefreshMsg{Contexts: []contextEntry{
		{Name: "staging", Auth: "iam", Brokers: "broker-1:9098"},
	}}) //nolint:errcheck
	v.Resize(120, 20)

	action, param := v.HandleKey(KeyEnter)
	if action != "switch_context" || param != "staging" {
		t.Errorf("enter: action=%q param=%q, want switch_context/staging", action, param)
	}
}

func TestContextViewCurrentMarker(t *testing.T) {
	t.Parallel()

	v := NewContextView(nil, "production")
	v.Update(ContextRefreshMsg{Contexts: []contextEntry{
		{Name: "staging", Auth: "iam", Brokers: "broker-1:9098", Current: false},
		{Name: "production", Auth: "iam", Brokers: "broker-2:9098", Current: true},
	}}) //nolint:errcheck

	rows := v.table.Rows()
	// Rows are sorted: production first, staging second.
	found := false
	for _, row := range rows {
		if row[1] == "production" && row[0] == "*" {
			found = true
		}
	}
	if !found {
		t.Error("expected current context to have '*' marker")
	}
}

func TestContextViewPreservesCursorAcrossRefresh(t *testing.T) {
	t.Parallel()

	// Mirrors the periodic tick refresh: after the user moves their selection
	// off the current-context row, a subsequent ContextRefreshMsg must not snap
	// the cursor back to the `*` row.
	v := NewContextView(nil, "production-us")
	entries := []contextEntry{
		{Name: "production-eu", Auth: "iam", Brokers: "b-1:9098"},
		{Name: "production-us", Auth: "iam", Brokers: "b-2:9098", Current: true},
		{Name: "staging-us", Auth: "iam", Brokers: "b-3:9098"},
	}
	v.Update(ContextRefreshMsg{Contexts: entries}) //nolint:errcheck
	v.Resize(120, 20)

	// Sorted order: production-eu (0), production-us (1, current), staging-us (2).
	if got := v.table.Cursor(); got != 1 {
		t.Fatalf("initial cursor = %d, want 1 (current context)", got)
	}

	// User moves to staging-us.
	v.table.SetCursor(2)

	// A second refresh arrives from the periodic tick.
	v.Update(ContextRefreshMsg{Contexts: entries}) //nolint:errcheck

	if got := v.table.Cursor(); got != 2 {
		t.Errorf("cursor after refresh = %d, want 2 (preserved user selection)", got)
	}
}

func TestContextViewLoading(t *testing.T) {
	t.Parallel()

	v := NewContextView(nil, "")
	if v.Loading() {
		t.Error("Loading() should always return false")
	}
}

func TestContextViewSetFilter(t *testing.T) {
	t.Parallel()

	v := NewContextView(nil, "")
	// SetFilter is a no-op; should not panic.
	v.SetFilter("test")
	v.SetFilter("")
}

// ---------------------------------------------------------------------------
// CreateTopicView
// ---------------------------------------------------------------------------

func TestCreateTopicViewInit(t *testing.T) {
	t.Parallel()

	v := NewCreateTopicView(nil)
	cmd := v.Init()
	if cmd == nil {
		t.Error("Init() should return non-nil cmd")
	}
}

func TestCreateTopicViewResultCreated(t *testing.T) {
	t.Parallel()

	v := NewCreateTopicView(nil)
	v.Update(CreateTopicResultMsg{Created: true, Topic: "t1"}) //nolint:errcheck

	view := v.View()
	if !strings.Contains(view, "successfully") {
		t.Errorf("expected 'successfully' in view, got %q", view)
	}
}

func TestCreateTopicViewResultExists(t *testing.T) {
	t.Parallel()

	v := NewCreateTopicView(nil)
	v.Update(CreateTopicResultMsg{Created: false, Topic: "t1"}) //nolint:errcheck

	view := v.View()
	if !strings.Contains(view, "already exists") {
		t.Errorf("expected 'already exists' in view, got %q", view)
	}
}

func TestCreateTopicViewResultError(t *testing.T) {
	t.Parallel()

	v := NewCreateTopicView(nil)
	v.Update(CreateTopicResultMsg{Err: fmt.Errorf("fail"), Topic: "t1"}) //nolint:errcheck

	view := v.View()
	if !strings.Contains(view, "Error") {
		t.Errorf("expected 'Error' in view, got %q", view)
	}
}

func TestCreateTopicViewCountAndLoading(t *testing.T) {
	t.Parallel()

	v := NewCreateTopicView(nil)
	if v.Count() != 0 {
		t.Errorf("Count() = %d, want 0", v.Count())
	}
	if v.Loading() {
		t.Error("Loading() should return false")
	}
}

// ---------------------------------------------------------------------------
// CreateACLView
// ---------------------------------------------------------------------------

func TestCreateACLViewInit(t *testing.T) {
	t.Parallel()

	v := NewCreateACLView(nil)
	cmd := v.Init()
	if cmd == nil {
		t.Error("Init() should return non-nil cmd")
	}
}

func TestCreateACLViewResultSuccess(t *testing.T) {
	t.Parallel()

	v := NewCreateACLView(nil)
	v.Update(CreateACLResultMsg{}) //nolint:errcheck

	view := v.View()
	if !strings.Contains(view, "ACL created") {
		t.Errorf("expected 'ACL created' in view, got %q", view)
	}
}

func TestCreateACLViewResultError(t *testing.T) {
	t.Parallel()

	v := NewCreateACLView(nil)
	v.Update(CreateACLResultMsg{Err: fmt.Errorf("fail")}) //nolint:errcheck

	view := v.View()
	if !strings.Contains(view, "Error") {
		t.Errorf("expected 'Error' in view, got %q", view)
	}
}

func TestCreateACLViewCountAndLoading(t *testing.T) {
	t.Parallel()

	v := NewCreateACLView(nil)
	if v.Count() != 0 {
		t.Errorf("Count() = %d, want 0", v.Count())
	}
	if v.Loading() {
		t.Error("Loading() should return false")
	}
}

func TestCreateACLViewHandleKey(t *testing.T) {
	t.Parallel()

	v := NewCreateACLView(nil)
	action, param := v.HandleKey("x")
	if action != "" || param != "" {
		t.Errorf("HandleKey(x): action=%q param=%q, want empty", action, param)
	}
}

// ---------------------------------------------------------------------------
// ProduceView
// ---------------------------------------------------------------------------

func TestProduceViewInit(t *testing.T) {
	t.Parallel()

	v := NewProduceView(nil, testTopicTail)
	cmd := v.Init()
	if cmd == nil {
		t.Error("Init() should return non-nil cmd")
	}
}

func TestProduceViewProgress(t *testing.T) {
	t.Parallel()

	v := NewProduceView(nil, testTopicTail)
	// Move to running state by simulating progress.
	v.state = produceStateRunning
	v.Update(ProduceProgressMsg{
		Result: kafka.ProduceResult{Key: "k1", Sequence: 1, Duration: time.Millisecond},
		Total:  5,
	}) //nolint:errcheck

	if v.Count() != 1 {
		t.Errorf("Count() = %d, want 1", v.Count())
	}
}

func TestProduceViewDone(t *testing.T) {
	t.Parallel()

	v := NewProduceView(nil, testTopicTail)
	v.state = produceStateRunning
	v.Update(ProduceDoneMsg{Sent: 3, Failed: 1}) //nolint:errcheck

	view := v.View()
	if !strings.Contains(view, "Done") {
		t.Errorf("expected 'Done' in view, got %q", view)
	}
}

func TestProduceViewStop(t *testing.T) {
	t.Parallel()

	v := NewProduceView(nil, testTopicTail)
	// cancel is nil in form state — Stop() should not panic.
	v.Stop()
}

func TestProduceViewCountFormState(t *testing.T) {
	t.Parallel()

	v := NewProduceView(nil, testTopicTail)
	if v.Count() != 0 {
		t.Errorf("Count() in form state = %d, want 0", v.Count())
	}
}

func TestProduceViewLoading(t *testing.T) {
	t.Parallel()

	v := NewProduceView(nil, testTopicTail)
	if v.Loading() {
		t.Error("Loading() should return false")
	}
}

func TestProduceViewRefresh(t *testing.T) {
	t.Parallel()

	v := NewProduceView(nil, testTopicTail)
	if cmd := v.Refresh(); cmd != nil {
		t.Error("Refresh() should return nil")
	}
}

// ---------------------------------------------------------------------------
// ResetOffsetsView (extending coverage)
// ---------------------------------------------------------------------------

func TestResetOffsetsViewInitNonNil(t *testing.T) {
	t.Parallel()

	// Init() requires a non-nil client to build the tea.Cmd, but the cmd
	// itself (a closure) is non-nil regardless of whether the client works.
	// We create the view and check Init returns a non-nil cmd.
	v := NewResetOffsetsView(nil, testGroup)
	cmd := v.Init()
	if cmd == nil {
		t.Error("Init() should return non-nil cmd")
	}
}

func TestResetOffsetsViewTopicsError(t *testing.T) {
	t.Parallel()

	v := NewResetOffsetsView(nil, testGroup)
	v.Update(ResetOffsetsTopicsMsg{Err: fmt.Errorf("fetch failed")}) //nolint:errcheck

	view := v.View()
	if !strings.Contains(view, "Error") {
		t.Errorf("expected 'Error' in view, got %q", view)
	}
}

func TestResetOffsetsViewTopicsEmpty(t *testing.T) {
	t.Parallel()

	v := NewResetOffsetsView(nil, testGroup)
	v.Update(ResetOffsetsTopicsMsg{Topics: nil}) //nolint:errcheck

	view := v.View()
	if !strings.Contains(view, "No committed offsets") {
		t.Errorf("expected 'No committed offsets' in view, got %q", view)
	}
}

func TestResetOffsetsViewResultSuccess(t *testing.T) {
	t.Parallel()

	v := NewResetOffsetsView(nil, testGroup)
	// Load topics first so the view is in form state.
	v.Update(ResetOffsetsTopicsMsg{Topics: []string{"orders"}}) //nolint:errcheck
	// Now send a success result.
	v.Update(ResetOffsetsResultMsg{Err: nil}) //nolint:errcheck

	view := v.View()
	if !strings.Contains(view, "reset successfully") {
		t.Errorf("expected 'reset successfully' in view, got %q", view)
	}
}

func TestResetOffsetsViewResultError(t *testing.T) {
	t.Parallel()

	v := NewResetOffsetsView(nil, testGroup)
	v.Update(ResetOffsetsResultMsg{Err: fmt.Errorf("offset error")}) //nolint:errcheck

	view := v.View()
	if !strings.Contains(view, "Error") {
		t.Errorf("expected 'Error' in view, got %q", view)
	}
}

func TestResetOffsetsViewCountAndLoading(t *testing.T) {
	t.Parallel()

	v := NewResetOffsetsView(nil, testGroup)
	if v.Count() != 0 {
		t.Errorf("Count() = %d, want 0", v.Count())
	}
	if v.Loading() {
		t.Error("Loading() should return false")
	}
}

// ---------------------------------------------------------------------------
// View() rendering tests — cover the View output for all views
// ---------------------------------------------------------------------------

func TestTopicsViewViewOutput(t *testing.T) {
	t.Parallel()

	v := NewTopicsView(nil)
	v.Resize(80, 30)
	v.Update(TopicsRefreshMsg{Topics: []kafka.TopicInfo{
		{Name: "orders", Partitions: 6, ReplicationFactor: 3},
	}})

	out := v.View()
	if out == "" {
		t.Error("View() returned empty")
	}
}

func TestTopicsViewViewError(t *testing.T) {
	t.Parallel()

	v := NewTopicsView(nil)
	v.Update(TopicsRefreshMsg{Err: fmt.Errorf("conn refused")})

	out := v.View()
	if !strings.Contains(out, "Error") {
		t.Error("expected error in View()")
	}
}

func TestGroupsViewViewOutput(t *testing.T) {
	t.Parallel()

	v := NewGroupsView(nil)
	v.Resize(80, 30)
	v.Update(GroupsRefreshMsg{Groups: []kafka.GroupInfo{
		{GroupID: "cg-1", State: "Stable", Members: 3, Topics: -1, TotalLag: -1, Coordinator: -1},
	}})

	out := v.View()
	if out == "" {
		t.Error("View() returned empty")
	}
}

func TestClusterViewViewOutput(t *testing.T) {
	t.Parallel()

	v := NewClusterView(nil)
	v.Resize(80, 30)
	v.Update(ClusterRefreshMsg{
		Cluster: &kafka.ClusterInfo{
			ClusterID:    "abc",
			ControllerID: 1,
			Brokers:      []kafka.BrokerInfo{{ID: 1, Host: "broker1", Port: 9092}},
		},
		Latency: "5ms",
	})

	out := v.View()
	if !strings.Contains(out, "Cluster:") {
		t.Error("expected cluster label in View()")
	}
}

func TestContextViewViewOutput(t *testing.T) {
	t.Parallel()

	v := NewContextView(nil, "")
	v.Resize(80, 30)
	v.Update(ContextRefreshMsg{Contexts: []contextEntry{
		{Name: "staging", Auth: "iam", Brokers: "b:9092"},
	}})

	out := v.View()
	if !strings.Contains(out, "Select a context") {
		t.Error("expected context header in View()")
	}
}

func TestGroupDetailViewViewOutput(t *testing.T) {
	t.Parallel()

	v := NewGroupDetailView(nil, "test-group")
	v.Resize(80, 30)
	v.Update(GroupDetailRefreshMsg{Detail: &kafka.GroupDetail{
		GroupID: "test-group",
		State:   "Stable",
		Offsets: []kafka.GroupOffsetInfo{
			{Topic: "t1", Partition: 0, CommittedOffset: 100, HighWatermark: 150, Lag: 50},
		},
	}})

	out := v.View()
	if out == "" {
		t.Error("View() returned empty")
	}
}

func TestTopicDetailViewViewOutput(t *testing.T) {
	t.Parallel()

	v := NewTopicDetailView(nil, "my-topic")
	v.Resize(80, 30)
	v.Update(TopicDetailRefreshMsg{Detail: &kafka.TopicDetail{
		Name:       "my-topic",
		Partitions: []kafka.PartitionInfo{{ID: 0, Leader: 1, Replicas: []int{1, 2, 3}, ISR: []int{1, 2, 3}}},
	}})

	out := v.View()
	if out == "" {
		t.Error("View() returned empty")
	}
}

func TestTopicConfigViewViewOutput(t *testing.T) {
	t.Parallel()

	v := NewTopicConfigView(nil, "my-topic")
	v.Resize(80, 30)
	v.Update(TopicConfigRefreshMsg{Configs: []kafka.ConfigEntry{
		{Name: "retention.ms", Value: "604800000", Source: "DYNAMIC_TOPIC"},
	}})

	out := v.View()
	if out == "" {
		t.Error("View() returned empty")
	}
}

func TestBrokerDetailViewViewOutput(t *testing.T) {
	t.Parallel()

	v := NewBrokerDetailView(nil, kafka.BrokerInfo{ID: 1, Host: "b1", Port: 9092}, true)
	v.Resize(80, 30)
	v.Update(BrokerDetailRefreshMsg{Configs: []kafka.ConfigEntry{
		{Name: "log.retention.hours", Value: "168", Source: "DEFAULT", IsDefault: true},
	}})

	out := v.View()
	if out == "" {
		t.Error("View() returned empty")
	}
}

func TestMessagesViewViewOutput(t *testing.T) {
	t.Parallel()

	v := NewMessagesView(nil, testTopicTail)
	v.Resize(80, 30)

	// Manually inject messages (same as existing pattern).
	v.Update(MessageMsg(kafka.ConsumedMessage{
		Topic: testTopicTail, Partition: 0, Offset: 1, Key: "k", Value: `{"a":1}`,
	}))

	out := v.View()
	if out == "" {
		t.Error("View() returned empty")
	}
}

func TestMessagesViewViewEmpty(t *testing.T) {
	t.Parallel()

	v := NewMessagesView(nil, testTopicTail)
	v.Resize(80, 30)
	v.loading = false

	out := v.View()
	if !strings.Contains(out, "No messages") {
		t.Error("expected empty state message")
	}
}

// ---------------------------------------------------------------------------
// Partition cycling in MessagesView
// ---------------------------------------------------------------------------

func TestMessagesViewPartitionCycling(t *testing.T) {
	t.Parallel()

	v := NewMessagesView(nil, testTopicTail)
	v.Resize(80, 30)

	// Inject messages from partitions 0, 1, 2.
	for _, p := range []int{0, 1, 2} {
		v.Update(MessageMsg(kafka.ConsumedMessage{
			Topic: testTopicTail, Partition: p, Offset: int64(p), Key: "k", Value: "v",
		}))
	}

	if v.PartitionFilter() != -1 {
		t.Errorf("initial PartitionFilter() = %d, want -1 (all)", v.PartitionFilter())
	}

	v.HandleKey("p") // cycle forward
	if v.PartitionFilter() < 0 {
		t.Error("expected partition filter set after 'p'")
	}

	v.HandleKey("P") // cycle backward
	// Should be a different partition or wrap around.

	v.HandleKey("a") // reset to all
	if v.PartitionFilter() != -1 {
		t.Errorf("after 'a', PartitionFilter() = %d, want -1", v.PartitionFilter())
	}
}

// ---------------------------------------------------------------------------
// Horizontal scroll in MessagesView
// ---------------------------------------------------------------------------

func TestMessagesViewHorizontalScroll(t *testing.T) {
	t.Parallel()

	v := NewMessagesView(nil, testTopicTail)
	v.Resize(80, 30)
	v.Update(MessageMsg(kafka.ConsumedMessage{
		Topic: testTopicTail, Partition: 0, Offset: 0, Key: "k", Value: "some long value here",
	}))

	v.HandleKey("l") // scroll right
	v.HandleKey("l")
	v.HandleKey("h") // scroll left
	// Should not panic; scroll state is internal.
}

// ---------------------------------------------------------------------------
// formatRetention and formatRetentionBytes
// ---------------------------------------------------------------------------

func TestFormatRetention(t *testing.T) {
	t.Parallel()

	tests := []struct {
		want  string
		ms    int64
		bytes int64
	}{
		{want: "7d", ms: 604800000, bytes: -1},
		{want: "1h", ms: 3600000, bytes: -1},
		{want: "1.0GB", ms: -1, bytes: 1073741824},
		{want: "1h / 1.0GB", ms: 3600000, bytes: 1073741824},
		{want: "∞", ms: -1, bytes: -1},
	}
	for _, tt := range tests {
		got := formatRetention(tt.ms, tt.bytes)
		if got != tt.want {
			t.Errorf("formatRetention(%d, %d) = %q, want %q", tt.ms, tt.bytes, got, tt.want)
		}
	}
}

func TestFormatRetentionBytes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		want string
		b    int64
	}{
		{want: "∞", b: -1},
		{want: "500B", b: 500},
		{want: "1.0KB", b: 1024},
		{want: "1.0MB", b: 1048576},
		{want: "1.0GB", b: 1073741824},
	}
	for _, tt := range tests {
		got := formatRetentionBytes(tt.b)
		if got != tt.want {
			t.Errorf("formatRetentionBytes(%d) = %q, want %q", tt.b, got, tt.want)
		}
	}
}

// ---------------------------------------------------------------------------
// BrokerByID
// ---------------------------------------------------------------------------

func TestClusterViewBrokerByID(t *testing.T) {
	t.Parallel()

	v := NewClusterView(nil)
	v.Update(ClusterRefreshMsg{
		Cluster: &kafka.ClusterInfo{
			ControllerID: 1,
			Brokers:      []kafka.BrokerInfo{{ID: 1, Host: "b1", Port: 9092}, {ID: 2, Host: "b2", Port: 9092}},
		},
	})

	b, isController := v.BrokerByID(1)
	if b == nil {
		t.Fatal("expected broker 1")
	}
	if !isController {
		t.Error("broker 1 should be controller")
	}

	b, isController = v.BrokerByID(2)
	if b == nil {
		t.Fatal("expected broker 2")
	}
	if isController {
		t.Error("broker 2 should not be controller")
	}

	b, _ = v.BrokerByID(99)
	if b != nil {
		t.Error("expected nil for unknown broker")
	}
}

// ---------------------------------------------------------------------------
// TestAllViewsBasicMethods — exercise all small 0% methods across all views
// ---------------------------------------------------------------------------

func TestAllViewsBasicMethods(t *testing.T) {
	t.Parallel()

	// TopicsView
	t.Run("TopicsView", func(t *testing.T) {
		t.Parallel()
		v := NewTopicsView(nil)
		v.Resize(80, 30)
		// UpdateTable shouldn't panic
		v.UpdateTable(tea.KeyPressMsg{Code: 'j'})
		// Loading is true initially
		if !v.Loading() {
			t.Error("expected loading=true initially")
		}
		// Refresh returns non-nil cmd
		if v.Refresh() == nil {
			t.Error("Refresh() should return non-nil")
		}
		// SetFilter + rebuild
		v.SetFilter("test")
		v.SetFilter("")
	})

	// GroupsView
	t.Run("GroupsView", func(t *testing.T) {
		t.Parallel()
		v := NewGroupsView(nil)
		v.Resize(80, 30)
		v.UpdateTable(tea.KeyPressMsg{Code: 'j'})
		if !v.Loading() {
			t.Error("expected loading=true")
		}
		if v.Refresh() == nil {
			t.Error("Refresh() should return non-nil")
		}
		v.SetFilter("test")
	})

	// ClusterView
	t.Run("ClusterView", func(t *testing.T) {
		t.Parallel()
		v := NewClusterView(nil)
		v.Resize(80, 30)
		v.UpdateTable(tea.KeyPressMsg{Code: 'j'})
		if !v.Loading() {
			t.Error("expected loading=true")
		}
		if v.Refresh() == nil {
			t.Error("Refresh() should return non-nil")
		}
		v.SetFilter("test") // no-op but should not panic
	})

	// TopicDetailView
	t.Run("TopicDetailView", func(t *testing.T) {
		t.Parallel()
		v := NewTopicDetailView(nil, "my-topic")
		v.Resize(80, 30)
		v.UpdateTable(tea.KeyPressMsg{Code: 'j'})
		if !v.Loading() {
			t.Error("expected loading=true")
		}
		if v.Refresh() == nil {
			t.Error("Refresh() should return non-nil")
		}
		v.SetFilter("test")
		if v.Topic() != "my-topic" {
			t.Error("wrong topic")
		}
	})

	// GroupDetailView
	t.Run("GroupDetailView", func(t *testing.T) {
		t.Parallel()
		v := NewGroupDetailView(nil, testGroup)
		v.Resize(80, 30)
		v.UpdateTable(tea.KeyPressMsg{Code: 'j'})
		if !v.Loading() {
			t.Error("expected loading=true")
		}
		if v.Refresh() == nil {
			t.Error("Refresh() should return non-nil")
		}
		v.SetFilter("test")
		if v.GroupID() != testGroup {
			t.Error("wrong group")
		}
	})

	// TopicConfigView
	t.Run("TopicConfigView", func(t *testing.T) {
		t.Parallel()
		v := NewTopicConfigView(nil, "my-topic")
		v.Resize(80, 30)
		v.UpdateTable(tea.KeyPressMsg{Code: 'j'})
		if !v.Loading() {
			t.Error("expected loading=true")
		}
		if v.Refresh() == nil {
			t.Error("Refresh() should return non-nil")
		}
		v.SetFilter("test")
		if v.Topic() != "my-topic" {
			t.Error("wrong topic")
		}
	})

	// BrokerDetailView
	t.Run("BrokerDetailView", func(t *testing.T) {
		t.Parallel()
		v := NewBrokerDetailView(nil, kafka.BrokerInfo{ID: 1, Host: "b1", Port: 9092}, true)
		v.Resize(80, 30)
		v.UpdateTable(tea.KeyPressMsg{Code: 'j'})
		if !v.Loading() {
			t.Error("expected loading=true")
		}
		if v.Refresh() == nil {
			t.Error("Refresh() should return non-nil")
		}
		v.SetFilter("test")
	})

	// ACLsView
	t.Run("ACLsView", func(t *testing.T) {
		t.Parallel()
		v := NewACLsView(nil)
		v.Resize(80, 30)
		v.UpdateTable(tea.KeyPressMsg{Code: 'j'})
		if !v.Loading() {
			t.Error("expected loading=true")
		}
		if v.Refresh() == nil {
			t.Error("Refresh() should return non-nil")
		}
		v.SetFilter("test")
	})

	// ContextView
	t.Run("ContextView", func(t *testing.T) {
		t.Parallel()
		v := NewContextView(nil, "")
		v.Resize(80, 30)
		v.UpdateTable(tea.KeyPressMsg{Code: 'j'})
		if v.Loading() {
			t.Error("expected loading=false")
		}
		if v.Refresh() == nil {
			t.Error("Refresh() should return non-nil")
		}
		v.SetFilter("test")
	})

	// MessageDetailView
	t.Run("MessageDetailView", func(t *testing.T) {
		t.Parallel()
		msg := &kafka.ConsumedMessage{Topic: "t", Key: "k", Value: `{"a":1}`, Partition: 0, Offset: 1}
		v := NewMessageDetailView(msg)
		v.Resize(80, 30)
		v.UpdateTable(tea.KeyPressMsg{Code: 'j'})
		if v.Loading() {
			t.Error("expected loading=false")
		}
		if v.Refresh() != nil {
			t.Error("Refresh() should return nil")
		}
		v.SetFilter("test")
		v.Update(nil)
	})

	// MessagesView
	t.Run("MessagesView", func(t *testing.T) {
		t.Parallel()
		v := NewMessagesView(nil, testTopicTail)
		v.Resize(80, 30)
		v.UpdateTable(tea.KeyPressMsg{Code: 'j'})
		if !v.Loading() {
			t.Error("expected loading=true")
		}
		if v.Refresh() != nil {
			t.Error("Refresh() should return nil for messages")
		}
		v.SetFilter("test")
		v.Stop() // should not panic
	})

	// ProduceView
	t.Run("ProduceView", func(t *testing.T) {
		t.Parallel()
		v := NewProduceView(nil, testTopicTail)
		v.Resize(80, 30)
		v.UpdateTable(tea.KeyPressMsg{Code: 'j'})
		if v.Loading() {
			t.Error("expected loading=false")
		}
		v.SetFilter("test")
		v.HandleKey("x")
		v.HandleKeyMsg(tea.KeyPressMsg{Code: 'a'})
	})

	// CreateTopicView
	t.Run("CreateTopicView", func(t *testing.T) {
		t.Parallel()
		v := NewCreateTopicView(nil)
		v.Resize(80, 30)
		v.UpdateTable(tea.KeyPressMsg{Code: 'j'})
		if v.Loading() {
			t.Error("expected loading=false")
		}
		if v.Refresh() != nil {
			t.Error("Refresh() should return nil")
		}
		v.SetFilter("test")
		v.HandleKey("x")
	})

	// CreateACLView
	t.Run("CreateACLView", func(t *testing.T) {
		t.Parallel()
		v := NewCreateACLView(nil)
		v.Resize(80, 30)
		v.UpdateTable(tea.KeyPressMsg{Code: 'j'})
		if v.Loading() {
			t.Error("expected loading=false")
		}
		if v.Refresh() != nil {
			t.Error("Refresh() should return nil")
		}
		v.SetFilter("test")
		v.HandleKeyMsg(tea.KeyPressMsg{Code: 'a'})
	})

	// ResetOffsetsView
	t.Run("ResetOffsetsView", func(t *testing.T) {
		t.Parallel()
		v := NewResetOffsetsView(nil, testGroup)
		v.Resize(80, 30)
		v.UpdateTable(tea.KeyPressMsg{Code: 'j'})
		if v.Loading() {
			t.Error("expected loading=false")
		}
		if v.Refresh() != nil {
			t.Error("Refresh() should return nil")
		}
		v.SetFilter("test")
		v.HandleKey("x")
	})
}

// ---------------------------------------------------------------------------
// formatRetentionMs
// ---------------------------------------------------------------------------

func TestTopicsViewViewFormatRetentionMs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want string
		ms   int64
	}{
		{name: "infinite", ms: 0, want: "∞"},
		{name: "negative", ms: -1, want: "∞"},
		{name: "days", ms: 172800000, want: "2d"},
		{name: "hours", ms: 7200000, want: "2h"},
		{name: "minutes", ms: 120000, want: "2m"},
		{name: "milliseconds", ms: 1500, want: "1500ms"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := formatRetentionMs(tt.ms); got != tt.want {
				t.Errorf("formatRetentionMs(%d) = %q, want %q", tt.ms, got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// maxFixedColsWidth
// ---------------------------------------------------------------------------

func TestMessagesViewMaxFixedColsWidth(t *testing.T) {
	t.Parallel()

	w := maxFixedColsWidth()
	if w <= 0 {
		t.Errorf("maxFixedColsWidth() = %d, want > 0", w)
	}
}

// ---------------------------------------------------------------------------
// sortACLs
// ---------------------------------------------------------------------------

func TestACLsSortACLs(t *testing.T) {
	t.Parallel()

	acls := []kafka.ACLEntry{
		{Principal: "User:bob", ResourceType: "topic", ResourceName: "b"},
		{Principal: "User:alice", ResourceType: "topic", ResourceName: "a"},
		{Principal: "User:alice", ResourceType: "group", ResourceName: "g"},
	}
	sortACLs(acls)
	if acls[0].Principal != "User:alice" {
		t.Errorf("first ACL should be alice, got %s", acls[0].Principal)
	}
	if acls[1].ResourceType != "topic" {
		t.Errorf("second ACL should be alice's topic, got %s", acls[1].ResourceType)
	}
}

// ---------------------------------------------------------------------------
// TopicDetailView.HandleKey variations
// ---------------------------------------------------------------------------

func TestTopicDetailViewHandleKey(t *testing.T) {
	t.Parallel()

	v := NewTopicDetailView(nil, "my-topic")
	v.Resize(80, 30)
	v.Update(TopicDetailRefreshMsg{Detail: &kafka.TopicDetail{
		Name:       "my-topic",
		Partitions: []kafka.PartitionInfo{{ID: 0, Leader: 1, Replicas: []int{1}, ISR: []int{1}}},
	}})

	// 'm' should return messages action
	action, param := v.HandleKey("m")
	if action != "messages" {
		t.Errorf("action = %q, want messages", action)
	}
	if param != "my-topic" {
		t.Errorf("param = %q, want my-topic", param)
	}

	// 'e' should return topic_config
	action, _ = v.HandleKey("e")
	if action != "topic_config" {
		t.Errorf("action = %q, want topic_config", action)
	}

	// 'p' should return increase_partitions
	action, _ = v.HandleKey("p")
	if action != "increase_partitions" {
		t.Errorf("action = %q, want increase_partitions", action)
	}

	// ctrl+d should return confirm_purge_topic
	action, _ = v.HandleKey("ctrl+d")
	if action != "confirm_purge_topic" {
		t.Errorf("action = %q, want confirm_purge_topic", action)
	}
}

// ---------------------------------------------------------------------------
// GroupDetailView.HandleKey
// ---------------------------------------------------------------------------

func TestGroupDetailViewHandleKeyAll(t *testing.T) {
	t.Parallel()

	v := NewGroupDetailView(nil, testGroup)
	v.Update(GroupDetailRefreshMsg{Detail: &kafka.GroupDetail{
		GroupID: testGroup, State: "Empty",
		Offsets: []kafka.GroupOffsetInfo{{Topic: "t1", Partition: 0}},
	}})
	v.Resize(80, 30)

	action, _ := v.HandleKey("R")
	if action != "reset_offsets" {
		t.Errorf("action = %q, want reset_offsets", action)
	}
}

// ---------------------------------------------------------------------------
// ACLsView — View() with empty ACLs
// ---------------------------------------------------------------------------

func TestACLsViewViewEmpty(t *testing.T) {
	t.Parallel()

	v := NewACLsView(nil)
	v.Update(ACLsRefreshMsg{ACLs: nil}) //nolint:errcheck

	out := v.View()
	if !strings.Contains(out, "No ACLs") {
		t.Errorf("View() = %q, expected to contain 'No ACLs'", out)
	}
}

// ---------------------------------------------------------------------------
// ACLsView — HandleKey("a") creates ACL
// ---------------------------------------------------------------------------

func TestACLsViewHandleKeyA(t *testing.T) {
	t.Parallel()

	v := NewACLsView(nil)
	action, param := v.HandleKey("a")
	if action != "create_acl" {
		t.Errorf("HandleKey('a'): action = %q, want create_acl", action)
	}
	if param != "" {
		t.Errorf("HandleKey('a'): param = %q, want empty", param)
	}
}

// ---------------------------------------------------------------------------
// ACLsView — HandleKey("ctrl+d") deletes ACL
// ---------------------------------------------------------------------------

func TestACLsViewHandleKeyCtrlD(t *testing.T) {
	t.Parallel()

	v := NewACLsView(nil)
	v.Update(ACLsRefreshMsg{ACLs: sampleACLs()}) //nolint:errcheck
	v.Resize(120, 20)

	action, param := v.HandleKey("ctrl+d")
	if action != "confirm_delete_acl" {
		t.Errorf("HandleKey('ctrl+d'): action = %q, want confirm_delete_acl", action)
	}
	if param == "" {
		t.Error("HandleKey('ctrl+d'): expected non-empty param (ACL key)")
	}
}

// ---------------------------------------------------------------------------
// TopicConfigView — Update with configs
// ---------------------------------------------------------------------------

func TestTopicConfigViewUpdate(t *testing.T) {
	t.Parallel()

	v := NewTopicConfigView(nil, testTopicTail)
	v.Update(TopicConfigRefreshMsg{Configs: sampleConfigs()}) //nolint:errcheck

	// Default: showDefaults=false, so only non-default configs.
	if v.Count() != 2 {
		t.Errorf("Count() = %d, want 2 (non-defaults only)", v.Count())
	}
}

// ---------------------------------------------------------------------------
// TopicConfigView — View() with error
// ---------------------------------------------------------------------------

func TestTopicConfigViewViewWithError(t *testing.T) {
	t.Parallel()

	v := NewTopicConfigView(nil, testTopicTail)
	v.Update(TopicConfigRefreshMsg{Err: fmt.Errorf("config fetch failed")}) //nolint:errcheck

	out := v.View()
	if !strings.Contains(out, "Error") {
		t.Errorf("View() = %q, expected to contain 'Error'", out)
	}
}

// ---------------------------------------------------------------------------
// BrokerDetailView — Update with configs
// ---------------------------------------------------------------------------

func TestBrokerDetailViewUpdate(t *testing.T) {
	t.Parallel()

	broker := kafka.BrokerInfo{ID: 1, Host: "broker-1", Port: 9092}
	v := NewBrokerDetailView(nil, broker, true)
	v.Update(BrokerDetailRefreshMsg{Configs: sampleBrokerConfigs()}) //nolint:errcheck

	// showDefaults=false by default, so only non-default configs.
	if v.Count() != 2 {
		t.Errorf("Count() = %d, want 2 (non-defaults only)", v.Count())
	}
}

// ---------------------------------------------------------------------------
// BrokerDetailView — HandleKey("d") toggles defaults
// ---------------------------------------------------------------------------

func TestBrokerDetailViewHandleKeyD(t *testing.T) {
	t.Parallel()

	broker := kafka.BrokerInfo{ID: 1, Host: "broker-1", Port: 9092}
	v := NewBrokerDetailView(nil, broker, false)
	v.Update(BrokerDetailRefreshMsg{Configs: sampleBrokerConfigs()}) //nolint:errcheck
	v.Resize(120, 20)

	// Initially 2 non-default configs.
	if v.Count() != 2 {
		t.Fatalf("initial Count() = %d, want 2", v.Count())
	}

	// Toggle defaults on.
	v.HandleKey("d")
	if v.Count() != 3 {
		t.Errorf("Count() after toggle on = %d, want 3", v.Count())
	}

	// Toggle defaults off.
	v.HandleKey("d")
	if v.Count() != 2 {
		t.Errorf("Count() after toggle off = %d, want 2", v.Count())
	}
}

// ---------------------------------------------------------------------------
// GroupDetailView — SetFilter
// ---------------------------------------------------------------------------

func TestGroupDetailViewSetFilter(t *testing.T) {
	t.Parallel()

	v := NewGroupDetailView(nil, testGroup)
	v.Update(GroupDetailRefreshMsg{Detail: &kafka.GroupDetail{
		GroupID: testGroup,
		State:   "Stable",
		Offsets: []kafka.GroupOffsetInfo{
			{Topic: "orders", Partition: 0, CommittedOffset: 100, HighWatermark: 150, Lag: 50},
		},
	}})

	// SetFilter should not panic.
	v.SetFilter("orders")
	v.SetFilter("")
}

// ---------------------------------------------------------------------------
// TopicDetailView — SetFilter
// ---------------------------------------------------------------------------

func TestTopicDetailViewSetFilter(t *testing.T) {
	t.Parallel()

	v := NewTopicDetailView(nil, testTopic)
	v.Update(TopicDetailRefreshMsg{Detail: &kafka.TopicDetail{
		Name:       testTopic,
		Partitions: []kafka.PartitionInfo{{ID: 0}},
	}})

	// SetFilter should not panic.
	v.SetFilter("partition")
	v.SetFilter("")
}

// ---------------------------------------------------------------------------
// MessagesView — follow mode defaults and toggle
// ---------------------------------------------------------------------------

func TestMessagesViewFollowMode(t *testing.T) {
	t.Parallel()

	v := NewMessagesView(nil, testTopicTail)
	v.Resize(80, 20)

	// Inject a message so follow mode is exercised.
	v.Update(MessageMsg(kafka.ConsumedMessage{
		Topic: testTopicTail, Partition: 0, Offset: 1, Key: "k", Value: "v",
	}))

	// Follow should be true by default.
	if !v.follow {
		t.Error("expected follow=true by default")
	}

	// Toggle follow off.
	v.HandleKey("f")
	if v.follow {
		t.Error("expected follow=false after 'f'")
	}

	// Toggle follow on.
	v.HandleKey("f")
	if !v.follow {
		t.Error("expected follow=true after second 'f'")
	}
}

// ---------------------------------------------------------------------------
// MessagesView — goto keys
// ---------------------------------------------------------------------------

func TestMessagesViewGotoKeys(t *testing.T) {
	t.Parallel()

	v := NewMessagesView(nil, testTopicTail)
	v.Resize(80, 20)
	v.Update(MessageMsg(kafka.ConsumedMessage{
		Topic: testTopicTail, Partition: 0, Offset: 1, Key: "k", Value: "v",
	}))

	// 'g' enables follow.
	v.follow = false
	v.HandleKey("g")
	if !v.follow {
		t.Error("expected follow=true after 'g'")
	}

	// 'G' disables follow.
	v.HandleKey("G")
	if v.follow {
		t.Error("expected follow=false after 'G'")
	}
}

// ---------------------------------------------------------------------------
// MessagesView — 's' opens the search bar via the "search" action.
// Single-message save lives on the message-detail view's 's'.
// ---------------------------------------------------------------------------

func TestMessagesViewSearchKey(t *testing.T) {
	t.Parallel()

	v := NewMessagesView(nil, testTopicTail)
	v.Resize(80, 20)
	v.Update(MessageMsg(kafka.ConsumedMessage{
		Topic: testTopicTail, Partition: 0, Offset: 42, Key: "k", Value: "v",
	}))

	action, param := v.HandleKey("s")
	if action != "search" {
		t.Errorf("HandleKey('s'): action = %q, want search", action)
	}
	if param != testTopicTail {
		t.Errorf("HandleKey('s'): param = %q, want %q (topic)", param, testTopicTail)
	}
}

// ---------------------------------------------------------------------------
// MessagesView — 'o' returns topic_detail
// ---------------------------------------------------------------------------

func TestMessagesViewOverviewKey(t *testing.T) {
	t.Parallel()

	v := NewMessagesView(nil, testTopicTail)
	v.Resize(80, 20)

	action, param := v.HandleKey("o")
	if action != actionTopicDetail {
		t.Errorf("HandleKey('o'): action = %q, want topic_detail", action)
	}
	if param != testTopicTail {
		t.Errorf("HandleKey('o'): param = %q, want test-topic", param)
	}
}
