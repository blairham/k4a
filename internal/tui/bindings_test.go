// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package tui

import (
	"slices"
	"strings"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/blairham/tuikit/viewfsm"

	"github.com/blairham/k4a/internal/config"
	"github.com/blairham/k4a/internal/kafka"
	"github.com/blairham/k4a/internal/tui/style"
	"github.com/blairham/k4a/internal/tui/views"
)

// allViewTypes is every view the app can show.
var allViewTypes = []style.ViewType{
	style.ViewTopics, style.ViewTopicDetail, style.ViewGroups, style.ViewGroupDetail,
	style.ViewCluster, style.ViewMessages, style.ViewMessageDetail, style.ViewProduce,
	style.ViewCreateTopic, style.ViewCreateACL, style.ViewContext, style.ViewACLs,
	style.ViewTopicConfig, style.ViewResetOffsets, style.ViewBrokerDetail, style.ViewSearch,
	style.ViewQuotas,
}

// keyName turns a hint-bar key ("<ctrl-d>", "<shift-g>") into the string
// bubbletea reports for that keypress ("ctrl+d", "G").
func keyName(hint string) string {
	k := strings.TrimSuffix(strings.TrimPrefix(hint, "<"), ">")
	switch {
	case k == "shift-g":
		return "G"
	case strings.HasPrefix(k, "ctrl-"), strings.HasPrefix(k, "shift-"):
		return strings.Replace(k, "-", "+", 1)
	}
	return k
}

// keyMsg builds the keypress whose String() is name.
func keyMsg(t *testing.T, name string) tea.KeyPressMsg {
	t.Helper()
	var m tea.KeyPressMsg
	switch name {
	case "enter":
		m = tea.KeyPressMsg{Code: tea.KeyEnter}
	case "esc":
		m = tea.KeyPressMsg{Code: tea.KeyEscape}
	case "tab":
		m = tea.KeyPressMsg{Code: tea.KeyTab}
	case "shift+tab":
		m = tea.KeyPressMsg{Code: tea.KeyTab, Mod: tea.ModShift}
	default:
		if c, ok := strings.CutPrefix(name, "ctrl+"); ok && len(c) == 1 {
			m = tea.KeyPressMsg{Code: rune(c[0]), Mod: tea.ModCtrl}
		} else if len(name) == 1 {
			m = keyPress(rune(name[0]))
		}
	}
	if m.String() != name {
		t.Fatalf("keyMsg(%q) builds %q", name, m.String())
	}
	return m
}

// bindingFixtures builds each view with enough data that every key it binds
// has something to act on: a selected row, an internal topic to reveal, a
// default config to toggle, a long message to page through.
func bindingFixtures(t *testing.T) map[style.ViewType]func() views.View {
	t.Helper()
	t.Setenv("HOME", t.TempDir()) // the produce form looks for ~/.k4a/schemas

	configs := []kafka.ConfigEntry{
		{Name: "retention.ms", Value: "1000", Source: "DYNAMIC_TOPIC"},
		{Name: "segment.ms", Value: "604800000", Source: "DEFAULT", IsDefault: true},
	}
	sized := func(v views.View) views.View {
		v.Resize(140, 30)
		return v
	}
	msg := func(p int, off int64) kafka.ConsumedMessage {
		return kafka.ConsumedMessage{
			Topic: "orders", Partition: p, Offset: off, Key: "k", Value: `{"a":1}`,
			Time: time.Unix(1_700_000_000+off, 0),
		}
	}

	return map[style.ViewType]func() views.View{
		style.ViewTopics: func() views.View {
			v := views.NewTopicsView(nil)
			v.Update(views.TopicsRefreshMsg{Topics: []kafka.TopicInfo{ //nolint:errcheck
				{Name: "orders", Partitions: 3, ReplicationFactor: 1},
				{Name: "__consumer_offsets", Partitions: 50, ReplicationFactor: 1, Internal: true},
			}})
			return sized(v)
		},
		style.ViewTopicDetail: func() views.View {
			v := views.NewTopicDetailView(nil, "orders")
			v.Update(views.TopicDetailRefreshMsg{Detail: &kafka.TopicDetail{ //nolint:errcheck
				Name: "orders", ReplicationFactor: 1,
				Partitions: []kafka.PartitionInfo{{ID: 0, Leader: 1, Replicas: []int{1}, ISR: []int{1}}},
			}})
			return sized(v)
		},
		style.ViewTopicConfig: func() views.View {
			v := views.NewTopicConfigView(nil, "orders")
			v.Update(views.TopicConfigRefreshMsg{Configs: configs}) //nolint:errcheck
			return sized(v)
		},
		style.ViewGroups: func() views.View {
			v := views.NewGroupsView(nil)
			v.Update(views.GroupsRefreshMsg{Groups: []kafka.GroupInfo{{GroupID: "billing", State: "Stable"}}}) //nolint:errcheck
			return sized(v)
		},
		style.ViewGroupDetail: func() views.View { return sized(views.NewGroupDetailView(nil, "billing")) },
		style.ViewCluster: func() views.View {
			v := views.NewClusterView(nil)
			v.Update(views.ClusterRefreshMsg{Cluster: &kafka.ClusterInfo{ //nolint:errcheck
				Brokers: []kafka.BrokerInfo{{ID: 1, Host: "b1", Port: 9092}},
			}})
			return sized(v)
		},
		style.ViewBrokerDetail: func() views.View {
			v := views.NewBrokerDetailView(nil, kafka.BrokerInfo{ID: 1}, false)
			v.Update(views.BrokerDetailRefreshMsg{Configs: configs}) //nolint:errcheck
			return sized(v)
		},
		style.ViewMessages: func() views.View {
			v := views.NewMessagesView(nil, "orders")
			for _, m := range []kafka.ConsumedMessage{msg(0, 1), msg(0, 2), msg(0, 3), msg(1, 4)} {
				v.Update(views.MessageMsg(m)) //nolint:errcheck
			}
			v.Resize(140, 30)
			// Leave each key something to undo: a partition filter for a
			// to clear, a pan for h, and the cursor off the newest row (out
			// of follow mode) for f and g.
			for _, k := range []string{"p", "l", "j"} {
				v.HandleKey(k)
			}
			v.UpdateTable(tea.KeyPressMsg{Code: tea.KeyDown})
			return v
		},
		style.ViewMessageDetail: func() views.View {
			list := []kafka.ConsumedMessage{msg(0, 3), msg(0, 2), msg(0, 1)}
			list[1].Value = `[` + strings.Repeat(`{"line":"x"},`, 60) + `{}]`
			d := views.NewMessageDetailView(&list[1])
			d.SetSiblings(list, 1) // in the middle, so n and N both have somewhere to go
			v := sized(d)
			v.UpdateTable(tea.KeyPressMsg{Code: tea.KeyPgDown}) // so PgUp has somewhere to go
			return v
		},
		style.ViewContext: func() views.View {
			v := views.NewContextView(&config.Config{Contexts: map[string]config.Context{
				"dev": {Brokers: "localhost:9092"}, "prod": {Brokers: "prod:9092"},
			}}, "dev")
			v.Update(v.Init()()) //nolint:errcheck
			return sized(v)
		},
		style.ViewACLs: func() views.View {
			v := views.NewACLsView(nil)
			v.Update(views.ACLsRefreshMsg{ACLs: []kafka.ACLEntry{{ //nolint:errcheck
				ResourceType: "TOPIC", ResourceName: "orders", PatternType: "LITERAL",
				Principal: "User:alice", Host: "*", Operation: "READ", Permission: "ALLOW",
			}}})
			return sized(v)
		},
		style.ViewSearch: func() views.View {
			v := views.NewSearchView(nil, "orders", "a")
			v.Update(views.SearchMatchMsg{Msg: msg(0, 1)}) //nolint:errcheck
			return sized(v)
		},
		style.ViewQuotas: func() views.View {
			v := views.NewQuotasView(nil)
			e, _ := kafka.ParseQuotaEntity("user=alice")
			v.Update(views.QuotasRefreshMsg{Quotas: []kafka.ClientQuota{ //nolint:errcheck
				{Entity: e, Key: "producer_byte_rate", Value: 1024},
			}})
			return sized(v)
		},
		style.ViewProduce:     func() views.View { return sized(views.NewProduceView(nil, "orders")) },
		style.ViewCreateTopic: func() views.View { return sized(views.NewCreateTopicView(nil)) },
		style.ViewCreateACL:   func() views.View { return sized(views.NewCreateACLView(nil)) },
		style.ViewResetOffsets: func() views.View {
			v := views.NewResetOffsetsView(nil, "billing")
			v.Update(views.ResetOffsetsTopicsMsg{Topics: []string{"orders"}}) //nolint:errcheck
			return sized(v)
		},
	}
}

// press sends key to v the way handleKey does once the global keys have had
// their turn, and reports whether v did anything with it.
func press(t *testing.T, v views.View, key string) bool {
	t.Helper()
	before := v.View()
	if fv, ok := v.(views.FormView); ok {
		cmd := fv.HandleKeyMsg(keyMsg(t, key))
		return cmd != nil || v.View() != before
	}
	if action, _ := v.HandleKey(key); action != "" {
		return true
	}
	v.UpdateTable(viewfsm.TranslateNavKey(keyMsg(t, key)))
	return v.View() != before
}

// globalKeys never reach a view outside a form: handleGlobalKey takes them.
var globalKeys = []string{"1", "2", "3", "4", "?", "esc", ":", "/", "r"}

// TestBindingsAreHandled: every key a view's hint bar or help offers must do
// something there. The help overlay used to be one static list for every
// view, and hint bars advertised keys their views ignored.
func TestBindingsAreHandled(t *testing.T) {
	fixtures := bindingFixtures(t)
	for _, vt := range allViewTypes {
		fixture, ok := fixtures[vt]
		if !ok {
			t.Errorf("%s has no fixture", style.ViewName(vt))
			continue
		}
		if len(viewBindings[vt]) == 0 {
			t.Errorf("%s has no bindings", style.ViewName(vt))
		}
		for _, b := range viewBindings[vt] {
			key := keyName(b.key)
			if cmd, ok := strings.CutPrefix(key, ":"); ok {
				if !slices.Contains(knownCommands, cmd) {
					t.Errorf("%s offers %s, which command mode does not complete", style.ViewName(vt), b.key)
				}
				continue
			}
			if key == "esc" {
				continue // global, and every view's way back
			}
			if slices.Contains(globalKeys, key) && !formViews[vt] {
				t.Errorf("%s binds %s, which the global keys take first", style.ViewName(vt), b.key)
				continue
			}
			if !press(t, fixture(), key) {
				t.Errorf("%s offers %s %q, but the view does nothing with it", style.ViewName(vt), b.key, b.desc)
			}
		}
	}
}

// TestHandledKeysAreBound is the other direction: a key a view acts on must
// be in its help, or nobody finds it (Topics' c and i, Messages' o, g, p, P,
// a, h and l were all wired and unlisted).
func TestHandledKeysAreBound(t *testing.T) {
	fixtures := bindingFixtures(t)
	universe := []string{"enter", "ctrl+d", "ctrl+e", "ctrl+r", "ctrl+s"}
	for c := 'a'; c <= 'z'; c++ {
		universe = append(universe, string(c), strings.ToUpper(string(c)))
	}
	for _, vt := range allViewTypes {
		if formViews[vt] {
			continue // a form takes every printable key as input
		}
		listed := map[string]bool{}
		for _, s := range helpPanel(vt).Sections {
			for _, e := range s.Entries {
				listed[keyName(e.Key)] = true
			}
		}
		v := fixtures[vt]() // rebuilt only after a key changed it: fixtures render slowly under -race
		for _, key := range universe {
			if slices.Contains(globalKeys, key) || listed[key] {
				continue
			}
			before := v.View()
			action, _ := v.HandleKey(key)
			if action != "" || v.View() != before {
				t.Errorf("%s acts on %q (action %q), but its help does not list it", style.ViewName(vt), key, action)
				v = fixtures[vt]()
			}
		}
	}
}

// TestHelpPanelIsPerView: no key appears twice with two meanings (the old
// static panel listed <p> and <ctrl-d> twice each), and everything the hint
// bar offers is explained in the help.
func TestHelpPanelIsPerView(t *testing.T) {
	t.Parallel()
	for _, vt := range allViewTypes {
		seen := map[string]string{}
		for _, s := range helpPanel(vt).Sections {
			for _, e := range s.Entries {
				if prev, dup := seen[e.Key]; dup {
					t.Errorf("%s help lists %s twice: %q and %q", style.ViewName(vt), e.Key, prev, e.Desc)
				}
				seen[e.Key] = e.Desc
			}
		}
		for _, s := range actionShortcuts(vt) {
			if _, ok := seen[s.Key]; !ok && s.Key != "<:q>" {
				t.Errorf("%s hint bar offers %s, which its help does not explain", style.ViewName(vt), s.Key)
			}
		}
	}

	// The panel differs by view: Topic Detail's <p> adds partitions, Topics'
	// produces, and a form has no navigation section at all.
	descOf := func(vt style.ViewType, key string) string {
		for _, s := range helpPanel(vt).Sections {
			for _, e := range s.Entries {
				if e.Key == key {
					return e.Desc
				}
			}
		}
		return ""
	}
	if a, b := descOf(style.ViewTopics, keyP), descOf(style.ViewTopicDetail, keyP); a == b {
		t.Errorf("<p> reads %q on both Topics and Topic Detail", a)
	}
	if n := len(helpPanel(style.ViewProduce).Sections); n != 2 {
		t.Errorf("Produce help has %d sections, want RESOURCE and GENERAL only", n)
	}
}

// TestKnownCommandsDispatch: autocomplete offers exactly the commands
// dispatchCommand runs (it was missing :s, :q, :topic, :group and :brokers).
func TestKnownCommandsDispatch(t *testing.T) {
	t.Setenv("HOME", t.TempDir())

	// The commands dispatchCommand matches outside its two tables; the ones
	// that quit, restart or redial are not run below.
	switchCommands := []string{
		"q",
		"q!",
		"quit",
		"exit",
		"reset",
		"reconnect",
		"search",
		"s",
		"upgrade",
		"logo",
		"logoless",
	}
	notRun := []string{"q", "q!", "quit", "exit", "reconnect", "upgrade"}

	dispatched := map[string]bool{"produce": true}
	for _, c := range switchCommands {
		dispatched[c] = true
	}
	for c := range commandViews {
		dispatched[c] = true
	}
	for c := range commandActions {
		dispatched[c] = true
	}
	for c := range dispatched {
		if !slices.Contains(knownCommands, c) {
			t.Errorf(":%s runs but is not offered by autocomplete", c)
		}
	}
	for _, c := range knownCommands {
		if !dispatched[c] {
			t.Errorf(":%s is offered by autocomplete but is not a command", c)
		}
		if slices.Contains(notRun, c) {
			continue
		}
		app := NewApp(nil, ConnInfo{}, nil, "", "dev")
		app.width, app.height = 120, 40
		if msg, _ := app.dispatchCommand(c); strings.HasPrefix(msg, "unknown command") {
			t.Errorf(":%s is offered by autocomplete but dispatches as %q", c, msg)
		}
	}
	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	if msg, _ := app.dispatchCommand("nope"); !strings.HasPrefix(msg, "unknown command") {
		t.Errorf("an unknown command dispatched as %q; the check above proves nothing", msg)
	}
}
