// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package tui

import (
	"context"
	"strings"
	"testing"

	tea "charm.land/bubbletea/v2"
	"github.com/twmb/franz-go/pkg/kfake"

	"github.com/blairham/k4a/internal/kafka"
	"github.com/blairham/k4a/internal/tui/style"
	"github.com/blairham/k4a/internal/tui/views"
)

const testBrokerKey = "log.retention.ms"

// brokerDetailApp is an App on Broker Detail for broker 0 of an in-process
// cluster, with the cursor on testBrokerKey.
func brokerDetailApp(t *testing.T, readonly bool) (*App, *kafka.Client) {
	t.Helper()
	t.Setenv("HOME", t.TempDir())
	fake, err := kfake.NewCluster(kfake.NumBrokers(1))
	if err != nil {
		t.Fatalf("kfake cluster: %v", err)
	}
	t.Cleanup(fake.Close)
	client, err := kafka.NewClient(kafka.AuthConfig{Method: kafka.AuthPlaintext}, strings.Join(fake.ListenAddrs(), ","))
	if err != nil {
		t.Fatalf("kafka client: %v", err)
	}
	t.Cleanup(client.Close)

	app := NewApp(client, ConnInfo{}, nil, "", "dev")
	app.width, app.height = 140, 40
	app.readonly = readonly
	v := views.NewBrokerDetailView(client, kafka.BrokerInfo{ID: 0}, true)
	v.Update(views.BrokerDetailRefreshMsg{Configs: []kafka.ConfigEntry{ //nolint:errcheck
		{Name: testBrokerKey, Value: "604800000", Source: "STATIC_BROKER"},
	}})
	v.Resize(140, 30)
	app.setView(style.ViewBrokerDetail, v)
	app.view = style.ViewBrokerDetail
	return app, client
}

// typeKeys sends each key through the app, returning the last command.
func typeKeys(t *testing.T, app *App, keys ...tea.KeyPressMsg) tea.Cmd {
	t.Helper()
	var cmd tea.Cmd
	for _, k := range keys {
		_, cmd = app.handleKey(k)
	}
	return cmd
}

func runes(s string) []tea.KeyPressMsg {
	out := make([]tea.KeyPressMsg, 0, len(s))
	for _, r := range s {
		out = append(out, keyPress(r))
	}
	return out
}

func brokerOverride(t *testing.T, c *kafka.Client) (string, bool) {
	t.Helper()
	entries, err := c.FetchBrokerConfigs(context.Background(), "0")
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.Name == testBrokerKey && e.Source == dynamicBrokerSource {
			return e.Value, true
		}
	}
	return "", false
}

// TestBrokerConfigEditConfirms: enter, a value and enter only ask; nothing
// reaches the broker until y.
func TestBrokerConfigEditConfirms(t *testing.T) {
	app, client := brokerDetailApp(t, false)
	enter := tea.KeyPressMsg{Code: tea.KeyEnter}

	typeKeys(t, app, enter)
	if !app.prompt.Active() {
		t.Fatal("enter on a broker config did not open the value prompt")
	}
	typeKeys(t, app, append(runes("3600000"), enter)...)
	if !app.confirm.Active() {
		t.Fatal("submitting a value did not ask for confirmation")
	}
	if q := app.confirm.Prompt(); !strings.Contains(q, testBrokerKey+"=3600000") || !strings.Contains(q, "broker 0") {
		t.Errorf("confirm reads %q, want the key, value and broker", q)
	}
	if _, ok := brokerOverride(t, client); ok {
		t.Fatal("the config changed before it was confirmed")
	}

	cmd := typeKeys(t, app, keyPress('y'))
	if cmd == nil {
		t.Fatal("y returned no command")
	}
	app.Update(cmd()) //nolint:errcheck
	if app.errFlash != "" {
		t.Fatalf("errFlash = %q", app.errFlash)
	}
	if got, ok := brokerOverride(t, client); !ok || got != "3600000" {
		t.Errorf("override = %q (present %v), want 3600000", got, ok)
	}
}

func TestBrokerConfigEditDeclined(t *testing.T) {
	app, client := brokerDetailApp(t, false)
	enter := tea.KeyPressMsg{Code: tea.KeyEnter}
	// Whatever n returns is run, so a dispatch that applied the change
	// regardless of the answer would show up below.
	if cmd := typeKeys(
		t,
		app,
		append(append([]tea.KeyPressMsg{enter}, runes("1")...), enter, keyPress('n'))...,
	); cmd != nil {
		app.Update(cmd()) //nolint:errcheck
	}
	if app.confirm.Active() || app.prompt.Active() {
		t.Error("n left a bar open")
	}
	if _, ok := brokerOverride(t, client); ok {
		t.Error("a declined change reached the broker")
	}
}

// TestBrokerConfigEditRefusals: the prompt stays open, with the reason, on
// input that would do nothing; readonly blocks the edit outright.
func TestBrokerConfigEditRefusals(t *testing.T) {
	enter := tea.KeyPressMsg{Code: tea.KeyEnter}

	app, _ := brokerDetailApp(t, false)
	if msg, _ := app.confirmBrokerConfig(
		"0",
		testBrokerKey,
		"604800000",
		"STATIC_BROKER",
		"",
	); !strings.Contains(
		msg,
		"no override",
	) {
		t.Errorf("empty value on a static config: %q, want no override to remove", msg)
	}
	if msg, _ := app.confirmBrokerConfig(
		"0",
		testBrokerKey,
		"604800000",
		"STATIC_BROKER",
		"604800000",
	); msg != "value unchanged" {
		t.Errorf("unchanged value: %q", msg)
	}
	if msg, _ := app.confirmBrokerConfig(
		"0",
		testBrokerKey,
		"1",
		dynamicBrokerSource,
		"",
	); msg != "" ||
		!app.confirm.Active() ||
		!strings.Contains(app.confirm.Prompt(), "remove") {
		t.Errorf("empty value on an override: msg %q, confirm %q; want a remove confirmation", msg, app.confirm.Prompt())
	}

	ro, _ := brokerDetailApp(t, true)
	typeKeys(t, ro, enter)
	if ro.prompt.Active() {
		t.Error("readonly mode opened the broker config prompt")
	}
	if ro.errFlash != "readonly mode" {
		t.Errorf("readonly errFlash = %q", ro.errFlash)
	}
}

// TestBrokerConfigRemoveOverride removes a dynamic override end to end.
func TestBrokerConfigRemoveOverride(t *testing.T) {
	app, client := brokerDetailApp(t, false)
	v := "1000"
	if err := client.AlterBrokerConfig(context.Background(), "0", testBrokerKey, &v); err != nil {
		t.Fatal(err)
	}
	app.confirmBrokerConfig("0", testBrokerKey, v, dynamicBrokerSource, "") //nolint:errcheck
	cmd := typeKeys(t, app, keyPress('y'))
	app.Update(cmd()) //nolint:errcheck
	if got, ok := brokerOverride(t, client); ok {
		t.Errorf("override %q survived its removal", got)
	}
	if !strings.Contains(app.flash, "removed") {
		t.Errorf("flash = %q", app.flash)
	}
}
