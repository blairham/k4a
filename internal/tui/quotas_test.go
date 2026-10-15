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

// TestTopLevelViewsExist: every tab key and view command lands on a view.
// The 4 tab switched to ACLs with nothing in viewMap, so it drew an empty
// box from the first release on.
func TestTopLevelViewsExist(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	targets := map[string]style.ViewType{}
	for k, vt := range viewKeys {
		targets["key "+k] = vt
	}
	for c, vt := range commandViews {
		targets[":"+c] = vt
	}
	for how, vt := range targets {
		app := NewApp(nil, ConnInfo{}, nil, "", "dev")
		app.width, app.height = 120, 40
		app.switchView(vt) //nolint:errcheck // the refresh needs a client; only the view matters here
		if app.activeView() == nil {
			t.Errorf("%s switches to %s, which has no view", how, style.ViewName(vt))
		}
	}
}

// quotasApp is an App on the Client Quotas view of an in-process cluster.
func quotasApp(t *testing.T, readonly bool) (*App, *kafka.Client) {
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
	app.loading = false
	if msg, cmd := app.dispatchCommand("quotas"); msg != "" || cmd == nil {
		t.Fatalf(":quotas = %q, cmd %v", msg, cmd != nil)
	}
	return app, client
}

// refreshQuotas runs the quotas view's fetch and applies it.
func refreshQuotas(t *testing.T, app *App) {
	t.Helper()
	app.Update(app.activeView().Refresh()()) //nolint:errcheck
}

func clusterQuotas(t *testing.T, c *kafka.Client) []string {
	t.Helper()
	got, err := c.FetchClientQuotas(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	out := make([]string, 0, len(got))
	for _, q := range got {
		out = append(out, q.Entity.String()+" "+q.Key+"="+kafka.FormatQuotaValue(q.Value))
	}
	return out
}

// answer presses key on an open confirmation and applies what it returns.
func answer(t *testing.T, app *App, key rune) {
	t.Helper()
	if !app.confirm.Active() {
		t.Fatal("no confirmation is open")
	}
	if cmd := typeKeys(t, app, keyPress(key)); cmd != nil {
		app.Update(cmd()) //nolint:errcheck
	}
}

// TestQuotasAddEditRemove drives the view's three writes through the keys,
// each asking first and changing nothing on n.
func TestQuotasAddEditRemove(t *testing.T) {
	app, client := quotasApp(t, false)
	enter := tea.KeyPressMsg{Code: tea.KeyEnter}
	ctrlD := tea.KeyPressMsg{Code: 'd', Mod: tea.ModCtrl}

	typeKeys(t, app, keyPress('a'))
	if !app.prompt.Active() {
		t.Fatal("a did not open the quota prompt")
	}
	typeKeys(t, app, append(runes("user=alice producer_byte_rate=1024"), enter)...)
	if q := app.confirm.Prompt(); !strings.Contains(q, "set producer_byte_rate=1024 for user=alice") {
		t.Fatalf("confirm = %q", q)
	}
	answer(t, app, 'n')
	if got := clusterQuotas(t, client); len(got) != 0 {
		t.Fatalf("a declined add set %q", got)
	}

	typeKeys(
		t,
		app,
		append(append([]tea.KeyPressMsg{keyPress('a')}, runes("user=alice producer_byte_rate=1024")...), enter)...,
	)
	answer(t, app, 'y')
	if got := clusterQuotas(t, client); len(got) != 1 || got[0] != "user=alice producer_byte_rate=1024" {
		t.Fatalf("after add: %q", got)
	}

	refreshQuotas(t, app)
	typeKeys(t, app, append([]tea.KeyPressMsg{enter}, append(runes("2048"), enter)...)...)
	answer(t, app, 'y')
	if got := clusterQuotas(t, client); len(got) != 1 || got[0] != "user=alice producer_byte_rate=2048" {
		t.Fatalf("after edit: %q", got)
	}

	refreshQuotas(t, app)
	typeKeys(t, app, ctrlD)
	if q := app.confirm.Prompt(); !strings.Contains(q, "remove producer_byte_rate from user=alice") {
		t.Fatalf("remove confirm = %q", q)
	}
	answer(t, app, 'y')
	if got := clusterQuotas(t, client); len(got) != 0 {
		t.Errorf("after remove: %q", got)
	}
}

// TestQuotasRefusals: malformed input keeps the prompt open with the
// reason, and readonly mode offers no writes.
func TestQuotasRefusals(t *testing.T) {
	app, _ := quotasApp(t, false)
	typeKeys(t, app, keyPress('a'))
	for _, in := range []string{"user=alice", "group=x producer_byte_rate=1", "user=alice producer_byte_rate", "user=alice producer_byte_rate=-1"} {
		if msg, _ := app.promptDispatch(in); msg == "" {
			t.Errorf("%q was accepted", in)
		}
	}
	if app.confirm.Active() {
		t.Error("a refused input opened a confirmation")
	}

	ro, _ := quotasApp(t, true)
	typeKeys(t, ro, keyPress('a'))
	if ro.prompt.Active() || ro.errFlash != msgReadonly {
		t.Errorf("readonly: prompt open %v, errFlash %q", ro.prompt.Active(), ro.errFlash)
	}
	if _, ok := ro.activeView().(*views.QuotasView); !ok {
		t.Error("readonly mode cannot even list quotas")
	}
}
