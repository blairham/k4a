// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package tui

import (
	"context"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kfake"

	"github.com/blairham/k4a/internal/kafka"
	"github.com/blairham/k4a/internal/tui/style"
	"github.com/blairham/k4a/internal/tui/views"
)

var testACL = kafka.CreateACLConfig{
	ResourceType: "topic", ResourceName: "orders", PatternType: "literal",
	Principal: "User:alice", Host: "*", Operation: "read", Permission: "allow",
}

// aclsApp is an App on the ACLs view of an in-process cluster holding
// testACL, with that binding selected.
func aclsApp(t *testing.T, readonly bool) (*App, *kafka.Client) {
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
	if err := client.CreateACL(context.Background(), testACL); err != nil {
		t.Fatalf("seed ACL: %v", err)
	}

	app := NewApp(client, ConnInfo{}, nil, "", "dev")
	app.width, app.height = 140, 40
	app.readonly = readonly
	app.loading = false
	lv := views.NewACLsView(client)
	lv.Update(lv.Init()()) //nolint:errcheck
	lv.Resize(140, 30)
	if lv.Count() != 1 {
		t.Fatalf("ACLs view shows %d bindings, want the seeded one", lv.Count())
	}
	app.setView(style.ViewACLs, lv)
	app.view = style.ViewACLs
	return app, client
}

func clusterACLs(t *testing.T, c *kafka.Client) []kafka.ACLEntry {
	t.Helper()
	got, err := c.FetchACLs(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return got
}

// TestACLEditReplacesAfterConfirm: e opens the form on the binding, and the
// submitted edit replaces it only after y.
func TestACLEditReplacesAfterConfirm(t *testing.T) {
	app, client := aclsApp(t, false)
	typeKeys(t, app, keyPress('e'))
	if app.view != style.ViewCreateACL {
		t.Fatalf("e opened view %s, want the ACL form", style.ViewName(app.view))
	}
	if !strings.Contains(app.renderActiveView(), "editing allow read on topic:orders") {
		t.Errorf("the form does not say which ACL it edits:\n%s", app.renderActiveView())
	}

	next := testACL
	next.Operation = "write"
	submit := views.EditACLSubmitMsg{Old: kafka.ACLEntry(testACL), New: next}

	// Declined: nothing changes.
	app.Update(submit) //nolint:errcheck
	if !app.confirm.Active() || !strings.Contains(app.confirm.Prompt(), "allow write on topic:orders") {
		t.Fatalf("no confirmation naming the new ACL; prompt %q", app.confirm.Prompt())
	}
	if cmd := typeKeys(t, app, keyPress('n')); cmd != nil {
		app.Update(cmd()) //nolint:errcheck
	}
	if got := clusterACLs(t, client); len(got) != 1 || got[0] != kafka.ACLEntry(testACL) {
		t.Fatalf("a declined edit changed the ACLs: %+v", got)
	}

	// Confirmed: replaced.
	app.Update(submit) //nolint:errcheck
	cmd := typeKeys(t, app, keyPress('y'))
	if cmd == nil {
		t.Fatal("y returned no command")
	}
	app.Update(cmd()) //nolint:errcheck
	if got := clusterACLs(t, client); len(got) != 1 || got[0] != kafka.ACLEntry(next) {
		t.Errorf("after the edit ACLs = %+v, want only %+v", got, next)
	}
	if !strings.Contains(app.renderActiveView(), "ACL replaced") {
		t.Errorf("the form does not report the replacement:\n%s", app.renderActiveView())
	}
}

// TestACLEditRefusals: an unchanged edit asks nothing and sends nothing;
// readonly mode does not open the form.
func TestACLEditRefusals(t *testing.T) {
	app, client := aclsApp(t, false)
	typeKeys(t, app, keyPress('e'))
	app.Update(views.EditACLSubmitMsg{Old: kafka.ACLEntry(testACL), New: testACL}) //nolint:errcheck
	if app.confirm.Active() {
		t.Error("an unchanged ACL asked for confirmation")
	}
	if !strings.Contains(app.errFlash, "nothing changed") {
		t.Errorf("errFlash = %q", app.errFlash)
	}
	if got := clusterACLs(t, client); len(got) != 1 {
		t.Errorf("ACLs = %+v", got)
	}

	ro, _ := aclsApp(t, true)
	typeKeys(t, ro, keyPress('e'))
	if ro.view != style.ViewACLs || ro.errFlash != msgReadonly {
		t.Errorf("readonly: view %s, errFlash %q; want the edit refused", style.ViewName(ro.view), ro.errFlash)
	}
}
