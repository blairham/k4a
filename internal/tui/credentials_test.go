// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package tui

import (
	"strings"
	"testing"

	"charm.land/bubbles/v2/textinput"
	tea "charm.land/bubbletea/v2"

	"github.com/blairham/k4a/internal/config"
	"github.com/blairham/k4a/internal/secrets"
	"github.com/blairham/k4a/internal/tui/style"
)

// passwordApp is on the picker with one SCRAM context that has a username
// but no password. No secret store is set unless a test sets one, so nothing
// here can reach the real keychain.
func passwordApp(t *testing.T, ctx config.Context) *App {
	t.Helper()
	app := testApp(t)
	app.cfg = &config.Config{Contexts: map[string]config.Context{"prod": ctx}}
	app.SetAwaitingContext()
	app.view = style.ViewContext
	return app
}

var scramNoPassword = config.Context{Brokers: "127.0.0.1:1", Auth: "scram", Username: "alice"}

// Switching to a context with no password asks for one, masked, and the
// typed password builds the client; the mask comes off with the prompt.
func TestSwitchContextAsksForMissingPassword(t *testing.T) {
	app := passwordApp(t, scramNoPassword)

	msg, ok := app.doSwitchContext("prod")().(switchContextMsg)
	if !ok || msg.needPassword != "alice" || msg.client != nil {
		t.Fatalf("switch = %+v, want a request for alice's password and no client", msg)
	}
	app.applySwitchContext(msg)
	if !app.prompt.Active() {
		t.Fatal("no password prompt opened")
	}
	if app.prompt.Input().EchoMode != textinput.EchoPassword {
		t.Fatal("password prompt echoes what is typed")
	}
	if !strings.Contains(app.prompt.Input().Prompt, "alice@prod") {
		t.Errorf("prompt reads %q, want the user and context", app.prompt.Input().Prompt)
	}

	cmd := typeKeys(t, app, append(runes("hunter2"), tea.KeyPressMsg{Code: tea.KeyEnter})...)
	if app.prompt.Active() {
		t.Fatal("prompt still open after enter")
	}
	if app.prompt.Input().EchoMode != textinput.EchoNormal {
		t.Error("the next prompt would still be masked")
	}
	if cmd == nil {
		t.Fatal("enter did not start the switch")
	}
	got, _ := cmd().(switchContextMsg)
	if got.err != nil || got.client == nil || got.needPassword != "" {
		t.Fatalf("switch after typing = %+v, want a client", got)
	}
	defer got.client.Close()
}

// esc cancels without switching and unmasks the bar for its next use.
func TestPasswordPromptEscUnmasks(t *testing.T) {
	app := passwordApp(t, scramNoPassword)
	app.applySwitchContext(switchContextMsg{name: "prod", needPassword: "alice"})

	if cmd := typeKeys(t, app, tea.KeyPressMsg{Code: tea.KeyEscape}); cmd != nil {
		t.Error("esc started a switch")
	}
	if app.prompt.Active() || app.prompt.Input().EchoMode != textinput.EchoNormal {
		t.Error("esc left the prompt open or masked")
	}
	if !app.awaitingContext {
		t.Error("a canceled prompt released the picker hold")
	}
}

// A context whose password is in the keychain does not ask.
func TestSwitchContextReadsKeychain(t *testing.T) {
	ctx := scramNoPassword
	ctx.Keychain.Password = "prod/password"

	app := passwordApp(t, ctx)
	msg, _ := app.doSwitchContext("prod")().(switchContextMsg)
	if msg.err == nil {
		t.Fatalf("switch = %+v; a keychain reference with no store must fail, not prompt or connect", msg)
	}

	store := secrets.NewMemory()
	_ = store.Set("prod/password", "from-keychain")
	app.SetSecretStore(store)
	msg, _ = app.doSwitchContext("prod")().(switchContextMsg)
	if msg.err != nil || msg.needPassword != "" || msg.client == nil {
		t.Fatalf("switch = %+v, want a client from the keychain password", msg)
	}
	defer msg.client.Close()
}
