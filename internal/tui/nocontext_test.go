// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package tui

import (
	"errors"
	"testing"

	"github.com/blairham/k4a/internal/config"
	"github.com/blairham/k4a/internal/kafka"
	"github.com/blairham/k4a/internal/tui/style"
)

// Launched with no current-context there is no client, so every data view
// would query a nil one. The app holds on the picker until a context is
// chosen — that view is both the only one that works and the only way out.

func awaitingApp(t *testing.T) *App {
	t.Helper()
	cfg := &config.Config{Contexts: map[string]config.Context{
		"staging-us": {Brokers: "stg:9098", Auth: "iam"},
	}}
	app := NewApp(nil, ConnInfo{}, cfg, "", "dev")
	app.SetAwaitingContext()
	app.view = style.ViewContext
	return app
}

func TestAwaitingContextRefusesToLeaveThePicker(t *testing.T) {
	app := awaitingApp(t)

	if cmd := app.switchView(style.ViewGroups); cmd != nil {
		t.Error("switchView returned a cmd that would query a nil client")
	}
	if app.view != style.ViewContext {
		t.Errorf("view = %d, want ViewContext — the app left the picker with no client", app.view)
	}
	if app.errFlash == "" {
		t.Error("the refusal was silent; the user needs to know why nothing happened")
	}
}

func TestAwaitingContextStillAllowsThePickerItself(t *testing.T) {
	app := awaitingApp(t)
	app.view = style.ViewTopics

	app.switchView(style.ViewContext)
	if app.view != style.ViewContext {
		t.Errorf("view = %d, want ViewContext — the picker must stay reachable", app.view)
	}
}

// Once a context is live the hold comes off, or the app would be stuck on the
// picker for the rest of the session.
func TestApplySwitchContextClearsTheHold(t *testing.T) {
	app := awaitingApp(t)

	app.applySwitchContext(switchContextMsg{
		client: &kafka.Client{},
		info:   ConnInfo{Context: "staging-us"},
		name:   "staging-us",
	})

	if app.awaitingContext {
		t.Error("still awaiting a context after one was selected")
	}
	if cmd := app.switchView(style.ViewGroups); cmd == nil {
		t.Error("navigation still blocked after a context was selected")
	}
}

// A failed switch leaves the user where they were, with no client — the hold
// must survive, or the next keystroke dereferences nil.
func TestAFailedSwitchKeepsTheHold(t *testing.T) {
	app := awaitingApp(t)

	app.applySwitchContext(switchContextMsg{err: errors.New("dial failed"), name: "staging-us"})

	if !app.awaitingContext {
		t.Error("hold released by a switch that never connected")
	}
}
