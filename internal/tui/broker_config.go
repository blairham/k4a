// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package tui

import (
	"context"
	"fmt"
	"strings"

	tea "charm.land/bubbletea/v2"
	"github.com/blairham/tuikit/chrome"

	"github.com/blairham/k4a/internal/tui/style"
	"github.com/blairham/k4a/internal/tui/views"
)

const actionEditBrokerConfig = "edit_broker_config"

// dynamicBrokerSource is the config source of a per-broker dynamic override,
// the only kind removing an override can act on.
const dynamicBrokerSource = "DYNAMIC_BROKER"

// brokerConfigMsg carries the result of an async broker config change.
type brokerConfigMsg struct {
	err      error
	brokerID string
	key      string
	removed  bool
}

// startBrokerConfigEdit opens the prompt pre-filled with the selected
// config's value. Submitting it asks for confirmation before anything is
// sent: a broker config applies to every client of that broker at once.
func (a *App) startBrokerConfigEdit(name string) (tea.Model, tea.Cmd) {
	v := typedView[*views.BrokerDetailView](a, style.ViewBrokerDetail)
	if v == nil {
		return a, nil
	}
	entry := v.Selected()
	if entry == nil || entry.Name != name {
		return a, nil
	}
	brokerID, current, source := v.BrokerID(), entry.Value, entry.Source
	a.promptDispatch = func(input string) (string, tea.Cmd) {
		return a.confirmBrokerConfig(brokerID, name, current, source, strings.TrimSpace(input))
	}
	cmd := a.prompt.Open(current, chrome.OpenOpts{
		Prompt:      fmt.Sprintf("broker %s %s:", brokerID, name),
		Placeholder: "empty removes the broker's override",
		SelectAll:   true,
	})
	a.resizeActiveView()
	return a, cmd
}

// confirmBrokerConfig turns the typed value into a y/n confirmation, or
// keeps the prompt open with the reason it cannot. An empty value removes
// the broker's dynamic override.
func (a *App) confirmBrokerConfig(brokerID, key, current, source, input string) (errMsg string, cmd tea.Cmd) {
	var value *string
	var question string
	switch {
	case input == "" && source != dynamicBrokerSource:
		return fmt.Sprintf("%s has no override on broker %s to remove", key, brokerID), nil
	case input == "":
		question = fmt.Sprintf("remove the override of %s on broker %s?", key, brokerID)
	case input == current:
		return "value unchanged", nil
	default:
		value = &input
		question = fmt.Sprintf("set %s=%s on broker %s?", key, input, brokerID)
	}
	a.confirmDispatch = func(yes bool) (string, tea.Cmd) {
		if !yes {
			return "", nil
		}
		if a.readonly {
			return msgReadonly, nil
		}
		a.loading = true
		return "", a.doAlterBrokerConfig(brokerID, key, value)
	}
	a.confirm.Open(question)
	return "", nil
}

// doAlterBrokerConfig sets (or with a nil value removes) key on one broker.
func (a *App) doAlterBrokerConfig(brokerID, key string, value *string) tea.Cmd {
	client := a.client
	return func() tea.Msg {
		err := client.AlterBrokerConfig(context.Background(), brokerID, key, value)
		return brokerConfigMsg{err: err, brokerID: brokerID, key: key, removed: value == nil}
	}
}

// applyBrokerConfigResult reports the change and refreshes the broker's
// configs so the new value and its source show.
func (a *App) applyBrokerConfigResult(msg brokerConfigMsg) tea.Cmd {
	a.loading = false
	if msg.err != nil {
		a.errFlash = msg.err.Error()
		return nil
	}
	if msg.removed {
		a.flash = fmt.Sprintf("removed the override of %s on broker %s", msg.key, msg.brokerID)
	} else {
		a.flash = fmt.Sprintf("set %s on broker %s", msg.key, msg.brokerID)
	}
	if v := typedView[*views.BrokerDetailView](a, style.ViewBrokerDetail); v != nil && v.BrokerID() == msg.brokerID {
		return v.Refresh()
	}
	return nil
}
