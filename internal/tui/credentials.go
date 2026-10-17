// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package tui

import (
	"charm.land/bubbles/v2/textinput"
	tea "charm.land/bubbletea/v2"
	"github.com/blairham/tuikit/chrome"

	"github.com/blairham/k4a/internal/secrets"
)

// SetSecretStore sets the keychain that context switches read referenced
// secrets from.
func (a *App) SetSecretStore(s secrets.Store) { a.secrets = s }

// askContextPassword opens the prompt bar, masked, for a context that has a
// username but no password. The typed password goes straight into the new
// client and is not kept: a later switch or :reconnect asks again, and
// `k4a set-credential` is how to stop being asked.
func (a *App) askContextPassword(name, username string) tea.Cmd {
	a.prompt.Input().EchoMode = textinput.EchoPassword
	a.promptDispatch = func(input string) (string, tea.Cmd) {
		a.loading = true
		a.flash = "connecting to " + name + "..."
		return "", a.doSwitchContextWith(name, input)
	}
	return a.prompt.Open("", chrome.OpenOpts{
		Prompt:      "password for " + username + "@" + name + ":",
		Placeholder: "esc cancels · k4a set-credential " + name + " stores it in the keychain",
	})
}

// unmaskPrompt restores normal echo once the prompt closes, so the next
// prompt opened from anywhere shows what is typed.
func unmaskPrompt(p *chrome.Prompt) {
	p.Input().EchoMode = textinput.EchoNormal
}
