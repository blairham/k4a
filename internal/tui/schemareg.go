// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package tui

import (
	"github.com/blairham/tuikit/chrome"

	"github.com/blairham/k4a/internal/config"
	"github.com/blairham/k4a/internal/schemareg"
	"github.com/blairham/k4a/internal/tui/style"
	"github.com/blairham/k4a/internal/tui/views"
)

// SetSchemaRegistry sets the registry the starting connection decodes with
// (nil for none) and the flag overrides a context switch applies over the
// next context's schema-registry block.
func (a *App) SetSchemaRegistry(reg *schemareg.Registry, flags schemareg.Config) {
	a.registry = reg
	a.registryFlags = flags
}

// registryForContext builds the registry for a context being switched to.
// It runs off the UI loop, so it takes the flags rather than reading the App.
func registryForContext(ctx config.Context, flags schemareg.Config) (*schemareg.Registry, error) {
	return schemareg.NewIfSet(ctx.SchemaRegistry.Config().Overlay(flags))
}

// newMessagesView builds a Messages view that decodes with the active
// registry, if there is one.
func (a *App) newMessagesView(topic string) *views.MessagesView {
	v := views.NewMessagesView(a.client, topic)
	if a.registry != nil {
		v.SetDecoder(a.registry)
	}
	return v
}

// newMessageDetailView opens a message in the raw/decoded mode the Messages
// view is in, so drilling in does not flip what the user was looking at.
func (a *App) newMessageDetailView(v *views.MessageDetailView) *views.MessageDetailView {
	if mv := typedView[*views.MessagesView](a, style.ViewMessages); mv != nil && mv.Raw() {
		v.SetRaw(true)
	}
	return v
}

const keyToggleRaw = "<" + views.KeyToggleRaw + ">"

// hasDecodeToggle reports whether the active view has the raw/decoded
// toggle and there is a registry for it to toggle. It is not in viewBindings
// because without a registry the key does nothing, which the binding tests
// rightly refuse.
func (a *App) hasDecodeToggle() bool {
	return a.registry != nil && (a.view == style.ViewMessages || a.view == style.ViewMessageDetail)
}

// withDecodeHelp lists the toggle in the help overlay where it works.
func (a *App) withDecodeHelp(p chrome.HelpPanel) chrome.HelpPanel {
	if !a.hasDecodeToggle() || len(p.Sections) == 0 {
		return p
	}
	p.Sections[0].Entries = append(p.Sections[0].Entries, chrome.HelpEntry{Key: keyToggleRaw, Desc: "Raw/Decoded"})
	sortHelpEntries(p.Sections[0].Entries)
	return p
}

// withDecodeShortcut advertises the raw/decoded toggle on the two views that
// have it, and only when a registry is configured: without one there is
// nothing decoded to toggle.
func (a *App) withDecodeShortcut(actions []chrome.Shortcut) []chrome.Shortcut {
	if !a.hasDecodeToggle() {
		return actions
	}
	actions = append(actions, chrome.Shortcut{Key: keyToggleRaw, Desc: "Raw"})
	sortShortcuts(actions)
	return actions
}
