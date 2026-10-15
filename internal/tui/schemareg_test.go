// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package tui

import (
	"strings"
	"testing"

	"github.com/blairham/k4a/internal/schemareg"
	"github.com/blairham/k4a/internal/tui/style"
)

// The raw/decoded toggle is advertised only where it does something: on
// Messages and Message Detail, with a registry configured, and within the
// six-row action column.
func TestDecodeShortcut(t *testing.T) {
	t.Parallel()
	reg, err := schemareg.New(schemareg.Config{URL: "http://127.0.0.1:1"})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		reg  *schemareg.Registry
		view style.ViewType
		want bool
	}{
		{nil, style.ViewMessages, false},
		{reg, style.ViewMessages, true},
		{reg, style.ViewMessageDetail, true},
		{reg, style.ViewTopics, false},
	} {
		app := NewApp(nil, ConnInfo{}, nil, "", "dev")
		app.width, app.height = 140, 40
		app.view = tc.view
		app.SetSchemaRegistry(tc.reg, schemareg.Config{})
		lines := app.renderShortcuts()
		got := strings.Contains(strings.Join(lines, "\n"), keyToggleRaw)
		if got != tc.want {
			t.Errorf("%s (registry %v): advertises %s = %v", style.ViewName(tc.view), tc.reg != nil, keyToggleRaw, got)
		}
		inHelp := false
		for _, sec := range app.withDecodeHelp(helpPanel(tc.view)).Sections {
			for _, e := range sec.Entries {
				inHelp = inHelp || e.Key == keyToggleRaw
			}
		}
		if inHelp != tc.want {
			t.Errorf("%s (registry %v): help lists %s = %v", style.ViewName(tc.view), tc.reg != nil, keyToggleRaw, inHelp)
		}
		if len(lines) > 6 {
			t.Errorf("%s: %d shortcut rows, the column holds 6", style.ViewName(tc.view), len(lines))
		}
	}
}
