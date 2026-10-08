// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package views

import (
	"strings"
	"testing"

	"charm.land/bubbles/v2/table"
	"charm.land/lipgloss/v2"
	tktable "github.com/blairham/tuikit/table"
	"github.com/blairham/tuikit/theme"
)

func styledTable() table.Model {
	tbl := table.New(
		table.WithColumns([]table.Column{{Title: "NAME", Width: 12}, {Title: "STATE", Width: 8}}),
		table.WithStyles(tableStylesWithWidth(20)),
		table.WithFocused(true),
	)
	// Sized the way the views size it; an unsized table renders no rows.
	tbl.SetWidth(20)
	tbl.SetHeight(10)
	ok := lipgloss.NewStyle().Foreground(lipgloss.Color("#00FF00")).Render("ok")
	tbl.SetRows([]table.Row{{"orders", ok}, {"fills", ok}, {"quotes", ok}})
	return tbl
}

// fixSelectedRow moved from FixSelectedRow, which assumes the default
// theme's colors, to FixRows, which reads them from pkgTheme. On k4a's
// theme the two must paint identically, or the switch moved the look.
func TestFixSelectedRow_MatchesTheDefaultThemePaint(t *testing.T) {
	view := styledTable().View()
	want := tktable.FixSelectedRow(view, tktable.PaintModeFor(pkgTheme))

	if got := fixSelectedRow(view); got != want {
		t.Fatalf("fixSelectedRow drifted from the default-theme paint\ngot:  %q\nwant: %q", got, want)
	}
	if want == view {
		t.Fatal("the selected row was not repainted; the test table no longer exercises the fix")
	}
}

// The selected row is drawn in the theme's selection text color, which
// the styled cell's own green would otherwise override after its reset.
func TestFixSelectedRow_SelectedRowUsesSelectionText(t *testing.T) {
	selected := strings.Split(fixSelectedRow(styledTable().View()), "\n")
	selBg := strings.TrimSuffix(strings.TrimPrefix(
		theme.BackgroundSeq(pkgTheme.Selection), "\x1b[",
	), "m")

	var line string
	for _, l := range selected {
		if strings.Contains(l, selBg) {
			line = l
		}
	}
	if line == "" {
		t.Fatal("no line carries the selection background")
	}
	if strings.Contains(line, "38;2;0;255;0") {
		t.Fatalf("selected row kept the cell's green foreground: %q", line)
	}
}
