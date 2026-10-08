// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package views

import (
	"charm.land/bubbles/v2/table"
	tktable "github.com/blairham/tuikit/table"
	"github.com/blairham/tuikit/theme"

	"github.com/blairham/k4a/internal/tui/style"
)

// pkgTheme is k4a's theme. NoPaintBackground means cell padding stays on
// the terminal default — the table's Selected.Background overrides cleanly
// per-row without fighting a forced canvas color.
var pkgTheme = theme.NoPaintBackground()

// keyCtrlD is the bubbletea key string for ctrl+d (delete drill-in target,
// purge messages, etc.). Re-exported for view code.
const keyCtrlD = "ctrl+d"

// rowFilter is a regex-based filter with leading-`!` negation and
// invalid-regex literal fallback. Re-exported from tuikit/table.
//
// View code calls f.Empty() and f.MatchesAny(...string).
type rowFilter = tktable.RowFilter

// parseFilter compiles a filter expression.
func parseFilter(s string) rowFilter { return tktable.ParseFilter(s) }

// tableKeyMap returns the bubbles table keymap with j/k stripped.
func tableKeyMap() table.KeyMap { return tktable.KeyMap() }

// tableStyles returns themed bubbles table styles.
func tableStyles() table.Styles { return tktable.Styles(pkgTheme) }

// tableStylesWithWidth pads Selected to the full table width so the
// highlight stretches edge-to-edge.
func tableStylesWithWidth(width int) table.Styles {
	return tktable.StylesWithWidth(pkgTheme, width)
}

// fixSelectedRow post-processes bubbles table output to repaint selected
// rows in pkgTheme's selection colors. With theme.NoPaintBackground the
// paint mode is None so non-selected rows pass through unchanged.
func fixSelectedRow(view string) string {
	return tktable.FixRows(view, pkgTheme)
}

// truncate shortens s to maxLen terminal cells, appending "…" if it was
// cut. Wide runes count as two cells and ANSI sequences survive the cut.
func truncate(s string, maxLen int) string { return tktable.Truncate(s, maxLen) }

// Keep the style import live; many views reach palette colors through
// this file's import chain rather than re-importing themselves.
var _ = style.ColorBg
