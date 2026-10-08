// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

// Package style provides shared TUI styles and constants. It's a thin
// compatibility shim over [github.com/blairham/tuikit/theme] — colors
// and pre-built styles flow from a [theme.NoPaintBackground] base so
// k4a's table rendering can override per-cell foregrounds without
// fighting a forced black background.
//
// The [ViewType] enum and its [ViewName] / [ViewResource] helpers stay
// here because they're referenced throughout k4a's view code (dispatch,
// keymap, renderers). They're not in tuikit because they're app-specific.
package style

import (
	"image/color"

	"charm.land/lipgloss/v2"
	"github.com/blairham/tuikit/theme"
)

// Default theme everything below derives from. k4a uses
// NoPaintBackground because its table styling relies on
// Selected.Foreground overrides; explicit cell backgrounds would
// conflict.
var t = theme.NoPaintBackground()

// Color exports — referenced by every view file.
var (
	ColorBg = t.Bg

	ColorAqua         color.Color = t.Accent
	ColorCadetBlue    color.Color = lipgloss.Color("#5F9EA0")
	ColorWhite                    = t.Value
	ColorCyan                     = t.Accent
	ColorSteelBlue    color.Color = t.Selection
	ColorGreen        color.Color = t.Status.OK
	ColorYellow       color.Color = t.Filter
	ColorOrange       color.Color = t.Logo
	ColorRed          color.Color = t.Status.Error
	ColorLightSkyBlue color.Color = t.Selection
	ColorOrangeRed    color.Color = lipgloss.Color("#FF4500")
	ColorDarkOrange   color.Color = lipgloss.Color("#FF8C00")
	ColorGreenYellow  color.Color = lipgloss.Color("#ADFF2F")
	ColorMediumPurple color.Color = lipgloss.Color("#9370DB")
	ColorSlateGray    color.Color = lipgloss.Color("#778899")
	ColorSelection                = t.Selection
	ColorFuchsia                  = t.AccentAlt
	ColorPapayaWhip               = t.AccentBold
	ColorSeaGreen     color.Color = lipgloss.Color("#2E8B57")
	ColorGray                     = t.Muted
	ColorDarkGray                 = t.BreadcrumbBg
	ColorDodgerBlue               = t.Border
	ColorBlue                     = t.Border
	ColorBorder                   = t.Border
)

// Info panel styles (top-left).
var (
	InfoLabel = lipgloss.NewStyle().Foreground(ColorOrange)
	InfoValue = lipgloss.NewStyle().Foreground(ColorWhite).Bold(true)
)

// Shortcut panel styles.
var (
	ShortcutKey  = lipgloss.NewStyle().Foreground(ColorDodgerBlue).Bold(true)
	ShortcutDesc = lipgloss.NewStyle().Foreground(ColorGray)
)

// Logo style.
var Logo = lipgloss.NewStyle().Foreground(ColorOrange).Bold(true)

// ResourceBar styles the centered resource count label.
var ResourceBar = lipgloss.NewStyle().
	Foreground(ColorCyan).
	Bold(true).
	Align(lipgloss.Center)

// Title for detail views.
var Title = lipgloss.NewStyle().Foreground(ColorCyan).Bold(true)

// Error / Success / Muted.
var (
	Error   = lipgloss.NewStyle().Foreground(ColorRed).Bold(true)
	Success = lipgloss.NewStyle().Foreground(ColorGreen).Bold(true)
	Muted   = lipgloss.NewStyle().Foreground(ColorGray)
)

// Footer / bottom prompt.
var (
	Footer = lipgloss.NewStyle().Foreground(ColorGray)
	Prompt = lipgloss.NewStyle().
		Background(ColorOrange).
		Foreground(ColorBg).
		Bold(true).
		Padding(0, 1)
	Filter = lipgloss.NewStyle().Foreground(ColorYellow).Bold(true)
)

// Status dot styles.
var (
	DotGreen  = lipgloss.NewStyle().Foreground(ColorGreen)
	DotRed    = lipgloss.NewStyle().Foreground(ColorRed)
	DotYellow = lipgloss.NewStyle().Foreground(ColorYellow)
	DotGray   = lipgloss.NewStyle().Foreground(ColorGray)
	DotBlue   = lipgloss.NewStyle().Foreground(ColorBlue)
)

// Status text styles — for colorizing values like "Running", "Stable", etc.
var (
	StatusRunning = lipgloss.NewStyle().Foreground(ColorGreen)
	StatusError   = lipgloss.NewStyle().Foreground(ColorRed)
	StatusWarn    = lipgloss.NewStyle().Foreground(ColorYellow)
	StatusInfo    = lipgloss.NewStyle().Foreground(ColorBlue)
)

// Status for health indicators.
var Status = lipgloss.NewStyle().Foreground(ColorGreen)

// TableBorder returns a lipgloss border style for the table section.
var TableBorder = lipgloss.NewStyle().
	BorderStyle(lipgloss.RoundedBorder()).
	BorderForeground(ColorBorder)

// ViewType identifies which view is active.
type ViewType int

// View type constants.
const (
	ViewTopics ViewType = iota
	ViewTopicDetail
	ViewGroups
	ViewGroupDetail
	ViewCluster
	ViewMessages
	ViewMessageDetail
	ViewProduce
	ViewCreateTopic
	ViewCreateACL
	ViewContext
	ViewACLs
	ViewTopicConfig
	ViewResetOffsets
	ViewBrokerDetail
	ViewSearch
)

// viewInfo is each view's display name and the resource tag its bottom
// prompt shows.
var viewInfo = map[ViewType]struct{ name, resource string }{
	ViewTopics:        {"Topics", "topic"},
	ViewTopicDetail:   {"Topic Detail", "partition"},
	ViewGroups:        {"Groups", "group"},
	ViewGroupDetail:   {"Group Detail", "offset"},
	ViewCluster:       {"Cluster", "broker"},
	ViewMessages:      {"Messages", "message"},
	ViewMessageDetail: {"Message", "message"},
	ViewProduce:       {"Produce", "produce"},
	ViewCreateTopic:   {"Create Topic", "topic"},
	ViewCreateACL:     {"Create ACL", "acl"},
	ViewContext:       {"Contexts", "context"},
	ViewACLs:          {"ACLs", "acl"},
	ViewTopicConfig:   {"Topic Config", "config"},
	ViewResetOffsets:  {"Reset Offsets", "offset"},
	ViewBrokerDetail:  {"Broker Detail", "config"},
	ViewSearch:        {"Search", "match"},
}

// ViewName returns the display name for a view.
func ViewName(v ViewType) string {
	if info, ok := viewInfo[v]; ok {
		return info.name
	}
	return "Unknown"
}

// ViewResource returns the resource tag for the bottom prompt.
func ViewResource(v ViewType) string {
	return viewInfo[v].resource
}
