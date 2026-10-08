// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package views

import (
	"encoding/json"
	"fmt"
	"strings"

	"charm.land/bubbles/v2/viewport"
	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	"github.com/charmbracelet/glamour"

	"github.com/blairham/k4a/internal/kafka"
	"github.com/blairham/k4a/internal/tui/style"
)

// MessageDetailView shows the full raw content of a single message.
type MessageDetailView struct {
	msg      *kafka.ConsumedMessage
	viewport viewport.Model
	width    int
	height   int
	ready    bool
}

// NewMessageDetailView creates a new message detail view.
func NewMessageDetailView(msg *kafka.ConsumedMessage) *MessageDetailView {
	return &MessageDetailView{msg: msg}
}

// Init returns nil.
func (v *MessageDetailView) Init() tea.Cmd { return nil }

// Update is a no-op.
func (v *MessageDetailView) Update(_ tea.Msg) tea.Cmd { return nil }

// UpdateTable delegates scroll events to the viewport (value box only).
func (v *MessageDetailView) UpdateTable(msg tea.Msg) tea.Cmd {
	var cmd tea.Cmd
	v.viewport, cmd = v.viewport.Update(msg)
	return cmd
}

// Resize sets dimensions. The viewport only gets the space below the sticky cards.
func (v *MessageDetailView) Resize(width, height int) {
	v.width = width
	v.height = height

	// Viewport width is inside the value border box: borders(2) + padding(2).
	vpWidth := width - 4
	if vpWidth < 20 {
		vpWidth = 20
	}
	// Measure the actual card height so the viewport fills the remaining space.
	// Value box overhead: newline(1) + bottom margin(1).
	cardsH := lipgloss.Height(v.renderCards())
	vpHeight := height - cardsH - 2
	if vpHeight < 3 {
		vpHeight = 3
	}

	if !v.ready {
		v.viewport = viewport.New(viewport.WithWidth(vpWidth), viewport.WithHeight(vpHeight))
		v.ready = true
	} else {
		v.viewport.SetWidth(vpWidth)
		v.viewport.SetHeight(vpHeight)
	}
	v.viewport.SetContent(v.renderValue())
}

// Count returns 1.
func (v *MessageDetailView) Count() int { return 1 }

// HandleKey processes view-specific keys.
func (v *MessageDetailView) HandleKey(key string) (string, string) {
	if key == "s" {
		return "save_message", ""
	}
	return "", ""
}

// GetMessage returns the displayed message.
func (v *MessageDetailView) GetMessage() *kafka.ConsumedMessage { return v.msg }

// Loading returns false — message detail has no loading state.
func (v *MessageDetailView) Loading() bool { return false }

// Refresh is a no-op for message detail.
func (v *MessageDetailView) Refresh() tea.Cmd { return nil }

// SetFilter is a no-op.
func (v *MessageDetailView) SetFilter(_ string) {}

// View renders the sticky cards + scrollable value in a bordered box.
func (v *MessageDetailView) View() string {
	if !v.ready || v.msg == nil {
		return "Loading..."
	}

	cards := v.renderCards()
	cardsH := lipgloss.Height(cards)
	valueBoxHeight := v.height - cardsH
	valueBox := lipgloss.NewStyle().
		Width(v.width).
		Height(valueBoxHeight).
		BorderStyle(lipgloss.RoundedBorder()).
		BorderForeground(style.ColorBorder).
		Padding(0, 1).
		Render(v.viewport.View())

	return cards + "\n" + valueBox
}

// renderCards renders the fixed header: metadata + key (left) | headers (right).
func (v *MessageDetailView) renderCards() string {
	m := v.msg
	label := lipgloss.NewStyle().Foreground(style.ColorCyan).Bold(true)
	val := lipgloss.NewStyle().Foreground(style.ColorWhite).Bold(true)
	dim := lipgloss.NewStyle().Foreground(style.ColorGray)
	borderColor := style.ColorBorder

	cardWidth := v.width

	// Inner width accounts for the outer border box: border(2) + padding(2) + separator(1).
	innerWidth := cardWidth - 5
	if innerWidth < 20 {
		innerWidth = 20
	}
	halfWidth := innerWidth / 2
	rightWidth := innerWidth - halfWidth

	const labelW = 11 // "Topic:     " etc.
	leftValW := halfWidth - labelW
	if leftValW < 5 {
		leftValW = 5
	}

	// Left: metadata + key.
	var left strings.Builder
	left.WriteString(label.Render("Topic:     ") + val.Render(truncate(m.Topic, leftValW)) + "\n")
	left.WriteString(label.Render("Partition: ") + val.Render(fmt.Sprintf("%d", m.Partition)) + "\n")
	left.WriteString(label.Render("Offset:    ") + val.Render(fmt.Sprintf("%d", m.Offset)) + "\n")
	left.WriteString(label.Render("Timestamp: ") + val.Render(m.Time.Format("1/2/2006, 15:04:05.000")) + "\n")
	left.WriteString(label.Render("Key:       ") + val.Render(truncate(keyDisplay(m.Key), leftValW)) + "\n")
	left.WriteString(label.Render("Val Size:  ") + val.Render(formatBytes(int64(len(m.Value)))))

	// Right: headers.
	var right strings.Builder
	right.WriteString(label.Render("Headers") + "\n")
	if len(m.Headers) == 0 {
		right.WriteString(dim.Render("(none)"))
	} else {
		for i, h := range m.Headers {
			hLabel := h.Key + ": "
			hValW := rightWidth - len(hLabel)
			if hValW < 5 {
				hValW = 5
			}
			right.WriteString(label.Render(hLabel) + val.Render(truncate(h.Value, hValW)))
			if i < len(m.Headers)-1 {
				right.WriteString("\n")
			}
		}
	}

	leftBlock := lipgloss.NewStyle().Width(halfWidth).MaxWidth(halfWidth).Render(left.String())
	rightBlock := lipgloss.NewStyle().Width(rightWidth).MaxWidth(rightWidth).Render(right.String())

	// Build a separator column that spans the full card height.
	leftHeight := lipgloss.Height(leftBlock)
	rightHeight := lipgloss.Height(rightBlock)
	sepHeight := leftHeight
	if rightHeight > sepHeight {
		sepHeight = rightHeight
	}
	sepStyle := lipgloss.NewStyle().Foreground(borderColor)
	sepLines := make([]string, 0, sepHeight)
	for range sepHeight {
		sepLines = append(sepLines, sepStyle.Render("│"))
	}
	sep := strings.Join(sepLines, "\n")

	cardContent := lipgloss.JoinHorizontal(lipgloss.Top, leftBlock, sep, rightBlock)
	return lipgloss.NewStyle().
		Width(cardWidth).
		BorderStyle(lipgloss.RoundedBorder()).
		BorderForeground(borderColor).
		Padding(0, 1).
		Render(cardContent)
}

// renderValue renders the scrollable message value with syntax-highlighted JSON.
func (v *MessageDetailView) renderValue() string {
	if v.msg == nil || v.msg.Value == "" {
		return lipgloss.NewStyle().Foreground(style.ColorGray).Render("(empty)")
	}
	// Use viewport width for word wrap, accounting for border(2) + padding(2) + margin.
	width := v.width - 6
	if width < 40 {
		width = 40
	}
	return prettyJSON(v.msg.Value, width)
}

func keyDisplay(k string) string {
	if k == "" {
		return lipgloss.NewStyle().Foreground(style.ColorGray).Render("(null)")
	}
	return k
}

// prettyJSON formats and syntax-highlights JSON using glamour's markdown renderer.
func prettyJSON(raw string, width int) string {
	var obj any
	if err := json.Unmarshal([]byte(raw), &obj); err != nil {
		return raw
	}
	pretty, err := json.MarshalIndent(obj, "", "  ")
	if err != nil {
		return raw
	}

	// Wrap in a JSON fenced code block for glamour to syntax-highlight.
	md := "```json\n" + string(pretty) + "\n```"

	renderer, err := glamour.NewTermRenderer(
		glamour.WithStandardStyle("dracula"),
		glamour.WithWordWrap(width),
	)
	if err != nil {
		return string(pretty)
	}

	rendered, err := renderer.Render(md)
	if err != nil {
		return string(pretty)
	}

	// Trim leading/trailing whitespace glamour adds.
	return strings.TrimSpace(rendered)
}

func formatBytes(b int64) string {
	switch {
	case b >= 1024*1024:
		return fmt.Sprintf("%.1f MB", float64(b)/(1024*1024))
	case b >= 1024:
		return fmt.Sprintf("%.1f KB", float64(b)/1024)
	default:
		return fmt.Sprintf("%d bytes", b)
	}
}
