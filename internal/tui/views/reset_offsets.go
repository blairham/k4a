// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package views

import (
	"context"
	"fmt"
	"sort"
	"strconv"

	tea "charm.land/bubbletea/v2"
	"charm.land/huh/v2"

	"github.com/blairham/k4a/internal/kafka"
	"github.com/blairham/k4a/internal/tui/style"
)

// ResetOffsetsResultMsg carries the result of an offset reset attempt.
type ResetOffsetsResultMsg struct {
	Err error
}

// ResetOffsetsTopicsMsg carries the list of topics for a group.
type ResetOffsetsTopicsMsg struct {
	Err    error
	Topics []string
}

type resetOffsetsState int

const (
	resetOffsetsStateLoading resetOffsetsState = iota
	resetOffsetsStateForm
	resetOffsetsStateResetting
	resetOffsetsStateDone
)

// ResetOffsetsView handles interactive consumer group offset reset.
type ResetOffsetsView struct {
	err     error
	client  *kafka.Client
	form    *huh.Form
	result  string
	groupID string
	topic   string
	mode    string
	value   string
	topics  []string
	state   resetOffsetsState
	width   int
	height  int
}

// NewResetOffsetsView creates a new reset offsets view.
func NewResetOffsetsView(client *kafka.Client, groupID string) *ResetOffsetsView {
	return &ResetOffsetsView{
		client:  client,
		groupID: groupID,
		mode:    "earliest",
		state:   resetOffsetsStateLoading,
	}
}

// Init fetches the group's subscribed topics.
func (v *ResetOffsetsView) Init() tea.Cmd {
	groupID := v.groupID
	return func() tea.Msg {
		topics, err := v.client.FetchGroupTopics(context.Background(), groupID)
		return ResetOffsetsTopicsMsg{Topics: topics, Err: err}
	}
}

func (v *ResetOffsetsView) buildForm() {
	topicOptions := make([]huh.Option[string], 0, len(v.topics))
	for _, t := range v.topics {
		topicOptions = append(topicOptions, huh.NewOption(t, t))
	}

	if len(topicOptions) > 0 {
		v.topic = topicOptions[0].Value
	}

	v.form = huh.NewForm(
		huh.NewGroup(
			huh.NewSelect[string]().
				Title("Topic").
				Key("topic").
				Options(topicOptions...).
				Value(&v.topic),
			huh.NewSelect[string]().
				Title("Reset Mode").
				Key("mode").
				Options(
					huh.NewOption("Earliest (beginning)", "earliest"),
					huh.NewOption("Latest (end)", "latest"),
					huh.NewOption("Specific Offset", "offset"),
					huh.NewOption("Shift By N", "shift"),
				).
				Value(&v.mode),
			huh.NewInput().
				Title("Value (for offset/shift)").
				Key("value").
				Placeholder("offset or shift amount").
				Value(&v.value),
		),
	).WithTheme(huh.ThemeFunc(huh.ThemeDracula)).
		WithWidth(60)
}

// Update handles messages.
func (v *ResetOffsetsView) Update(msg tea.Msg) tea.Cmd {
	switch msg := msg.(type) {
	case ResetOffsetsTopicsMsg:
		if msg.Err != nil {
			v.err = msg.Err
			v.state = resetOffsetsStateDone
			v.result = fmt.Sprintf("Error fetching topics: %v", msg.Err)
			return nil
		}
		v.topics = msg.Topics
		sort.Strings(v.topics)
		if len(v.topics) == 0 {
			v.state = resetOffsetsStateDone
			v.result = "No committed offsets found for this group"
			return nil
		}
		v.state = resetOffsetsStateForm
		v.buildForm()
		return v.form.Init()
	case ResetOffsetsResultMsg:
		v.state = resetOffsetsStateDone
		if msg.Err != nil {
			v.err = msg.Err
			v.result = fmt.Sprintf("Error: %v", msg.Err)
		} else {
			v.result = fmt.Sprintf("Offsets for %q on %q reset successfully", v.groupID, v.topic)
		}
		return nil
	}

	if v.state == resetOffsetsStateForm && v.form != nil {
		model, cmd := v.form.Update(msg)
		if f, ok := model.(*huh.Form); ok {
			v.form = f
		}
		if v.form.State == huh.StateCompleted {
			return v.resetOffsets()
		}
		return cmd
	}

	return nil
}

// HandleKeyMsg processes tea.KeyMsg for form interaction.
func (v *ResetOffsetsView) HandleKeyMsg(msg tea.KeyMsg) tea.Cmd {
	if v.state != resetOffsetsStateForm {
		return nil
	}
	return v.Update(msg)
}

// UpdateTable is a no-op for form views.
func (v *ResetOffsetsView) UpdateTable(_ tea.Msg) tea.Cmd { return nil }

// HandleKey is a no-op — form keys are handled via HandleKeyMsg.
func (v *ResetOffsetsView) HandleKey(_ string) (string, string) { return "", "" }

// SetFilter is a no-op for form views.
func (v *ResetOffsetsView) SetFilter(_ string) {}

// Loading returns false — form views track their own state.
func (v *ResetOffsetsView) Loading() bool { return false }

// Refresh is a no-op for form views.
func (v *ResetOffsetsView) Refresh() tea.Cmd { return nil }

// Resize updates dimensions.
func (v *ResetOffsetsView) Resize(width, height int) {
	v.width = width
	v.height = height
	if v.form != nil {
		v.form.WithWidth(width - 4) //nolint:errcheck // builder returns self
	}
}

// Count returns 0 (form view).
func (v *ResetOffsetsView) Count() int { return 0 }

// View renders the view.
func (v *ResetOffsetsView) View() string {
	switch v.state {
	case resetOffsetsStateLoading:
		return "\n  Loading group topics..."
	case resetOffsetsStateForm:
		if v.form != nil {
			return v.form.View()
		}
	case resetOffsetsStateResetting:
		return "\n  Resetting offsets..."
	case resetOffsetsStateDone:
		if v.err != nil {
			return "\n  " + style.Error.Render(v.result) + "\n\n" + style.Muted.Render("  press esc to go back")
		}
		return "\n  " + style.StatusRunning.Render(v.result) + "\n\n" + style.Muted.Render("  press esc to go back")
	}
	return ""
}

func (v *ResetOffsetsView) resetOffsets() tea.Cmd {
	strategy, err := v.buildStrategy()
	if err != nil {
		return func() tea.Msg { return ResetOffsetsResultMsg{Err: err} }
	}

	v.state = resetOffsetsStateResetting
	groupID := v.groupID
	topic := v.topic

	return func() tea.Msg {
		resetErr := v.client.ResetGroupOffsets(context.Background(), groupID, topic, strategy)
		return ResetOffsetsResultMsg{Err: resetErr}
	}
}

func (v *ResetOffsetsView) buildStrategy() (kafka.OffsetResetStrategy, error) {
	switch v.mode {
	case "earliest":
		return kafka.OffsetResetStrategy{Mode: kafka.ResetToEarliest}, nil
	case "latest":
		return kafka.OffsetResetStrategy{Mode: kafka.ResetToLatest}, nil
	case "offset":
		n, err := strconv.ParseInt(v.value, 10, 64)
		if err != nil {
			return kafka.OffsetResetStrategy{}, fmt.Errorf("invalid offset %q: %w", v.value, err)
		}
		return kafka.OffsetResetStrategy{Mode: kafka.ResetToOffset, Value: n}, nil
	case "shift":
		n, err := strconv.ParseInt(v.value, 10, 64)
		if err != nil {
			return kafka.OffsetResetStrategy{}, fmt.Errorf("invalid shift value %q: %w", v.value, err)
		}
		return kafka.OffsetResetStrategy{Mode: kafka.ResetShiftBy, Value: n}, nil
	default:
		return kafka.OffsetResetStrategy{}, fmt.Errorf("unknown mode %q", v.mode)
	}
}
