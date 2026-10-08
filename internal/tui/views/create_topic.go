// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package views

import (
	"context"
	"fmt"
	"strconv"

	tea "charm.land/bubbletea/v2"
	"charm.land/huh/v2"

	"github.com/blairham/k4a/internal/kafka"
	"github.com/blairham/k4a/internal/tui/style"
)

// CreateTopicResultMsg carries the result of a topic creation attempt.
type CreateTopicResultMsg struct {
	Err     error
	Topic   string
	Created bool
}

// createTopicState tracks the create topic view state.
type createTopicState int

const (
	createTopicStateForm createTopicState = iota
	createTopicStateCreating
	createTopicStateDone
)

// CreateTopicView handles interactive topic creation.
type CreateTopicView struct {
	err         error
	client      *kafka.Client
	form        *huh.Form
	result      string
	topic       string
	partitions  string
	replication string
	state       createTopicState
	width       int
	height      int
}

// NewCreateTopicView creates a new create topic view.
func NewCreateTopicView(client *kafka.Client) *CreateTopicView {
	v := &CreateTopicView{
		client:      client,
		partitions:  "3",
		replication: "3",
		state:       createTopicStateForm,
	}

	v.form = huh.NewForm(
		huh.NewGroup(
			huh.NewInput().
				Title("Topic").
				Key("topic").
				Placeholder("topic name").
				Value(&v.topic).
				Validate(huh.ValidateNotEmpty()),
			huh.NewInput().
				Title("Partitions").
				Key("partitions").
				Placeholder("number of partitions").
				Value(&v.partitions),
			huh.NewInput().
				Title("Replication Factor").
				Key("replication").
				Placeholder("replication factor").
				Value(&v.replication),
		),
	).WithTheme(huh.ThemeFunc(huh.ThemeDracula)).
		WithWidth(60)

	return v
}

// Init returns the initial command — starts the huh form.
func (v *CreateTopicView) Init() tea.Cmd {
	return v.form.Init()
}

// Update handles messages.
func (v *CreateTopicView) Update(msg tea.Msg) tea.Cmd {
	if res, ok := msg.(CreateTopicResultMsg); ok {
		v.state = createTopicStateDone
		switch {
		case res.Err != nil:
			v.err = res.Err
			v.result = fmt.Sprintf("Error creating topic %q: %v", res.Topic, res.Err)
		case res.Created:
			v.result = fmt.Sprintf("Topic %q created successfully", res.Topic)
		default:
			v.result = fmt.Sprintf("Topic %q already exists", res.Topic)
		}
		return nil
	}

	// Delegate to huh form when in form state.
	if v.state == createTopicStateForm {
		model, cmd := v.form.Update(msg)
		if f, ok := model.(*huh.Form); ok {
			v.form = f
		}
		if v.form.State == huh.StateCompleted {
			return v.createTopic()
		}
		return cmd
	}

	return nil
}

// HandleKeyMsg processes tea.KeyMsg for form interaction.
func (v *CreateTopicView) HandleKeyMsg(msg tea.KeyMsg) tea.Cmd {
	if v.state != createTopicStateForm {
		return nil
	}
	return v.Update(msg)
}

// UpdateTable is a no-op for form views.
func (v *CreateTopicView) UpdateTable(_ tea.Msg) tea.Cmd { return nil }

// HandleKey is a no-op — form keys are handled via HandleKeyMsg.
func (v *CreateTopicView) HandleKey(_ string) (string, string) { return "", "" }

// SetFilter is a no-op for form views.
func (v *CreateTopicView) SetFilter(_ string) {}

// Count returns 0 for form views.
func (v *CreateTopicView) Count() int { return 0 }

// Loading returns false — form views track their own state.
func (v *CreateTopicView) Loading() bool { return false }

// Refresh is a no-op for form views.
func (v *CreateTopicView) Refresh() tea.Cmd { return nil }

// Resize updates dimensions.
func (v *CreateTopicView) Resize(width, height int) {
	v.width = width
	v.height = height
	v.form.WithWidth(width - 4) //nolint:errcheck // builder returns self
}

// View renders the view.
func (v *CreateTopicView) View() string {
	switch v.state {
	case createTopicStateForm:
		return v.form.View()
	case createTopicStateCreating:
		return "\n  Creating topic..."
	case createTopicStateDone:
		if v.err != nil {
			return "\n  " + style.Error.Render(v.result) + "\n\n" + style.Muted.Render("  press esc to go back")
		}
		return "\n  " + style.StatusRunning.Render(v.result) + "\n\n" + style.Muted.Render("  press esc to go back")
	}
	return ""
}

func (v *CreateTopicView) createTopic() tea.Cmd {
	if v.topic == "" {
		return nil
	}

	partitions, err := strconv.Atoi(v.partitions)
	if err != nil || partitions <= 0 {
		partitions = 3
	}

	replication, err := strconv.Atoi(v.replication)
	if err != nil || replication <= 0 {
		replication = 3
	}

	v.state = createTopicStateCreating

	cfg := kafka.CreateTopicConfig{
		Topic:             v.topic,
		NumPartitions:     partitions,
		ReplicationFactor: replication,
	}

	return func() tea.Msg {
		created, createErr := v.client.CreateTopic(context.Background(), cfg)
		return CreateTopicResultMsg{Topic: v.topic, Created: created, Err: createErr}
	}
}
