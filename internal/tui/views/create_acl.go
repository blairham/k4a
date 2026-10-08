// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package views

import (
	"context"
	"fmt"

	tea "charm.land/bubbletea/v2"
	"charm.land/huh/v2"

	"github.com/blairham/k4a/internal/kafka"
	"github.com/blairham/k4a/internal/tui/style"
)

// CreateACLResultMsg carries the result of an ACL creation attempt.
type CreateACLResultMsg struct {
	Err error
}

// createACLState tracks the create ACL view state.
type createACLState int

const (
	createACLStateForm createACLState = iota
	createACLStateCreating
	createACLStateDone
)

// CreateACLView handles interactive ACL creation.
type CreateACLView struct {
	err          error
	client       *kafka.Client
	form         *huh.Form
	result       string
	resourceType string
	resourceName string
	patternType  string
	principal    string
	host         string
	operation    string
	permission   string
	state        createACLState
	width        int
	height       int
}

// NewCreateACLView creates a new create ACL view.
func NewCreateACLView(client *kafka.Client) *CreateACLView {
	v := &CreateACLView{
		client:       client,
		resourceType: "topic",
		patternType:  "literal",
		host:         "*",
		operation:    "read",
		permission:   "allow",
		state:        createACLStateForm,
	}

	v.form = huh.NewForm(
		huh.NewGroup(
			huh.NewSelect[string]().
				Title("Resource Type").
				Key("resource_type").
				Options(
					huh.NewOption("topic", "topic"),
					huh.NewOption("group", "group"),
					huh.NewOption("cluster", "cluster"),
					huh.NewOption("transactionalid", "transactionalid"),
				).
				Value(&v.resourceType),
			huh.NewInput().
				Title("Resource Name").
				Key("resource_name").
				Placeholder("resource name").
				Value(&v.resourceName).
				Validate(huh.ValidateNotEmpty()),
			huh.NewSelect[string]().
				Title("Pattern Type").
				Key("pattern_type").
				Options(
					huh.NewOption("literal", "literal"),
					huh.NewOption("prefixed", "prefixed"),
				).
				Value(&v.patternType),
			huh.NewInput().
				Title("Principal").
				Key("principal").
				Placeholder("User:alice").
				Value(&v.principal).
				Validate(huh.ValidateNotEmpty()),
			huh.NewInput().
				Title("Host").
				Key("host").
				Placeholder("* for all hosts").
				Value(&v.host),
			huh.NewSelect[string]().
				Title("Operation").
				Key("operation").
				Options(
					huh.NewOption("all", "all"),
					huh.NewOption("read", "read"),
					huh.NewOption("write", "write"),
					huh.NewOption("create", "create"),
					huh.NewOption("delete", "delete"),
					huh.NewOption("alter", "alter"),
					huh.NewOption("describe", "describe"),
				).
				Value(&v.operation),
			huh.NewSelect[string]().
				Title("Permission").
				Key("permission").
				Options(
					huh.NewOption("allow", "allow"),
					huh.NewOption("deny", "deny"),
				).
				Value(&v.permission),
		),
	).WithTheme(huh.ThemeFunc(huh.ThemeDracula)).
		WithWidth(60)

	return v
}

// Init returns the initial command — starts the huh form.
func (v *CreateACLView) Init() tea.Cmd {
	return v.form.Init()
}

// Update handles messages.
func (v *CreateACLView) Update(msg tea.Msg) tea.Cmd {
	if res, ok := msg.(CreateACLResultMsg); ok {
		v.state = createACLStateDone
		if res.Err != nil {
			v.err = res.Err
			v.result = fmt.Sprintf("Error creating ACL: %v", res.Err)
		} else {
			v.result = fmt.Sprintf("ACL created: %s %s on %s:%s for %s",
				v.permission, v.operation, v.resourceType, v.resourceName, v.principal)
		}
		return nil
	}

	// Delegate to huh form when in form state.
	if v.state == createACLStateForm {
		model, cmd := v.form.Update(msg)
		if f, ok := model.(*huh.Form); ok {
			v.form = f
		}
		if v.form.State == huh.StateCompleted {
			return v.createACL()
		}
		return cmd
	}

	return nil
}

// HandleKeyMsg processes tea.KeyMsg for form interaction.
func (v *CreateACLView) HandleKeyMsg(msg tea.KeyMsg) tea.Cmd {
	if v.state != createACLStateForm {
		return nil
	}
	return v.Update(msg)
}

// UpdateTable is a no-op for form views.
func (v *CreateACLView) UpdateTable(_ tea.Msg) tea.Cmd { return nil }

// HandleKey is a no-op — form keys are handled via HandleKeyMsg.
func (v *CreateACLView) HandleKey(_ string) (string, string) { return "", "" }

// SetFilter is a no-op for form views.
func (v *CreateACLView) SetFilter(_ string) {}

// Count returns 0 for form views.
func (v *CreateACLView) Count() int { return 0 }

// Loading returns false — form views track their own state.
func (v *CreateACLView) Loading() bool { return false }

// Refresh is a no-op for form views.
func (v *CreateACLView) Refresh() tea.Cmd { return nil }

// Resize updates dimensions.
func (v *CreateACLView) Resize(width, height int) {
	v.width = width
	v.height = height
	v.form.WithWidth(width - 4) //nolint:errcheck // builder returns self
}

// View renders the view.
func (v *CreateACLView) View() string {
	switch v.state {
	case createACLStateForm:
		return v.form.View()
	case createACLStateCreating:
		return "\n  Creating ACL..."
	case createACLStateDone:
		if v.err != nil {
			return "\n  " + style.Error.Render(v.result) + "\n\n" + style.Muted.Render("  press esc to go back")
		}
		return "\n  " + style.StatusRunning.Render(v.result) + "\n\n" + style.Muted.Render("  press esc to go back")
	}
	return ""
}

func (v *CreateACLView) createACL() tea.Cmd {
	if v.resourceName == "" || v.principal == "" {
		return nil
	}

	v.state = createACLStateCreating

	cfg := kafka.CreateACLConfig{
		ResourceType: v.resourceType,
		ResourceName: v.resourceName,
		PatternType:  v.patternType,
		Principal:    v.principal,
		Host:         v.host,
		Operation:    v.operation,
		Permission:   v.permission,
	}

	return func() tea.Msg {
		err := v.client.CreateACL(context.Background(), cfg)
		return CreateACLResultMsg{Err: err}
	}
}
