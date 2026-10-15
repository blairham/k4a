// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package views

import (
	"context"
	"fmt"
	"slices"

	tea "charm.land/bubbletea/v2"
	"charm.land/huh/v2"

	"github.com/blairham/k4a/internal/kafka"
	"github.com/blairham/k4a/internal/tui/style"
)

// CreateACLResultMsg carries the result of an ACL creation (or, in edit
// mode, replacement) attempt.
type CreateACLResultMsg struct {
	Err error
}

// EditACLSubmitMsg asks the App to confirm replacing Old with New. An edit
// is a delete and a create, so it is never sent without a y/n.
type EditACLSubmitMsg struct {
	Old kafka.ACLEntry
	New kafka.CreateACLConfig
}

// createACLState tracks the create ACL view state.
type createACLState int

const (
	createACLStateForm createACLState = iota
	createACLStateConfirm
	createACLStateCreating
	createACLStateDone
)

// CreateACLView handles interactive ACL creation.
type CreateACLView struct {
	err          error
	client       *kafka.Client
	editing      *kafka.ACLEntry // the binding being replaced; nil when creating
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
	v.buildForm()
	return v
}

// NewEditACLView opens the form on an existing binding. Submitting it asks
// the App (EditACLSubmitMsg) to confirm replacing the binding.
func NewEditACLView(client *kafka.Client, entry kafka.ACLEntry) *CreateACLView {
	v := &CreateACLView{
		client:       client,
		editing:      &entry,
		resourceType: entry.ResourceType,
		resourceName: entry.ResourceName,
		patternType:  entry.PatternType,
		principal:    entry.Principal,
		host:         entry.Host,
		operation:    entry.Operation,
		permission:   entry.Permission,
		state:        createACLStateForm,
	}
	v.buildForm()
	return v
}

// stringOptions offers values, plus current when it is not among them, so
// an edited binding with an operation the form does not list keeps it.
func stringOptions(current string, values ...string) []huh.Option[string] {
	if current != "" && !slices.Contains(values, current) {
		values = append(values, current)
	}
	return huh.NewOptions(values...)
}

func (v *CreateACLView) buildForm() {
	v.form = huh.NewForm(
		huh.NewGroup(
			huh.NewSelect[string]().
				Title("Resource Type").
				Key("resource_type").
				Options(stringOptions(v.resourceType, "topic", "group", "cluster", "transactionalid")...).
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
				Options(stringOptions(v.patternType, "literal", "prefixed")...).
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
				Options(stringOptions(v.operation,
					"all", "read", "write", "create", "delete", "alter", "describe")...).
				Value(&v.operation),
			huh.NewSelect[string]().
				Title("Permission").
				Key("permission").
				Options(stringOptions(v.permission, "allow", "deny")...).
				Value(&v.permission),
		),
	).WithTheme(huh.ThemeFunc(huh.ThemeDracula)).
		WithWidth(60)
}

// Init returns the initial command — starts the huh form.
func (v *CreateACLView) Init() tea.Cmd {
	return v.form.Init()
}

// Update handles messages.
func (v *CreateACLView) Update(msg tea.Msg) tea.Cmd {
	if res, ok := msg.(CreateACLResultMsg); ok {
		v.state = createACLStateDone
		verb, doing := "created", "creating"
		if v.editing != nil {
			verb, doing = "replaced", "replacing"
		}
		if res.Err != nil {
			v.err = res.Err
			v.result = fmt.Sprintf("Error %s ACL: %v", doing, res.Err)
		} else {
			v.result = fmt.Sprintf("ACL %s: %s", verb, ACLLabel(kafka.ACLEntry(v.config())))
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
			if v.editing != nil {
				return v.submitEdit()
			}
			return v.createACL()
		}
		return cmd
	}

	return nil
}

// Editing reports whether the form is still taking input.
func (v *CreateACLView) Editing() bool { return v.state == createACLStateForm }

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
		if v.editing != nil {
			return "\n  " + style.Muted.Render("editing "+ACLLabel(*v.editing)) + "\n\n" + v.form.View()
		}
		return v.form.View()
	case createACLStateConfirm:
		return "\n  " + style.Muted.Render("confirm the replacement below")
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

	cfg := v.config()
	return func() tea.Msg {
		err := v.client.CreateACL(context.Background(), cfg)
		return CreateACLResultMsg{Err: err}
	}
}

// config is the binding the form describes.
func (v *CreateACLView) config() kafka.CreateACLConfig {
	return kafka.CreateACLConfig{
		ResourceType: v.resourceType,
		ResourceName: v.resourceName,
		PatternType:  v.patternType,
		Principal:    v.principal,
		Host:         v.host,
		Operation:    v.operation,
		Permission:   v.permission,
	}
}

// submitEdit hands the edit to the App for confirmation.
func (v *CreateACLView) submitEdit() tea.Cmd {
	if v.resourceName == "" || v.principal == "" {
		return nil
	}
	v.state = createACLStateConfirm
	msg := EditACLSubmitMsg{Old: *v.editing, New: v.config()}
	return func() tea.Msg { return msg }
}

// Replacing marks a confirmed edit as in flight.
func (v *CreateACLView) Replacing() { v.state = createACLStateCreating }

// CancelEdit records a declined replacement; nothing was sent.
func (v *CreateACLView) CancelEdit() {
	v.state = createACLStateDone
	v.result = "edit canceled; the ACL is unchanged"
}

// ACLLabel is a one-line description of a binding for prompts and results.
func ACLLabel(e kafka.ACLEntry) string {
	host := ""
	if e.Host != "" && e.Host != "*" {
		host = " from " + e.Host
	}
	return fmt.Sprintf("%s %s on %s:%s (%s) for %s%s",
		e.Permission, e.Operation, e.ResourceType, e.ResourceName, e.PatternType, e.Principal, host)
}
