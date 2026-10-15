// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package tui

import (
	"context"
	"fmt"

	tea "charm.land/bubbletea/v2"

	"github.com/blairham/k4a/internal/kafka"
	"github.com/blairham/k4a/internal/tui/style"
	"github.com/blairham/k4a/internal/tui/views"
)

// openACLEdit opens the ACL form on the binding key names.
func (a *App) openACLEdit(key string) (tea.Model, tea.Cmd) {
	lv := typedView[*views.ACLsView](a, style.ViewACLs)
	if lv == nil {
		return a, nil
	}
	entry := lv.SelectedACL(key)
	if entry == nil {
		return a, nil
	}
	a.setView(style.ViewCreateACL, views.NewEditACLView(a.client, *entry))
	return a.openView(style.ViewCreateACL, a.viewMap[style.ViewCreateACL].Init())
}

// confirmACLEdit asks before replacing a binding: the edit deletes one ACL
// and creates another, and either half can change who may do what.
func (a *App) confirmACLEdit(msg views.EditACLSubmitMsg) {
	fv := typedView[*views.CreateACLView](a, style.ViewCreateACL)
	if fv == nil {
		return
	}
	if kafka.ACLEntry(msg.New) == msg.Old {
		fv.CancelEdit()
		a.errFlash = "nothing changed: the ACL is the same as before"
		return
	}
	a.confirmDispatch = func(yes bool) (string, tea.Cmd) {
		if !yes {
			fv.CancelEdit()
			return "", nil
		}
		if a.readonly {
			fv.CancelEdit()
			return msgReadonly, nil
		}
		fv.Replacing()
		return "", a.doReplaceACL(msg.Old, msg.New)
	}
	a.confirm.Open(fmt.Sprintf("replace ACL %s with %s?",
		views.ACLLabel(msg.Old), views.ACLLabel(kafka.ACLEntry(msg.New))))
	a.resizeActiveView()
}

// doReplaceACL creates next and then deletes old (kafka.Client.ReplaceACL),
// reporting to the form as a create would.
func (a *App) doReplaceACL(old kafka.ACLEntry, next kafka.CreateACLConfig) tea.Cmd {
	client := a.client
	return func() tea.Msg {
		return views.CreateACLResultMsg{Err: client.ReplaceACL(context.Background(), old, next)}
	}
}
