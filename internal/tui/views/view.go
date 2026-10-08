// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package views

import tea "charm.land/bubbletea/v2"

// Compile-time interface checks.
var (
	_ View     = (*TopicsView)(nil)
	_ View     = (*TopicDetailView)(nil)
	_ View     = (*TopicConfigView)(nil)
	_ View     = (*GroupsView)(nil)
	_ View     = (*GroupDetailView)(nil)
	_ View     = (*ClusterView)(nil)
	_ View     = (*BrokerDetailView)(nil)
	_ View     = (*MessagesView)(nil)
	_ View     = (*MessageDetailView)(nil)
	_ View     = (*ACLsView)(nil)
	_ View     = (*ContextView)(nil)
	_ View     = (*SearchView)(nil)
	_ FormView = (*ProduceView)(nil)
	_ FormView = (*CreateTopicView)(nil)
	_ FormView = (*CreateACLView)(nil)
	_ FormView = (*ResetOffsetsView)(nil)
)

// View is the interface that all TUI views implement.
type View interface {
	Init() tea.Cmd
	Update(tea.Msg) tea.Cmd
	UpdateTable(tea.Msg) tea.Cmd
	View() string
	Resize(width, height int)
	HandleKey(key string) (action, param string)
	SetFilter(string)
	Count() int
	Loading() bool
	Refresh() tea.Cmd
}

// FormView is an optional interface for views that contain huh forms.
// Form views intercept key events before standard table navigation.
type FormView interface {
	View
	HandleKeyMsg(tea.KeyMsg) tea.Cmd
}

// Stoppable is an optional interface for views that manage background
// goroutines and need explicit cleanup.
type Stoppable interface {
	Stop()
}
