// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package tui

import (
	"fmt"
	"strings"

	tea "charm.land/bubbletea/v2"
	"github.com/blairham/tuikit/chrome"

	"github.com/blairham/k4a/internal/tui/style"
	"github.com/blairham/k4a/internal/tui/views"
	"github.com/blairham/k4a/internal/upgrade"
)

// knownCommands is the list of commands for fuzzy matching in command mode.
var knownCommands = []string{
	"q!", "quit", "exit",
	"topics", "groups", "cluster", "acls",
	"produce", "create-topic", "ct", "create-acl", "acl",
	"context", "ctx", "reconnect", "reset",
	"search",
	"upgrade",
	"logo", "logoless",
}

// fuzzyMatch finds the best matching command for the given input.
// It checks if the input characters appear as a subsequence in each command,
// preferring prefix matches and shorter commands.
func fuzzyMatch(input string) string {
	if input == "" {
		return ""
	}
	lower := strings.ToLower(input)

	// Prefer exact prefix matches first.
	var best string
	for _, cmd := range knownCommands {
		if strings.HasPrefix(cmd, lower) {
			if best == "" || len(cmd) < len(best) {
				best = cmd
			}
		}
	}
	if best != "" {
		return best
	}

	// Fall back to subsequence matching.
	for _, cmd := range knownCommands {
		if isSubsequence(lower, cmd) {
			if best == "" || len(cmd) < len(best) {
				best = cmd
			}
		}
	}
	return best
}

// isSubsequence checks if all characters in needle appear in order in haystack.
func isSubsequence(needle, haystack string) bool {
	hi := 0
	for _, c := range needle {
		found := false
		for hi < len(haystack) {
			if rune(haystack[hi]) == c {
				hi++
				found = true
				break
			}
			hi++
		}
		if !found {
			return false
		}
	}
	return true
}

// handleHelpKey processes a keypress while the help overlay is open.
// `esc` / `?` dismiss the overlay; `:` / `/` open the command or
// filter bar *on top of* the help overlay so users can issue a
// command without losing their place in the help. Help dismisses
// later when the command triggers a view switch (see switchView).
// All other keys are swallowed.
func (a *App) handleHelpKey(key string) (tea.Model, tea.Cmd) {
	switch key {
	case "esc", "?":
		a.showHelp = false
		return a, nil
	case ":":
		cmd := a.commandBar.Open()
		a.resizeActiveView()
		return a, cmd
	case "/":
		cmd := a.filterBar.OpenWith(a.filter)
		a.resizeActiveView()
		return a, cmd
	}
	return a, nil
}

func (a *App) handleKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	key := msg.String()

	if key == "ctrl+c" {
		a.shutdown()
		return a, tea.Quit
	}

	// Clear any flash on keypress.
	a.errFlash = ""
	a.flash = ""

	if cmd, handled := a.dispatchToBar(msg); handled {
		return a, cmd
	}

	if a.showHelp {
		return a.handleHelpKey(key)
	}

	if cmd, handled := a.handleGlobalKey(key); handled {
		return a, cmd
	}

	// Check view-specific actions first (enter, m, i, etc.).
	action, param := a.activeViewHandleKey(key)
	if action != "" {
		return a.handleAction(action, param)
	}

	// Delegate remaining keys (j/k/g/G/pgup/pgdown) to the bubbles table.
	cmd := a.updateActiveTable(msg)
	return a, cmd
}

// dispatchToBar routes the key to whichever bar is active. Each widget
// reports handled=true when it consumed the key, including the case where
// the dispatch closes the bar.
func (a *App) dispatchToBar(msg tea.KeyMsg) (tea.Cmd, bool) {
	var (
		handled bool
		cmd     tea.Cmd
	)
	switch {
	case a.commandBar.Active():
		handled, cmd = a.commandBar.Update(msg, a.dispatchCommand)
	case a.filterBar.Active():
		handled, cmd = a.filterBar.Update(msg, a.onFilterChange)
	case a.prompt.Active():
		handled, cmd = a.prompt.Update(msg, a.promptDispatch)
	case a.confirm.Active():
		handled, cmd = a.confirm.Update(msg, a.confirmDispatch)
	}
	if handled {
		a.resizeActiveView()
	}
	return cmd, handled
}

// viewKeys are the number keys that jump straight to a top-level view.
var viewKeys = map[string]style.ViewType{
	"1": style.ViewTopics,
	"2": style.ViewGroups,
	"3": style.ViewCluster,
	"4": style.ViewACLs,
}

// handleGlobalKey handles the navigation keys every view shares, reporting
// whether key was one of them.
func (a *App) handleGlobalKey(key string) (tea.Cmd, bool) {
	if vt, ok := viewKeys[key]; ok {
		return a.switchView(vt), true
	}
	switch key {
	case "?":
		a.showHelp = true
		return nil, true
	case "esc":
		// If filtering, clear the filter first.
		if a.filter != "" {
			a.filter = ""
			a.setActiveFilter("")
			return nil, true
		}
		// Otherwise go back.
		if len(a.viewStack) > 0 {
			a.popView()
			return a.refreshActiveView(), true
		}
		return nil, true
	case ":":
		cmd := a.commandBar.Open()
		a.resizeActiveView()
		return cmd, true
	case "/":
		cmd := a.filterBar.OpenWith(a.filter)
		a.resizeActiveView()
		return cmd, true
	case "r":
		return a.refreshActiveView(), true
	}
	return nil, false
}

// onFilterChange is the FilterBar callback. Mirrors the legacy
// `setActiveFilter` integration — every keystroke pushes the filter
// down to the active view, and Esc fires this with "" to clear.
func (a *App) onFilterChange(value string) {
	a.filter = value
	_ = a.setActiveFilter(value)
}

// dispatchCommand is the CommandBar dispatch. It parses the typed
// command and either switches views, opens a destructive prompt, or
// flashes "unknown command:".
func (a *App) dispatchCommand(input string) (errMsg string, cmd tea.Cmd) {
	lower := strings.ToLower(strings.TrimSpace(input))

	// Handle commands with arguments first.
	if strings.HasPrefix(lower, "produce") {
		topic := strings.TrimSpace(strings.TrimPrefix(input, "produce"))
		_, c := a.handleAction(actionProduce, topic)
		return "", c
	}

	if vt, ok := commandViews[lower]; ok {
		return "", a.switchView(vt)
	}
	if action, ok := commandActions[lower]; ok {
		_, c := a.handleAction(action, "")
		return "", c
	}

	switch lower {
	case "q", "q!", "quit", "exit":
		a.shutdown()
		return "", tea.Quit
	case "reset":
		if a.readonly {
			return "readonly mode", nil
		}
		_, c := a.promptDeleteOffset()
		return "", c
	case "reconnect":
		_, c := a.reconnect()
		return "", c
	case "search", "s":
		// Open the search bar (same as pressing `s` in Messages).
		topic := ""
		if v := typedView[*views.MessagesView](a, style.ViewMessages); v != nil {
			topic = v.Topic()
		}
		_, c := a.handleAction(actionSearch, topic)
		return "", c
	case "upgrade":
		// Checked before quitting: the upgrade itself runs after the TUI
		// exits, and a Homebrew install would only refuse there.
		if upgrade.HomebrewManaged() {
			return upgrade.ErrHomebrewManaged.Error(), nil
		}
		a.upgradeRequested = true
		a.shutdown()
		return "", tea.Quit
	case "logo", "logoless":
		a.setLogoless(!a.logoless)
		return "", nil
	}
	return "unknown command: " + lower, nil
}

// commandViews are the commands that switch to a top-level view.
var commandViews = map[string]style.ViewType{
	"topics":  style.ViewTopics,
	"topic":   style.ViewTopics,
	"groups":  style.ViewGroups,
	"group":   style.ViewGroups,
	"cluster": style.ViewCluster,
	"brokers": style.ViewCluster,
	"acls":    style.ViewACLs,
}

// commandActions are the commands that run an action with no parameter.
var commandActions = map[string]string{
	"create-topic": actionCreateTopic,
	"ct":           actionCreateTopic,
	"create-acl":   actionCreateACL,
	"acl":          actionCreateACL,
	"context":      actionContextView,
	"ctx":          actionContextView,
}

// setLogoless toggles the chrome's logo on/off in-memory. Mirrors
// k9s's Ctrl+L behavior — config persistence stays the responsibility
// of `k4a config set ui.logoless <bool>`.
func (a *App) setLogoless(v bool) {
	a.logoless = v
	if v {
		a.chrome.Logo = nil
	} else {
		a.chrome.Logo = logoLines
	}
	a.resizeActiveView()
}

// executeSearch dispatches the search action with the typed query for the
// current Messages topic. Called from the search-mode Prompt dispatcher.
func (a *App) executeSearch(query string) (tea.Model, tea.Cmd) {
	query = strings.TrimSpace(query)
	if query == "" {
		return a, nil
	}
	v := typedView[*views.MessagesView](a, style.ViewMessages)
	if v == nil {
		return a, nil
	}
	// Tier 1 session: remember the query so we can offer it as a
	// suggestion on the next search-bar open. Best-effort — failures
	// don't block the search.
	if a.session != nil {
		_ = a.session.AddRecentSearch(query) //nolint:errcheck // best-effort session persistence
	}
	return a.handleAction(actionSearch, v.Topic()+"\x00"+query)
}

// executeValueAction dispatches a value-prompt action (the non-confirm
// branches of the old executePendingAction): alter_topic_config,
// increase_partitions, change_replication, purge_topic, save_message.
// Returns (errMsg, cmd) per chrome.Dispatch — non-empty errMsg keeps
// the prompt open with the typed value preserved.
func (a *App) executeValueAction(pc pendingConfirm, input string) (errMsg string, cmd tea.Cmd) {
	switch pc.action {
	case "alter_topic_config":
		parts := strings.SplitN(pc.param, "\x00", 2)
		if len(parts) == 2 {
			a.loading = true
			return "", a.doAlterTopicConfig(parts[0], parts[1], strings.TrimSpace(input))
		}
		return "", nil
	case actionIncreasePartitions:
		return a.applyPartitionCount(pc.param, input)
	case actionChangeReplication:
		return a.applyReplicationFactor(pc.param, input)
	case "purge_topic":
		return a.applyPurge(pc.param, input)
	case "save_message":
		return a.saveMessage(pc.param, input)
	}
	return "", nil
}

// applyPartitionCount raises topic's partition count to the typed value,
// which must be a number above the current count.
func (a *App) applyPartitionCount(topic, input string) (errMsg string, cmd tea.Cmd) {
	newCount := 0
	if _, err := fmt.Sscanf(strings.TrimSpace(input), "%d", &newCount); err != nil || newCount < 1 {
		return "invalid partition count", nil
	}
	current := 0
	if v := typedView[*views.TopicDetailView](a, style.ViewTopicDetail); v != nil {
		current = v.PartitionCount()
	}
	if newCount <= current {
		return fmt.Sprintf("partition count must be greater than current (%d)", current), nil
	}
	a.loading = true
	return "", a.doIncreasePartitions(topic, newCount)
}

// applyReplicationFactor changes topic's replication factor to the typed
// value, which must be a number other than the current factor.
func (a *App) applyReplicationFactor(topic, input string) (errMsg string, cmd tea.Cmd) {
	newRF := 0
	if _, err := fmt.Sscanf(strings.TrimSpace(input), "%d", &newRF); err != nil || newRF < 1 {
		return "invalid replication factor", nil
	}
	current := 0
	if v := typedView[*views.TopicDetailView](a, style.ViewTopicDetail); v != nil {
		current = v.ReplicationFactor()
	}
	if newRF == current {
		return fmt.Sprintf("replication factor is already %d", current), nil
	}
	a.loading = true
	return "", a.doChangeReplication(topic, newRF)
}

// applyPurge purges topic by the typed duration, or sets (or with 0, resets)
// its retention when the duration is marked permanent.
func (a *App) applyPurge(topic, input string) (errMsg string, cmd tea.Cmd) {
	retentionMs, permanent, err := parseDuration(strings.TrimSpace(input))
	if err != nil {
		return err.Error(), nil
	}
	a.loading = true
	if !permanent {
		return "", a.doPurgeTopic(topic, retentionMs)
	}
	if retentionMs == 0 {
		return "", a.doResetRetention(topic)
	}
	return "", a.doSetRetention(topic, retentionMs)
}

// saveMessage writes the message param identifies to the typed path.
func (a *App) saveMessage(param, input string) (errMsg string, cmd tea.Cmd) {
	msg := a.resolveMessage(param)
	if msg == nil {
		return "", nil
	}
	path := strings.TrimSpace(input)
	if err := saveMessageToFile(msg, path); err != nil {
		return err.Error(), nil
	}
	a.flash = fmt.Sprintf("Saved to %s", path)
	return "", nil
}

// executeConfirmAction dispatches a y/n confirm action. Returns nil
// for the no branch; on yes, fires the destructive command.
func (a *App) executeConfirmAction(pc pendingConfirm, yes bool) tea.Cmd {
	if !yes {
		return nil
	}
	switch pc.action {
	case "delete_topic":
		a.loading = true
		return a.doDeleteTopic(pc.param)
	case "delete_acl":
		a.loading = true
		return a.doDeleteACL(pc.param)
	case "delete_group":
		a.loading = true
		return a.doDeleteGroup(pc.param)
	case "delete_offset":
		parts := strings.SplitN(pc.param, "\x00", 3)
		if len(parts) == 3 {
			partition := 0
			fmt.Sscanf(parts[2], "%d", &partition) //nolint:errcheck // best-effort parse
			a.loading = true
			return a.doDeleteGroupOffset(parts[0], parts[1], partition)
		}
	}
	return nil
}

// openValuePrompt routes a value-prompt action through the chrome.Prompt
// widget. The dispatch closure captures the (action, param) pair so
// the bar widget can be a single shared instance reused across many
// callsites.
//
//nolint:unparam // selectAll is true for every current callsite; keeping it explicit so future non-mute callsites are obvious
func (a *App) openValuePrompt(action, param, value, label, placeholder string, selectAll bool) tea.Cmd {
	pc := pendingConfirm{action: action, param: param}
	a.promptDispatch = func(input string) (string, tea.Cmd) {
		return a.executeValueAction(pc, input)
	}
	return a.prompt.Open(value, chrome.OpenOpts{
		Prompt:      label,
		Placeholder: placeholder,
		SelectAll:   selectAll,
	})
}

// openConfirmPrompt routes a y/n confirm through the chrome.Confirm
// widget. The dispatch closure captures (action, param). Returns nil
// today (Confirm doesn't surface a focus cmd) but keeps the signature
// symmetric with openValuePrompt so callers don't have to special-case.
//
//nolint:unparam // return type kept for symmetry with openValuePrompt
func (a *App) openConfirmPrompt(action, param, prompt string) tea.Cmd {
	pc := pendingConfirm{action: action, param: param}
	a.confirmDispatch = func(yes bool) (string, tea.Cmd) {
		return "", a.executeConfirmAction(pc, yes)
	}
	a.confirm.Open(prompt)
	return nil
}
