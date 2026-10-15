// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package tui

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"

	tea "charm.land/bubbletea/v2"
	"github.com/blairham/tuikit/chrome"

	"github.com/blairham/k4a/internal/kafka"
	"github.com/blairham/k4a/internal/tui/style"
	"github.com/blairham/k4a/internal/tui/views"
)

// lazyTopLevelViews are the views reached by a tab key or command that are
// built on first use rather than at startup. ACLs was missing from viewMap
// entirely, so the 4 tab rendered nothing.
var lazyTopLevelViews = map[style.ViewType]func(*kafka.Client) views.View{
	style.ViewACLs:   func(c *kafka.Client) views.View { return views.NewACLsView(c) },
	style.ViewQuotas: func(c *kafka.Client) views.View { return views.NewQuotasView(c) },
}

// ensureTopLevelView builds v for the current client if it is a lazy
// top-level view not built yet (or dropped by a context switch).
func (a *App) ensureTopLevelView(v style.ViewType) {
	if _, ok := a.viewMap[v]; ok {
		return
	}
	if build, ok := lazyTopLevelViews[v]; ok {
		a.setView(v, build(a.client))
	}
}

// quotaChangedMsg carries the result of an async client quota change.
type quotaChangedMsg struct {
	err  error
	what string
}

// promptAddQuota asks for an entity and a quota in one line.
func (a *App) promptAddQuota() (tea.Model, tea.Cmd) {
	a.promptDispatch = func(input string) (string, tea.Cmd) {
		entityText, setting, ok := strings.Cut(strings.TrimSpace(input), " ")
		if !ok {
			return "want: ENTITY KEY=VALUE, for example user=alice producer_byte_rate=1048576", nil
		}
		entity, err := kafka.ParseQuotaEntity(entityText)
		if err != nil {
			return err.Error(), nil
		}
		key, valueText, ok := strings.Cut(strings.TrimSpace(setting), "=")
		if !ok || strings.TrimSpace(key) == "" {
			return "want KEY=VALUE after the entity, for example producer_byte_rate=1048576", nil
		}
		value, err := parseQuotaValue(valueText)
		if err != nil {
			return err.Error(), nil
		}
		a.confirmQuota(entity, strings.TrimSpace(key), &value)
		return "", nil
	}
	cmd := a.prompt.Open("", chrome.OpenOpts{
		Prompt:      "quota:",
		Placeholder: "user=alice producer_byte_rate=1048576",
	})
	a.resizeActiveView()
	return a, cmd
}

// promptEditQuota opens the selected quota's value; an empty value removes it.
func (a *App) promptEditQuota(param string) (tea.Model, tea.Cmd) {
	q, ok := a.selectedQuota(param)
	if !ok {
		return a, nil
	}
	current := kafka.FormatQuotaValue(q.Value)
	a.promptDispatch = func(input string) (string, tea.Cmd) {
		input = strings.TrimSpace(input)
		if input == "" {
			a.confirmQuota(q.Entity, q.Key, nil)
			return "", nil
		}
		value, err := parseQuotaValue(input)
		if err != nil {
			return err.Error(), nil
		}
		if value == q.Value {
			return "value unchanged", nil
		}
		a.confirmQuota(q.Entity, q.Key, &value)
		return "", nil
	}
	cmd := a.prompt.Open(current, chrome.OpenOpts{
		Prompt:      fmt.Sprintf("%s %s:", q.Entity, q.Key),
		Placeholder: "empty removes the quota",
		SelectAll:   true,
	})
	a.resizeActiveView()
	return a, cmd
}

// promptDeleteQuota asks before removing the selected quota.
func (a *App) promptDeleteQuota(param string) (tea.Model, tea.Cmd) {
	if q, ok := a.selectedQuota(param); ok {
		a.confirmQuota(q.Entity, q.Key, nil)
		a.resizeActiveView()
	}
	return a, nil
}

func (a *App) selectedQuota(param string) (kafka.ClientQuota, bool) {
	v := typedView[*views.QuotasView](a, style.ViewQuotas)
	idx, ok := parseIndex(param)
	if v == nil || !ok {
		return kafka.ClientQuota{}, false
	}
	return v.Visible(idx)
}

// confirmQuota asks y/n before setting (or, with a nil value, removing) a
// quota: it throttles every client the entity covers as soon as it lands.
func (a *App) confirmQuota(entity kafka.QuotaEntity, key string, value *float64) {
	what := fmt.Sprintf("remove %s from %s", key, entity)
	if value != nil {
		what = fmt.Sprintf("set %s=%s for %s", key, kafka.FormatQuotaValue(*value), entity)
	}
	a.confirmDispatch = func(yes bool) (string, tea.Cmd) {
		if !yes {
			return "", nil
		}
		if a.readonly {
			return msgReadonly, nil
		}
		a.loading = true
		client := a.client
		return "", func() tea.Msg {
			return quotaChangedMsg{err: client.AlterClientQuota(context.Background(), entity, key, value), what: what}
		}
	}
	a.confirm.Open(what + "?")
}

// applyQuotaResult reports the change and refreshes the list.
func (a *App) applyQuotaResult(msg quotaChangedMsg) tea.Cmd {
	a.loading = false
	if msg.err != nil {
		a.errFlash = msg.err.Error()
		return nil
	}
	a.flash = "quota: " + msg.what
	if v := a.viewMap[style.ViewQuotas]; v != nil {
		return v.Refresh()
	}
	return nil
}

// parseQuotaValue reads a quota value: bytes per second for the rate
// quotas, a percentage for request_percentage. Kafka refuses zero and
// negative values itself; NaN and infinities never reach it.
func parseQuotaValue(s string) (float64, error) {
	v, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	if err != nil || math.IsNaN(v) || math.IsInf(v, 0) || v < 0 {
		return 0, fmt.Errorf("quota value %q: want a non-negative number", s)
	}
	return v, nil
}
