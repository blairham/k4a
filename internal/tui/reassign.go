// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package tui

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"

	tea "charm.land/bubbletea/v2"
	"github.com/blairham/tuikit/chrome"

	"github.com/blairham/k4a/internal/kafka"
	"github.com/blairham/k4a/internal/tui/style"
	"github.com/blairham/k4a/internal/tui/views"
)

// reassignMsg carries the result of an async partition reassignment.
type reassignMsg struct {
	err       error
	topic     string
	partition int
	cancel    bool
}

// promptReassign opens the selected partition's replica list for editing.
// Topic Detail's refresh then shows the move's progress in REPLICAS.
func (a *App) promptReassign(param string) (tea.Model, tea.Cmd) {
	v := typedView[*views.TopicDetailView](a, style.ViewTopicDetail)
	id, err := strconv.Atoi(param)
	if v == nil || err != nil {
		return a, nil
	}
	p, ok := v.Partition(id)
	if !ok {
		return a, nil
	}
	topic := v.Topic()
	current := replicaText(p.Replicas)
	a.promptDispatch = func(input string) (string, tea.Cmd) {
		return a.confirmReassign(topic, p, input)
	}
	cmd := a.prompt.Open(current, chrome.OpenOpts{
		Prompt:      fmt.Sprintf("%s-%d replicas:", topic, id),
		Placeholder: "broker IDs, preferred leader first; empty cancels a move in progress",
		SelectAll:   true,
	})
	a.resizeActiveView()
	return a, cmd
}

// confirmReassign checks the typed replica list and asks y/n before sending
// it: moving a replica copies the partition's whole log between brokers.
func (a *App) confirmReassign(topic string, p kafka.PartitionInfo, input string) (errMsg string, cmd tea.Cmd) {
	replicas, err := kafka.ParseReplicaList(input)
	if err != nil {
		return err.Error(), nil
	}
	moving := len(p.Adding)+len(p.Removing) > 0
	var question string
	switch {
	case len(replicas) == 0 && !moving:
		return "no reassignment in progress to cancel", nil
	case len(replicas) == 0:
		question = fmt.Sprintf("cancel the reassignment of %s-%d?", topic, p.ID)
	case slices.Equal(replicas, p.Replicas):
		return "replicas unchanged", nil
	default:
		question = fmt.Sprintf(
			"reassign %s-%d from [%s] to [%s]?",
			topic,
			p.ID,
			replicaText(p.Replicas),
			replicaText(replicas),
		)
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
			err := client.ReassignPartition(context.Background(), topic, p.ID, replicas)
			return reassignMsg{err: err, topic: topic, partition: p.ID, cancel: len(replicas) == 0}
		}
	}
	a.confirm.Open(question)
	return "", nil
}

// applyReassignResult reports the request and refreshes Topic Detail, whose
// periodic refresh then follows the move.
func (a *App) applyReassignResult(msg reassignMsg) tea.Cmd {
	a.loading = false
	if msg.err != nil {
		a.errFlash = msg.err.Error()
		return nil
	}
	if msg.cancel {
		a.flash = fmt.Sprintf("canceled the reassignment of %s-%d", msg.topic, msg.partition)
	} else {
		a.flash = fmt.Sprintf(
			"reassigning %s-%d: REPLICAS shows +adding -removing until it completes",
			msg.topic,
			msg.partition,
		)
	}
	if v := typedView[*views.TopicDetailView](a, style.ViewTopicDetail); v != nil && v.Topic() == msg.topic {
		return v.Refresh()
	}
	return nil
}

func replicaText(ids []int) string {
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		out = append(out, strconv.Itoa(id))
	}
	return strings.Join(out, ",")
}
