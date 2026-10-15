// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package tui

import (
	"slices"
	"strings"
	"testing"

	tea "charm.land/bubbletea/v2"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/blairham/k4a/internal/kafka"
	"github.com/blairham/k4a/internal/tui/style"
	"github.com/blairham/k4a/internal/tui/views"
)

// topicDetailApp is an App on Topic Detail for a one-partition topic of a
// three-broker in-process cluster. sent collects the replica lists of every
// reassignment that reaches the cluster.
func topicDetailApp(t *testing.T, readonly bool) (app *App, sent *[][]int32) {
	t.Helper()
	t.Setenv("HOME", t.TempDir())
	fake, err := kfake.NewCluster(kfake.NumBrokers(3), kfake.SeedTopics(1, "orders"))
	if err != nil {
		t.Fatalf("kfake cluster: %v", err)
	}
	t.Cleanup(fake.Close)
	var got [][]int32
	fake.ControlKey(int16(kmsg.AlterPartitionAssignments), func(req kmsg.Request) (kmsg.Response, error, bool) {
		fake.KeepControl()
		r, ok := req.(*kmsg.AlterPartitionAssignmentsRequest)
		if !ok {
			return nil, nil, false //nolint:nilnil // kfake reads (nil, nil, false) as "not handled here"
		}
		for _, rt := range r.Topics {
			for _, rp := range rt.Partitions {
				got = append(got, rp.Replicas)
			}
		}
		return nil, nil, false //nolint:nilnil // kfake reads (nil, nil, false) as "let kfake answer"
	})
	client, err := kafka.NewClient(kafka.AuthConfig{Method: kafka.AuthPlaintext}, strings.Join(fake.ListenAddrs(), ","))
	if err != nil {
		t.Fatalf("kafka client: %v", err)
	}
	t.Cleanup(client.Close)

	app = NewApp(client, ConnInfo{}, nil, "", "dev")
	app.width, app.height = 140, 40
	app.readonly = readonly
	app.loading = false
	v := views.NewTopicDetailView(client, "orders")
	v.Update(v.Init()()) //nolint:errcheck
	v.Resize(140, 30)
	app.setView(style.ViewTopicDetail, v)
	app.view = style.ViewTopicDetail
	return app, &got
}

// TestReassignAsksFirst: a opens the selected partition's replicas, and the
// edited list reaches the cluster only after y, in the order typed.
func TestReassignAsksFirst(t *testing.T) {
	app, sent := topicDetailApp(t, false)
	enter := tea.KeyPressMsg{Code: tea.KeyEnter}
	p, _ := typedView[*views.TopicDetailView](app, style.ViewTopicDetail).SelectedPartition()

	typeKeys(t, app, keyPress('a'))
	if !app.prompt.Active() {
		t.Fatal("a did not open the replica editor")
	}
	if got := app.prompt.Input().Placeholder; got != replicaText(p.Replicas) {
		t.Errorf("editor opened on %q, want the current replicas %q", got, replicaText(p.Replicas))
	}
	target := "2,0"
	if replicaText(p.Replicas) == target {
		target = "1,0"
	}
	typeKeys(t, app, append(runes(target), enter)...)
	if q := app.confirm.Prompt(); !strings.Contains(q, "reassign orders-0") || !strings.Contains(q, "to ["+target+"]") {
		t.Fatalf("confirm = %q", q)
	}
	answer(t, app, 'n')
	if len(*sent) != 0 {
		t.Fatalf("a declined reassignment was sent: %v", *sent)
	}

	typeKeys(t, app, append(append([]tea.KeyPressMsg{keyPress('a')}, runes(target)...), enter)...)
	answer(t, app, 'y')
	want, _ := kafka.ParseReplicaList(target)
	if len(*sent) != 1 || !slices.Equal(brokerIntsForTest((*sent)[0]), want) {
		t.Errorf("sent %v, want [%s]", *sent, target)
	}
	if app.errFlash != "" || !strings.Contains(app.flash, "reassigning orders-0") {
		t.Errorf("flash %q, errFlash %q", app.flash, app.errFlash)
	}
}

// TestReassignRefusals keeps the editor open on input that cannot be sent,
// and readonly mode never opens it.
func TestReassignRefusals(t *testing.T) {
	app, sent := topicDetailApp(t, false)
	p, _ := typedView[*views.TopicDetailView](app, style.ViewTopicDetail).SelectedPartition()
	for in, want := range map[string]string{
		"1,1":                   "twice",
		"x":                     "not a broker ID",
		"":                      "no reassignment in progress",
		replicaText(p.Replicas): "unchanged",
	} {
		if msg, _ := app.confirmReassign("orders", p, in); !strings.Contains(msg, want) {
			t.Errorf("%q: %q, want it to say %q", in, msg, want)
		}
	}
	// A move in progress can be canceled with an empty list.
	moving := p
	moving.Adding = []int{2}
	if msg, _ := app.confirmReassign(
		"orders",
		moving,
		"",
	); msg != "" ||
		!strings.Contains(app.confirm.Prompt(), "cancel the reassignment") {
		t.Errorf("cancel: msg %q, confirm %q", msg, app.confirm.Prompt())
	}
	if len(*sent) != 0 {
		t.Errorf("nothing was confirmed, but %v was sent", *sent)
	}

	ro, _ := topicDetailApp(t, true)
	typeKeys(t, ro, keyPress('a'))
	if ro.prompt.Active() || ro.errFlash != msgReadonly {
		t.Errorf("readonly: prompt %v, errFlash %q", ro.prompt.Active(), ro.errFlash)
	}
}

func brokerIntsForTest(ids []int32) []int {
	out := make([]int, 0, len(ids))
	for _, id := range ids {
		out = append(out, int(id))
	}
	return out
}
