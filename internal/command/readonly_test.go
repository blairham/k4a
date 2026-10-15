// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package command

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/blairham/k4a/internal/state"
)

func writeConfig(t *testing.T, dir, body string) string {
	t.Helper()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestCheckWritable(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cases := []struct {
		name    string
		path    string
		wantErr bool
	}{
		{"readonly", writeConfig(t, t.TempDir(), "ui:\n  readonly: true\n"), true},
		{"writable", writeConfig(t, t.TempDir(), "ui:\n  readonly: false\n"), false},
		{"no config", filepath.Join(dir, "missing.yaml"), false},
		{"broken config", writeConfig(t, t.TempDir(), "ui: [\n"), true},
	}
	for _, tc := range cases {
		if err := checkWritable(tc.path, "op"); (err != nil) != tc.wantErr {
			t.Errorf("%s: checkWritable err=%v, wantErr=%v", tc.name, err, tc.wantErr)
		}
	}
}

const readonlyTopic = "readonly.guard.v1"

type runner interface{ Run(args []string) int }

// newGuardCluster is a fake cluster with one existing topic, so each
// mutating command has something real to act on.
func newGuardCluster(t *testing.T) (brokers string, adm *kadm.Client) {
	t.Helper()
	fake, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, readonlyTopic))
	if err != nil {
		t.Fatalf("kfake: %v", err)
	}
	t.Cleanup(fake.Close)
	seeds := fake.ListenAddrs()
	cl, err := kgo.NewClient(kgo.SeedBrokers(seeds...))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	return strings.Join(seeds, ","), kadm.NewClient(cl)
}

func topicExists(t *testing.T, adm *kadm.Client, topic string) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tds, err := adm.ListTopics(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	return tds.Has(topic)
}

func endOffset(t *testing.T, adm *kadm.Client) int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ends, err := adm.ListEndOffsets(ctx, readonlyTopic)
	if err != nil {
		t.Fatal(err)
	}
	o, _ := ends.Lookup(readonlyTopic, 0)
	return o.Offset
}

// Every mutating headless command must refuse under ui.readonly, and the
// same invocation with it off must succeed: the control run proves the
// refusal comes from the guard and not from a command that could not have
// worked anyway. Where the cluster can show the effect, it is checked too.
func TestMutatingCommandsHonorReadonly(t *testing.T) {
	t.Parallel()

	cases := []struct {
		cmd    func() runner
		effect func(t *testing.T, adm *kadm.Client) bool
		name   string
		args   []string
	}{
		{
			name: "create-topic",
			cmd:  func() runner { return &CreateTopicCmd{} },
			args: []string{"-t", "readonly.created", "-n", "1", "--replication-factor", "1"},
			effect: func(t *testing.T, adm *kadm.Client) bool {
				t.Helper()
				return topicExists(t, adm, "readonly.created")
			},
		},
		{
			name: "delete-topic",
			cmd:  func() runner { return &DeleteTopicCmd{} },
			args: []string{"-t", readonlyTopic, "--yes"},
			effect: func(t *testing.T, adm *kadm.Client) bool {
				t.Helper()
				return !topicExists(t, adm, readonlyTopic)
			},
		},
		{
			name: "produce",
			cmd:  func() runner { return &ProduceCommand{} },
			args: []string{"-t", readonlyTopic, "-c", "1", "-i", "0s"},
			effect: func(t *testing.T, adm *kadm.Client) bool {
				t.Helper()
				return endOffset(t, adm) > 0
			},
		},
		{
			name: "alter-config",
			cmd:  func() runner { return &AlterConfigCmd{} },
			args: []string{readonlyTopic, "--set", "retention.ms=60000"},
		},
		{
			name: "alter-replication",
			cmd:  func() runner { return &AlterReplicationCmd{} },
			args: []string{"-t", readonlyTopic, "--replication-factor", "1"},
		},
		{
			name: "create-acl",
			cmd:  func() runner { return &CreateACLCmd{} },
			args: []string{
				"--resource-type", "topic", "--resource-name", readonlyTopic,
				"--principal", "User:alice", "--operation", "read",
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			for _, readonly := range []bool{true, false} {
				brokers, adm := newGuardCluster(t)
				cfg := writeConfig(t, t.TempDir(), "ui:\n  readonly: "+map[bool]string{true: "true", false: "false"}[readonly]+"\n")
				args := append([]string{"--config", cfg, "-b", brokers}, tc.args...)

				code := tc.cmd().Run(args)
				want := 0
				if readonly {
					want = 1
				}
				if code != want {
					t.Fatalf("readonly=%t: Run = %d, want %d", readonly, code, want)
				}
				if tc.effect != nil && tc.effect(t, adm) == readonly {
					t.Errorf("readonly=%t: cluster change = %t, want %t", readonly, !readonly, !readonly)
				}
			}
		})
	}
}

func TestLocalClearCommandsHonorReadonly(t *testing.T) {
	root := state.NewRoot(t.TempDir())
	writeConfig(t, root.Path(), "ui:\n  readonly: true\n")
	var out bytes.Buffer
	if code := (&IndexCommand{Root: root, Out: &out}).Run([]string{"clear", "--all"}); code != 1 {
		t.Errorf("index clear --all under readonly = %d, want 1", code)
	}
	writeConfig(t, root.Path(), "ui:\n  readonly: false\n")
	if code := (&IndexCommand{Root: root, Out: &out}).Run([]string{"clear", "--all"}); code != 0 {
		t.Errorf("index clear --all, writable = %d, want 0", code)
	}

	// state clear has no root override: it follows HOME.
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	if err := os.MkdirAll(filepath.Join(home, ".k4a"), 0o700); err != nil {
		t.Fatal(err)
	}
	writeConfig(t, filepath.Join(home, ".k4a"), "ui:\n  readonly: true\n")
	if code := (&StateCommand{}).Run([]string{"clear", "all"}); code != 1 {
		t.Errorf("state clear all under readonly = %d, want 1", code)
	}
	writeConfig(t, filepath.Join(home, ".k4a"), "ui:\n  readonly: false\n")
	if code := (&StateCommand{}).Run([]string{"clear", "all"}); code != 0 {
		t.Errorf("state clear all, writable = %d, want 0", code)
	}
}

func TestDeleteTopicConfirmation(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name        string
		input       string
		interactive bool
		wantErr     bool
	}{
		{"yes", "y\n", true, false},
		{"full yes", "YES\n", true, false},
		{"no", "n\n", true, true},
		{"empty answer", "\n", true, true},
		{"eof", "", true, true},
		// A pipe or /dev/null must refuse, not wait or read a stray "y".
		{"not a terminal", "y\n", false, true},
	}
	for _, tc := range cases {
		var out bytes.Buffer
		c := &DeleteTopicCmd{
			In:          strings.NewReader(tc.input),
			Out:         &out,
			Interactive: func() bool { return tc.interactive },
		}
		if err := c.confirm("orders"); (err != nil) != tc.wantErr {
			t.Errorf("%s: confirm err=%v, wantErr=%v", tc.name, err, tc.wantErr)
		}
		if tc.interactive && !strings.Contains(out.String(), `"orders"`) {
			t.Errorf("%s: prompt %q does not name the topic", tc.name, out.String())
		}
	}
}

// Without --yes and with no terminal, delete-topic refuses before it
// connects: the topic survives.
func TestDeleteTopicWithoutYesRefusesNonInteractive(t *testing.T) {
	t.Parallel()

	brokers, adm := newGuardCluster(t)
	cfg := writeConfig(t, t.TempDir(), "")
	c := &DeleteTopicCmd{In: strings.NewReader(""), Out: &bytes.Buffer{}, Interactive: func() bool { return false }}
	if code := c.Run([]string{"--config", cfg, "-b", brokers, "-t", readonlyTopic}); code != 1 {
		t.Fatalf("Run = %d, want 1", code)
	}
	if !topicExists(t, adm, readonlyTopic) {
		t.Error("topic was deleted without confirmation")
	}
}
