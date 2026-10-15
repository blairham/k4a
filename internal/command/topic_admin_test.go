// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package command

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/blairham/k4a/internal/kafka"
)

var parityACLArgs = []string{
	"--resource-type", "topic", "--resource-name", parityTopic,
	"--principal", "User:alice", "--operation", "read",
}

// seedACL creates the binding parityACLArgs names.
func seedACL(t *testing.T, adm *kadm.Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	b := kadm.NewACLs().Topics(parityTopic).ResourcePatternType(kadm.ACLPatternLiteral).
		Allow("User:alice").AllowHosts("*").Operations(kadm.OpRead)
	res, err := adm.CreateACLs(ctx, b)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range res {
		if r.Err != nil {
			t.Fatal(r.Err)
		}
	}
}

func aclCount(t *testing.T, brokers string) int {
	t.Helper()
	cl, err := kafka.NewClient(kafka.AuthConfig{}, brokers)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	entries, err := cl.FetchACLs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return len(entries)
}

func partitionCount(t *testing.T, adm *kadm.Client) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tds, err := adm.ListTopics(ctx, parityTopic)
	if err != nil {
		t.Fatal(err)
	}
	return len(tds[parityTopic].Partitions)
}

func startOffset(t *testing.T, adm *kadm.Client) int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	starts, err := adm.ListStartOffsets(ctx, parityTopic)
	if err != nil {
		t.Fatal(err)
	}
	o, _ := starts.Lookup(parityTopic, 0)
	return o.Offset
}

func TestDeleteACLCommand(t *testing.T) {
	t.Parallel()
	brokers := newParityCluster(t)
	seedACL(t, parityAdmin(t, brokers))

	// A binding that differs in one field (deny, not allow) matches nothing.
	deny := append(append([]string{}, parityACLArgs...), "--permission", "deny", "--yes")
	if code := runAdmin(t, &DeleteACLCmd{Prompt{Out: &bytes.Buffer{}}}, brokers, false, deny...); code != 1 {
		t.Errorf("delete of a non-existent binding = %d, want 1", code)
	}
	if n := aclCount(t, brokers); n != 1 {
		t.Fatalf("ACLs after a non-matching delete = %d, want 1", n)
	}

	// Enum spelling is loose: READ matches the listed "read".
	upper := append(append([]string{}, parityACLArgs...), "--yes")
	upper[len(upper)-2] = "READ"
	if code := runAdmin(t, &DeleteACLCmd{Prompt{Out: &bytes.Buffer{}}}, brokers, false, upper...); code != 0 {
		t.Fatalf("delete-acl = %d", code)
	}
	if n := aclCount(t, brokers); n != 0 {
		t.Errorf("ACLs after delete = %d, want 0", n)
	}
}

func TestAddPartitionsCommand(t *testing.T) {
	t.Parallel()
	brokers := newParityCluster(t)
	adm := parityAdmin(t, brokers)

	if code := runAdmin(t, &AddPartitionsCmd{Prompt{Out: &bytes.Buffer{}}}, brokers, false,
		"-t", parityTopic, "-n", "2", "--yes"); code != 1 {
		t.Errorf("add-partitions to the current count = %d, want 1", code)
	}
	if code := runAdmin(t, &AddPartitionsCmd{Prompt{Out: &bytes.Buffer{}}}, brokers, false,
		"-t", parityTopic, "-n", "4", "--yes"); code != 0 {
		t.Fatalf("add-partitions = %d", code)
	}
	if n := partitionCount(t, adm); n != 4 {
		t.Errorf("partitions = %d, want 4", n)
	}
}

func TestPurgeTopicAll(t *testing.T) {
	t.Parallel()
	brokers := newParityCluster(t)
	if code := runAdmin(t, &PurgeTopicCmd{Prompt{Out: &bytes.Buffer{}}}, brokers, false,
		"-t", parityTopic, "--all", "--yes"); code != 0 {
		t.Fatalf("purge-topic --all = %d", code)
	}
	if got := startOffset(t, parityAdmin(t, brokers)); got != 3 {
		t.Errorf("log start offset = %d, want 3", got)
	}
}

func TestPurgeTarget(t *testing.T) {
	t.Parallel()
	if ms, _, err := purgeTarget(false, "2h"); err != nil || ms != 2*60*60*1000 {
		t.Errorf("2h = %d, %v", ms, err)
	}
	if ms, _, err := purgeTarget(false, "7d"); err != nil || ms != 7*24*60*60*1000 {
		t.Errorf("7d = %d, %v", ms, err)
	}
	if _, _, err := purgeTarget(true, ""); err != nil {
		t.Errorf("--all: %v", err)
	}
	for _, tc := range []struct {
		olderThan string
		all       bool
	}{
		{"", false},   // neither
		{"1h", true},  // both
		{"1s", false}, // would silently mean everything
		{"500ms", false},
		{"-1h", false},
		{"soon", false},
	} {
		if _, _, err := purgeTarget(tc.all, tc.olderThan); err == nil {
			t.Errorf("purgeTarget(%t, %q) accepted", tc.all, tc.olderThan)
		}
	}
}

// TestTopicMutationsGuarded is TestGroupMutationsGuarded for the topic and
// ACL mutations: readonly and a missing --yes on a non-terminal both refuse
// and leave the cluster as it was; the control run changes it.
func TestTopicMutationsGuarded(t *testing.T) {
	t.Parallel()
	refused := func() Prompt {
		return Prompt{In: strings.NewReader("y\n"), Out: &bytes.Buffer{}, Interactive: func() bool { return false }}
	}
	cases := []struct {
		cmd       func(Prompt) runner
		unchanged func(t *testing.T, brokers string) bool
		name      string
		args      []string
	}{
		{
			name: "delete-acl",
			cmd:  func(p Prompt) runner { return &DeleteACLCmd{p} },
			args: parityACLArgs,
			unchanged: func(t *testing.T, brokers string) bool {
				t.Helper()
				return aclCount(t, brokers) == 1
			},
		},
		{
			name: "add-partitions",
			cmd:  func(p Prompt) runner { return &AddPartitionsCmd{p} },
			args: []string{"-t", parityTopic, "-n", "3"},
			unchanged: func(t *testing.T, brokers string) bool {
				t.Helper()
				return partitionCount(t, parityAdmin(t, brokers)) == 2
			},
		},
		{
			name: "purge-topic",
			cmd:  func(p Prompt) runner { return &PurgeTopicCmd{p} },
			args: []string{"-t", parityTopic, "--all"},
			unchanged: func(t *testing.T, brokers string) bool {
				t.Helper()
				return startOffset(t, parityAdmin(t, brokers)) == 0
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			brokers := newParityCluster(t)
			seedACL(t, parityAdmin(t, brokers))

			if code := runAdmin(t, tc.cmd(refused()), brokers, true, append(tc.args, "--yes")...); code != 1 {
				t.Errorf("readonly: Run = %d, want 1", code)
			}
			if code := runAdmin(t, tc.cmd(refused()), brokers, false, tc.args...); code != 1 {
				t.Errorf("no --yes, no terminal: Run = %d, want 1", code)
			}
			if !tc.unchanged(t, brokers) {
				t.Fatal("a refused run changed the cluster")
			}
			if code := runAdmin(t, tc.cmd(refused()), brokers, false, append(tc.args, "--yes")...); code != 0 {
				t.Fatalf("control: Run = %d, want 0", code)
			}
			if tc.unchanged(t, brokers) {
				t.Error("control run left the cluster unchanged")
			}
		})
	}
}
