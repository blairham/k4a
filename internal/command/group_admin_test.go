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
	"github.com/twmb/franz-go/pkg/kgo"
)

// parityAdmin is a kadm client on brokers, for checking what a command did.
func parityAdmin(t *testing.T, brokers string) *kadm.Client {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(brokers, ",")...))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	return kadm.NewClient(cl)
}

// committed returns parityGroup's committed offset on parityTopic partition
// 0, or -1 when there is none.
func committed(t *testing.T, adm *kadm.Client) int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	offs, err := adm.FetchOffsets(ctx, parityGroup)
	if err != nil {
		t.Fatal(err)
	}
	if o, ok := offs.Lookup(parityTopic, 0); ok && o.Err == nil {
		return o.At
	}
	return -1
}

func commitEmptyPartition(t *testing.T, adm *kadm.Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var offs kadm.Offsets
	offs.AddOffset(parityTopic, 1, 0, -1)
	if err := adm.CommitAllOffsets(ctx, parityGroup, offs); err != nil {
		t.Fatal(err)
	}
}

func groupExists(t *testing.T, adm *kadm.Client) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	listed, err := adm.ListGroups(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, ok := listed[parityGroup]
	return ok
}

// runAdmin runs cmd against brokers with a config whose ui.readonly is set
// as given.
func runAdmin(t *testing.T, cmd runner, brokers string, readonly bool, args ...string) int {
	t.Helper()
	cfg := writeConfig(t, t.TempDir(), "ui:\n  readonly: "+map[bool]string{true: "true", false: "false"}[readonly]+"\n")
	return cmd.Run(append([]string{"--config", cfg, "-b", brokers}, args...))
}

// TestResetOffsetsModes drives every --to mode against a group committed at
// 1 on a 3-record partition.
func TestResetOffsetsModes(t *testing.T) {
	t.Parallel()
	cases := []struct {
		args []string
		want int64
	}{
		{[]string{"--to", "earliest"}, 0},
		{[]string{"--to", "latest"}, 3},
		{[]string{"--to", "offset", "--value", "2"}, 2},
		{[]string{"--to", "shift", "--value", "1"}, 2},
		{[]string{"--to", "shift", "--value=-5"}, 0},
	}
	for _, tc := range cases {
		t.Run(strings.Join(tc.args, " "), func(t *testing.T) {
			t.Parallel()
			brokers := newParityCluster(t)
			// shift refuses a partition with nothing committed, as the TUI
			// does, so give the empty partition a commit too.
			commitEmptyPartition(t, parityAdmin(t, brokers))
			args := append([]string{"-g", parityGroup, "-t", parityTopic, "--yes"}, tc.args...)
			if code := runAdmin(t, &ResetOffsetsCmd{Prompt{Out: &bytes.Buffer{}}}, brokers, false, args...); code != 0 {
				t.Fatalf("Run = %d", code)
			}
			if got := committed(t, parityAdmin(t, brokers)); got != tc.want {
				t.Errorf("committed = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestParseResetStrategyRejects(t *testing.T) {
	t.Parallel()
	for _, tc := range [][2]string{
		{"earliest", "3"}, // a value the mode would silently ignore
		{"offset", ""},
		{"offset", "-1"},
		{"shift", "x"},
	} {
		if _, err := parseResetStrategy(tc[0], tc[1]); err == nil {
			t.Errorf("parseResetStrategy(%q, %q) accepted", tc[0], tc[1])
		}
	}
}

func TestDeleteOffsetsAndGroup(t *testing.T) {
	t.Parallel()
	brokers := newParityCluster(t)
	adm := parityAdmin(t, brokers)

	if code := runAdmin(t, &DeleteOffsetsCmd{Prompt{Out: &bytes.Buffer{}}}, brokers, false,
		"-g", parityGroup, "-t", parityTopic, "-P", "0", "--yes"); code != 0 {
		t.Fatalf("delete-offsets = %d", code)
	}
	if got := committed(t, adm); got != -1 {
		t.Errorf("committed after delete-offsets = %d, want none", got)
	}

	if code := runAdmin(t, &DeleteGroupCmd{Prompt{Out: &bytes.Buffer{}}}, brokers, false,
		"-g", parityGroup, "--yes"); code != 0 {
		t.Fatalf("delete-group = %d", code)
	}
	if groupExists(t, adm) {
		t.Error("group still listed after delete-group")
	}
}

// TestGroupMutationsGuarded checks the two gates every group mutation shares:
// ui.readonly refuses it, and without --yes a non-terminal stdin refuses it.
// The control run (writable, --yes) proves each refusal came from the gate.
func TestGroupMutationsGuarded(t *testing.T) {
	t.Parallel()
	refused := func() Prompt {
		return Prompt{In: strings.NewReader("y\n"), Out: &bytes.Buffer{}, Interactive: func() bool { return false }}
	}
	cases := []struct {
		cmd       func(Prompt) runner
		unchanged func(t *testing.T, adm *kadm.Client) bool
		name      string
		args      []string
	}{
		{
			name: "reset-offsets",
			cmd:  func(p Prompt) runner { return &ResetOffsetsCmd{p} },
			args: []string{"-g", parityGroup, "-t", parityTopic, "--to", "latest"},
			unchanged: func(t *testing.T, adm *kadm.Client) bool {
				t.Helper()
				return committed(t, adm) == 1
			},
		},
		{
			name: "delete-offsets",
			cmd:  func(p Prompt) runner { return &DeleteOffsetsCmd{p} },
			args: []string{"-g", parityGroup, "-t", parityTopic, "-P", "0"},
			unchanged: func(t *testing.T, adm *kadm.Client) bool {
				t.Helper()
				return committed(t, adm) == 1
			},
		},
		{
			name: "delete-group",
			cmd:  func(p Prompt) runner { return &DeleteGroupCmd{p} },
			args: []string{"-g", parityGroup},
			unchanged: func(t *testing.T, adm *kadm.Client) bool {
				t.Helper()
				return groupExists(t, adm)
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			brokers := newParityCluster(t)
			adm := parityAdmin(t, brokers)

			if code := runAdmin(t, tc.cmd(refused()), brokers, true, append(tc.args, "--yes")...); code != 1 {
				t.Errorf("readonly: Run = %d, want 1", code)
			}
			if code := runAdmin(t, tc.cmd(refused()), brokers, false, tc.args...); code != 1 {
				t.Errorf("no --yes, no terminal: Run = %d, want 1", code)
			}
			if !tc.unchanged(t, adm) {
				t.Fatal("a refused run changed the cluster")
			}
			if code := runAdmin(t, tc.cmd(refused()), brokers, false, append(tc.args, "--yes")...); code != 0 {
				t.Fatalf("control: Run = %d, want 0", code)
			}
			if tc.unchanged(t, adm) {
				t.Error("control run left the cluster unchanged")
			}
		})
	}
}
