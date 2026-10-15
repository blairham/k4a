// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package command

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/blairham/k4a/internal/kafka"
)

const (
	parityTopic = "parity.orders.v1"
	parityGroup = "parity-consumer"
)

// newParityCluster is a fake cluster holding parityTopic (2 partitions, 3
// records on partition 0) and parityGroup, which committed offset 1 on
// partition 0, so it lags by 2.
func newParityCluster(t *testing.T) string {
	t.Helper()
	fake, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(2, parityTopic))
	if err != nil {
		t.Fatalf("kfake: %v", err)
	}
	t.Cleanup(fake.Close)
	seeds := fake.ListenAddrs()
	cl, err := kgo.NewClient(kgo.SeedBrokers(seeds...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for range 3 {
		if err := cl.ProduceSync(ctx, &kgo.Record{Topic: parityTopic, Partition: 0, Value: []byte("v")}).
			FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	adm := kadm.NewClient(cl)
	var offs kadm.Offsets
	offs.AddOffset(parityTopic, 0, 1, -1)
	if err := adm.CommitAllOffsets(ctx, parityGroup, offs); err != nil {
		t.Fatalf("commit: %v", err)
	}
	return strings.Join(seeds, ",")
}

// runJSON runs cmd with --format json against brokers and decodes its output.
func runJSON(t *testing.T, cmd runner, out *bytes.Buffer, brokers string, v any, args ...string) {
	t.Helper()
	cfg := writeConfig(t, t.TempDir(), "ui:\n  readonly: false\n")
	full := append([]string{"--config", cfg, "-b", brokers, "--format", "json"}, args...)
	if code := cmd.Run(full); code != 0 {
		t.Fatalf("Run(%v) = %d, output:\n%s", args, code, out)
	}
	if err := json.Unmarshal(out.Bytes(), v); err != nil {
		t.Fatalf("decoding %q: %v", out, err)
	}
}

func TestListTopicsCommand(t *testing.T) {
	t.Parallel()
	brokers := newParityCluster(t)

	var out bytes.Buffer
	var rows []topicRow
	runJSON(t, &ListTopicsCmd{Out: &out}, &out, brokers, &rows, "--counts")

	var found *topicRow
	for i := range rows {
		if rows[i].Internal {
			t.Errorf("internal topic %q listed without --internal", rows[i].Name)
		}
		if rows[i].Name == parityTopic {
			found = &rows[i]
		}
	}
	if found == nil {
		t.Fatalf("%s not listed: %+v", parityTopic, rows)
	}
	if found.Partitions != 2 || found.Messages == nil || *found.Messages != 3 {
		t.Errorf("row = %+v (messages %v), want 2 partitions and 3 messages", *found, found.Messages)
	}
}

func TestListGroupsCommand(t *testing.T) {
	t.Parallel()
	brokers := newParityCluster(t)

	var out bytes.Buffer
	var rows []groupRow
	runJSON(t, &ListGroupsCmd{Out: &out}, &out, brokers, &rows, "--lag")
	if len(rows) != 1 || rows[0].GroupID != parityGroup {
		t.Fatalf("rows = %+v, want just %s", rows, parityGroup)
	}
	if rows[0].Lag == nil || *rows[0].Lag != 2 {
		t.Errorf("lag = %v, want 2", rows[0].Lag)
	}

	// The text table carries the same row.
	out.Reset()
	cfg := writeConfig(t, t.TempDir(), "")
	if code := (&ListGroupsCmd{Out: &out}).Run([]string{"--config", cfg, "-b", brokers, "--lag"}); code != 0 {
		t.Fatalf("text run = %d", code)
	}
	if fields := strings.Fields(strings.Split(out.String(), "\n")[1]); len(fields) != 6 || fields[0] != parityGroup ||
		fields[4] != "2" {
		t.Errorf("text row = %q, want %s ... lag 2", fields, parityGroup)
	}
}

func TestDescribeGroupCommand(t *testing.T) {
	t.Parallel()
	brokers := newParityCluster(t)

	var out bytes.Buffer
	var got []groupDetailJSON
	runJSON(t, &DescribeGroupCmd{Out: &out}, &out, brokers, &got, parityGroup)
	if len(got) != 1 || got[0].GroupID != parityGroup || got[0].TotalLag != 2 {
		t.Fatalf("detail = %+v, want %s with total lag 2", got, parityGroup)
	}
	o := got[0].Offsets
	if len(o) != 1 || o[0].Partition != 0 || o[0].Committed != 1 || o[0].HighWatermark != 3 {
		t.Errorf("offsets = %+v, want partition 0 committed 1 of 3", o)
	}
	if got[0].Members == nil {
		t.Error("members must encode as [] rather than null")
	}

	// An unknown group is an error, not an empty success.
	cfg := writeConfig(t, t.TempDir(), "")
	if code := (&DescribeGroupCmd{Out: &bytes.Buffer{}}).Run(
		[]string{"--config", cfg, "-b", brokers, "no-such-group"},
	); code == 0 {
		t.Error("describe-group of a missing group succeeded")
	}
}

func TestDescribeBrokerConfigCommand(t *testing.T) {
	t.Parallel()
	brokers := newParityCluster(t)

	var out bytes.Buffer
	var all []brokerConfigRow
	runJSON(t, &DescribeBrokerConfigCmd{Out: &out}, &out, brokers, &all, "--all")
	if len(all) == 0 {
		t.Fatal("--all returned no broker configs")
	}
	key := all[0].Name

	out.Reset()
	var one []brokerConfigRow
	runJSON(t, &DescribeBrokerConfigCmd{Out: &out}, &out, brokers, &one, "--config-key", key)
	if len(one) != 1 || one[0].Name != key {
		t.Errorf("--config-key %s = %+v, want that one key", key, one)
	}

	if code := (&DescribeBrokerConfigCmd{}).Run([]string{"-b", brokers, "not-a-number"}); code == 0 {
		t.Error("a non-numeric broker ID was accepted")
	}
}

func TestFilterBrokerConfigsHidesDefaultsAndSecrets(t *testing.T) {
	t.Parallel()
	entries := []kafka.ConfigEntry{
		{Name: "a", Value: "1", Source: "DEFAULT", IsDefault: true},
		{Name: "b", Value: "2", Source: "STATIC_BROKER"},
		{Name: "c", Value: "hunter2", Source: "DYNAMIC_BROKER", Sensitive: true},
	}
	rows := filterBrokerConfigs(1, entries, "", false)
	if len(rows) != 2 || rows[0].Name != "b" || rows[1].Name != "c" {
		t.Fatalf("rows = %+v, want b and c", rows)
	}
	if rows[1].Value != "" {
		t.Errorf("sensitive value leaked: %q", rows[1].Value)
	}
	if got := filterBrokerConfigs(1, entries, "", true); len(got) != 3 {
		t.Errorf("--all kept %d rows, want 3", len(got))
	}
}
