// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package command

import (
	"context"
	"io"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// captureStdout runs fn with os.Stdout redirected and returns what it
// printed. os.Stdout is process-wide, so callers must not be parallel: Go
// holds every parallel test until the sequential ones have finished.
func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	orig := os.Stdout
	os.Stdout = w
	done := make(chan string, 1)
	go func() {
		b, _ := io.ReadAll(r)
		_ = r.Close()
		done <- string(b)
	}()
	defer func() { os.Stdout = orig }()
	fn()
	os.Stdout = orig
	_ = w.Close()
	return <-done
}

// lineWith returns the first line of out that contains every one of parts.
func lineWith(out string, parts ...string) (string, bool) {
	for line := range strings.SplitSeq(out, "\n") {
		if !slices.ContainsFunc(parts, func(p string) bool { return !strings.Contains(line, p) }) {
			return line, true
		}
	}
	return "", false
}

func TestVersionCommandPrintsEveryField(t *testing.T) {
	out := captureStdout(t, func() {
		if code := (&VersionCommand{Version: "v1.2.3", Commit: "abc1234", Date: "2026-10-15"}).Run(nil); code != 0 {
			t.Errorf("Run = %d, want 0", code)
		}
	})
	if want := "k4a v1.2.3 (commit: abc1234, built: 2026-10-15)\n"; out != want {
		t.Errorf("printed %q, want %q", out, want)
	}
}

// contexts prints one line per context, marks only the current one, and
// shows an SSM parameter as the broker source when there are no brokers.
func TestContextsCommandListsContexts(t *testing.T) {
	cfg := writeConfig(t, t.TempDir(), `current-context: staging
contexts:
  staging:
    brokers: broker:9092
    auth: scram
  production:
    ssm-brokers: /kafka/prod/brokers
    auth: iam
`)
	out := captureStdout(t, func() {
		if code := (&ContextsCommand{}).Run([]string{"--config", cfg}); code != 0 {
			t.Errorf("Run = %d, want 0", code)
		}
	})

	staging, ok := lineWith(out, "staging")
	if !ok {
		t.Fatalf("no staging line in %q", out)
	}
	if !strings.HasPrefix(staging, "* ") || !strings.Contains(staging, "auth=scram") ||
		!strings.Contains(staging, "brokers=broker:9092") {
		t.Errorf("staging line = %q, want current marker, auth=scram, brokers=broker:9092", staging)
	}
	prod, ok := lineWith(out, "production")
	if !ok {
		t.Fatalf("no production line in %q", out)
	}
	if strings.HasPrefix(prod, "*") || !strings.Contains(prod, "auth=iam") ||
		!strings.Contains(prod, "brokers=ssm:/kafka/prod/brokers") {
		t.Errorf("production line = %q, want unmarked, auth=iam, brokers=ssm:/kafka/prod/brokers", prod)
	}
	if n := strings.Count(out, "\n"); n != 2 {
		t.Errorf("printed %d lines, want one per context:\n%s", n, out)
	}
}

func newCluster(t *testing.T, opts ...kfake.Opt) (fake *kfake.Cluster, brokers string, adm *kadm.Client) {
	t.Helper()
	fake, err := kfake.NewCluster(opts...)
	if err != nil {
		t.Fatalf("kfake: %v", err)
	}
	t.Cleanup(fake.Close)
	cl, err := kgo.NewClient(kgo.SeedBrokers(fake.ListenAddrs()...))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	return fake, strings.Join(fake.ListenAddrs(), ","), kadm.NewClient(cl)
}

func TestVerifyCommandReportsClusterMetadata(t *testing.T) {
	_, brokers, _ := newCluster(t, kfake.NumBrokers(3), kfake.ClusterID("k4a-verify-cluster"))
	cfg := writeConfig(t, t.TempDir(), "")

	out := captureStdout(t, func() {
		if code := (&VerifyCommand{}).Run([]string{"--config", cfg, "-b", brokers}); code != 0 {
			t.Errorf("Run = %d, want 0", code)
		}
	})
	for _, want := range []string{"Connection successful!", "Cluster ID:  k4a-verify-cluster", "Brokers:     3"} {
		if !strings.Contains(out, want) {
			t.Errorf("output is missing %q:\n%s", want, out)
		}
	}
	if n := strings.Count(out, "\n  - "); n != 3 {
		t.Errorf("listed %d brokers, want 3:\n%s", n, out)
	}
}

// create-acl writes exactly the binding it was asked for, and list-acls
// reads it back; on an empty cluster list-acls says so.
func TestCreateACLThenListACLs(t *testing.T) {
	_, brokers, adm := newCluster(t, kfake.NumBrokers(1))
	cfg := writeConfig(t, t.TempDir(), "")
	conn := []string{"--config", cfg, "-b", brokers}

	empty := captureStdout(t, func() {
		if code := (&ListACLsCmd{}).Run(conn); code != 0 {
			t.Errorf("list-acls on an empty cluster = %d, want 0", code)
		}
	})
	if !strings.Contains(empty, "No ACLs found.") {
		t.Errorf("empty list-acls printed %q", empty)
	}

	_ = captureStdout(t, func() {
		args := append(slices.Clone(conn),
			"--resource-type", "topic", "--resource-name", "orders.v1",
			"--principal", "User:alice", "--operation", "write", "--permission", "deny",
			"--pattern-type", "prefixed")
		if code := (&CreateACLCmd{}).Run(args); code != 0 {
			t.Errorf("create-acl = %d, want 0", code)
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	results, err := adm.DescribeACLs(ctx, kadm.NewACLs().AnyResource().Allow().Deny().AllowHosts().DenyHosts().
		Operations(kadm.OpAny).ResourcePatternType(kadm.ACLPatternAny))
	if err != nil {
		t.Fatal(err)
	}
	var got []kadm.DescribedACL
	for _, r := range results {
		if r.Err != nil {
			t.Fatal(r.Err)
		}
		got = append(got, r.Described...)
	}
	if len(got) != 1 {
		t.Fatalf("cluster has %d ACLs after create-acl, want 1: %+v", len(got), got)
	}
	a := got[0]
	if a.Principal != "User:alice" || a.Name != "orders.v1" || a.Type != kmsg.ACLResourceTypeTopic ||
		a.Operation != kmsg.ACLOperationWrite || a.Permission != kmsg.ACLPermissionTypeDeny ||
		a.Pattern != kmsg.ACLResourcePatternTypePrefixed || a.Host != "*" {
		t.Errorf("created ACL = %+v, want deny write on prefixed topic orders.v1 for User:alice from *", a)
	}

	listed := captureStdout(t, func() {
		if code := (&ListACLsCmd{}).Run(conn); code != 0 {
			t.Errorf("list-acls = %d, want 0", code)
		}
	})
	if _, ok := lineWith(listed, "User:alice", "topic", "orders.v1", "prefixed", "write", "deny"); !ok {
		t.Errorf("list-acls does not show the created binding:\n%s", listed)
	}
}

// alter-replication sends a reassignment that grows every partition to the
// requested factor, keeping the current replica first; asking for more
// replicas than brokers is refused before anything is sent.
func TestAlterReplicationSubmitsResizedAssignments(t *testing.T) {
	fake, brokers, adm := newCluster(t, kfake.NumBrokers(3))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	const topic = "alter.replication.v1"
	if _, err := adm.CreateTopic(ctx, 2, 1, nil, topic); err != nil {
		t.Fatal(err)
	}
	tds, err := adm.ListTopics(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	leaders := map[int32]int32{}
	for _, p := range tds[topic].Partitions {
		leaders[p.Partition] = p.Replicas[0]
	}

	var mu sync.Mutex
	var sent []*kmsg.AlterPartitionAssignmentsRequest
	fake.ControlKey(int16(kmsg.AlterPartitionAssignments), func(req kmsg.Request) (kmsg.Response, error, bool) {
		fake.KeepControl()
		if r, ok := req.(*kmsg.AlterPartitionAssignmentsRequest); ok {
			mu.Lock()
			sent = append(sent, r)
			mu.Unlock()
		}
		return nil, nil, false //nolint:nilnil // kfake's "not handled": let the fake answer as usual
	})
	cfg := writeConfig(t, t.TempDir(), "")

	_ = captureStdout(t, func() {
		if code := (&AlterReplicationCmd{}).Run(
			[]string{"--config", cfg, "-b", brokers, "-t", topic, "--replication-factor", "4"},
		); code != 1 {
			t.Errorf("RF 4 on 3 brokers = %d, want 1", code)
		}
	})
	mu.Lock()
	if len(sent) != 0 {
		t.Errorf("RF above the broker count still sent %d reassignment requests", len(sent))
	}
	mu.Unlock()

	out := captureStdout(t, func() {
		if code := (&AlterReplicationCmd{}).Run(
			[]string{"--config", cfg, "-b", brokers, "-t", topic, "--replication-factor", "3"},
		); code != 0 {
			t.Errorf("RF 3 = %d, want 0", code)
		}
	})
	if !strings.Contains(out, "submitted successfully") {
		t.Errorf("output %q does not report the submission", out)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(sent) != 1 {
		t.Fatalf("sent %d reassignment requests, want 1", len(sent))
	}
	var parts int
	for _, rt := range sent[0].Topics {
		if rt.Topic != topic {
			t.Errorf("reassignment names topic %q, want %q", rt.Topic, topic)
		}
		for _, p := range rt.Partitions {
			parts++
			r := slices.Clone(p.Replicas)
			if len(r) != 3 || r[0] != leaders[p.Partition] {
				t.Errorf("partition %d replicas = %v, want 3 starting with current %d", p.Partition, r, leaders[p.Partition])
			}
			slices.Sort(r)
			if len(slices.Compact(r)) != len(p.Replicas) {
				t.Errorf("partition %d replicas %v repeat a broker", p.Partition, p.Replicas)
			}
		}
	}
	if parts != 2 {
		t.Errorf("reassigned %d partitions, want both", parts)
	}
}
