// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package index

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/blairham/k4a/internal/state"
)

// buildIndex writes a real, closed index for (cluster, topic).
func buildIndex(t *testing.T, root *state.Root, cluster, topic string) {
	t.Helper()
	ix, err := Open(root, cluster, topic)
	if err != nil {
		t.Fatal(err)
	}
	if err := ix.Index(context.Background(), sampleMsg(0, 1, "k", "v", time.Now())); err != nil {
		t.Fatal(err)
	}
	if err := ix.FlushBookkeeper(); err != nil {
		t.Fatal(err)
	}
	if err := ix.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestClearTopic_RemovesEveryClusterAndBackups(t *testing.T) {
	t.Parallel()

	root := state.NewRoot(t.TempDir())
	buildIndex(t, root, "c1", "orders")
	buildIndex(t, root, "c2", "orders")
	buildIndex(t, root, "c1", "payments")
	backup := topicDir(root, "c1", "orders") + ".bak.schema.20260101T000000"
	if err := os.MkdirAll(backup, 0o700); err != nil {
		t.Fatal(err)
	}

	res, err := ClearTopic(root, "", "orders")
	if err != nil {
		t.Fatal(err)
	}
	if res.Cleared != 3 || res.Bytes == 0 || len(res.Locked) != 0 {
		t.Errorf("result = %+v, want 3 cleared, some bytes, none locked", res)
	}
	for _, dir := range []string{topicDir(root, "c1", "orders"), topicDir(root, "c2", "orders"), backup} {
		if _, err := os.Stat(dir); !os.IsNotExist(err) {
			t.Errorf("%s survived clear: %v", dir, err)
		}
	}
	if !HasIndex(root, "c1", "payments") {
		t.Error("clearing orders removed payments")
	}
	// c2 held only orders, so its directory goes too.
	if _, err := os.Stat(filepath.Join(root.File(state.SubsystemIndex), "c2")); !os.IsNotExist(err) {
		t.Errorf("empty cluster dir survived: %v", err)
	}
}

func TestClearTopic_ClusterScoped(t *testing.T) {
	t.Parallel()

	root := state.NewRoot(t.TempDir())
	buildIndex(t, root, "c1", "orders")
	buildIndex(t, root, "c2", "orders")

	if _, err := ClearTopic(root, "c1", "orders"); err != nil {
		t.Fatal(err)
	}
	if HasIndex(root, "c1", "orders") {
		t.Error("c1/orders survived")
	}
	if !HasIndex(root, "c2", "orders") {
		t.Error("--cluster c1 cleared c2/orders")
	}
}

func TestClear_RefusesWhileWriterHoldsLock(t *testing.T) {
	t.Parallel()

	root := state.NewRoot(t.TempDir())
	buildIndex(t, root, "c1", "idle")
	live, err := Open(root, "c1", "live")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = live.Close() })
	if ierr := live.Index(context.Background(), sampleMsg(0, 1, "k", "v", time.Now())); ierr != nil {
		t.Fatal(ierr)
	}
	if ferr := live.FlushBookkeeper(); ferr != nil {
		t.Fatal(ferr)
	}

	res, err := ClearAll(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Locked) != 1 || res.Locked[0] != "c1/live" {
		t.Errorf("Locked = %v, want [c1/live]", res.Locked)
	}
	if res.Cleared != 1 || HasIndex(root, "c1", "idle") {
		t.Errorf("idle index should have been cleared: %+v", res)
	}
	if !HasIndex(root, "c1", "live") {
		t.Error("clear removed an index a writer holds")
	}
	if _, err := live.DocCount(); err != nil {
		t.Errorf("live index broken after clear: %v", err)
	}
}

func TestClearTopic_RejectsPathEscapes(t *testing.T) {
	t.Parallel()

	root := state.NewRoot(t.TempDir())
	for _, bad := range []string{"", ".", "..", "../x", `a\b`, "a/b"} {
		if _, err := ClearTopic(root, "", bad); err == nil {
			t.Errorf("topic %q accepted", bad)
		}
		if _, err := ClearTopic(root, bad, "orders"); err == nil && bad != "" {
			t.Errorf("cluster %q accepted", bad)
		}
	}
}
