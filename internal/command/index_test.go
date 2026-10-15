// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package command

import (
	"bytes"
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/blairham/k4a/internal/index"
	"github.com/blairham/k4a/internal/kafka"
	"github.com/blairham/k4a/internal/state"
)

func seedIndex(t *testing.T, root *state.Root, cluster, topic string) {
	t.Helper()
	ix, err := index.Open(root, cluster, topic)
	if err != nil {
		t.Fatal(err)
	}
	msg := kafka.ConsumedMessage{Topic: topic, Key: "k", Value: "v", Offset: 3, Time: time.Now()}
	if err := ix.Index(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if err := ix.FlushBookkeeper(); err != nil {
		t.Fatal(err)
	}
	if err := ix.Close(); err != nil {
		t.Fatal(err)
	}
}

func runIndexCmd(root *state.Root, args ...string) (int, string) {
	var out bytes.Buffer
	code := (&IndexCommand{Root: root, Out: &out}).Run(args)
	return code, out.String()
}

// The default root is ~/.k4a; HOME is redirected so this never sees the
// real one.
func TestIndexCommand_DefaultRootFollowsHome(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	root := state.NewRoot(filepath.Join(home, ".k4a"))
	seedIndex(t, root, "c1", "orders")

	code, out := runIndexCmd(nil, "status")
	if code != 0 || !strings.Contains(out, "orders") || !strings.Contains(out, filepath.Join(home, ".k4a")) {
		t.Fatalf("status = %d, %q", code, out)
	}
	if code, _ := runIndexCmd(nil, "clear", "orders"); code != 0 {
		t.Fatalf("clear exit = %d", code)
	}
	if index.HasIndex(root, "c1", "orders") {
		t.Error("clear left the index in place")
	}
}

func TestIndexCommand_StatusEmpty(t *testing.T) {
	t.Parallel()

	code, out := runIndexCmd(state.NewRoot(t.TempDir()))
	if code != 0 || !strings.Contains(out, "No local topic indexes") {
		t.Errorf("status = %d, %q", code, out)
	}
}

func TestIndexCommand_StatusListsTopics(t *testing.T) {
	t.Parallel()

	root := state.NewRoot(t.TempDir())
	seedIndex(t, root, "c1", "orders")
	seedIndex(t, root, "c2", "payments")

	code, out := runIndexCmd(root, "status")
	if code != 0 {
		t.Fatalf("exit = %d", code)
	}
	for _, want := range []string{"c1", "orders", "c2", "payments", "2 topic(s)"} {
		if !strings.Contains(out, want) {
			t.Errorf("status output missing %q:\n%s", want, out)
		}
	}
}

func TestIndexCommand_ClearArgs(t *testing.T) {
	t.Parallel()

	root := state.NewRoot(t.TempDir())
	for _, args := range [][]string{
		{"clear"},
		{"clear", "--all", "orders"},
		{"clear", "--all", "--cluster", "c1"},
		{"clear", "../escape"},
		{"bogus"},
	} {
		if code, _ := runIndexCmd(root, args...); code == 0 {
			t.Errorf("%v: exit 0, want failure", args)
		}
	}
}

func TestIndexCommand_ClearAll(t *testing.T) {
	t.Parallel()

	root := state.NewRoot(t.TempDir())
	seedIndex(t, root, "c1", "orders")
	seedIndex(t, root, "c2", "payments")

	code, out := runIndexCmd(root, "clear", "--all")
	if code != 0 || !strings.Contains(out, "Cleared 2") {
		t.Fatalf("clear --all = %d, %q", code, out)
	}
	if inv, _ := index.List(root); len(inv) != 0 {
		t.Errorf("indexes left after --all: %+v", inv)
	}
	if code, out := runIndexCmd(root, "clear", "--all"); code != 0 || !strings.Contains(out, "nothing to clear") {
		t.Errorf("second clear --all = %d, %q", code, out)
	}
}

func TestIndexCommand_ClearRefusesLiveWriter(t *testing.T) {
	t.Parallel()

	root := state.NewRoot(t.TempDir())
	seedIndex(t, root, "c1", "orders")
	live, err := index.Open(root, "c1", "orders")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = live.Close() })

	if code, _ := runIndexCmd(root, "clear", "orders"); code == 0 {
		t.Error("clear of a locked index exited 0")
	}
	if !index.HasIndex(root, "c1", "orders") {
		t.Error("clear removed an index another process is writing")
	}
}
