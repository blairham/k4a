// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package tui

import (
	"context"
	"errors"
	"os/exec"
	"slices"
	"strings"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"

	"github.com/blairham/k4a/internal/kafka"
	"github.com/blairham/k4a/internal/tui/style"
	"github.com/blairham/k4a/internal/tui/views"
)

type clipRun struct {
	name  string
	stdin string
	args  []string
}

// fakeClipboard is a Linux clipboard with the given environment and
// installed tools. runs records each tool run; a nil runs fails the test
// on any run.
func fakeClipboard(t *testing.T, mode string, env map[string]string, installed []string) *clipboard {
	t.Helper()
	return fakeClipboardRuns(t, mode, env, installed, nil, nil)
}

func fakeClipboardRuns(
	t *testing.T, mode string, env map[string]string, installed []string, runs *[]clipRun, runErr error,
) *clipboard {
	t.Helper()
	return &clipboard{
		mode:   mode,
		goos:   "linux",
		getenv: func(k string) string { return env[k] },
		lookPath: func(name string) (string, error) {
			if slices.Contains(installed, name) {
				return "/usr/bin/" + name, nil
			}
			return "", exec.ErrNotFound
		},
		run: func(_ context.Context, name string, args []string, stdin string) error {
			if runs == nil {
				t.Errorf("clipboard tool %s run in a test that expects none", name)
				return nil
			}
			*runs = append(*runs, clipRun{name: name, args: args, stdin: stdin})
			return runErr
		},
	}
}

func TestClipboardCandidates(t *testing.T) {
	t.Parallel()

	names := func(c *clipboard) []string {
		tools := c.candidates()
		out := make([]string, 0, len(tools))
		for _, tool := range tools {
			out = append(out, tool.name)
		}
		return out
	}
	cases := []struct {
		env  map[string]string
		goos string
		want []string
	}{
		{nil, "darwin", []string{"pbcopy"}},
		{nil, "windows", []string{"clip.exe"}},
		{map[string]string{"WAYLAND_DISPLAY": "wayland-0", "DISPLAY": ":0"}, "linux", []string{"wl-copy", "xclip", "xsel"}},
		{map[string]string{"DISPLAY": ":0"}, "linux", []string{"xclip", "xsel"}},
		{nil, "linux", []string{}},
	}
	for _, tc := range cases {
		c := fakeClipboard(t, ClipboardAuto, tc.env, nil)
		c.goos = tc.goos
		if got := names(c); !slices.Equal(got, tc.want) {
			t.Errorf("%s %v: candidates = %v, want %v", tc.goos, tc.env, got, tc.want)
		}
	}
}

func TestClipboardAutoUsesNativeLocally(t *testing.T) {
	t.Parallel()

	var runs []clipRun
	// wl-copy is not installed, so xclip is the first that is.
	c := fakeClipboardRuns(t, ClipboardAuto, map[string]string{"WAYLAND_DISPLAY": "w", "DISPLAY": ":0"},
		[]string{"xclip", "xsel"}, &runs, nil)
	via, cmd := c.write("hello")
	if cmd != nil {
		t.Error("a successful native copy also sent OSC 52")
	}
	want := []clipRun{{name: "xclip", args: []string{"-selection", "clipboard"}, stdin: "hello"}}
	if len(runs) != 1 || runs[0].name != want[0].name || !slices.Equal(runs[0].args, want[0].args) ||
		runs[0].stdin != "hello" {
		t.Errorf("runs = %+v, want %+v", runs, want)
	}
	if !strings.Contains(via, "xclip") {
		t.Errorf("via = %q, want it to name xclip", via)
	}
}

func TestClipboardAutoUsesOSC52OverSSH(t *testing.T) {
	t.Parallel()

	c := fakeClipboard(t, ClipboardAuto, map[string]string{"DISPLAY": ":0", "SSH_CONNECTION": "1 2 3 4"},
		[]string{"xclip"})
	if _, cmd := c.write("hello"); cmd == nil {
		t.Error("over SSH no OSC 52 was sent")
	}
}

func TestClipboardOSC52ModeNeverRunsATool(t *testing.T) {
	t.Parallel()

	c := fakeClipboard(t, ClipboardOSC52, map[string]string{"DISPLAY": ":0"}, []string{"xclip"})
	if via, cmd := c.write("hello"); cmd == nil || via != "" {
		t.Errorf("osc52 mode: via %q, cmd %v", via, cmd)
	}
}

func TestClipboardNativeFailureFallsBackToOSC52(t *testing.T) {
	t.Parallel()

	var runs []clipRun
	c := fakeClipboardRuns(t, ClipboardNative, map[string]string{"SSH_TTY": "/dev/pts/1", "DISPLAY": ":0"},
		[]string{"xclip"}, &runs, errors.New("Error: Can't open display"))
	via, cmd := c.write("hello")
	if len(runs) != 1 {
		t.Errorf("native mode ran %d tools over SSH, want 1 (native is forced)", len(runs))
	}
	if cmd == nil {
		t.Fatal("a failed native copy fell back to nothing")
	}
	if !strings.Contains(via, "failed") {
		t.Errorf("via = %q, want it to say the tool failed", via)
	}

	none := fakeClipboard(t, ClipboardNative, nil, nil)
	if via, cmd := none.write("hello"); cmd == nil || !strings.Contains(via, "no pbcopy") {
		t.Errorf("native mode with no tool: via %q, cmd %v", via, cmd)
	}
}

// searchWithMatches is a Search view holding three matches.
func searchWithMatches(app *App) *views.SearchView {
	sv := views.NewSearchView(nil, "orders", "a")
	for i, v := range []string{"alpha", "bravo", "charlie"} {
		sv.Update(views.SearchMatchMsg{Msg: kafka.ConsumedMessage{ //nolint:errcheck // no scan running
			Topic: "orders", Offset: int64(i), Key: "k-" + v, Value: v, Time: time.Unix(int64(i), 0),
		}})
	}
	app.setView(style.ViewSearch, sv)
	app.view = style.ViewSearch
	app.resizeActiveView()
	return sv
}

func TestCopyFromSearchView(t *testing.T) {
	app := testApp(t)
	sv := searchWithMatches(app)
	sv.SetFilter("bravo")

	for key, want := range map[string]string{views.KeyCopyValue: "bravo", views.KeyCopyKey: "k-bravo"} {
		action, param := sv.HandleKey(key)
		if action != views.ActionCopy {
			t.Fatalf("%s in Search returned action %q, want copy", key, action)
		}
		if got := copied(t, app, param); got != want {
			t.Errorf("%s copied %q from the filtered match, want %q", key, got, want)
		}
	}
	if rec := copied(t, app, views.KeyCopyRecord); !strings.Contains(rec, `"offset": 1`) {
		t.Errorf("record copy is not the selected match:\n%s", rec)
	}
}

func TestCopyFromSearchViewUsesNativeTool(t *testing.T) {
	app := testApp(t)
	searchWithMatches(app)
	var runs []clipRun
	app.clip = fakeClipboardRuns(t, ClipboardAuto, map[string]string{"DISPLAY": ":0"}, []string{"xsel"}, &runs, nil)

	_, cmd := app.handleAction(views.ActionCopy, views.KeyCopyValue)
	if cmd != nil {
		if _, isRaw := cmd().(tea.RawMsg); isRaw {
			t.Error("native copy also sent OSC 52")
		}
	}
	if len(runs) != 1 || runs[0].name != "xsel" || runs[0].stdin != "alpha" {
		t.Errorf("runs = %+v, want xsel with the selected value", runs)
	}
	if !strings.Contains(app.flash, "xsel") || strings.Contains(app.flash, "alpha") {
		t.Errorf("flash %q should name the tool and not the content", app.flash)
	}
}

func TestSearchCopyWithNoMatchDoesNothing(t *testing.T) {
	t.Parallel()

	sv := views.NewSearchView(nil, "orders", "a")
	if action, _ := sv.HandleKey(views.KeyCopyValue); action != "" {
		t.Errorf("copy with no match returned action %q", action)
	}
}
