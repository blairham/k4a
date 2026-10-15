// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package tui

import (
	"context"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"

	"github.com/blairham/k4a/internal/config"
)

// Clipboard modes (ui.clipboard).
const (
	ClipboardAuto   = "auto"   // a native tool on a local session, else OSC 52
	ClipboardOSC52  = "osc52"  // always OSC 52
	ClipboardNative = "native" // always the native tool, OSC 52 if it fails
)

// nativeTimeout bounds a clipboard tool. xclip and wl-copy fork to serve
// the selection and return at once; one that does not should not hang k4a.
const nativeTimeout = 2 * time.Second

// clipTool is a native clipboard command that reads the text on stdin.
type clipTool struct {
	name string
	args []string
}

// clipboard decides how a copy reaches the system clipboard. OSC 52 asks
// the terminal to do it, which is the only way that works over SSH but
// does nothing in a terminal without OSC 52 support (Terminal.app, many
// older terminals, a tmux with set-clipboard off). A native tool works in
// any local terminal. Lookups and the command runner are fields so tests
// never touch the real clipboard.
type clipboard struct {
	getenv   func(string) string
	lookPath func(string) (string, error)
	run      func(ctx context.Context, name string, args []string, stdin string) error
	mode     string
	goos     string
}

// clipboardMode is the config's ui.clipboard, auto when unset.
func clipboardMode(cfg *config.Config) string {
	if cfg == nil || cfg.UI.Clipboard == "" {
		return ClipboardAuto
	}
	return cfg.UI.Clipboard
}

func newClipboard(mode string) *clipboard {
	return &clipboard{
		mode:     mode,
		goos:     runtime.GOOS,
		getenv:   os.Getenv,
		lookPath: exec.LookPath,
		run:      runClipTool,
	}
}

func runClipTool(ctx context.Context, name string, args []string, stdin string) error {
	cmd := exec.CommandContext(ctx, name, args...) //nolint:gosec // a fixed tool from candidates()
	cmd.Stdin = strings.NewReader(stdin)
	// No stdout or stderr pipes: a tool that forks to keep serving the
	// selection would hold them open and Wait would block on it.
	return cmd.Run()
}

// candidates lists the native tools for this platform and session, best
// first: wl-copy only under Wayland, xclip and xsel only under X.
func (c *clipboard) candidates() []clipTool {
	switch c.goos {
	case "darwin":
		return []clipTool{{name: "pbcopy"}}
	case "windows":
		return []clipTool{{name: "clip.exe"}}
	}
	var out []clipTool
	if c.getenv("WAYLAND_DISPLAY") != "" {
		out = append(out, clipTool{name: "wl-copy"})
	}
	if c.getenv("DISPLAY") != "" {
		out = append(out,
			clipTool{name: "xclip", args: []string{"-selection", "clipboard"}},
			clipTool{name: "xsel", args: []string{"--clipboard", "--input"}})
	}
	return out
}

// native returns the first installed candidate.
func (c *clipboard) native() (clipTool, bool) {
	for _, t := range c.candidates() {
		if _, err := c.lookPath(t.name); err == nil {
			return t, true
		}
	}
	return clipTool{}, false
}

// remote reports an SSH session, where a native tool would write the
// clipboard of the machine k4a runs on rather than the user's.
func (c *clipboard) remote() bool {
	return c.getenv("SSH_CONNECTION") != "" || c.getenv("SSH_TTY") != ""
}

// write puts text on the clipboard and returns how, for the flash (empty
// for OSC 52), and the command to run, if any. A native tool that fails
// falls back to OSC 52 rather than copying nothing.
func (c *clipboard) write(text string) (via string, cmd tea.Cmd) {
	useNative := c.mode == ClipboardNative || (c.mode != ClipboardOSC52 && !c.remote())
	if useNative {
		if tool, ok := c.native(); ok {
			ctx, cancel := context.WithTimeout(context.Background(), nativeTimeout)
			err := c.run(ctx, tool.name, tool.args, text)
			cancel()
			if err == nil {
				return " (" + tool.name + ")", nil
			}
			via = " (" + tool.name + " failed; sent OSC 52)"
		} else if c.mode == ClipboardNative {
			via = " (no pbcopy, wl-copy, xclip or xsel found; sent OSC 52)"
		}
	}
	return via, tea.Raw(osc52(text, c.getenv("TMUX") != ""))
}
