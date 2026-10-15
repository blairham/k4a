// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package tui

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	tea "charm.land/bubbletea/v2"

	"github.com/blairham/k4a/internal/kafka"
	"github.com/blairham/k4a/internal/tui/views"
)

// osc52 is the escape sequence that asks the terminal to put text on the
// system clipboard. It is the terminal that writes the clipboard, so it works
// over SSH where pbcopy and xclip would reach the wrong machine.
//
// Inside tmux the sequence is sent twice: plain, for tmux's own set-clipboard
// handling (on by default since 3.3), and wrapped in tmux's DCS passthrough
// with every ESC doubled, for a tmux that forwards it only with
// allow-passthrough on. A terminal that gets both sets the same text twice.
func osc52(text string, inTmux bool) string {
	seq := "\x1b]52;c;" + base64.StdEncoding.EncodeToString([]byte(text)) + "\x07"
	if !inTmux {
		return seq
	}
	return seq + "\x1bPtmux;" + strings.ReplaceAll(seq, "\x1b", "\x1b\x1b") + "\x1b\\"
}

// copyMessage puts part of the selected or displayed message on the
// clipboard, as currently shown (decoded or raw), and flashes what it copied
// without its content: a value can hold anything, and the flash is on screen.
func (a *App) copyMessage(what string) tea.Cmd {
	msg, raw := a.displayedMessage()
	if msg == nil {
		a.errFlash = "no message selected"
		return nil
	}
	var (
		text, label string
		dec         kafka.Decoding
	)
	switch what {
	case views.KeyCopyKey:
		text, label, dec = msg.KeyDecoding.Shown(msg.Key, raw), "key", msg.KeyDecoding
	case views.KeyCopyRecord:
		data, err := json.MarshalIndent(newExportRecord(msg, raw), "", "  ")
		if err != nil {
			a.errFlash = "copy: " + err.Error()
			return nil
		}
		text, label = string(data), "record JSON"
		dec = msg.ValueDecoding
		if !dec.Decoded() {
			dec = msg.KeyDecoding
		}
	default:
		text, label, dec = msg.ValueDecoding.Shown(msg.Value, raw), "value", msg.ValueDecoding
	}
	if text == "" {
		// An empty OSC 52 payload clears the clipboard rather than copying.
		a.errFlash = label + " is empty: nothing copied"
		return nil
	}
	if dec.Decoded() {
		label += map[bool]string{true: " (raw)", false: " (decoded)"}[raw]
	}
	a.flash = fmt.Sprintf("copied %s, %s, to the clipboard", label, formatSize(len(text)))
	return tea.Raw(osc52(text, os.Getenv("TMUX") != ""))
}

// displayedMessage is the message the active view shows or has selected,
// and whether its decoded fields are being shown raw.
func (a *App) displayedMessage() (*kafka.ConsumedMessage, bool) {
	switch v := a.activeView().(type) {
	case *views.MessageDetailView:
		return v.GetMessage(), v.Raw()
	case *views.MessagesView:
		return v.SelectedMessage(), v.Raw()
	}
	return nil, false
}

// formatSize renders a byte count for a flash.
func formatSize(n int) string {
	switch {
	case n >= 1<<20:
		return fmt.Sprintf("%.1f MB", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.1f KB", float64(n)/(1<<10))
	default:
		return fmt.Sprintf("%d bytes", n)
	}
}
