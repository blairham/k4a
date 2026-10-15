// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package tui

import (
	"testing"

	"github.com/blairham/k4a/internal/tui/views"
)

// t in Messages opens the seek prompt; a seek the prompt cannot read keeps
// it open with the reason instead of restarting the tail.
func TestSeekPrompt(t *testing.T) {
	t.Parallel()
	app := NewApp(nil, ConnInfo{}, nil, "", "dev")
	app.width, app.height = 100, 40

	app.handleAction(views.ActionSeek, "orders") //nolint:errcheck
	if !app.prompt.Active() {
		t.Fatal("seek did not open the prompt")
	}
	if msg, _ := app.applySeek("yesterday"); msg == "" {
		t.Error("an unreadable seek was accepted")
	}
	// Readonly does not block a seek: it reads.
	app.readonly = true
	app.handleAction(views.ActionSeek, "orders") //nolint:errcheck
	if app.errFlash == msgReadonly {
		t.Error("seek was refused as a write in readonly mode")
	}
}
