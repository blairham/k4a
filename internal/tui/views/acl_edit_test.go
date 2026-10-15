// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package views

import (
	"strings"
	"testing"

	"github.com/blairham/k4a/internal/kafka"
)

// TestEditACLView: the form opens on the binding's values, including an
// operation it does not list, and submitting hands both bindings to the App
// instead of creating anything.
func TestEditACLView(t *testing.T) {
	old := kafka.ACLEntry{
		ResourceType: "cluster", ResourceName: "kafka-cluster", PatternType: "literal",
		Principal: "User:ops", Host: "10.0.0.1", Operation: "idempotent_write", Permission: "deny",
	}
	v := NewEditACLView(nil, old)
	if got := kafka.ACLEntry(v.config()); got != old {
		t.Fatalf("form opened on %+v, want %+v", got, old)
	}
	v.Resize(120, 30)
	if out := v.View(); !strings.Contains(out, "idempotent_write") {
		t.Errorf("the form dropped the binding's operation:\n%s", out)
	}

	v.operation = "describe"
	cmd := v.submitEdit()
	if cmd == nil {
		t.Fatal("submitEdit returned no command")
	}
	msg, ok := cmd().(EditACLSubmitMsg)
	if !ok {
		t.Fatalf("submitEdit sent %T, want EditACLSubmitMsg", cmd())
	}
	if msg.Old != old || msg.New.Operation != "describe" || msg.New.ResourceName != old.ResourceName {
		t.Errorf("submitted %+v", msg)
	}
	if v.Editing() {
		t.Error("the form still takes input while the App confirms")
	}

	v.CancelEdit()
	if !strings.Contains(v.View(), "unchanged") {
		t.Errorf("a declined edit does not say so:\n%s", v.View())
	}
}
