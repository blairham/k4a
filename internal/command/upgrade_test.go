// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package command

import "testing"

// A typo or stray argument must be refused before anything reaches the
// network or replaces the binary; these cases all used to fall through to an
// install.
func TestUpgradeCommandRejectsUnknownArgs(t *testing.T) {
	t.Parallel()

	for _, args := range [][]string{
		{"--chek"},
		{"-x"},
		{"check"},
		{"--check", "extra"},
	} {
		cmd := &UpgradeCommand{Version: "v0.0.0"}
		if got := cmd.Run(args); got != 1 {
			t.Errorf("Run(%q) = %d, want 1", args, got)
		}
	}
}

func TestUpgradeCommandHelpDoesNotInstall(t *testing.T) {
	t.Parallel()

	cmd := &UpgradeCommand{Version: "v0.0.0"}
	if got := cmd.Run([]string{"--help"}); got != 0 {
		t.Errorf("Run(--help) = %d, want 0", got)
	}
}
