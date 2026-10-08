// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

//go:build windows

package command

import (
	"os"
	"os/exec"
)

// reexec on Windows spawns a fresh process (since there's no exec(2)
// equivalent that replaces the image) and exits the current one.
// User experience matches Unix from the terminal's perspective: the
// old TUI disappears, the new one starts immediately.
func reexec(exe string, args, env []string) error {
	cmd := exec.Command(exe, args[1:]...)
	cmd.Env = env
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return err
	}
	os.Exit(0)
	return nil
}
