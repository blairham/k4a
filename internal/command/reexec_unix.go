// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

//go:build unix

package command

import "syscall"

// reexec replaces the current process image with exe, preserving the
// PID — the same approach Unix shells take for builtins like `exec`.
// Used by the in-TUI upgrade flow so the user's terminal session
// transitions seamlessly to the new binary.
func reexec(exe string, args, env []string) error {
	return syscall.Exec(exe, args, env)
}
