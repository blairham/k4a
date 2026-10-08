// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

//go:build unix

package state

import (
	"errors"
	"os"
	"syscall"
)

// lockFile takes an exclusive non-blocking flock on f. Returns
// ErrLocked if another process holds the lock.
//
//nolint:gosec // FD fits in int by definition
func lockFile(f *os.File) error {
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return ErrLocked
		}
		return err
	}
	return nil
}

// unlockFile releases the flock on f. Best-effort — closing the file
// descriptor releases the lock anyway, so this is mostly for hygiene.
//
//nolint:gosec // FD fits in int by definition
func unlockFile(f *os.File) error {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
}
