// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

//go:build windows

package state

import (
	"errors"
	"os"

	"golang.org/x/sys/windows"
)

// lockFile takes an exclusive non-blocking lock on the entire file via
// LockFileEx. Returns ErrLocked if another process holds the lock —
// Windows surfaces that as ERROR_LOCK_VIOLATION (33). The OS releases
// the lock automatically when the file handle closes, including on
// process exit, matching the Unix flock semantics.
func lockFile(f *os.File) error {
	var ol windows.Overlapped
	err := windows.LockFileEx(
		windows.Handle(f.Fd()),
		windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY,
		0,
		1, 0, // lock 1 byte — enough to make the file appear locked to other openers
		&ol,
	)
	if err != nil {
		if errors.Is(err, windows.ERROR_LOCK_VIOLATION) || errors.Is(err, windows.ERROR_IO_PENDING) {
			return ErrLocked
		}
		return err
	}
	return nil
}

// unlockFile releases the LockFileEx range. Best-effort — closing the
// handle releases anyway.
func unlockFile(f *os.File) error {
	var ol windows.Overlapped
	return windows.UnlockFileEx(windows.Handle(f.Fd()), 0, 1, 0, &ol)
}
