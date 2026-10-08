// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package state

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// ErrLocked is returned by AcquireWriter when another process holds the
// writer lock for the requested state directory. The caller should fall
// back to read-only operation.
var ErrLocked = errors.New("state: another writer holds the lock")

// Lock is an advisory single-writer lock on a state directory. Held
// until Release() is called; released automatically by the OS if the
// process exits. Implementations live in lock_unix.go and
// lock_windows.go.
type Lock struct {
	file *os.File
	path string
}

// AcquireWriter takes an advisory exclusive lock on the given state
// directory. Returns ErrLocked if another process already holds it.
//
// The dir is created (with mode 0700) if it doesn't exist. The lock
// file lives at dir/lock and is empty — only its OS-level lock state
// matters.
func AcquireWriter(dir string) (*Lock, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("creating state dir: %w", err)
	}
	path := filepath.Join(dir, "lock")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600) //nolint:gosec // path is under state dir, not user-tainted
	if err != nil {
		return nil, fmt.Errorf("opening lock file: %w", err)
	}
	if err := lockFile(f); err != nil {
		f.Close() //nolint:errcheck // best-effort cleanup
		if errors.Is(err, ErrLocked) {
			return nil, ErrLocked
		}
		return nil, fmt.Errorf("locking %s: %w", path, err)
	}
	return &Lock{file: f, path: path}, nil
}

// Release drops the lock. Safe to call multiple times.
func (l *Lock) Release() error {
	if l == nil || l.file == nil {
		return nil
	}
	_ = unlockFile(l.file) //nolint:errcheck // closing releases anyway
	err := l.file.Close()
	l.file = nil
	return err
}

// Path returns the lock file's path. Useful for diagnostic messages.
func (l *Lock) Path() string { return l.path }
