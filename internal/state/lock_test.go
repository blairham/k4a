// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package state

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestAcquireWriter_CreatesDir(t *testing.T) {
	t.Parallel()

	dir := filepath.Join(t.TempDir(), "deep", "nested", "subsys")
	lock, err := AcquireWriter(dir)
	if err != nil {
		t.Fatalf("AcquireWriter: %v", err)
	}
	t.Cleanup(func() { _ = lock.Release() })

	if _, err := os.Stat(dir); err != nil {
		t.Errorf("expected dir created, stat err = %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "lock")); err != nil {
		t.Errorf("expected lock file created, stat err = %v", err)
	}
}

func TestAcquireWriter_SecondCallerBlocked(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	first, err := AcquireWriter(dir)
	if err != nil {
		t.Fatalf("first AcquireWriter: %v", err)
	}
	t.Cleanup(func() { _ = first.Release() })

	second, err := AcquireWriter(dir)
	if !errors.Is(err, ErrLocked) {
		t.Fatalf("second AcquireWriter err = %v, want ErrLocked", err)
	}
	if second != nil {
		t.Errorf("second lock should be nil")
	}
}

func TestAcquireWriter_ReleaseLetsNextAcquire(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	first, err := AcquireWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
	if rerr := first.Release(); rerr != nil {
		t.Fatalf("Release: %v", rerr)
	}

	second, err := AcquireWriter(dir)
	if err != nil {
		t.Fatalf("second AcquireWriter after release: %v", err)
	}
	t.Cleanup(func() { _ = second.Release() })
}

func TestLock_Release_Idempotent(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	lock, err := AcquireWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := lock.Release(); err != nil {
		t.Fatalf("first Release: %v", err)
	}
	if err := lock.Release(); err != nil {
		t.Errorf("second Release should be no-op, got %v", err)
	}
}

func TestLock_NilSafe(t *testing.T) {
	t.Parallel()
	var l *Lock
	if err := l.Release(); err != nil {
		t.Errorf("nil Release should be no-op, got %v", err)
	}
}
