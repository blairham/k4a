// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package secrets

import (
	"errors"
	"testing"
)

func TestMemoryRoundTrip(t *testing.T) {
	t.Parallel()

	s := NewMemory()
	if _, err := s.Get("prod/password"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get on empty store: err = %v, want ErrNotFound", err)
	}
	if err := s.Set("prod/password", "hunter2"); err != nil {
		t.Fatal(err)
	}
	if got, err := s.Get("prod/password"); err != nil || got != "hunter2" {
		t.Fatalf("Get = %q, %v", got, err)
	}
	if err := s.Delete("prod/password"); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete("prod/password"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("second Delete: err = %v, want ErrNotFound", err)
	}
}

func TestAccount(t *testing.T) {
	t.Parallel()

	if got := Account("prod", "oauth-token"); got != "prod/oauth-token" {
		t.Fatalf("Account = %q", got)
	}
}
